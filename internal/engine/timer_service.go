package engine

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/observability"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Proposer is the leader-side interface that turns a Command into a Raft
// proposal. The TimerService uses it to surface TimerFired commands back into
// the partition's apply path.
//
// Mirrors restate crates/worker/src/partition/leadership/self_proposer.rs:36-58
// minus the Bifrost-specific batching — dragonboat's SyncPropose already
// blocks until commit.
type Proposer interface {
	ProposeSelf(ctx context.Context, cmd *enginev1.Command) error
}

type timerHeapEntry struct {
	fireAtMs uint64
	id       *enginev1.InvocationId
	sleepIdx uint32
	process  *enginev1.ProcessTimer // non-nil → fire as Command_ProcessEvent
	rawID    []byte                 // cached canonical form for tie-breaking + Delete lookup
}

type timerHeap []timerHeapEntry

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].fireAtMs != h[j].fireAtMs {
		return h[i].fireAtMs < h[j].fireAtMs
	}
	return bytes.Compare(h[i].rawID, h[j].rawID) < 0
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push and Pop satisfy heap.Interface; called by container/heap only.
func (h *timerHeap) Push(x any) { *h = append(*h, x.(timerHeapEntry)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// TimerService is the leader-only durable timer driver.
//
// Architecture (mirrors restate crates/timer/src/lib.rs:21-35 and
// crates/worker/src/partition/leadership/mod.rs:148-154):
//
//   - Timers are persisted in TimerTable by the FSM apply path. The FSM
//     also pushes an ActRegisterTimer that the leader runner forwards here.
//   - On leader gain, Rebuild() reloads the heap from TimerTable.
//   - Run() drives the heap with time.NewTimer — wakes exactly at the next
//     deadline. OS-level mechanisms (timerfd / kqueue EVFILT_TIMER /
//     CreateWaitableTimer) would not improve precision here: fire latency is
//     dominated by the Raft commit round-trip, not the wake mechanism.
//     Dragonboat's internal tick (nodehost.go tickWorkerMain) is RTT-resolution
//     (100–200ms), unexported, and global, so we can't piggyback on it either.
//   - We NEVER hold the mutex across a Propose call. Failed proposes
//     re-push the entry so the timer is not lost; the next fireDue cycle
//     retries.
type TimerService struct {
	table    tables.TimerTable
	proposer Proposer
	now      func() uint64
	log      *slog.Logger
	metrics  *observability.Metrics

	mu   sync.Mutex
	heap timerHeap

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

// TimerServiceOptions tunes a TimerService for tests.
type TimerServiceOptions struct {
	Now     func() uint64 // injected wall clock; defaults to time.Now()
	Log     *slog.Logger  // structured logger
	Metrics *observability.Metrics
}

// NewTimerService constructs the service. proposer may be nil if the service
// is being built on a follower (no firing will happen).
func NewTimerService(table tables.TimerTable, proposer Proposer, opts TimerServiceOptions) *TimerService {
	if opts.Now == nil {
		opts.Now = func() uint64 { return uint64(time.Now().UnixMilli()) }
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &TimerService{
		table:    table,
		proposer: proposer,
		now:      opts.Now,
		log:      opts.Log,
		metrics:  opts.Metrics,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Rebuild reloads the in-memory heap from the persistent table. Called on
// leader gain. Existing in-memory entries are replaced.
func (ts *TimerService) Rebuild() error {
	loaded := make(timerHeap, 0)
	err := ts.table.ScanAll(func(e tables.TimerEntry) error {
		raw, encErr := keys.EncodeInvocationID(e.ID)
		if encErr != nil {
			return encErr
		}
		loaded = append(loaded, timerHeapEntry{
			fireAtMs: e.FireAtMs,
			id:       e.ID,
			sleepIdx: e.SleepIdx,
			process:  e.Process,
			rawID:    raw,
		})
		return nil
	})
	if err != nil {
		return err
	}
	heap.Init(&loaded)

	ts.mu.Lock()
	ts.heap = loaded
	ts.mu.Unlock()
	ts.signalWake()
	return nil
}

// Push adds a sleep / run-retry timer to the in-memory heap. Called by the
// leader runner when it drains an ActRegisterTimer Action with no Process.
func (ts *TimerService) Push(fireAtMs uint64, id *enginev1.InvocationId, sleepIdx uint32) error {
	return ts.push(timerHeapEntry{fireAtMs: fireAtMs, id: id, sleepIdx: sleepIdx})
}

// PushProcess adds a process timer to the heap; it fires as a
// Command_ProcessEvent{timer_fired} rather than a Command_TimerFired.
func (ts *TimerService) PushProcess(fireAtMs uint64, id *enginev1.InvocationId, pt *enginev1.ProcessTimer) error {
	return ts.push(timerHeapEntry{fireAtMs: fireAtMs, id: id, process: pt})
}

func (ts *TimerService) push(e timerHeapEntry) error {
	raw, err := keys.EncodeInvocationID(e.id)
	if err != nil {
		return err
	}
	e.rawID = raw
	ts.mu.Lock()
	heap.Push(&ts.heap, e)
	ts.mu.Unlock()
	ts.signalWake()
	return nil
}

// Delete removes a timer from the heap. O(n).
func (ts *TimerService) Delete(fireAtMs uint64, id *enginev1.InvocationId) error {
	raw, err := keys.EncodeInvocationID(id)
	if err != nil {
		return err
	}
	ts.mu.Lock()
	for i := range ts.heap {
		if ts.heap[i].fireAtMs == fireAtMs && bytes.Equal(ts.heap[i].rawID, raw) {
			heap.Remove(&ts.heap, i)
			break
		}
	}
	ts.mu.Unlock()
	ts.signalWake()
	return nil
}

// Run drives the timer loop. Blocks until ctx is canceled or Stop is called.
func (ts *TimerService) Run(ctx context.Context) error {
	defer close(ts.done)
	for {
		// Always honor cancellation at the top of the loop. fireDue can
		// re-push timers on propose failures; without this check a tight
		// fire→fail→repush cycle would never observe ctx.Done().
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ts.stop:
			return nil
		default:
		}

		ts.mu.Lock()
		var nextFire uint64
		if len(ts.heap) > 0 {
			nextFire = ts.heap[0].fireAtMs
		}
		ts.mu.Unlock()

		now := ts.now()
		if nextFire > 0 && nextFire <= now {
			ts.fireDue(ctx)
			continue
		}

		// Compute how long to wait. Use 1h as the idle bound — wake will
		// preempt us when a new timer is pushed.
		var waitDur time.Duration
		switch {
		case nextFire == 0:
			waitDur = time.Hour
		case nextFire > now:
			waitDur = time.Duration(nextFire-now) * time.Millisecond
		default:
			waitDur = 0
		}

		// Go 1.23+: timer channels are unbuffered, Stop guarantees no
		// further sends, and unreferenced timers are GC-recoverable. The
		// pre-1.23 `if !timer.Stop() { <-timer.C }` drain would deadlock
		// here — Stop is enough.
		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-ts.stop:
			timer.Stop()
			return nil
		case <-ts.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Stop signals Run to return. Idempotent.
func (ts *TimerService) Stop() {
	select {
	case <-ts.stop:
		return
	default:
	}
	close(ts.stop)
}

// Done returns a channel that's closed when Run has returned.
func (ts *TimerService) Done() <-chan struct{} { return ts.done }

func (ts *TimerService) signalWake() {
	select {
	case ts.wake <- struct{}{}:
	default:
	}
}

// fireDue pops every due timer and proposes a TimerFired for each. Mutex is
// NOT held across the Propose call — we drain to a local slice first.
func (ts *TimerService) fireDue(ctx context.Context) {
	if ts.proposer == nil {
		ts.log.Warn("timer service has no proposer; dropping fires", "now_ms", ts.now())
		ts.mu.Lock()
		ts.heap = ts.heap[:0]
		ts.mu.Unlock()
		return
	}

	now := ts.now()
	var due []timerHeapEntry
	ts.mu.Lock()
	for len(ts.heap) > 0 && ts.heap[0].fireAtMs <= now {
		e := heap.Pop(&ts.heap).(timerHeapEntry)
		due = append(due, e)
	}
	ts.mu.Unlock()

	for _, e := range due {
		cmd := timerFireCommand(e)
		propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := ts.proposer.ProposeSelf(propCtx, cmd)
		cancel()
		if err == nil {
			if ts.metrics != nil {
				ts.metrics.TimerFired.Inc()
			}
			continue
		}
		// Shutdown-class errors are terminal: do NOT re-push or the next
		// leader will keep retrying a propose against a dead shard. The
		// timer row is still on disk; rebuild on the next leader gain
		// picks it up.
		if errors.Is(err, context.Canceled) || errors.Is(err, ErrShardClosed) {
			return
		}
		// Transient failures (DeadlineExceeded, dragonboat busy/timeout):
		// re-push so the timer is not lost on this leader.
		ts.mu.Lock()
		heap.Push(&ts.heap, e)
		ts.mu.Unlock()
		ts.log.Warn("timer propose failed; re-queued",
			"err", err,
			"fire_at_ms", e.fireAtMs,
			"sleep_idx", e.sleepIdx,
		)
	}
}

// timerFireCommand builds the command a due timer proposes. A process timer
// (process != nil) fires a Command_ProcessEvent{timer_fired} addressed to the
// instance; everything else fires the plain Command_TimerFired. logical_time_ms
// is the scheduled fire instant so the re-driven turn is deterministic.
func timerFireCommand(e timerHeapEntry) *enginev1.Command {
	if e.process != nil {
		return &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
			Pk:            e.id.GetPartitionKey(),
			Service:       e.process.GetService(),
			InstanceKey:   e.process.GetInstanceKey(),
			LogicalTimeMs: e.fireAtMs,
			Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TimerFired{
				TimerFired: &enginev1.ProcessTimerFired{
					NodeId: e.process.GetNodeId(),
					Slot:   e.process.GetSlot(),
				},
			}},
		}}}
	}
	return &enginev1.Command{Kind: &enginev1.Command_TimerFired{TimerFired: &enginev1.TimerFired{
		InvocationId: e.id,
		SleepIndex:   e.sleepIdx,
		FireAtMs:     e.fireAtMs,
	}}}
}

// heapLen returns the current heap length (for tests).
func (ts *TimerService) heapLen() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.heap)
}
