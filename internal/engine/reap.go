package engine

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/engine/limits"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// reapEntry is one scheduled retention sweep. The reaper is target-agnostic so
// one ReapService drains either plane: invocation reaps (keyed by InvocationId →
// Command_ReapInvocation) and process-instance reaps (keyed by
// pk+service+instance_key → Command_ReapProcessInstance) both implement it. A
// single ReapService instance only ever holds one concrete type, so lessThan
// may assume its argument is the same type.
type reapEntry interface {
	fireAtMs() uint64
	// lessThan is the deterministic tiebreak at equal fire times, so every
	// replica drains the heap in the same order.
	lessThan(other reapEntry) bool
	// command is what the reaper proposes (via ProposeSelf) when the entry is due.
	command() *enginev1.Command
	// String is a compact identity for failure logs.
	String() string
}

// invocationReapEntry reaps a Completed invocation's durable footprint.
type invocationReapEntry struct {
	fireAt uint64
	id     *enginev1.InvocationId
}

func (e invocationReapEntry) fireAtMs() uint64 { return e.fireAt }

func (e invocationReapEntry) lessThan(o reapEntry) bool {
	if of := o.fireAtMs(); e.fireAt != of {
		return e.fireAt < of
	}
	oo := o.(invocationReapEntry)
	if pi, pj := e.id.GetPartitionKey(), oo.id.GetPartitionKey(); pi != pj {
		return pi < pj
	}
	return bytes.Compare(e.id.GetUuid(), oo.id.GetUuid()) < 0
}

func (e invocationReapEntry) command() *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ReapInvocation{
		ReapInvocation: &enginev1.ReapInvocation{InvocationId: e.id, FireAtMs: e.fireAt},
	}}
}

func (e invocationReapEntry) String() string {
	return fmt.Sprintf("inv %d/%x", e.id.GetPartitionKey(), e.id.GetUuid())
}

// processReapEntry reaps a terminal process instance's retained record.
type processReapEntry struct {
	fireAt      uint64
	pk          uint64
	service     string
	instanceKey string
}

func (e processReapEntry) fireAtMs() uint64 { return e.fireAt }

func (e processReapEntry) lessThan(o reapEntry) bool {
	if of := o.fireAtMs(); e.fireAt != of {
		return e.fireAt < of
	}
	oo := o.(processReapEntry)
	if e.pk != oo.pk {
		return e.pk < oo.pk
	}
	if e.service != oo.service {
		return e.service < oo.service
	}
	return e.instanceKey < oo.instanceKey
}

func (e processReapEntry) command() *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ReapProcessInstance{
		ReapProcessInstance: &enginev1.ReapProcessInstance{
			Pk: e.pk, Service: e.service, InstanceKey: e.instanceKey, FireAtMs: e.fireAt,
		},
	}}
}

func (e processReapEntry) String() string {
	return fmt.Sprintf("proc %d/%s/%s", e.pk, e.service, e.instanceKey)
}

type reapHeap []reapEntry

func (h reapHeap) Len() int           { return len(h) }
func (h reapHeap) Less(i, j int) bool { return h[i].lessThan(h[j]) }
func (h reapHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *reapHeap) Push(x any)        { *h = append(*h, x.(reapEntry)) }
func (h *reapHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ReapService is the leader-only loop that drives retention cleanup. Mirrors
// TimerService: an in-memory heap of reapEntry persisted in a backing table. On
// leader gain Rebuild reloads the heap; the Run loop wakes at the head's
// fire_at_ms and proposes the entry's command against the local apply path. The
// service is target-agnostic (the scanAll closure + entry.command bind it to a
// table + command), so the runner instantiates one for invocation retention
// (ReapTable → ReapInvocation) and one for process-instance retention
// (ProcessReapTable → ReapProcessInstance).
type ReapService struct {
	scanAll    func(func(reapEntry) error) error
	proposer   Proposer
	now        func() uint64
	log        *slog.Logger
	maxPending int

	mu   sync.Mutex
	heap reapHeap

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

// ReapServiceOptions tunes a ReapService for tests.
type ReapServiceOptions struct {
	Now func() uint64 // injected wall clock; defaults to time.Now()
	Log *slog.Logger
	// MaxPending caps how many pending reaps the heap holds before the
	// soonest-to-expire are fired early (the count-cap backstop). 0 →
	// limits.DefaultMaxPendingReaps. Negative disables the backstop.
	MaxPending int
}

// NewReapService constructs the service. scanAll reloads the heap from the
// backing table on Rebuild (each scanned row emitted as a reapEntry). proposer
// may be nil on followers; firing is a no-op then.
func NewReapService(scanAll func(func(reapEntry) error) error, proposer Proposer, opts ReapServiceOptions) *ReapService {
	if opts.Now == nil {
		opts.Now = func() uint64 { return uint64(time.Now().UnixMilli()) }
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.MaxPending == 0 {
		opts.MaxPending = limits.DefaultMaxPendingReaps
	}
	return &ReapService{
		scanAll:    scanAll,
		proposer:   proposer,
		now:        opts.Now,
		log:        opts.Log,
		maxPending: opts.MaxPending,
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// Rebuild reloads the in-memory heap from the persistent table. Called
// on leader gain.
func (s *ReapService) Rebuild() error {
	loaded := make(reapHeap, 0)
	if err := s.scanAll(func(e reapEntry) error {
		loaded = append(loaded, e)
		return nil
	}); err != nil {
		return err
	}
	heap.Init(&loaded)
	s.mu.Lock()
	s.heap = loaded
	s.mu.Unlock()
	s.signalWake()
	return nil
}

// Push enqueues a freshly-written reap row. Called inline from the
// runner's dispatchActions handler for ActScheduleReap / ActScheduleProcessReap.
func (s *ReapService) Push(e reapEntry) {
	s.mu.Lock()
	heap.Push(&s.heap, e)
	s.mu.Unlock()
	s.signalWake()
}

// Run drives the heap. Blocks until ctx is canceled or Stop is called.
func (s *ReapService) Run(ctx context.Context) error {
	defer close(s.done)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-s.stop:
			return nil
		default:
		}

		s.mu.Lock()
		var nextFire uint64
		if len(s.heap) > 0 {
			nextFire = s.heap[0].fireAtMs()
		}
		s.mu.Unlock()

		now := s.now()
		if nextFire > 0 && nextFire <= now {
			s.fireDue(ctx)
			continue
		}

		// Count-cap backstop: if the standing pending set exceeds the cap,
		// reap the soonest-to-expire entries early regardless of window so
		// a burst can't accumulate rows for the full retention window.
		if s.fireOverflow(ctx) {
			continue
		}

		var waitDur time.Duration
		switch {
		case nextFire == 0:
			waitDur = time.Hour
		case nextFire > now:
			waitDur = time.Duration(nextFire-now) * time.Millisecond
		default:
			waitDur = 0
		}

		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-s.stop:
			timer.Stop()
			return nil
		case <-s.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Stop signals Run to return. Idempotent.
func (s *ReapService) Stop() {
	select {
	case <-s.stop:
		return
	default:
	}
	close(s.stop)
}

// Done returns a channel closed when Run has returned.
func (s *ReapService) Done() <-chan struct{} { return s.done }

func (s *ReapService) signalWake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// fireDue pops every due entry and proposes its command.
func (s *ReapService) fireDue(ctx context.Context) {
	if s.dropIfNoProposer() {
		return
	}
	now := s.now()
	var due []reapEntry
	s.mu.Lock()
	for len(s.heap) > 0 && s.heap[0].fireAtMs() <= now {
		due = append(due, heap.Pop(&s.heap).(reapEntry))
	}
	s.mu.Unlock()
	s.proposeReaps(ctx, due)
}

// fireOverflow reaps the soonest-to-expire entries early when the pending
// set exceeds maxPending. Returns true if it fired anything (so Run loops
// again). The early reap is logged once per overflow event — it's a flood
// signal, not a routine sweep, so it must not be silent.
func (s *ReapService) fireOverflow(ctx context.Context) bool {
	if s.maxPending <= 0 {
		return false
	}
	s.mu.Lock()
	excess := len(s.heap) - s.maxPending
	s.mu.Unlock()
	if excess <= 0 {
		return false
	}
	if s.dropIfNoProposer() {
		return true
	}

	s.mu.Lock()
	over := make([]reapEntry, 0, excess)
	for i := 0; i < excess && len(s.heap) > 0; i++ {
		over = append(over, heap.Pop(&s.heap).(reapEntry))
	}
	remaining := len(s.heap)
	s.mu.Unlock()

	s.log.Warn("reap count-cap exceeded; reaping soonest-to-expire early",
		"max_pending", s.maxPending, "reaped_early", len(over), "remaining", remaining)
	s.proposeReaps(ctx, over)
	return true
}

// proposeReaps proposes each entry's command. Mutex is NOT held across the
// Propose call. Transient failures re-push the entry so the retention sweep is
// not lost; shutdown-class errors drop on the floor (the persistent row
// survives, next leader rebuild picks it up).
func (s *ReapService) proposeReaps(ctx context.Context, entries []reapEntry) {
	for _, e := range entries {
		propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.proposer.ProposeSelf(propCtx, e.command())
		cancel()
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, ErrShardClosed) {
			return
		}
		s.mu.Lock()
		heap.Push(&s.heap, e)
		s.mu.Unlock()
		s.log.Warn("reap propose failed; re-queued", "err", err, "entry", e.String())
	}
}

// dropIfNoProposer clears the heap on a follower (no proposer) so a leader
// loss doesn't leave stale fires queued. Returns true when there's no
// proposer and the caller should stop.
func (s *ReapService) dropIfNoProposer() bool {
	if s.proposer != nil {
		return false
	}
	s.log.Warn("reap service has no proposer; dropping fires", "now_ms", s.now())
	s.mu.Lock()
	s.heap = s.heap[:0]
	s.mu.Unlock()
	return true
}
