package engine

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/engine/limits"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// reapHeapEntry is a single scheduled retention sweep ordered by
// fire_at_ms. Tie-breaking by (partition_key, uuid) keeps ordering
// deterministic across replicas.
type reapHeapEntry struct {
	fireAtMs uint64
	id       *enginev1.InvocationId
}

type reapHeap []reapHeapEntry

func (h reapHeap) Len() int { return len(h) }
func (h reapHeap) Less(i, j int) bool {
	if h[i].fireAtMs != h[j].fireAtMs {
		return h[i].fireAtMs < h[j].fireAtMs
	}
	if pi, pj := h[i].id.GetPartitionKey(), h[j].id.GetPartitionKey(); pi != pj {
		return pi < pj
	}
	return bytes.Compare(h[i].id.GetUuid(), h[j].id.GetUuid()) < 0
}
func (h reapHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *reapHeap) Push(x any)   { *h = append(*h, x.(reapHeapEntry)) }
func (h *reapHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ReapService is the leader-only loop that drives invocation retention
// cleanup. Mirrors TimerService: an in-memory heap of (fire_at_ms,
// inv_id) entries persisted in ReapTable. On leader gain Rebuild reloads
// the heap; the Run loop wakes at the head's fire_at_ms and proposes
// Command.ReapInvocation against the local apply path. One service
// reaps every kind — plain invocations, virtual-object calls, and
// workflow runs — because every Completed invocation schedules a row
// here; the apply arm decides whether to also sweep entity-scoped state.
type ReapService struct {
	table      tables.ReapTable
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

// NewReapService constructs the service. proposer may be nil on
// followers; firing is a no-op then.
func NewReapService(table tables.ReapTable, proposer Proposer, opts ReapServiceOptions) *ReapService {
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
		table:      table,
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
	if err := s.table.ScanAll(func(r tables.ReapRow) error {
		loaded = append(loaded, reapHeapEntry{fireAtMs: r.FireAtMs, id: r.ID})
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
// runner's dispatchActions handler for ActScheduleReap.
func (s *ReapService) Push(fireAtMs uint64, id *enginev1.InvocationId) {
	s.mu.Lock()
	heap.Push(&s.heap, reapHeapEntry{fireAtMs: fireAtMs, id: id})
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
			nextFire = s.heap[0].fireAtMs
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

// fireDue pops every due entry and proposes a ReapInvocation for each.
func (s *ReapService) fireDue(ctx context.Context) {
	if s.dropIfNoProposer() {
		return
	}
	now := s.now()
	var due []reapHeapEntry
	s.mu.Lock()
	for len(s.heap) > 0 && s.heap[0].fireAtMs <= now {
		due = append(due, heap.Pop(&s.heap).(reapHeapEntry))
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
	over := make([]reapHeapEntry, 0, excess)
	for i := 0; i < excess && len(s.heap) > 0; i++ {
		over = append(over, heap.Pop(&s.heap).(reapHeapEntry))
	}
	remaining := len(s.heap)
	s.mu.Unlock()

	s.log.Warn("reap count-cap exceeded; reaping soonest-to-expire early",
		"max_pending", s.maxPending, "reaped_early", len(over), "remaining", remaining)
	s.proposeReaps(ctx, over)
	return true
}

// proposeReaps proposes a ReapInvocation for each entry. Mutex is NOT held
// across the Propose call. Transient failures re-push the entry so the
// retention sweep is not lost; shutdown-class errors drop on the floor
// (the persistent row survives, next leader rebuild picks it up).
func (s *ReapService) proposeReaps(ctx context.Context, entries []reapHeapEntry) {
	for _, e := range entries {
		cmd := &enginev1.Command{
			Kind: &enginev1.Command_ReapInvocation{
				ReapInvocation: &enginev1.ReapInvocation{
					InvocationId: e.id,
					FireAtMs:     e.fireAtMs,
				},
			},
		}
		propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.proposer.ProposeSelf(propCtx, cmd)
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
		s.log.Warn("reap propose failed; re-queued",
			"err", err,
			"fire_at_ms", e.fireAtMs,
			"partition_key", e.id.GetPartitionKey(),
		)
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
