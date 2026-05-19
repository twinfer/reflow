package engine

import (
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// reapHeapEntry is a single scheduled retention sweep ordered by
// fire_at_ms. Tie-breaking by (service, workflow_key) lex order keeps
// ordering deterministic across replicas.
type reapHeapEntry struct {
	fireAtMs    uint64
	service     string
	workflowKey string
}

type reapHeap []reapHeapEntry

func (h reapHeap) Len() int { return len(h) }
func (h reapHeap) Less(i, j int) bool {
	if h[i].fireAtMs != h[j].fireAtMs {
		return h[i].fireAtMs < h[j].fireAtMs
	}
	if h[i].service != h[j].service {
		return h[i].service < h[j].service
	}
	return h[i].workflowKey < h[j].workflowKey
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

// WorkflowReapService is the leader-only loop that drives workflow
// retention cleanup. Mirrors TimerService: an in-memory heap of
// (fire_at_ms, service, workflow_key) entries persisted in
// WorkflowReapTable. On leader gain Rebuild reloads the heap; the
// Run loop wakes at the head's fire_at_ms and proposes
// Command.ReapWorkflow against the local apply path.
type WorkflowReapService struct {
	table    tables.WorkflowReapTable
	proposer Proposer
	now      func() uint64
	log      *slog.Logger

	mu   sync.Mutex
	heap reapHeap

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

// WorkflowReapServiceOptions tunes a WorkflowReapService for tests.
type WorkflowReapServiceOptions struct {
	Now func() uint64 // injected wall clock; defaults to time.Now()
	Log *slog.Logger
}

// NewWorkflowReapService constructs the service. proposer may be nil
// on followers; firing is a no-op then.
func NewWorkflowReapService(table tables.WorkflowReapTable, proposer Proposer, opts WorkflowReapServiceOptions) *WorkflowReapService {
	if opts.Now == nil {
		opts.Now = func() uint64 { return uint64(time.Now().UnixMilli()) }
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &WorkflowReapService{
		table:    table,
		proposer: proposer,
		now:      opts.Now,
		log:      opts.Log,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Rebuild reloads the in-memory heap from the persistent table.
// Called on leader gain.
func (s *WorkflowReapService) Rebuild() error {
	loaded := make(reapHeap, 0)
	if err := s.table.ScanAll(func(r tables.ReapRow) error {
		loaded = append(loaded, reapHeapEntry{
			fireAtMs:    r.FireAtMs,
			service:     r.Service,
			workflowKey: r.WorkflowKey,
		})
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
// runner's dispatchActions handler for ActScheduleWorkflowReap.
func (s *WorkflowReapService) Push(fireAtMs uint64, service, workflowKey string) {
	s.mu.Lock()
	heap.Push(&s.heap, reapHeapEntry{
		fireAtMs:    fireAtMs,
		service:     service,
		workflowKey: workflowKey,
	})
	s.mu.Unlock()
	s.signalWake()
}

// Run drives the heap. Blocks until ctx is canceled or Stop is called.
func (s *WorkflowReapService) Run(ctx context.Context) error {
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
func (s *WorkflowReapService) Stop() {
	select {
	case <-s.stop:
		return
	default:
	}
	close(s.stop)
}

// Done returns a channel closed when Run has returned.
func (s *WorkflowReapService) Done() <-chan struct{} { return s.done }

func (s *WorkflowReapService) signalWake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// fireDue pops every due entry and proposes a ReapWorkflow for each.
// Mutex is NOT held across the Propose call. Transient failures
// re-push the entry so the retention sweep is not lost; shutdown-class
// errors drop on the floor (the persistent row survives, next leader
// rebuild picks it up).
func (s *WorkflowReapService) fireDue(ctx context.Context) {
	if s.proposer == nil {
		s.log.Warn("workflow reap service has no proposer; dropping fires", "now_ms", s.now())
		s.mu.Lock()
		s.heap = s.heap[:0]
		s.mu.Unlock()
		return
	}

	now := s.now()
	var due []reapHeapEntry
	s.mu.Lock()
	for len(s.heap) > 0 && s.heap[0].fireAtMs <= now {
		e := heap.Pop(&s.heap).(reapHeapEntry)
		due = append(due, e)
	}
	s.mu.Unlock()

	for _, e := range due {
		cmd := &enginev1.Command{
			Kind: &enginev1.Command_ReapWorkflow{
				ReapWorkflow: &enginev1.ReapWorkflow{
					Service:     e.service,
					WorkflowKey: e.workflowKey,
					FireAtMs:    e.fireAtMs,
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
		s.log.Warn("workflow reap propose failed; re-queued",
			"err", err,
			"fire_at_ms", e.fireAtMs,
			"service", e.service,
			"workflow_key", e.workflowKey,
		)
	}
}
