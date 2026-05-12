package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/twinfer/reflow/internal/engine/invoker"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// PartitionRunner ties together the per-partition leader-only services: the
// proposer (Raft client), leadership state, action collector, timer service,
// outbox shuffler, and Invoker. It exposes a small API for tests/ingress to
// propose commands. The runner is constructed in Host.StartPartition and lives
// for the lifetime of the partition; the leader-only services (timers,
// outbox) are recreated per leader gain because their internal channels are
// single-use.
type PartitionRunner struct {
	ShardID uint64

	snapshotter *Snapshotter
	proposer    *RaftProposer
	leadership  *Leadership
	collector   *ActionCollector
	invoker     *invoker.Invoker
	// sender is the cross-shard dispatcher used by the OutboxService for
	// envelopes whose destination_shard_id is non-local. nil in single-
	// node deployments; populated by Host.StartPartition when multi-node
	// is configured. Phase 4.1.
	sender CrossShardSender
	log    *slog.Logger

	// timers and outbox are populated by onBecomeLeader and torn down by
	// onStepDown. dispatchActions reads them on the apply goroutine,
	// which is the same goroutine as onBecomeLeader/onStepDown — no
	// extra synchronisation needed.
	timers *TimerService
	outbox *OutboxService

	mu           sync.Mutex
	leaderCtx    context.Context
	leaderCancel context.CancelFunc
	timerDone    chan struct{}
	outboxDone   chan struct{}
}

// Proposer returns the partition's RaftProposer.
func (r *PartitionRunner) Proposer() *RaftProposer { return r.proposer }

// Leadership returns the partition's leadership state (read-only API for tests).
func (r *PartitionRunner) Leadership() *Leadership { return r.leadership }

// Snapshotter returns the underlying snapshotter. Mainly for tests that want
// to read state directly.
func (r *PartitionRunner) Snapshotter() *Snapshotter { return r.snapshotter }

// Invoker returns the per-partition Invoker. Exposed for tests; production
// code should reach the Invoker through actions dispatched by the FSM.
func (r *PartitionRunner) Invoker() *invoker.Invoker { return r.invoker }

// IsLeader is a convenience accessor.
func (r *PartitionRunner) IsLeader() bool { return r.leadership.IsLeader() }

// dispatchActions is called by the Partition FSM (inside its Update path,
// after the storage batch commits) with the actions accumulated on the
// leader. We may NOT propose to Raft here because we're still inside the
// dragonboat apply goroutine. Timer pushes / outbox pushes / invoker
// dispatch are all local and safe.
func (r *PartitionRunner) dispatchActions(actions []Action) {
	for _, a := range actions {
		switch act := a.(type) {
		case ActRegisterTimer:
			if r.timers == nil {
				r.log.Warn("runner: ActRegisterTimer with no timer service",
					"shard", r.ShardID)
				continue
			}
			if err := r.timers.Push(act.FireAtMs, act.ID, act.SleepIdx); err != nil {
				r.log.Warn("runner: timer push failed", "err", err, "shard", r.ShardID)
			}
		case ActDeleteTimer:
			if r.timers == nil {
				continue
			}
			if err := r.timers.Delete(act.FireAtMs, act.ID); err != nil {
				r.log.Warn("runner: timer delete failed", "err", err, "shard", r.ShardID)
			}
		case ActInvoke:
			if r.invoker == nil {
				r.log.Warn("runner: ActInvoke with no invoker", "shard", r.ShardID)
				continue
			}
			r.invoker.StartInvocation(act.ID, act.Target)
		case ActAbortInvocation:
			if r.invoker == nil {
				continue
			}
			r.invoker.AbortInvocation(act.ID)
		case ActDeliverNotification:
			if r.invoker == nil {
				continue
			}
			r.invoker.DeliverNotification(act.ID, act.CompletionID, act.Value, act.Failure, act.Void)
		case ActDeliverAwakeable:
			if r.invoker == nil {
				continue
			}
			r.invoker.DeliverAwakeable(act.ID, act.AwakeableID, act.Value, act.Failure)
		case ActDispatchOutbox:
			if r.outbox == nil {
				r.log.Warn("runner: ActDispatchOutbox with no outbox service",
					"shard", r.ShardID, "seq", act.Seq)
				continue
			}
			r.outbox.Push(act.Seq, act.Envelope)
		case ActIngressResponse:
			// Phase 2: ingress response routing lands with the gRPC
			// gateway (Step 13). Drop quietly until then so existing FSM
			// paths can populate the action without crashing.
		default:
			r.log.Warn("runner: unhandled action type", "type", a)
		}
	}
}

// onBecomeLeader rebuilds the timer heap + outbox queue from storage,
// rebinds the invoker's table views to the current snapshot store, and
// starts the leader-side service loops. The timer + outbox services are
// instantiated fresh on every leader gain because their done channels
// are single-use; reusing the prior instance would panic on the next
// `defer close(done)` invocation.
func (r *PartitionRunner) onBecomeLeader() {
	r.log.Info("partition: became leader", "shard", r.ShardID, "epoch", r.leadership.LeaderEpoch())

	store := r.snapshotter.Store()

	r.timers = NewTimerService(
		tables.TimerTable{S: store},
		r.proposer,
		TimerServiceOptions{Log: r.log},
	)
	r.outbox = NewOutboxService(
		tables.OutboxTable{S: store},
		r.proposer,
		r.sender,
		r.ShardID,
		r.log,
	)
	if r.invoker != nil {
		r.invoker.Rebind(
			tables.JournalTable{S: store},
			tables.InvocationTable{S: store},
			tables.StateTable{S: store},
		)
	}

	if err := r.timers.Rebuild(); err != nil {
		r.log.Error("partition: timer rebuild failed", "shard", r.ShardID, "err", err)
		return
	}
	if err := r.outbox.Rebuild(); err != nil {
		r.log.Error("partition: outbox rebuild failed", "shard", r.ShardID, "err", err)
		return
	}

	leaderCtx, cancel := context.WithCancel(context.Background())
	timerDone := make(chan struct{})
	outboxDone := make(chan struct{})

	r.mu.Lock()
	// Defensive: cancel any prior leader scope. Normal step-down clears
	// these; if we somehow re-enter without intervening onStepDown, abort
	// the prior scope before installing the new one.
	if r.leaderCancel != nil {
		r.leaderCancel()
	}
	r.leaderCtx = leaderCtx
	r.leaderCancel = cancel
	r.timerDone = timerDone
	r.outboxDone = outboxDone
	r.mu.Unlock()

	if r.invoker != nil {
		r.invoker.Start(leaderCtx)
		// Resume any non-terminal invocations that committed before this
		// leader scope. Required because apply-on-startup dispatches
		// ActInvoke through dispatchActions while the Invoker is not yet
		// started; those calls are dropped, so the new leader must
		// re-spawn sessions explicitly from the InvocationTable.
		if err := r.invoker.ResumeNonTerminal(tables.InvocationTable{S: store}); err != nil {
			r.log.Warn("partition: invoker resume failed", "shard", r.ShardID, "err", err)
		}
	}

	go func() {
		defer close(timerDone)
		if err := r.timers.Run(leaderCtx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("partition: timer run exited", "shard", r.ShardID, "err", err)
		}
	}()
	go func() {
		defer close(outboxDone)
		if err := r.outbox.Run(leaderCtx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("partition: outbox run exited", "shard", r.ShardID, "err", err)
		}
	}()
}

// onStepDown tears down the leader-side services. Order matters:
//
//  1. Cancel leaderCtx so the timer and outbox loops observe shutdown.
//  2. Stop the Invoker first (drains running sessions). The sessions
//     can no longer propose journal entries, so no further timer/outbox
//     actions arrive while we're stopping.
//  3. Wait for timer + outbox loops to return.
//
// We intentionally do NOT touch the underlying TimerService / OutboxService
// objects after waiting — the next onBecomeLeader will construct fresh
// instances. Holding on to the old ones would risk panic on second-Run.
func (r *PartitionRunner) onStepDown() {
	r.log.Info("partition: stepped down", "shard", r.ShardID)
	r.mu.Lock()
	cancel := r.leaderCancel
	timerDone := r.timerDone
	outboxDone := r.outboxDone
	r.leaderCtx = nil
	r.leaderCancel = nil
	r.timerDone = nil
	r.outboxDone = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if r.invoker != nil {
		r.invoker.Stop()
	}
	if timerDone != nil {
		<-timerDone
	}
	if outboxDone != nil {
		<-outboxDone
	}
}

// Compile-time check that LeadershipObserver is implemented.
var _ LeadershipObserver = (*Leadership)(nil)

// Phase 1 also exposes a tiny helper to fetch the InvocationStatus directly
// from the partition's store; tests use this to avoid a SyncRead round-trip.
func (r *PartitionRunner) StatusOf(id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	return (tables.InvocationTable{S: r.snapshotter.Store()}).Get(id)
}
