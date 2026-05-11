package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// PartitionRunner ties together the per-partition leader-only services: the
// proposer (Raft client), leadership state, timer service, and action
// collector. It exposes a small API for tests/ingress to propose commands.
type PartitionRunner struct {
	ShardID uint64

	snapshotter *Snapshotter
	proposer    *RaftProposer
	leadership  *Leadership
	collector   *ActionCollector
	timers      *TimerService
	log         *slog.Logger

	mu          sync.Mutex
	timerCancel context.CancelFunc
	timerDone   chan struct{}
}

// Proposer returns the partition's RaftProposer.
func (r *PartitionRunner) Proposer() *RaftProposer { return r.proposer }

// Leadership returns the partition's leadership state (read-only API for tests).
func (r *PartitionRunner) Leadership() *Leadership { return r.leadership }

// Snapshotter returns the underlying snapshotter. Mainly for tests that want
// to read state directly.
func (r *PartitionRunner) Snapshotter() *Snapshotter { return r.snapshotter }

// IsLeader is a convenience accessor.
func (r *PartitionRunner) IsLeader() bool { return r.leadership.IsLeader() }

// runnerTimerTable returns a TimerTable view bound to whichever store the
// snapshotter currently holds.
func runnerTimerTable(s *Snapshotter) tables.TimerTable {
	return tables.TimerTable{S: s.Store()}
}

// dispatchActions is called by the Partition FSM (inside its Update path,
// after the storage batch commits) with the actions accumulated on the
// leader. We may NOT propose to Raft here because we're still inside the
// dragonboat apply goroutine. Timer pushes are local and safe.
func (r *PartitionRunner) dispatchActions(actions []Action) {
	for _, a := range actions {
		switch act := a.(type) {
		case ActRegisterTimer:
			if err := r.timers.Push(act.FireAtMs, act.ID, act.SleepIdx); err != nil {
				r.log.Warn("runner: timer push failed", "err", err, "shard", r.ShardID)
			}
		case ActDeleteTimer:
			if err := r.timers.Delete(act.FireAtMs, act.ID); err != nil {
				r.log.Warn("runner: timer delete failed", "err", err, "shard", r.ShardID)
			}
		case ActInvoke:
			// Phase 1 has no invoker; Phase 2 will dispatch to the SDK
			// handler stream. For now we just log so the test harness can
			// observe the intent.
			r.log.Debug("runner: ActInvoke (no-op in Phase 1)",
				"shard", r.ShardID,
				"target", act.Target.GetServiceName()+"/"+act.Target.GetHandlerName(),
			)
		case ActAbortInvocation, ActIngressResponse:
			// Phase 1 no-op.
		default:
			r.log.Warn("runner: unhandled action type", "type", a)
		}
	}
}

// onBecomeLeader rebuilds the timer heap from storage and starts the
// TimerService run loop. Called by Leadership when we transition to Leader.
func (r *PartitionRunner) onBecomeLeader() {
	r.log.Info("partition: became leader", "shard", r.ShardID, "epoch", r.leadership.LeaderEpoch())

	// The snapshotter's store may have been swapped during a snapshot
	// recovery; rebind the timer table to the current store.
	r.timers.table = tables.TimerTable{S: r.snapshotter.Store()}

	if err := r.timers.Rebuild(); err != nil {
		r.log.Error("partition: timer rebuild failed", "shard", r.ShardID, "err", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	r.mu.Lock()
	// Defensive: cancel any prior timer loop (should not happen, but matches
	// step-down + immediate re-promote semantics).
	if r.timerCancel != nil {
		r.timerCancel()
	}
	r.timerCancel = cancel
	r.timerDone = done
	r.mu.Unlock()

	go func() {
		defer close(done)
		if err := r.timers.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("partition: timer run exited", "shard", r.ShardID, "err", err)
		}
	}()
}

// onStepDown stops the timer loop.
func (r *PartitionRunner) onStepDown() {
	r.log.Info("partition: stepped down", "shard", r.ShardID)
	r.mu.Lock()
	cancel := r.timerCancel
	done := r.timerDone
	r.timerCancel = nil
	r.timerDone = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	r.timers.Stop()
}

// Compile-time check that LeadershipObserver is implemented.
var _ LeadershipObserver = (*Leadership)(nil)

// Phase 1 also exposes a tiny helper to fetch the InvocationStatus directly
// from the partition's store; tests use this to avoid a SyncRead round-trip.
func (r *PartitionRunner) StatusOf(id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	return (tables.InvocationTable{S: r.snapshotter.Store()}).Get(id)
}
