package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/twinfer/reflow/internal/engine/invoker"
	"github.com/twinfer/reflow/internal/observability"
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
	// is configured.
	sender CrossShardSender
	// lpUploader ships LP-transfer SSTs out-of-band to every replica
	// hosting the destination shard. nil in single-node deployments;
	// populated by Host.StartPartition in multi-node deployments.
	lpUploader LPSSTUploader
	log        *slog.Logger
	metrics    *observability.Metrics

	// timers, outbox, reap, and lpTransfer are populated by
	// onBecomeLeader and torn down by onStepDown. dispatchActions reads
	// them on the apply goroutine, which is the same goroutine as
	// onBecomeLeader/onStepDown — no extra synchronisation needed.
	timers      *TimerService
	outbox      *OutboxService
	reap        *ReapService
	processReap *ReapService
	lpTransfer  *LPTransferService

	mu              sync.Mutex
	leaderCancel    context.CancelFunc
	timerDone       chan struct{}
	outboxDone      chan struct{}
	reapDone        chan struct{}
	processReapDone chan struct{}
	lpTransferDone  chan struct{}
	// storeRelease releases the Snapshotter lease acquired in
	// onBecomeLeader. Held for the lifetime of the current leader scope
	// so Host.Close → Snapshotter.Close waits until our leader-scoped
	// goroutines (timer.Run, outbox.Run, the invoker session pool) have
	// stopped touching the underlying pebble.DB.
	storeRelease func()
	// inflightOnLeader tracks in-flight onBecomeLeader goroutines (each
	// spawned by Leadership.OnAnnounceLeader via `go onBecomeLeader()`).
	// onStepDown Waits on this before reading r.storeRelease so it
	// observes any lease/cancel a concurrent onBecomeLeader is about to
	// install. Without the wait, onStepDown could run between
	// onBecomeLeader's Acquire and its r.mu.Lock + install, leaving the
	// timer/outbox Run goroutines spawned afterwards with a leaderCtx
	// nobody will cancel — and the snapshotter lease leaked forever.
	inflightOnLeader sync.WaitGroup
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

// IsLeader reports whether this partition replica currently believes itself
// to be the Raft leader.
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
			var err error
			if act.Process != nil {
				err = r.timers.PushProcess(act.FireAtMs, act.ID, act.Process)
			} else {
				err = r.timers.Push(act.FireAtMs, act.ID, act.SleepIdx)
			}
			if err != nil {
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
		case ActAdvanceProcess:
			if r.invoker == nil {
				r.log.Warn("runner: ActAdvanceProcess with no invoker", "shard", r.ShardID)
				continue
			}
			r.invoker.StartProcessTurn(act.Pk, act.Service, act.InstanceKey, act.Entry)
		case ActDispatchOutbox:
			if r.outbox == nil {
				r.log.Warn("runner: ActDispatchOutbox with no outbox service",
					"shard", r.ShardID, "seq", act.Seq)
				continue
			}
			r.outbox.Push(act.Seq, act.Envelope)
		case ActScheduleReap:
			if r.reap == nil {
				r.log.Warn("runner: ActScheduleReap with no reap service",
					"shard", r.ShardID)
				continue
			}
			r.reap.Push(invocationReapEntry{fireAt: act.FireAtMs, id: act.ID})
		case ActScheduleProcessReap:
			if r.processReap == nil {
				r.log.Warn("runner: ActScheduleProcessReap with no process reap service",
					"shard", r.ShardID)
				continue
			}
			r.processReap.Push(processReapEntry{
				fireAt: act.FireAtMs, pk: act.Pk, service: act.Service, instanceKey: act.InstanceKey,
			})
		case ActStartLPTransferScan:
			if r.lpTransfer == nil {
				r.log.Warn("runner: ActStartLPTransferScan with no lp transfer service",
					"shard", r.ShardID, "transfer_id", act.TransferID)
				continue
			}
			r.lpTransfer.PushScan(act.TransferID, act.LP, act.DestShard)
		case ActSignalLPTransferStaged:
			if r.lpTransfer == nil {
				r.log.Warn("runner: ActSignalLPTransferStaged with no lp transfer service",
					"shard", r.ShardID, "transfer_id", act.TransferID)
				continue
			}
			r.lpTransfer.PushAckStaged(act.TransferID, act.LP, act.SourceShard)
		case ActSignalLPTransferCleaned:
			if r.lpTransfer == nil {
				r.log.Warn("runner: ActSignalLPTransferCleaned with no lp transfer service",
					"shard", r.ShardID, "transfer_id", act.TransferID)
				continue
			}
			r.lpTransfer.PushAckCleaned(act.TransferID)
		case ActSignalLPTransferAbortAck:
			if r.lpTransfer == nil {
				r.log.Warn("runner: ActSignalLPTransferAbortAck with no lp transfer service",
					"shard", r.ShardID, "transfer_id", act.TransferID)
				continue
			}
			r.lpTransfer.PushAckAbort(act.TransferID)
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
	r.inflightOnLeader.Add(1)
	defer r.inflightOnLeader.Done()

	r.log.Info("partition: became leader", "shard", r.ShardID, "epoch", r.leadership.LeaderEpoch())

	store, release, ok := r.snapshotter.Acquire()
	if !ok {
		r.log.Info("partition: snapshotter closing; skipping leader gain",
			"shard", r.ShardID)
		return
	}
	// Lease released by onStepDown via storeRelease; if any Rebuild
	// failure short-circuits below, release in the failure path.

	// Build into locals first; install onto r only after Rebuild succeeds.
	// If we assigned r.timers/r.outbox up front, a Rebuild failure would
	// leave the fields pointing at services whose Run goroutine never
	// started — dispatchActions would push onto them and the work would
	// silently accumulate with no consumer.
	timers := NewTimerService(
		tables.TimerTable{S: store},
		r.proposer,
		TimerServiceOptions{Log: r.log, Metrics: r.metrics},
	)
	outbox := NewOutboxService(
		tables.OutboxTable{S: store},
		r.proposer,
		r.sender,
		r.ShardID,
		r.log,
	)
	reap := NewReapService(
		func(emit func(reapEntry) error) error {
			return (tables.ReapTable{S: store}).ScanAll(func(rr tables.ReapRow) error {
				return emit(invocationReapEntry{fireAt: rr.FireAtMs, id: rr.ID})
			})
		},
		r.proposer,
		ReapServiceOptions{Log: r.log},
	)
	processReap := NewReapService(
		func(emit func(reapEntry) error) error {
			return (tables.ProcessReapTable{S: store}).ScanAll(func(cmd *enginev1.ReapProcessInstance) error {
				return emit(processReapEntry{
					fireAt: cmd.GetFireAtMs(), pk: cmd.GetPk(),
					service: cmd.GetService(), instanceKey: cmd.GetInstanceKey(),
				})
			})
		},
		r.proposer,
		ReapServiceOptions{Log: r.log},
	)
	lpTransfer := NewLPTransferService(store, r.sender, r.lpUploader, r.ShardID, r.log, r.metrics)
	if r.invoker != nil {
		r.invoker.Rebind(
			tables.JournalTable{S: store},
			tables.InvocationTable{S: store},
			tables.StateTable{S: store},
			tables.ProcessInstanceTable{S: store},
			tables.ProcessInboxTable{S: store},
		)
	}

	if err := timers.Rebuild(); err != nil {
		release()
		r.log.Error("partition: timer rebuild failed", "shard", r.ShardID, "err", err)
		return
	}
	if err := outbox.Rebuild(); err != nil {
		release()
		r.log.Error("partition: outbox rebuild failed", "shard", r.ShardID, "err", err)
		return
	}
	if err := reap.Rebuild(); err != nil {
		release()
		r.log.Error("partition: reap rebuild failed", "shard", r.ShardID, "err", err)
		return
	}
	if err := processReap.Rebuild(); err != nil {
		release()
		r.log.Error("partition: process reap rebuild failed", "shard", r.ShardID, "err", err)
		return
	}
	if err := lpTransfer.Rebuild(context.Background()); err != nil {
		release()
		r.log.Error("partition: lp transfer rebuild failed", "shard", r.ShardID, "err", err)
		return
	}
	r.timers = timers
	r.outbox = outbox
	r.reap = reap
	r.processReap = processReap
	r.lpTransfer = lpTransfer

	// Reclaim SelfProposal dedup rows from epochs we have moved past.
	// Bounded by stale-leader churn — runs at most once per leader gain.
	// Preserves one prior epoch as a safety margin (envelopes that
	// committed during the transition still match a dedup row).
	if epoch := r.leadership.LeaderEpoch(); epoch > 1 {
		gcBatch := store.NewBatch()
		if err := (tables.DedupTable{S: store}).GCSelfBelowEpoch(gcBatch, epoch-1); err != nil {
			r.log.Warn("partition: dedup GC range failed", "shard", r.ShardID, "err", err)
			_ = gcBatch.Close()
		} else if err := gcBatch.Commit(true); err != nil {
			r.log.Warn("partition: dedup GC commit failed", "shard", r.ShardID, "err", err)
		}
	}

	leaderCtx, cancel := context.WithCancel(context.Background())
	timerDone := make(chan struct{})
	outboxDone := make(chan struct{})
	reapDone := make(chan struct{})
	processReapDone := make(chan struct{})
	lpTransferDone := make(chan struct{})

	r.mu.Lock()
	// Defensive: cancel any prior leader scope and release any prior
	// store lease. Normal step-down clears both; if we somehow re-enter
	// without intervening onStepDown (Leadership.OnAnnounceLeader fires
	// onBecomeLeader as an async goroutine per call), abort the prior
	// scope before installing the new one. Releasing the prior lease is
	// load-bearing — without it Snapshotter.Close would block on a leak.
	if r.leaderCancel != nil {
		r.leaderCancel()
	}
	priorRelease := r.storeRelease
	r.leaderCancel = cancel
	r.timerDone = timerDone
	r.outboxDone = outboxDone
	r.reapDone = reapDone
	r.processReapDone = processReapDone
	r.lpTransferDone = lpTransferDone
	r.storeRelease = release
	r.mu.Unlock()
	if priorRelease != nil {
		priorRelease()
	}

	if r.invoker != nil {
		r.invoker.Start(leaderCtx)
		// Resume any non-terminal invocations that committed before this
		// leader scope. Required because apply-on-startup dispatches
		// ActInvoke through dispatchActions while the Invoker is not yet
		// started; those calls are dropped, so the new leader must
		// re-spawn sessions explicitly from the InvocationTable.
		if err := r.invoker.ResumeNonTerminal(leaderCtx, tables.InvocationTable{S: store}); err != nil {
			r.log.Warn("partition: invoker resume failed", "shard", r.ShardID, "err", err)
		}
		if err := r.invoker.ResumeProcessTurns(leaderCtx, tables.ProcessInstanceTable{S: store}, tables.ProcessInboxTable{S: store}); err != nil {
			r.log.Warn("partition: process turn resume failed", "shard", r.ShardID, "err", err)
		}
	}

	// Capture the locally-built services for the Run goroutines so the
	// closures don't dereference r.timers/r.outbox concurrently with a
	// future onBecomeLeader that's already started replacing them.
	go func() {
		defer close(timerDone)
		if err := timers.Run(leaderCtx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("partition: timer run exited", "shard", r.ShardID, "err", err)
		}
	}()
	go func() {
		defer close(outboxDone)
		if err := outbox.Run(leaderCtx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("partition: outbox run exited", "shard", r.ShardID, "err", err)
		}
	}()
	go func() {
		defer close(reapDone)
		if err := reap.Run(leaderCtx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("partition: reap run exited", "shard", r.ShardID, "err", err)
		}
	}()
	go func() {
		defer close(processReapDone)
		if err := processReap.Run(leaderCtx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("partition: process reap run exited", "shard", r.ShardID, "err", err)
		}
	}()
	go func() {
		defer close(lpTransferDone)
		if err := lpTransfer.Run(leaderCtx); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Error("partition: lp transfer run exited", "shard", r.ShardID, "err", err)
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
	// Wait for any in-flight onBecomeLeader goroutine to finish installing
	// r.storeRelease + r.leaderCancel before we read them; otherwise a
	// concurrent onBecomeLeader still mid-Rebuild would leave the timer +
	// outbox Run goroutines it spawns afterwards with an uncancellable
	// leaderCtx, and the Snapshotter lease would leak. See the field doc
	// on r.inflightOnLeader.
	r.inflightOnLeader.Wait()

	r.log.Info("partition: stepped down", "shard", r.ShardID)
	r.mu.Lock()
	cancel := r.leaderCancel
	timerDone := r.timerDone
	outboxDone := r.outboxDone
	reapDone := r.reapDone
	processReapDone := r.processReapDone
	lpTransferDone := r.lpTransferDone
	release := r.storeRelease
	r.leaderCancel = nil
	r.timerDone = nil
	r.outboxDone = nil
	r.reapDone = nil
	r.processReapDone = nil
	r.lpTransferDone = nil
	r.storeRelease = nil
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
	if reapDone != nil {
		<-reapDone
	}
	if processReapDone != nil {
		<-processReapDone
	}
	if lpTransferDone != nil {
		<-lpTransferDone
	}
	// Release the Snapshotter lease last — after every leader-scoped
	// goroutine has stopped touching the store. Snapshotter.Close blocks
	// until this fires.
	if release != nil {
		release()
	}
}

// Compile-time check that LeadershipObserver is implemented.
var _ LeadershipObserver = (*Leadership)(nil)

// StatusOf fetches the InvocationStatus directly from the partition's store;
// tests use this to avoid a SyncRead round-trip.
func (r *PartitionRunner) StatusOf(id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	store, release, ok := r.snapshotter.Acquire()
	if !ok {
		return nil, errors.New("runner: snapshotter closed")
	}
	defer release()
	return (tables.InvocationTable{S: store}).Get(id)
}
