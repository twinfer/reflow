package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// lpMoverPollInterval is the base cadence for the lpMover's advance
// loop. Most ticks are no-ops (no in-flight transfers); when work is
// queued the per-tick action is bounded (one phase advance per row).
// The shard-0 LPTransfersTable notifier is the primary wake source;
// the ticker is the backstop for cases where an external signal
// (CrossShardSender.Send completion, stall detection) needs the loop
// to revisit the table.
const lpMoverPollInterval = 1 * time.Second

// lpMoverSyncReadTimeout is the per-tick SyncRead bound. Short so
// cancellation on leader step-down propagates promptly; the
// rebalancer uses the same 5s value for the same reason.
const lpMoverSyncReadTimeout = 5 * time.Second

// lpMoverStallTimeout is how long a non-terminal transfer can sit
// without progress before the mover transitions it to ABORTING. The
// dest's apply-emitted STAGED ack should arrive within seconds of a
// successful scan, so 5 minutes is a generous cap for slow networks
// or transient leader churn.
const lpMoverStallTimeout = 5 * time.Minute

// lpMoverGraceWindow is how long terminal rows (CLEANED / ABORTED)
// linger in the table for operator-facing list visibility before
// RemoveLPTransfer.
const lpMoverGraceWindow = 1 * time.Minute

// lpMover is the metadata-leader's orchestrator for cross-shard LP
// transfers. Owned by MetadataRunner; spawned in onBecomeLeader,
// torn down in onStepDown.
//
// Per-tick (1s) the mover SyncReads LPTransferTable from shard 0 and
// advances each non-terminal row by at most one phase. Per-phase
// actions:
//
//	INIT      → send BeginLPTransfer to source via CrossShardSender;
//	            the source's onBeginLPTransfer apply arm installs the
//	            freeze and the source-side LPTransferService kicks off
//	            the scan. No phase write here.
//	STAGED    → propose UpsertLpOwner{lp, dest} with CAS via
//	            ExpectedLpownersRevision. On success, propose
//	            UpdateLPTransferPhase{FLIPPED}. On CAS failure
//	            (concurrent ownership drift), transition to ABORTING.
//	FLIPPED   → send FinishLPTransfer to source + CommitLPTransfer to
//	            dest in parallel. Source's onFinishLPTransfer apply arm
//	            emits ActSignalLPTransferCleaned which the source's
//	            LPTransferService translates to phase=CLEANED.
//	CLEANED   → after lpMoverGraceWindow has elapsed since
//	            last_event_ms, propose RemoveLPTransfer.
//	ABORTING  → send AbortLPTransfer to both sides. Both emit
//	            ActSignalLPTransferAbortAck which translates to
//	            phase=ABORTED on shard 0 (monotonic apply absorbs the
//	            duplicate).
//	ABORTED   → after lpMoverGraceWindow has elapsed, propose
//	            RemoveLPTransfer.
//
// Stall detection runs first: any non-terminal row with
//
//	now - last_event_ms > lpMoverStallTimeout
//
// and a phase that can still be aborted (INIT, STAGED) transitions to
// ABORTING. FLIPPED stalls do NOT abort (the ownership flip is the
// point of no return); they retry indefinitely.
//
// Idempotent: every action arm is a no-op when the side it targets is
// already in the next phase, and the monotonic apply check on shard 0
// absorbs duplicate phase proposals.
type lpMover struct {
	host    *Host
	runner  *MetadataRunner
	log     *slog.Logger
	shardID uint64

	pollInterval time.Duration
	stallTimeout time.Duration
	graceWindow  time.Duration
}

func newLPMover(h *Host, r *MetadataRunner) *lpMover {
	return &lpMover{
		host:         h,
		runner:       r,
		log:          h.log,
		shardID:      r.ShardID,
		pollInterval: lpMoverPollInterval,
		stallTimeout: lpMoverStallTimeout,
		graceWindow:  lpMoverGraceWindow,
	}
}

func (m *lpMover) run(ctx context.Context) {
	t := time.NewTicker(m.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.tick(ctx)
		}
	}
}

func (m *lpMover) tick(ctx context.Context) {
	readCtx, cancel := context.WithTimeout(ctx, lpMoverSyncReadTimeout)
	defer cancel()
	list, err := m.host.LPTransfers(readCtx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			m.log.Debug("lpmover: read lp transfers failed", "err", err)
		}
		return
	}
	nowMs := uint64(time.Now().UnixMilli())
	for _, rec := range list.Records {
		if ctx.Err() != nil {
			return
		}
		m.advance(ctx, rec, nowMs)
	}
}

func (m *lpMover) advance(ctx context.Context, rec *enginev1.LPTransferRecord, nowMs uint64) {
	phase := rec.GetPhase()
	// Stall detection: forward-only phases past FLIPPED never time out.
	if isStallable(phase) {
		if nowMs-rec.GetLastEventMs() > uint64(m.stallTimeout/time.Millisecond) {
			m.log.Warn("lpmover: transfer stalled; transitioning to ABORTING",
				"transfer_id", rec.GetTransferId(),
				"phase", phase.String(),
				"age_ms", nowMs-rec.GetLastEventMs())
			m.proposePhase(ctx, rec.GetTransferId(), enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTING, 0)
			return
		}
	}
	switch phase {
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_INIT,
		enginev1.LPTransferPhase_LP_TRANSFER_PHASE_SHIPPING:
		// Send (or re-send) BeginLPTransfer to the source. Idempotent on
		// source side: a duplicate BeginLPTransfer for the same lp +
		// transfer_id is absorbed (Put-of-equivalent-row, plus the
		// scan goroutine rebuilds from LPFreezeTable on leader gain).
		m.sendPartitionCmd(ctx, rec.GetSourceShard(),
			fmt.Sprintf("lpmover-begin/%s", rec.GetTransferId()), 0,
			&enginev1.Command{Kind: &enginev1.Command_BeginLpTransfer{
				BeginLpTransfer: &enginev1.BeginLPTransfer{
					TransferId: rec.GetTransferId(),
					Lp:         rec.GetLp(),
					DestShard:  rec.GetDestShard(),
				},
			}})
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED:
		// Propose the ownership flip with CAS guarded by the expected
		// LPOwnersTable revision captured at INIT. On CAS failure (the
		// LPOwnersTable drifted — e.g. a concurrent admin operation),
		// transition to ABORTING.
		ok, postRev, err := m.proposeFlip(ctx, rec)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrShardClosed) {
				m.log.Warn("lpmover: UpsertLPOwner propose failed; will retry",
					"transfer_id", rec.GetTransferId(), "err", err)
			}
			return
		}
		if !ok {
			m.log.Warn("lpmover: LPOwners CAS failed; aborting transfer",
				"transfer_id", rec.GetTransferId(),
				"expected_revision", rec.GetExpectedLpownersRevision())
			m.proposePhase(ctx, rec.GetTransferId(), enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTING, 0)
			return
		}
		m.proposePhase(ctx, rec.GetTransferId(), enginev1.LPTransferPhase_LP_TRANSFER_PHASE_FLIPPED, postRev)
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_FLIPPED:
		// Fan out cleanup commands to both partition sides. The source's
		// onFinishLPTransfer emits ActSignalLPTransferCleaned which the
		// source's LPTransferService translates to phase=CLEANED on
		// shard 0; the dest's onCommitLPTransfer drops its staging row
		// silently.
		m.sendPartitionCmd(ctx, rec.GetSourceShard(),
			fmt.Sprintf("lpmover-finish/%s", rec.GetTransferId()), 0,
			&enginev1.Command{Kind: &enginev1.Command_FinishLpTransfer{
				FinishLpTransfer: &enginev1.FinishLPTransfer{
					TransferId: rec.GetTransferId(),
					Lp:         rec.GetLp(),
				},
			}})
		m.sendPartitionCmd(ctx, rec.GetDestShard(),
			fmt.Sprintf("lpmover-commit/%s", rec.GetTransferId()), 0,
			&enginev1.Command{Kind: &enginev1.Command_CommitLpTransfer{
				CommitLpTransfer: &enginev1.CommitLPTransfer{
					TransferId: rec.GetTransferId(),
					Lp:         rec.GetLp(),
				},
			}})
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTING:
		// Fan out aborts to both sides; each side's apply emits
		// ActSignalLPTransferAbortAck which translates to phase=ABORTED
		// on shard 0 (monotonic absorbs the duplicate).
		m.sendPartitionCmd(ctx, rec.GetSourceShard(),
			fmt.Sprintf("lpmover-abort-src/%s", rec.GetTransferId()), 0,
			&enginev1.Command{Kind: &enginev1.Command_AbortLpTransfer{
				AbortLpTransfer: &enginev1.AbortLPTransfer{
					TransferId: rec.GetTransferId(),
					Lp:         rec.GetLp(),
				},
			}})
		m.sendPartitionCmd(ctx, rec.GetDestShard(),
			fmt.Sprintf("lpmover-abort-dst/%s", rec.GetTransferId()), 0,
			&enginev1.Command{Kind: &enginev1.Command_AbortLpTransfer{
				AbortLpTransfer: &enginev1.AbortLPTransfer{
					TransferId: rec.GetTransferId(),
					Lp:         rec.GetLp(),
				},
			}})
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED,
		enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTED:
		// Terminal — after the grace window propose RemoveLPTransfer so
		// the operator-facing list eventually purges the row.
		if nowMs-rec.GetLastEventMs() < uint64(m.graceWindow/time.Millisecond) {
			return
		}
		m.proposeRemove(ctx, rec.GetTransferId())
	}
}

// proposeFlip proposes the UpsertLpOwner CAS that atomically flips the
// LPOwnersTable[lp] to point at the destination. Returns (ok, post-flip
// revision, err). ok=false signals CAS failure (revision drifted),
// which the caller maps to ABORTING.
func (m *lpMover) proposeFlip(ctx context.Context, rec *enginev1.LPTransferRecord) (bool, uint64, error) {
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertLpOwner{
			UpsertLpOwner: &enginev1.UpsertLPOwner{
				Record: &enginev1.LPOwnerRecord{
					Lp:      rec.GetLp(),
					ShardId: rec.GetDestShard(),
				},
			},
		},
	}
	pre := &enginev1.Precondition{IfTableRevisionEq: rec.GetExpectedLpownersRevision()}
	propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resultValue, err := m.runner.proposer.ProposeSelfCAS(propCtx, cmd, pre)
	if err != nil {
		return false, 0, err
	}
	// applyUpsertLPOwner returns ResultValueFailedPrecondition (1) on
	// CAS mismatch; any other resultValue (e.g. uint64(len(cmd))) is
	// the default success stamp.
	const failedPrecond = uint64(1)
	if resultValue == failedPrecond {
		return false, 0, nil
	}
	// On success the revision is now rec.ExpectedLpownersRevision + 1.
	return true, rec.GetExpectedLpownersRevision() + 1, nil
}

func (m *lpMover) proposePhase(ctx context.Context, transferID string, phase enginev1.LPTransferPhase, expectedLpownersRev uint64) {
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpdateLpTransferPhase{
			UpdateLpTransferPhase: &enginev1.UpdateLPTransferPhase{
				TransferId:               transferID,
				Phase:                    phase,
				ExpectedLpownersRevision: expectedLpownersRev,
			},
		},
	}
	propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := m.runner.proposer.ProposeSelf(propCtx, cmd); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrShardClosed) {
			m.log.Warn("lpmover: UpdateLPTransferPhase propose failed; will retry",
				"transfer_id", transferID, "phase", phase.String(), "err", err)
		}
	}
}

func (m *lpMover) proposeRemove(ctx context.Context, transferID string) {
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_RemoveLpTransfer{
			RemoveLpTransfer: &enginev1.RemoveLPTransfer{TransferId: transferID},
		},
	}
	propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := m.runner.proposer.ProposeSelf(propCtx, cmd); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrShardClosed) {
			m.log.Debug("lpmover: RemoveLPTransfer propose failed; will retry",
				"transfer_id", transferID, "err", err)
		}
	}
}

// sendPartitionCmd dispatches a Command to a partition shard via
// CrossShardSender. Single-node deployments (sender==nil) just log and
// skip — there's no separate partition for the command to reach (lp
// transfer between distinct shards requires multi-node anyway).
func (m *lpMover) sendPartitionCmd(ctx context.Context, destShard uint64, producerID string, seq uint64, cmd *enginev1.Command) {
	if destShard == 0 {
		return
	}
	if m.host.cfg.CrossShardSender == nil {
		m.log.Warn("lpmover: no CrossShardSender configured; cannot dispatch",
			"dest_shard", destShard, "producer", producerID)
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := m.host.cfg.CrossShardSender.Send(sendCtx, destShard, producerID, seq, cmd); err != nil {
		if !errors.Is(err, context.Canceled) {
			m.log.Warn("lpmover: send failed; will retry on next tick",
				"dest_shard", destShard, "producer", producerID, "err", err)
		}
	}
}

// isStallable reports whether a phase can be timed out and aborted.
// FLIPPED is never aborted (the LPOwners flip is the point of no
// return); CLEANED / ABORTED are terminal; UNSPECIFIED never happens
// in practice.
func isStallable(p enginev1.LPTransferPhase) bool {
	switch p {
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_INIT,
		enginev1.LPTransferPhase_LP_TRANSFER_PHASE_SHIPPING,
		enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED:
		return true
	}
	return false
}
