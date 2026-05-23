package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// partitionReconcileInterval is the periodic backstop tick. Matches the
// routing / secretstore / eventsource reconcilers so the fleet-wide
// cadence is uniform. The shard-0 PartitionTable notifier wake is the
// primary signal; the ticker is defense against missed bumps and the
// post-snapshot-recovery case where dragonboat replaces the on-disk
// state without firing the apply path.
const partitionReconcileInterval = 5 * time.Second

// partitionReadTimeout is the per-tick SyncRead bound. Short so
// cancellation on Host shutdown propagates promptly.
const partitionReadTimeout = 5 * time.Second

// PartitionTableReader is the seam RunPartitionTableReconciler uses to
// fetch the desired snapshot. Production wiring is a thin adapter over
// *Host that SyncReads shard 0 via dragonboat.Lookup(LookupPartitionTable{});
// tests hand in a fake.
type PartitionTableReader interface {
	SnapshotPartitionTable(ctx context.Context) (*enginev1.PartitionTable, error)
}

// PartitionTableApplier is the per-node consumer of the latest
// PartitionTable snapshot. The production implementation is *Host —
// it walks the table, starts locally-owned shards that are not yet
// running, and logs ownership losses that need an explicit
// StopPartition follow-up from the rebalancer.
type PartitionTableApplier interface {
	ReconcilePartitionTable(pt *enginev1.PartitionTable)
}

// RunPartitionTableReconciler is the production-mode reconcile loop for
// shard-0 PartitionTable changes. Wakes on the PartitionTable notifier
// (FSM post-commit Bump) or the 5s ticker, SyncReads the desired
// snapshot, and hands it to the applier. Errors are logged; the loop
// keeps running until ctx is cancelled.
//
// Goroutine affinity: own dedicated goroutine, one per node. Never runs
// on the FSM apply path — the notifier wake just signals; the SyncRead
// and ownership reconcile happen off-loop.
func RunPartitionTableReconciler(
	ctx context.Context,
	sub <-chan struct{},
	reader PartitionTableReader,
	applier PartitionTableApplier,
	log *slog.Logger,
) error {
	if reader == nil {
		return errors.New("engine: reader is required for partition reconcile loop")
	}
	if applier == nil {
		return errors.New("engine: applier is required for partition reconcile loop")
	}
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(partitionReconcileInterval)
	defer ticker.Stop()
	reconcilePartitionOnce(ctx, reader, applier, log)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			reconcilePartitionOnce(ctx, reader, applier, log)
		case <-ticker.C:
			reconcilePartitionOnce(ctx, reader, applier, log)
		}
	}
}

// reconcilePartitionOnce performs one SnapshotPartitionTable +
// ReconcilePartitionTable pass. A nil snapshot (table not yet
// bootstrapped) is skipped — there is nothing to converge against.
// Transient SyncRead failures leave local state untouched.
func reconcilePartitionOnce(ctx context.Context, reader PartitionTableReader, applier PartitionTableApplier, log *slog.Logger) {
	readCtx, cancel := context.WithTimeout(ctx, partitionReadTimeout)
	defer cancel()
	pt, err := reader.SnapshotPartitionTable(readCtx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Warn("engine: read partition table snapshot failed", "err", err)
		}
		return
	}
	if pt == nil {
		return
	}
	applier.ReconcilePartitionTable(pt)
}
