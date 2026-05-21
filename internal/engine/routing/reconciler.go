package routing

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// reconcileInterval is the periodic backstop tick. Matches the
// internal/secretstore + internal/ingress/eventsource reconcilers so the
// fleet-wide cadence is uniform. The TableNotifier wake is the primary
// signal; the ticker is defense against missed bumps.
const reconcileInterval = 5 * time.Second

// LPOwnersReader is the seam RunReconciler uses to fetch the desired
// snapshot. Production wiring is a thin adapter over engine.Host that
// SyncReads shard 0 via dragonboat.Lookup(LookupLPOwners{}); tests hand
// in a fake.
//
// SnapshotLPOwners returns (lp → shard_id, table_revision, error). The
// table_revision is exposed for future operator metrics; reconcileOnce
// itself doesn't act on it.
type LPOwnersReader interface {
	SnapshotLPOwners(ctx context.Context) (map[uint32]uint64, uint64, error)
}

// RunReconciler is the production-mode reconcile loop for the routing
// Partitioner. Wakes on the LPOwnersTable notifier (FSM post-commit
// Bump) or the 5s ticker, SyncRead's the desired snapshot, and atomically
// swaps it into the Partitioner. Errors are logged; the loop keeps
// running until ctx is cancelled.
//
// Goroutine affinity: own dedicated goroutine, one per node. Never runs
// on the FSM apply path — the notifier wake just signals; the SyncRead
// happens off-loop.
func RunReconciler(
	ctx context.Context,
	sub <-chan struct{},
	reader LPOwnersReader,
	p *Partitioner,
	log *slog.Logger,
) error {
	if reader == nil {
		return errors.New("routing: reader is required for reconcile loop")
	}
	if p == nil {
		return errors.New("routing: partitioner is required for reconcile loop")
	}
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	reconcileOnce(ctx, reader, p, log)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			reconcileOnce(ctx, reader, p, log)
		case <-ticker.C:
			reconcileOnce(ctx, reader, p, log)
		}
	}
}

// reconcileOnce performs one SnapshotLPOwners + atomic-swap pass. An
// empty snapshot is NOT installed (it would force the Partitioner onto
// the planner fallback for every routing decision); transient SyncRead
// failures and pre-bootstrap-seed empty tables both leave the previous
// snapshot in place.
func reconcileOnce(ctx context.Context, reader LPOwnersReader, p *Partitioner, log *slog.Logger) {
	snap, _, err := reader.SnapshotLPOwners(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Warn("routing: read lpowners snapshot failed", "err", err)
		}
		return
	}
	if len(snap) == 0 {
		return
	}
	p.SetLPOwnersSnapshot(snap)
}
