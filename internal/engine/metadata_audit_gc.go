package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// defaultAuditGCInterval is the cadence at which the metadata leader
// proposes a retention pass when HostConfig.Audit.GcInterval is zero.
// 1h is plenty: retention precision is bounded by this interval, and
// the bound on operator audit visibility (latest visible row) is set
// by the apply path, not by this loop.
const defaultAuditGCInterval = 1 * time.Hour

// auditGCSyncProposeTimeout caps how long we wait on a single
// Command_GcAuditLog propose. The arm itself does one range scan +
// per-key delete inside the batch; cheap on small tables, bounded by
// retention churn on large ones. 30s is generous.
const auditGCSyncProposeTimeout = 30 * time.Second

// auditGC is the metadata-leader's periodic audit-log retention
// scrubber. Owned by MetadataRunner; spawned in onBecomeLeader, torn
// down via leaderCtx in onStepDown — same lifecycle as lpMover and
// metadataRebalancer.
//
// Per-tick: compute before_ms = now - RetentionDuration; propose
// Command_GcAuditLog{BeforeTsMs: before_ms}. Idempotent on the apply
// side: re-applying with the same or an earlier bound is a no-op,
// so spurious wake-ups and duplicate proposals are harmless.
//
// Disabled when RetentionDuration == 0. The whole goroutine never
// spawns in that case (the check is at the spawn site in
// MetadataRunner.onBecomeLeader).
type auditGC struct {
	runner            *MetadataRunner
	log               *slog.Logger
	retentionDuration time.Duration
	gcInterval        time.Duration
}

func newAuditGC(r *MetadataRunner, cfg AuditConfig) *auditGC {
	interval := cfg.GcInterval
	if interval <= 0 {
		interval = defaultAuditGCInterval
	}
	return &auditGC{
		runner:            r,
		log:               r.log.With("component", "audit_gc"),
		retentionDuration: cfg.RetentionDuration,
		gcInterval:        interval,
	}
}

func (g *auditGC) run(ctx context.Context) {
	if g.retentionDuration <= 0 {
		return
	}
	t := time.NewTicker(g.gcInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.tick(ctx)
		}
	}
}

func (g *auditGC) tick(ctx context.Context) {
	// before_ms can never go negative — retentionDuration is positive
	// and time.Now().UnixMilli is monotonic on the wall clock. A
	// before_ms in the future (clock skew) is harmless: the apply arm
	// would delete every row, then quiesce.
	now := time.Now().UnixMilli()
	beforeMs := now - g.retentionDuration.Milliseconds()
	if beforeMs <= 0 {
		// Cluster bootstrap with retentionDuration larger than the
		// wall clock since epoch — never going to happen in practice
		// but the guard avoids proposing GcAuditLog{before=0} which
		// the apply arm logs and ignores.
		return
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_GcAuditLog{
			GcAuditLog: &enginev1.GcAuditLog{BeforeTsMs: uint64(beforeMs)},
		},
	}
	propCtx, cancel := context.WithTimeout(ctx, auditGCSyncProposeTimeout)
	defer cancel()
	if err := g.runner.proposer.ProposeSelf(propCtx, cmd); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		g.log.Warn("audit gc propose failed; will retry next tick",
			"before_ts_ms", beforeMs, "err", err)
	}
}
