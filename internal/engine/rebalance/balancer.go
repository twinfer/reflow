package rebalance

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/twinfer/reflw/internal/engine/cluster"
	"github.com/twinfer/reflw/internal/observability"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// HostReader is the subset of *engine.Host the balancer reads through.
// Defined as an interface so unit tests can swap in fakes without
// bringing up a real dragonboat cluster.
type HostReader interface {
	PartitionTable(ctx context.Context) (*enginev1.PartitionTable, error)
	LPOwners(ctx context.Context) (*cluster.LPOwnersList, error)
	LPTransfers(ctx context.Context) (*cluster.LPTransfersList, error)
	RebalanceDrains(ctx context.Context) (*cluster.RebalanceDrainList, error)
}

// Proposer is the shard-0 self-proposal entry point. Implemented by
// *engine.RaftProposer; the balancer uses the same method the lpMover
// and metadata bootstrap call.
type Proposer interface {
	ProposeSelf(ctx context.Context, cmd *enginev1.Command) error
}

// Config is the inert configuration for one Balancer. All fields except
// Mode are knob values already defaulted by pkg/reflw.withDefaults; the
// balancer trusts them.
type Config struct {
	Mode                       string
	MaxConcurrentTransfers     uint32
	MinSecondsBetweenTransfers uint32
	SkewEngagePct              uint32
	SkewDisengagePct           uint32
	// PollInterval is the ticker backstop. Zero defaults to 30s.
	// Tests override this to keep wall time bounded.
	PollInterval time.Duration
	// SyncReadTimeout caps each per-tick SyncRead. Zero defaults to 5s.
	SyncReadTimeout time.Duration
}

// Balancer is the leader-only control loop. Run blocks until ctx is
// cancelled (leader step-down).
type Balancer struct {
	cfg     Config
	host    HostReader
	prop    Proposer
	drainCh <-chan struct{}
	metrics *observability.Metrics
	log     *slog.Logger

	// engaged carries the hysteresis bit across ticks. Read/written by
	// Run only (single-goroutine).
	engaged bool
}

// New constructs a Balancer. Caller wires deps from the metadata-shard
// host + runner + cluster notifier.
func New(cfg Config, host HostReader, prop Proposer, drainCh <-chan struct{}, m *observability.Metrics, log *slog.Logger) *Balancer {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if cfg.SyncReadTimeout == 0 {
		cfg.SyncReadTimeout = 5 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Balancer{
		cfg:     cfg,
		host:    host,
		prop:    prop,
		drainCh: drainCh,
		metrics: m,
		log:     log,
	}
}

// Run executes the control loop until ctx is cancelled. Each iteration
// either fires on a TableNotifier wake (RebalanceDrainTable change) or
// on the PollInterval backstop. SyncReads are bounded per tick.
//
// Returns silently on ctx cancellation. Internal errors (SyncRead
// timeout, propose failure) are logged and absorbed — they do not
// surface here because the next tick will re-observe.
func (b *Balancer) Run(ctx context.Context) {
	b.emitModeGauge()
	if b.cfg.Mode == ModeOff {
		// Defensive: callers should not start the balancer in off
		// mode, but if they do, exit cleanly so the wait-group
		// completes.
		return
	}
	b.log.Info("rebalance: started",
		"mode", b.cfg.Mode,
		"max_concurrent", b.cfg.MaxConcurrentTransfers,
		"min_seconds_between", b.cfg.MinSecondsBetweenTransfers,
		"skew_engage_pct", b.cfg.SkewEngagePct,
		"skew_disengage_pct", b.cfg.SkewDisengagePct)
	defer b.log.Info("rebalance: stopped")
	t := time.NewTicker(b.cfg.PollInterval)
	defer t.Stop()
	// One immediate tick on start so a fresh leader doesn't wait the
	// full backstop interval to observe state.
	b.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.tick(ctx)
		case <-b.drainCh:
			// Drain table changed → re-evaluate without waiting for
			// the backstop.
			b.tick(ctx)
		}
	}
}

// tick gathers a snapshot, runs the advisor, emits metrics, and (in
// auto mode) proposes the selected moves. Each propose is a separate
// SyncPropose; errors are logged and tick returns — the next tick
// re-observes the now-pending transfers.
func (b *Balancer) tick(ctx context.Context) {
	state, ok := b.snapshot(ctx)
	if !ok {
		return
	}
	state.PreviouslyEngaged = b.engaged
	dec := Advise(state)
	b.engaged = dec.Engaged
	b.emitDecision(dec)

	if dec.Mode == ModeAdvisory {
		// Log + count "would_transfer" but never propose. Counter
		// reason left empty to keep the cardinality small.
		for _, mv := range dec.Proposed {
			b.log.Info("rebalance: would_transfer",
				"lp", mv.LP, "from", mv.FromShard, "to", mv.ToShard,
				"skew_pct", dec.SkewPct, "in_flight", dec.InFlight)
			b.incDecision("would_transfer", "")
		}
		return
	}

	// Auto mode: propose each move via Command_InitiateLPTransfer.
	for _, mv := range dec.Proposed {
		if ctx.Err() != nil {
			return
		}
		if err := b.proposeMove(ctx, mv); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			b.log.Warn("rebalance: propose InitiateLPTransfer failed",
				"lp", mv.LP, "from", mv.FromShard, "to", mv.ToShard, "err", err)
			b.incDecision("skipped", "propose_failed")
			return
		}
		b.log.Info("rebalance: transferred",
			"lp", mv.LP, "from", mv.FromShard, "to", mv.ToShard,
			"skew_pct", dec.SkewPct)
		b.incDecision("transferred", "")
	}
}

func (b *Balancer) snapshot(ctx context.Context) (State, bool) {
	readCtx, cancel := context.WithTimeout(ctx, b.cfg.SyncReadTimeout)
	defer cancel()

	pt, err := b.host.PartitionTable(readCtx)
	if err != nil {
		b.log.Debug("rebalance: read partition table failed", "err", err)
		return State{}, false
	}
	if pt == nil {
		// Pre-bootstrap; nothing to balance against. Don't log loudly.
		return State{}, false
	}
	active := make([]uint64, 0, len(pt.GetShards()))
	for id := range pt.GetShards() {
		active = append(active, id)
	}
	slices.Sort(active)

	owners, err := b.host.LPOwners(readCtx)
	if err != nil {
		b.log.Debug("rebalance: read lp owners failed", "err", err)
		return State{}, false
	}
	current := make(map[uint32]uint64, len(owners.Records))
	for _, rec := range owners.Records {
		current[rec.GetLp()] = rec.GetShardId()
	}

	drains, err := b.host.RebalanceDrains(readCtx)
	if err != nil {
		b.log.Debug("rebalance: read drains failed", "err", err)
		return State{}, false
	}
	drained := make([]uint64, 0, len(drains.Records))
	for _, rec := range drains.Records {
		drained = append(drained, rec.GetShardId())
	}

	transfers, err := b.host.LPTransfers(readCtx)
	if err != nil {
		b.log.Debug("rebalance: read lp transfers failed", "err", err)
		return State{}, false
	}
	inFlight := 0
	var mostRecent uint64
	for _, rec := range transfers.Records {
		switch rec.GetPhase() {
		case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED,
			enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTED:
			// terminal — not in flight, but its start time still
			// counts toward the cooldown.
		default:
			inFlight++
		}
		if rec.GetStartedAtMs() > mostRecent {
			mostRecent = rec.GetStartedAtMs()
		}
	}

	return State{
		Mode:              b.cfg.Mode,
		ActiveShards:      active,
		DrainedShards:     drained,
		CurrentOwners:     current,
		InFlight:          inFlight,
		MostRecentStartMs: mostRecent,
		NowMs:             uint64(time.Now().UnixMilli()),
		MaxConcurrent:     b.cfg.MaxConcurrentTransfers,
		MinSecondsBetween: b.cfg.MinSecondsBetweenTransfers,
		SkewEngagePct:     b.cfg.SkewEngagePct,
		SkewDisengagePct:  b.cfg.SkewDisengagePct,
	}, true
}

func (b *Balancer) proposeMove(ctx context.Context, mv Move) error {
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_InitiateLpTransfer{
			InitiateLpTransfer: &enginev1.InitiateLPTransfer{
				TransferId: uuid.NewString(),
				Lp:         mv.LP,
				DestShard:  mv.ToShard,
			},
		},
	}
	return b.prop.ProposeSelf(ctx, cmd)
}

// emitModeGauge stamps the mode gauge once on Run start.
func (b *Balancer) emitModeGauge() {
	if b.metrics == nil {
		return
	}
	var v float64
	switch b.cfg.Mode {
	case ModeAdvisory:
		v = 1
	case ModeAuto:
		v = 2
	default:
		v = 0
	}
	b.metrics.RebalanceMode.Set(v)
}

func (b *Balancer) emitDecision(dec Decision) {
	if b.metrics == nil {
		return
	}
	b.metrics.RebalanceSkewPct.Set(dec.SkewPct)
	b.metrics.RebalancePendingTransfers.Set(float64(dec.InFlight))
	b.metrics.RebalanceDrainedShards.Set(float64(len(dec.DrainedShards)))
	if dec.Engaged {
		b.metrics.RebalanceEngaged.Set(1)
	} else {
		b.metrics.RebalanceEngaged.Set(0)
	}
	// Reset the per-shard gauge to current snapshot. Counter sweep over
	// the LPsPerShard map; shards that left the cluster get cleared
	// implicitly because we Set on every shard we see.
	b.metrics.RebalanceLPsPerShard.Reset()
	for shard, n := range dec.LPsPerShard {
		b.metrics.RebalanceLPsPerShard.WithLabelValues(strconv.FormatUint(shard, 10)).Set(float64(n))
	}
	if dec.SkippedReason != "" {
		b.incDecision("skipped", dec.SkippedReason)
	}
}

func (b *Balancer) incDecision(outcome, reason string) {
	if b.metrics == nil {
		return
	}
	b.metrics.RebalanceDecisions.WithLabelValues(outcome, reason).Inc()
}
