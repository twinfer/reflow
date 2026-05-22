// Package rebalance implements the autonomous LP rebalancer. It is a
// leader-only control loop on the metadata shard (shard 0): observes
// the current LPOwnersTable vs the planner's desired distribution,
// decides which transfers would close the skew (subject to hysteresis +
// rate limits), and — in auto mode — proposes Command_InitiateLPTransfer
// through the same path the manual `reflowd cluster transfer-lp` CLI
// uses.
//
// Triggers in this version are limited to membership change and
// operator-requested drain; load- and capacity-based triggers are
// out of scope for PR 5.0.
package rebalance

import (
	"github.com/twinfer/reflow/internal/engine/routing"
)

// Mode constants match pkg/reflow.RebalanceConfig.Mode values. Stringly
// typed so the proto + config + Decision layers share one vocabulary.
const (
	ModeOff      = "off"
	ModeAdvisory = "advisory"
	ModeAuto     = "auto"
)

// State is the input to Advise — one tick's snapshot of cluster state
// plus the knob values, packaged for pure-function evaluation. Composed
// by the Balancer from SyncReads against shard 0.
type State struct {
	// Mode is the current rebalancer mode (advisory or auto; the
	// balancer never invokes Advise when Mode=off).
	Mode string
	// ActiveShards lists the partition shard ids in the current
	// PartitionTable (regardless of drain status). Drained shards are
	// still active in the dragonboat sense — drain only excludes them
	// from the planner's input set.
	ActiveShards []uint64
	// DrainedShards lists shard ids the operator has marked drained.
	DrainedShards []uint64
	// CurrentOwners is the current LPOwnersTable snapshot: lp → shard_id.
	CurrentOwners map[uint32]uint64
	// InFlight is the count of non-terminal rows in LPTransferTable on
	// this tick.
	InFlight int
	// MostRecentStartMs is the max started_at_ms across every
	// LPTransferTable row, terminal or not. Drives the cooldown gate.
	// Zero when the table is empty.
	MostRecentStartMs uint64
	// NowMs is the wall clock used for the cooldown comparison.
	NowMs uint64
	// PreviouslyEngaged is the balancer's engaged-bit from the prior
	// tick; carried forward so hysteresis can disengage at a different
	// threshold than it engaged.
	PreviouslyEngaged bool
	// MaxConcurrent / MinSecondsBetween / SkewEngagePct /
	// SkewDisengagePct mirror the RebalanceConfig knobs (already
	// defaulted to non-zero by withDefaults).
	MaxConcurrent     uint32
	MinSecondsBetween uint32
	SkewEngagePct     uint32
	SkewDisengagePct  uint32
}

// Move is one LP→shard transfer the advisor recommends or actuates.
type Move struct {
	LP        uint32
	FromShard uint64
	ToShard   uint64
}

// Decision is Advise's output. The Balancer consumes it to emit metrics
// and (in auto mode) propose Command_InitiateLPTransfer for each
// element of Proposed. SkippedReason is non-empty when Proposed is
// empty and explains which gate fired — surfaced on the
// reflow_rebalance_decisions_total counter and the RebalanceAdvise RPC.
type Decision struct {
	Mode          string
	Engaged       bool
	SkewPct       float64
	LPsPerShard   map[uint64]int
	DrainedShards []uint64
	InFlight      int
	SkippedReason string
	Proposed      []Move
}

// Advise evaluates one tick's state and returns the decision. Pure
// function — no I/O, no clock reads beyond what's in State. Designed
// so the Balancer and the RebalanceAdvise RPC share one code path.
//
// The pipeline:
//
//  1. Live shard set = active − drained. Empty → no work.
//  2. desired = routing.NewPlanner(live).PlanAll().
//  3. moves = routing.Diff(current, desired) (already LP-ascending).
//  4. Skew% = len(moves) / len(desired) * 100.
//  5. Hysteresis: engaged iff (was engaged AND skew > disengage) OR
//     (was not engaged AND skew ≥ engage). The two thresholds give the
//     hysteresis band — engage at engagePct, stay engaged down to
//     disengagePct, disengage below.
//  6. Cooldown: skip if NowMs − MostRecentStartMs < MinSecondsBetween×1000.
//  7. Capacity: cap proposals at MaxConcurrent − InFlight.
//  8. Filter moves: drop FromShard=0 (unseeded LPs — not the
//     rebalancer's job; the bootstrap seed handles those).
//  9. Take the first N moves in LP-ascending order. Determinism matters
//     so two leaders racing across a step-down produce the same intent.
func Advise(s State) Decision {
	dec := Decision{
		Mode:          s.Mode,
		LPsPerShard:   countLPsPerShard(s.CurrentOwners),
		DrainedShards: append([]uint64(nil), s.DrainedShards...),
		InFlight:      s.InFlight,
	}

	live := subtractDrained(s.ActiveShards, s.DrainedShards)
	if len(live) == 0 {
		dec.SkippedReason = "no_live_shards"
		return dec
	}
	planner := routing.NewPlanner(live)
	if planner == nil {
		dec.SkippedReason = "no_planner"
		return dec
	}
	desired := planner.PlanAll()
	moves := routing.Diff(s.CurrentOwners, desired)

	// Filter moves where FromShard==0. Those are LPs the planner wants
	// to assign but no current owner exists — only happens before the
	// bootstrap seed runs. The InitiateLPTransfer apply arm rejects
	// FromShard==0 anyway; filtering here keeps the metric honest.
	filtered := moves[:0]
	for _, m := range moves {
		if m.FromShard == 0 {
			continue
		}
		filtered = append(filtered, m)
	}
	moves = filtered

	if len(desired) > 0 {
		dec.SkewPct = float64(len(moves)) / float64(len(desired)) * 100.0
	}

	// Hysteresis: stay engaged as long as skew > disengage; only
	// engage from below when skew ≥ engage.
	switch {
	case s.PreviouslyEngaged && dec.SkewPct > float64(s.SkewDisengagePct):
		dec.Engaged = true
	case !s.PreviouslyEngaged && dec.SkewPct >= float64(s.SkewEngagePct):
		dec.Engaged = true
	default:
		dec.Engaged = false
	}

	if !dec.Engaged {
		dec.SkippedReason = "skew_below_engage"
		return dec
	}

	// Cooldown gate.
	if s.MinSecondsBetween > 0 && s.MostRecentStartMs > 0 {
		elapsedMs := int64(s.NowMs) - int64(s.MostRecentStartMs)
		if elapsedMs < int64(s.MinSecondsBetween)*1000 {
			dec.SkippedReason = "cooldown"
			return dec
		}
	}

	// Capacity gate.
	maxConc := max(int(s.MaxConcurrent), 1)
	capacity := maxConc - s.InFlight
	if capacity <= 0 {
		dec.SkippedReason = "at_capacity"
		return dec
	}
	if len(moves) == 0 {
		dec.SkippedReason = "no_moves"
		return dec
	}
	if capacity > len(moves) {
		capacity = len(moves)
	}

	dec.Proposed = make([]Move, 0, capacity)
	for i := 0; i < capacity; i++ {
		m := moves[i]
		dec.Proposed = append(dec.Proposed, Move{
			LP:        m.LP,
			FromShard: m.FromShard,
			ToShard:   m.ToShard,
		})
	}
	return dec
}

// subtractDrained returns active without the shard ids in drained.
// Preserves input order; allocates a fresh slice so the planner's
// internal sort doesn't mutate the caller's view.
func subtractDrained(active, drained []uint64) []uint64 {
	if len(drained) == 0 {
		return append([]uint64(nil), active...)
	}
	set := make(map[uint64]struct{}, len(drained))
	for _, s := range drained {
		set[s] = struct{}{}
	}
	out := make([]uint64, 0, len(active))
	for _, s := range active {
		if _, dropped := set[s]; dropped {
			continue
		}
		out = append(out, s)
	}
	return out
}

// countLPsPerShard tallies the lp → shard map by shard. Empty map
// yields an empty (non-nil) result so callers can iterate it.
func countLPsPerShard(owners map[uint32]uint64) map[uint64]int {
	out := make(map[uint64]int, len(owners))
	for _, shard := range owners {
		out[shard]++
	}
	return out
}
