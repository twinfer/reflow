package rebalance

import (
	"testing"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
)

// plannedOwners returns the consistent-hash plan for shards as a
// current-owners map. Using the planner's own output gives a baseline
// where skew_pct=0 against the same input — the "balanced cluster"
// shape used by the skip-on-no-work tests.
func plannedOwners(shards []uint64) map[uint32]uint64 {
	return routing.NewPlanner(shards).PlanAll()
}

func TestAdvise_SkewBelowEngage_Skips(t *testing.T) {
	// 3 shards owning the planner's exact desired split → skew 0%.
	owners := plannedOwners([]uint64{1, 2, 3})
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3},
		CurrentOwners:     owners,
		MaxConcurrent:     1,
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
	})
	if dec.Engaged {
		t.Fatalf("engaged=true on a balanced cluster; want false")
	}
	if dec.SkippedReason != "skew_below_engage" {
		t.Fatalf("reason=%q; want skew_below_engage", dec.SkippedReason)
	}
	if len(dec.Proposed) != 0 {
		t.Fatalf("proposed=%d moves; want 0", len(dec.Proposed))
	}
}

func TestAdvise_SkewAboveEngage_ProposesUpToCapacity(t *testing.T) {
	// 3 shards owning everything; planner desires 4 shards.
	owners := plannedOwners([]uint64{1, 2, 3})
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3, 4},
		CurrentOwners:     owners,
		MaxConcurrent:     5,
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
	})
	if !dec.Engaged {
		t.Fatalf("engaged=false; want true (membership change should push skew above engage)")
	}
	if dec.SkewPct < 15 {
		t.Fatalf("skew_pct=%.2f; want ≥ 15", dec.SkewPct)
	}
	if len(dec.Proposed) != 5 {
		t.Fatalf("proposed=%d; want 5 (capacity cap)", len(dec.Proposed))
	}
	// All proposed moves should target shard 4 (the newly added shard
	// will be the destination for most rebalancing under bounded-load
	// consistent hashing).
	for _, m := range dec.Proposed {
		if m.FromShard == 0 {
			t.Fatalf("move %v has FromShard=0; should have been filtered", m)
		}
	}
	// LP-ascending order.
	for i := 1; i < len(dec.Proposed); i++ {
		if dec.Proposed[i-1].LP >= dec.Proposed[i].LP {
			t.Fatalf("proposed not in LP-ascending order: %d before %d",
				dec.Proposed[i-1].LP, dec.Proposed[i].LP)
		}
	}
}

func TestAdvise_Cooldown_Skips(t *testing.T) {
	owners := plannedOwners([]uint64{1, 2, 3})
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3, 4},
		CurrentOwners:     owners,
		MaxConcurrent:     1,
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
		MostRecentStartMs: 1_700_000_000_000,
		NowMs:             1_700_000_000_000 + 30_000, // 30s into 60s cooldown
	})
	if !dec.Engaged {
		t.Fatalf("engaged=false; want true even under cooldown")
	}
	if dec.SkippedReason != "cooldown" {
		t.Fatalf("reason=%q; want cooldown", dec.SkippedReason)
	}
	if len(dec.Proposed) != 0 {
		t.Fatalf("proposed=%d; want 0", len(dec.Proposed))
	}
}

func TestAdvise_Cooldown_Elapsed_Proposes(t *testing.T) {
	owners := plannedOwners([]uint64{1, 2, 3})
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3, 4},
		CurrentOwners:     owners,
		MaxConcurrent:     1,
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
		MostRecentStartMs: 1_700_000_000_000,
		NowMs:             1_700_000_000_000 + 61_000, // 1s past cooldown
	})
	if dec.SkippedReason == "cooldown" {
		t.Fatalf("still on cooldown after elapsed time")
	}
	if len(dec.Proposed) != 1 {
		t.Fatalf("proposed=%d; want 1", len(dec.Proposed))
	}
}

func TestAdvise_AtCapacity_Skips(t *testing.T) {
	owners := plannedOwners([]uint64{1, 2, 3})
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3, 4},
		CurrentOwners:     owners,
		MaxConcurrent:     1,
		InFlight:          1, // already at cap
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
	})
	if dec.SkippedReason != "at_capacity" {
		t.Fatalf("reason=%q; want at_capacity", dec.SkippedReason)
	}
	if len(dec.Proposed) != 0 {
		t.Fatalf("proposed=%d; want 0", len(dec.Proposed))
	}
}

func TestAdvise_Hysteresis_StayEngagedAboveDisengage(t *testing.T) {
	// Start from the converged 4-shard plan, then re-spoil ~10% of
	// LPs by re-assigning them to shard 1 to land inside the
	// hysteresis band (disengage=8, engage=15). The spoil count is a
	// fraction of LPCount so the resulting skew tracks the band
	// regardless of the LP-space size.
	owners := plannedOwners([]uint64{1, 2, 3, 4})
	spoiled := 0
	spoilTarget := int(keys.LPCount) / 10
	for lp := uint32(0); lp < keys.LPCount && spoiled < spoilTarget; lp++ {
		if owners[lp] != 1 {
			owners[lp] = 1
			spoiled++
		}
	}
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3, 4},
		CurrentOwners:     owners,
		MaxConcurrent:     1,
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
		PreviouslyEngaged: true,
	})
	if dec.SkewPct >= 15 || dec.SkewPct <= 8 {
		t.Fatalf("skew_pct=%.2f outside hysteresis band (8..15); test setup off", dec.SkewPct)
	}
	if !dec.Engaged {
		t.Fatalf("engaged=false under hysteresis (prior=true, skew=%.2f); want true", dec.SkewPct)
	}
}

func TestAdvise_Hysteresis_DisengageBelowDisengage(t *testing.T) {
	// Cluster has converged exactly; skew_pct = 0 < disengage (8).
	// Even though PreviouslyEngaged=true, we should now disengage.
	owners := plannedOwners([]uint64{1, 2, 3, 4})
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3, 4},
		CurrentOwners:     owners,
		MaxConcurrent:     1,
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
		PreviouslyEngaged: true,
	})
	if dec.Engaged {
		t.Fatalf("engaged=true after convergence (skew=%.2f); want false", dec.SkewPct)
	}
}

func TestAdvise_DrainedShard_ExcludedFromPlanner(t *testing.T) {
	// 4 shards, drain shard 4 → planner restricts to {1,2,3}; all
	// LPs currently on shard 4 should be relocated.
	owners := plannedOwners([]uint64{1, 2, 3, 4})
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3, 4},
		DrainedShards:     []uint64{4},
		CurrentOwners:     owners,
		MaxConcurrent:     16,
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
	})
	if !dec.Engaged {
		t.Fatalf("engaged=false on drain; want true")
	}
	for _, m := range dec.Proposed {
		if m.ToShard == 4 {
			t.Fatalf("proposed move to drained shard 4: %v", m)
		}
	}
}

func TestAdvise_NoLiveShards(t *testing.T) {
	owners := plannedOwners([]uint64{1, 2, 3})
	dec := Advise(State{
		Mode:          ModeAuto,
		ActiveShards:  []uint64{1, 2, 3},
		DrainedShards: []uint64{1, 2, 3},
		CurrentOwners: owners,
		MaxConcurrent: 1,
	})
	if dec.SkippedReason != "no_live_shards" {
		t.Fatalf("reason=%q; want no_live_shards", dec.SkippedReason)
	}
	if len(dec.Proposed) != 0 {
		t.Fatalf("proposed=%d; want 0", len(dec.Proposed))
	}
}

func TestAdvise_FiltersUnseededMoves(t *testing.T) {
	// Owners missing LPs 0..9 (current=0); planner wants shards. Diff
	// would emit From=0 moves; the advisor must filter those out
	// because Command_InitiateLPTransfer rejects From=0.
	owners := plannedOwners([]uint64{1, 2, 3})
	for lp := range uint32(10) {
		delete(owners, lp)
	}
	dec := Advise(State{
		Mode:              ModeAuto,
		ActiveShards:      []uint64{1, 2, 3, 4}, // induces real moves too
		CurrentOwners:     owners,
		MaxConcurrent:     100,
		MinSecondsBetween: 60,
		SkewEngagePct:     15,
		SkewDisengagePct:  8,
	})
	for _, m := range dec.Proposed {
		if m.FromShard == 0 {
			t.Fatalf("From=0 move leaked through: %v", m)
		}
	}
}
