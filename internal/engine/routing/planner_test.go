package routing

import (
	"math"
	"testing"

	"github.com/twinfer/reflw/internal/storage/keys"
)

// TestPlanner_NilOnEmpty confirms the constructor returns nil for an
// empty shard list. ShardForLP / PlanAll on a nil planner are both
// no-ops; the call sites (Partitioner fallback, seedLPOwners) handle
// nil by falling through.
func TestPlanner_NilOnEmpty(t *testing.T) {
	if p := NewPlanner(nil); p != nil {
		t.Errorf("NewPlanner(nil) = %v; want nil", p)
	}
	if p := NewPlanner([]uint64{}); p != nil {
		t.Errorf("NewPlanner(empty) = %v; want nil", p)
	}
	var nilP *Planner
	if got := nilP.ShardForLP(0); got != 0 {
		t.Errorf("nil.ShardForLP = %d; want 0", got)
	}
	if got := nilP.PlanAll(); got != nil {
		t.Errorf("nil.PlanAll = %v; want nil", got)
	}
}

// TestPlanner_SingleShardOwnsAll covers the degenerate one-shard cluster
// case (RF=1 single-node deployment). Every LP must map to that shard.
func TestPlanner_SingleShardOwnsAll(t *testing.T) {
	p := NewPlanner([]uint64{1})
	plan := p.PlanAll()
	if got := len(plan); got != int(keys.LPCount) {
		t.Fatalf("PlanAll size = %d; want %d", got, keys.LPCount)
	}
	for lp, shard := range plan {
		if shard != 1 {
			t.Fatalf("lp=%d shard=%d; want 1 (single-shard cluster)", lp, shard)
		}
	}
}

// TestPlanner_Determinism guards the cross-process / cross-arch
// invariant: two planners built from identical inputs must produce
// byte-identical mappings, regardless of the order shard ids are passed.
// Reflow relies on this so two metadata leaders racing the bootstrap
// seed write identical content.
func TestPlanner_Determinism(t *testing.T) {
	a := NewPlanner([]uint64{1, 2, 3, 4, 5})
	b := NewPlanner([]uint64{5, 4, 3, 2, 1}) // reversed order
	planA, planB := a.PlanAll(), b.PlanAll()
	if len(planA) != len(planB) {
		t.Fatalf("plan sizes diverge: %d vs %d", len(planA), len(planB))
	}
	for lp, shard := range planA {
		if planB[lp] != shard {
			t.Errorf("lp=%d: a=%d b=%d (order should not affect output)", lp, shard, planB[lp])
		}
	}
}

// TestPlanner_BoundedLoad checks the consistent-hashing-with-bounded-loads
// guarantee: with Load=1.25 and N shards, no shard owns more than
// ⌈avg * 1.25⌉ LPs. This is the bound the rebalancer relies on to keep
// hot-shard skew predictable.
func TestPlanner_BoundedLoad(t *testing.T) {
	for _, n := range []int{2, 3, 5, 8, 16} {
		shardIDs := make([]uint64, n)
		for i := range n {
			shardIDs[i] = uint64(i + 1)
		}
		p := NewPlanner(shardIDs)
		plan := p.PlanAll()
		counts := make(map[uint64]int, n)
		for _, shard := range plan {
			counts[shard]++
		}
		avg := float64(keys.LPCount) / float64(n)
		ceiling := int(math.Ceil(avg * plannerLoad))
		for shard, c := range counts {
			if c > ceiling {
				t.Errorf("n=%d shard=%d count=%d > ceiling=%d (avg=%.1f load=%.2f)",
					n, shard, c, ceiling, avg, plannerLoad)
			}
		}
		if len(counts) != n {
			t.Errorf("n=%d: only %d shards received LPs; want %d (every shard must own at least one)",
				n, len(counts), n)
		}
	}
}

// TestPlanner_BoundedMovesOnGrow is the headline property of consistent
// hashing: adding a new shard re-maps roughly 1/n of the LPs, not all of
// them. We don't need the exact ratio — just that "vastly fewer than
// LPCount" LPs change ownership. Without consistent hashing (e.g.
// lp % N), going from 3 → 4 shards re-maps ~75% of LPs.
func TestPlanner_BoundedMovesOnGrow(t *testing.T) {
	before := NewPlanner([]uint64{1, 2, 3}).PlanAll()
	after := NewPlanner([]uint64{1, 2, 3, 4}).PlanAll()
	moved := 0
	for lp, b := range before {
		if after[lp] != b {
			moved++
		}
	}
	// Theoretical minimum is ~LPCount/4 = 1024 (LPs moving to new shard).
	// Bounded-load can pull a few extra moves to satisfy the ceiling, so
	// we accept up to 2x the minimum. The hard ceiling is "much less
	// than LPCount * 0.75" (what mod-N would force).
	maxAcceptable := int(keys.LPCount) / 2
	if moved > maxAcceptable {
		t.Errorf("3→4 shards moved %d LPs; consistent hashing must keep this well under %d",
			moved, maxAcceptable)
	}
	if moved == 0 {
		t.Errorf("3→4 shards moved 0 LPs; new shard would own nothing (something is wrong)")
	}
}

// TestPlanner_BoundedMovesOnShrink is the dual property: removing a shard
// migrates only that shard's LPs (plus a handful needed to re-balance
// under the bounded-load ceiling), not the whole table. Useful when a
// node is evicted and we collapse a shard's ownership.
func TestPlanner_BoundedMovesOnShrink(t *testing.T) {
	before := NewPlanner([]uint64{1, 2, 3, 4}).PlanAll()
	after := NewPlanner([]uint64{1, 2, 3}).PlanAll()

	// Pre-shrink, shard 4 owns ~LPCount/4 LPs. Those MUST move. LPs not
	// owned by shard 4 should mostly stay put.
	movedFromShard4 := 0
	movedFromOther := 0
	for lp, b := range before {
		if after[lp] == b {
			continue
		}
		if b == 4 {
			movedFromShard4++
		} else {
			movedFromOther++
		}
	}
	if movedFromShard4 == 0 {
		t.Error("shrink: shard-4 LPs should all move but none did")
	}
	// "Other" LPs may move a small amount to satisfy bounded-load on the
	// 3 remaining shards, but the bulk should stay.
	if movedFromOther > movedFromShard4 {
		t.Errorf("shrink: non-shard-4 movement (%d) exceeds shard-4 movement (%d); consistent hashing should localize the disturbance",
			movedFromOther, movedFromShard4)
	}
}

// TestPlanner_DiffIdempotent confirms Diff against the same map returns
// no moves — the steady-state property the rebalancer relies on to know
// "we're done; no further work to schedule".
func TestPlanner_DiffIdempotent(t *testing.T) {
	p := NewPlanner([]uint64{1, 2, 3})
	plan := p.PlanAll()
	if got := Diff(plan, plan); len(got) != 0 {
		t.Errorf("Diff(p, p) = %d moves; want 0", len(got))
	}
}

// TestPlanner_DiffFromEmptyTreatedAsFreshSeed: a nil/empty current
// produces a move-from-shard-0 for every desired entry. This is the
// shape PR 3's transfer protocol can consume to seed an empty cluster
// from scratch.
func TestPlanner_DiffFromEmptyTreatedAsFreshSeed(t *testing.T) {
	desired := map[uint32]uint64{0: 1, 1: 2, 2: 1}
	moves := Diff(nil, desired)
	if len(moves) != 3 {
		t.Fatalf("Diff(nil, 3 entries) = %d moves; want 3", len(moves))
	}
	for _, m := range moves {
		if m.FromShard != 0 {
			t.Errorf("Diff(nil, ...) FromShard = %d; want 0 (fresh-seed sentinel)", m.FromShard)
		}
	}
}

// TestPlanner_DiffOrderedByLP confirms the LP-ascending sort. Callers
// (PR 3's transfer protocol) want stable iteration so log lines and
// proposed commands are reproducible across runs.
func TestPlanner_DiffOrderedByLP(t *testing.T) {
	current := map[uint32]uint64{0: 1, 5: 1, 10: 1, 100: 1}
	desired := map[uint32]uint64{0: 2, 5: 1, 10: 3, 100: 4}
	moves := Diff(current, desired)
	if len(moves) != 3 {
		t.Fatalf("Diff = %d moves; want 3", len(moves))
	}
	for i := 1; i < len(moves); i++ {
		if moves[i-1].LP >= moves[i].LP {
			t.Errorf("Diff not sorted by LP: %d before %d", moves[i-1].LP, moves[i].LP)
		}
	}
}

// TestPlanner_DiffNilDesired returns nil. Defensive: keeps PR 3 callers
// from needing their own nil-check before iterating the result.
func TestPlanner_DiffNilDesired(t *testing.T) {
	if got := Diff(map[uint32]uint64{1: 2}, nil); got != nil {
		t.Errorf("Diff(_, nil) = %v; want nil", got)
	}
}

// TestPlanner_ShardForLPMatchesPlanAll catches drift between the hot-path
// lookup (ShardForLP) and the bulk-seed path (PlanAll). They must be
// pointwise identical — otherwise warm-up routing via ShardForLP would
// disagree with what PlanAll wrote into LPOwnersTable.
func TestPlanner_ShardForLPMatchesPlanAll(t *testing.T) {
	p := NewPlanner([]uint64{1, 2, 3, 4, 5})
	plan := p.PlanAll()
	for lp := range keys.LPCount {
		if got := p.ShardForLP(lp); got != plan[lp] {
			t.Fatalf("lp=%d: ShardForLP=%d PlanAll=%d (must agree)", lp, got, plan[lp])
		}
	}
}
