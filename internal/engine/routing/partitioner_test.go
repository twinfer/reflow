package routing

import (
	"fmt"
	"testing"

	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func TestRouting_PartitionKeyDeterministic(t *testing.T) {
	a := PartitionKey("svc", "key-1")
	b := PartitionKey("svc", "key-1")
	if a != b {
		t.Fatalf("PartitionKey not deterministic: %d vs %d", a, b)
	}
}

func TestRouting_PartitionKeyDisambiguates(t *testing.T) {
	// ("ab","c") and ("a","bc") must NOT collide thanks to the 0x00
	// separator. If callers ever pass a service with a NUL byte we'd
	// regress here — the SDK API surface is responsible for rejecting
	// such ids before they reach routing.
	a := PartitionKey("ab", "c")
	b := PartitionKey("a", "bc")
	if a == b {
		t.Fatalf("PartitionKey collided on adjacent components: %d", a)
	}
}

func TestRouting_PartitionKeySpread(t *testing.T) {
	// Distinct keys spread across LPs (the FNV hash fills the LP region).
	seen := map[uint32]bool{}
	for i := range 64 {
		seen[keys.LPFromPartitionKey(PartitionKey("cart", fmt.Sprintf("k%d", i)))] = true
	}
	if len(seen) < 2 {
		t.Errorf("keys collapsed to %d LP(s); expected spread", len(seen))
	}
}

func TestRouting_ShardForKeyOneIndexed(t *testing.T) {
	p := NewPartitioner(3)
	// Hand-crafted partition keys that hit each shard.
	for _, pk := range []uint64{0, 1, 2, 3, 1<<63 + 7} {
		got := p.ShardForKey(pk)
		if got < 1 || got > 3 {
			t.Fatalf("ShardForKey(%d) = %d; want in [1,3]", pk, got)
		}
	}
}

// TestRouting_ZeroPartitionerShardOne covers the zero-value escape:
// no planner, no snapshot → defensive fallback to shard 1 so the
// single-partition single-node deployment shape works without any
// explicit construction.
func TestRouting_ZeroPartitionerShardOne(t *testing.T) {
	var p Partitioner
	if got := p.ShardForKey(42); got != 1 {
		t.Fatalf("zero Partitioner ShardForKey = %d; want 1", got)
	}
}

func TestRouting_ShardForTargetMatchesShardForInvocation(t *testing.T) {
	p := NewPartitioner(7)
	target := &enginev1.InvocationTarget{ServiceName: "S", ObjectKey: "k"}
	id := &enginev1.InvocationId{PartitionKey: PartitionKey("S", "k")}
	if p.ShardForTarget(target) != p.ShardForInvocation(id) {
		t.Fatalf("ShardForTarget != ShardForInvocation for same key tuple")
	}
}

// TestRouting_SnapshotOverridesPlanner confirms the LPOwners snapshot
// wins over the planner fallback. The override (999) lies outside the
// planner's value range [1, NumShards], so the test cannot pass via the
// fallback path.
func TestRouting_SnapshotOverridesPlanner(t *testing.T) {
	p := NewPartitioner(3)
	pk := PartitionKey("svc", "obj-1")
	lp := keys.LPFromPartitionKey(pk)
	const override uint64 = 999 // outside [1, 3]
	p.SetLPOwnersSnapshot(map[uint32]uint64{lp: override})
	if got := p.ShardForKey(pk); got != override {
		t.Fatalf("ShardForKey = %d; want override %d", got, override)
	}
}

// TestRouting_SnapshotMissFallsBackToPlanner covers the snapshot-miss
// path: when an LP is absent from the snapshot, ShardForKey falls
// through to the consistent-hash planner rather than returning a
// nonsense value like 0 (which would route to the metadata shard). The
// planner answer matches the bootstrap seed exactly — same ring built
// from the same shard ids.
func TestRouting_SnapshotMissFallsBackToPlanner(t *testing.T) {
	p := NewPartitioner(4)
	pk := PartitionKey("svc", "obj-2")
	lp := keys.LPFromPartitionKey(pk)
	// Snapshot contains every LP except this one.
	snap := map[uint32]uint64{}
	for x := range keys.LPCount {
		if x == lp {
			continue
		}
		snap[x] = 1
	}
	p.SetLPOwnersSnapshot(snap)
	want := NewPlanner([]uint64{1, 2, 3, 4}).ShardForLP(lp)
	if got := p.ShardForKey(pk); got != want {
		t.Fatalf("ShardForKey on snapshot miss = %d; want planner %d", got, want)
	}
}

// TestRouting_NilSnapshotFallsBackToPlanner covers the pre-warmup window
// before the reconciler has installed any snapshot. NewPartitioner sets
// up the atomic-pointer slot but leaves it nil until the first
// SetLPOwnersSnapshot call.
func TestRouting_NilSnapshotFallsBackToPlanner(t *testing.T) {
	p := NewPartitioner(5)
	pk := PartitionKey("svc", "obj-3")
	lp := keys.LPFromPartitionKey(pk)
	want := NewPlanner([]uint64{1, 2, 3, 4, 5}).ShardForLP(lp)
	if got := p.ShardForKey(pk); got != want {
		t.Fatalf("ShardForKey with no snapshot = %d; want planner %d", got, want)
	}
}

// TestRouting_SnapshotSwapVisibleAcrossCopies confirms that two value
// copies of the same singleton observe each other's snapshot swaps —
// the property the host.Partitioner() accessor relies on.
func TestRouting_SnapshotSwapVisibleAcrossCopies(t *testing.T) {
	p := NewPartitioner(2)
	a := *p
	b := *p
	pk := PartitionKey("svc", "obj-shared")
	lp := keys.LPFromPartitionKey(pk)
	p.SetLPOwnersSnapshot(map[uint32]uint64{lp: 42})
	if got := a.ShardForKey(pk); got != 42 {
		t.Fatalf("copy a.ShardForKey = %d; want 42 (post-swap)", got)
	}
	if got := b.ShardForKey(pk); got != 42 {
		t.Fatalf("copy b.ShardForKey = %d; want 42 (post-swap)", got)
	}
}

// TestRouting_PlannerSeedMatchesFallback is the warm-up-window
// invariant: post-seed routing (via snapshot lookup) returns the same
// shard as pre-seed routing (via planner fallback). This is what keeps
// the warm-up window from routing pks to different shards than the
// steady-state table will.
//
// Both paths must use NewPlanner with the same shard ids — that's the
// guarantee. The Partitioner.planner field is built from 1..NumShards;
// seedLPOwners (internal/engine/metadata_runner.go) calls NewPlanner
// with the PartitionTable.Shards keys, which the static bootstrap also
// emits as 1..len(peers). They line up.
func TestRouting_PlannerSeedMatchesFallback(t *testing.T) {
	const numShards uint64 = 5
	shardIDs := []uint64{1, 2, 3, 4, 5}
	seed := NewPlanner(shardIDs).PlanAll()
	seeded := NewPartitioner(numShards)
	seeded.SetLPOwnersSnapshot(seed)
	unseeded := NewPartitioner(numShards) // nil snapshot → fallback path

	for _, pk := range []uint64{
		0, 1, 2, 3, 1<<31 + 7, 1<<63 + 1, 0xffff_ffff_ffff_ffff,
		PartitionKey("svc", "alpha"),
		PartitionKey("svc", "beta"),
		PartitionKey("Other", ""),
	} {
		if s, u := seeded.ShardForKey(pk), unseeded.ShardForKey(pk); s != u {
			t.Errorf("ShardForKey(0x%x): seeded=%d unseeded=%d; warm-up routing must match steady-state", pk, s, u)
		}
	}
}
