package routing

import (
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

func TestRouting_ShardForKeyOneIndexed(t *testing.T) {
	p := Partitioner{NumShards: 3}
	// Hand-crafted partition keys that hit each shard.
	for _, pk := range []uint64{0, 1, 2, 3, 1<<63 + 7} {
		got := p.ShardForKey(pk)
		if got < 1 || got > 3 {
			t.Fatalf("ShardForKey(%d) = %d; want in [1,3]", pk, got)
		}
	}
}

func TestRouting_ShardForKeyFallback(t *testing.T) {
	p := Partitioner{NumShards: 0}
	if got := p.ShardForKey(42); got != 1 {
		t.Fatalf("zero-shard Partitioner = %d; want fallback 1", got)
	}
}

func TestRouting_FromPartitionTable(t *testing.T) {
	pt := &enginev1.PartitionTable{
		Shards: map[uint64]*enginev1.ReplicaSet{
			1: {NodeIds: []uint64{1, 2, 3}},
			2: {NodeIds: []uint64{1, 2, 3}},
			3: {NodeIds: []uint64{1, 2, 3}},
		},
	}
	p := FromPartitionTable(pt)
	if p.NumShards != 3 {
		t.Fatalf("NumShards = %d; want 3", p.NumShards)
	}
	if got := FromPartitionTable(nil).NumShards; got != 0 {
		t.Fatalf("FromPartitionTable(nil).NumShards = %d; want 0", got)
	}
}

func TestRouting_ShardForTargetMatchesShardForInvocation(t *testing.T) {
	p := Partitioner{NumShards: 7}
	target := &enginev1.InvocationTarget{ServiceName: "S", ObjectKey: "k"}
	id := &enginev1.InvocationId{PartitionKey: PartitionKey("S", "k")}
	if p.ShardForTarget(target) != p.ShardForInvocation(id) {
		t.Fatalf("ShardForTarget != ShardForInvocation for same key tuple")
	}
}

// TestRouting_SnapshotOverridesModulo confirms the LPOwners snapshot
// wins over the modulo fallback. Crafted so the per-LP override sends
// PartitionKey(svc,obj) to a shard that modulo would never pick.
func TestRouting_SnapshotOverridesModulo(t *testing.T) {
	p := NewPartitioner(3)
	pk := PartitionKey("svc", "obj-1")
	lp := keys.LPFromPartitionKey(pk)
	moduloShard := (pk % 3) + 1
	var override uint64 = 999
	if override == moduloShard {
		t.Fatalf("test setup: override %d collides with modulo answer", override)
	}
	p.SetLPOwnersSnapshot(map[uint32]uint64{lp: override})
	if got := p.ShardForKey(pk); got != override {
		t.Fatalf("ShardForKey = %d; want override %d", got, override)
	}
}

// TestRouting_SnapshotMissFallsBackToModulo covers the snapshot-miss
// path: when an LP is absent from the snapshot, ShardForKey falls
// through to the lp-modulo fallback rather than returning a nonsense
// value like 0 (which would route to the metadata shard). The fallback
// formula matches the bootstrap seed exactly.
func TestRouting_SnapshotMissFallsBackToModulo(t *testing.T) {
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
	want := (uint64(lp) % 4) + 1
	if got := p.ShardForKey(pk); got != want {
		t.Fatalf("ShardForKey on snapshot miss = %d; want lp-modulo %d", got, want)
	}
}

// TestRouting_NilSnapshotFallsBackToModulo covers the pre-warmup window
// before the reconciler has installed any snapshot. NewPartitioner sets
// up the atomic-pointer slot but leaves it nil until the first
// SetLPOwnersSnapshot call.
func TestRouting_NilSnapshotFallsBackToModulo(t *testing.T) {
	p := NewPartitioner(5)
	pk := PartitionKey("svc", "obj-3")
	lp := keys.LPFromPartitionKey(pk)
	want := (uint64(lp) % 5) + 1
	if got := p.ShardForKey(pk); got != want {
		t.Fatalf("ShardForKey with no snapshot = %d; want lp-modulo %d", got, want)
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

// TestRouting_IdentitySeedMatchesFallback is the warm-up-window
// invariant: post-seed routing (via snapshot lookup) returns the same
// shard as pre-seed routing (via lp-modulo fallback). This is what
// keeps the warm-up window from routing pks to different shards than
// the steady-state table will.
func TestRouting_IdentitySeedMatchesFallback(t *testing.T) {
	const numShards uint64 = 5
	seed := make(map[uint32]uint64, keys.LPCount)
	for lp := range keys.LPCount {
		seed[lp] = (uint64(lp) % numShards) + 1
	}
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
