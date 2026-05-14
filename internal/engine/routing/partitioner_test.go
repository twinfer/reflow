package routing

import (
	"testing"

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
