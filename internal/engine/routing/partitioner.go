// Package routing computes the destination partition shard for a given
// target. Phase 4.1 uses a static configuration (number of shards comes
// from the PartitionTable persisted on shard 0); Phase 4.2 will introduce
// sparse placement and consult a richer ShardView.
//
// The shard ids returned here are 1-indexed: shard 0 is reserved for the
// metadata Raft group (see internal/engine/cluster). When the cluster
// hosts N partition shards their ids are 1..N.
package routing

import (
	"hash/fnv"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Partitioner maps a logical key tuple to a partition shard id.
//
// NumShards is the partition count (RF agnostic; routing is by shard id,
// not by replica). It MUST be > 0 — callers are expected to read it from
// the persisted PartitionTable size.
type Partitioner struct {
	NumShards uint64
}

// PartitionKey returns the canonical 64-bit partition key for a (service,
// object_key) tuple. The tuple is hashed with FNV-1a so the result is
// platform-independent and identical across nodes. Empty object_key (used
// for unkeyed services) hashes consistently — every invocation of the same
// unkeyed service routes to the same shard, which is the expected
// per-service single-partition behavior in Phase 4.1.
func PartitionKey(service, objectKey string) uint64 {
	h := fnv.New64a()
	// Length-prefix each component so adjacent fields cannot collide
	// (e.g. ("ab","c") vs ("a","bc")). We use a single 0x00 separator
	// because service / object_key are user-facing identifiers that
	// MUST NOT contain NUL — the SDK rejects them at the API surface.
	h.Write([]byte(service))
	h.Write([]byte{0})
	h.Write([]byte(objectKey))
	return h.Sum64()
}

// ShardForKey maps an InvocationId.PartitionKey to its owning shard id.
// Returns 1 when NumShards is 0 — defensive only; tests guard against it.
func (p Partitioner) ShardForKey(partitionKey uint64) uint64 {
	if p.NumShards == 0 {
		return 1
	}
	return (partitionKey % p.NumShards) + 1
}

// ShardForTarget is a convenience for callers that have an
// InvocationTarget rather than a raw partition key.
func (p Partitioner) ShardForTarget(t *enginev1.InvocationTarget) uint64 {
	return p.ShardForKey(PartitionKey(t.GetServiceName(), t.GetObjectKey()))
}

// ShardForInvocation extracts the partition key already stamped on the
// InvocationId and maps it through ShardForKey. Used by the outbox when
// it has an InvokeCommand or DeliverCallResult variant in hand.
func (p Partitioner) ShardForInvocation(id *enginev1.InvocationId) uint64 {
	return p.ShardForKey(id.GetPartitionKey())
}

// FromPartitionTable constructs a Partitioner whose NumShards matches the
// size of the persisted PartitionTable. Returns a zero Partitioner (which
// ShardForKey treats as "fall back to shard 1") when pt is nil or empty,
// which keeps the same single-partition behavior the system used before
// Phase 4.1.
func FromPartitionTable(pt *enginev1.PartitionTable) Partitioner {
	if pt == nil {
		return Partitioner{}
	}
	return Partitioner{NumShards: uint64(len(pt.GetShards()))}
}
