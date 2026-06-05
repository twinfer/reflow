// Package routing computes the destination partition shard for a given
// target. Routing is table-driven: every Partitioner holds a shared
// atomic snapshot of shard 0's LPOwnersTable (lp → shard_id), populated
// by a per-node reconciler. The planner fallback below kicks in only
// during the pre-warmup window (reconciler has not run yet) or when the
// snapshot is missing a particular LP (a real bug post bootstrap-seed).
//
// The fallback uses a consistent-hash ring with bounded loads (see
// planner.go). It MUST match what the metadata-leader bootstrap seeds
// into LPOwnersTable — that's how the warm-up window routes to the same
// shard the steady-state table will. See seedLPOwners in
// internal/engine/metadata_runner.go; the two paths share
// NewPlanner+PlanAll.
//
// The shard ids returned here are 1-indexed: shard 0 is reserved for the
// metadata Raft group (see internal/engine/cluster). When the cluster
// hosts N partition shards their ids are 1..N.
package routing

import (
	"hash/fnv"
	"sync/atomic"

	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// Partitioner maps a logical key tuple to a partition shard id.
//
// lpOwners, when non-nil, holds a snapshot of shard 0's LPOwnersTable
// (lp → shard_id). ShardForKey consults the snapshot first; the planner
// fallback below kicks in only when (a) the snapshot has not been
// populated yet (pre-first-reconciler-tick warm-up), or (b) the LP is
// missing from the snapshot (a real bug post bootstrap-seed — not logged
// on this hot path).
//
// planner is the shared fallback ring (consistent hashing with bounded
// loads). Built once at construction from shard ids 1..N so the
// 4096-entry ring isn't re-allocated per call. Value-copies of a
// Partitioner share this pointer just like they share the lpOwners
// pointer; warm-up routing on a copy returns the same answer as on the
// original.
//
// The zero Partitioner is meaningful: ShardForKey returns 1 for every
// key, which is correct for the single-partition single-node deployment
// shape (see internal/engine/partition.go:Partitioner doc).
type Partitioner struct {
	lpOwners *atomic.Pointer[map[uint32]uint64]
	planner  *Planner
}

// NewPartitioner constructs a Partitioner with a fresh atomic-pointer
// snapshot slot and a freshly-built consistent-hash planner over shard
// ids 1..numShards. Reconcilers swap the LPOwners snapshot via
// SetLPOwnersSnapshot on the returned pointer; every value-copy of *p
// observes the swap.
//
// Passing numShards==0 yields a Partitioner with no planner — ShardForKey
// returns the defensive shard-1 fallback, preserving the single-partition
// behavior single-node deployments rely on.
func NewPartitioner(numShards uint64) *Partitioner {
	ids := make([]uint64, 0, numShards)
	for i := uint64(1); i <= numShards; i++ {
		ids = append(ids, i)
	}
	return &Partitioner{
		lpOwners: &atomic.Pointer[map[uint32]uint64]{},
		planner:  NewPlanner(ids),
	}
}

// SetLPOwnersSnapshot atomically swaps the routing snapshot. The
// reconciler calls this on each TableNotifier wake after a SyncRead.
// Passing nil clears the snapshot (subsequent ShardForKey calls fall
// back to the planner).
func (p *Partitioner) SetLPOwnersSnapshot(m map[uint32]uint64) {
	if p == nil || p.lpOwners == nil {
		return
	}
	if m == nil {
		p.lpOwners.Store(nil)
		return
	}
	p.lpOwners.Store(&m)
}

// LPOwnersSnapshot returns the current snapshot, or nil if no snapshot
// has been published. The returned map MUST NOT be mutated (it is shared
// across every reader).
func (p Partitioner) LPOwnersSnapshot() map[uint32]uint64 {
	if p.lpOwners == nil {
		return nil
	}
	mp := p.lpOwners.Load()
	if mp == nil {
		return nil
	}
	return *mp
}

// PartitionKey returns the canonical 64-bit partition key for a
// (service, object_key) tuple. The tuple is hashed with FNV-1a so the result
// is platform-independent and identical across nodes; the low log2(LPCount)
// bits are the LP (routing/sharding coordinate), the high bits keep the hash
// entropy that uuid-derivation, the invoker sessionKey, and idempotency keying
// rely on for collision-freedom. Empty object_key (unkeyed services) hashes
// consistently — every invocation of the same unkeyed service routes to the
// same shard.
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
//
// Lookup order:
//   - LPOwners snapshot (the authoritative route post-bootstrap-seed).
//   - Planner fallback (consistent hashing with bounded loads) — covers
//     the pre-warmup window and the snapshot-miss case. This is the SAME
//     ring the metadata-leader bootstrap uses to seed the table, so the
//     warm-up window routes to the same shard the post-seed table will.
//
// Returns 1 when both the snapshot and the planner are absent — defensive
// only; tests guard against the zero-NumShards case.
func (p Partitioner) ShardForKey(partitionKey uint64) uint64 {
	return p.ShardForLP(keys.LPFromPartitionKey(partitionKey))
}

// ShardForLP maps a logical partition id directly to its owning shard, sharing
// ShardForKey's lookup order (LPOwners snapshot, then planner fallback, then 1).
// Used by fan-out reads that enumerate LPs — e.g. ListProcessInstances —
// rather than a single partition key.
func (p Partitioner) ShardForLP(lp uint32) uint64 {
	if p.lpOwners != nil {
		if mp := p.lpOwners.Load(); mp != nil {
			if shard, ok := (*mp)[lp]; ok {
				return shard
			}
		}
	}
	if p.planner != nil {
		return p.planner.ShardForLP(lp)
	}
	return 1
}

// ShardForTarget is a convenience for callers that have an InvocationTarget
// rather than a raw partition key.
func (p Partitioner) ShardForTarget(t *enginev1.InvocationTarget) uint64 {
	return p.ShardForKey(PartitionKey(t.GetServiceName(), t.GetObjectKey()))
}

// ShardForInvocation extracts the partition key already stamped on the
// InvocationId and maps it through ShardForKey. Used by the outbox when
// it has an InvokeCommand or DeliverCallResult variant in hand.
func (p Partitioner) ShardForInvocation(id *enginev1.InvocationId) uint64 {
	return p.ShardForKey(id.GetPartitionKey())
}
