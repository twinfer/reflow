// Package routing computes the destination partition shard for a given
// target. PR 1 made routing table-driven: every Partitioner holds a
// shared atomic snapshot of shard 0's LPOwnersTable (lp → shard_id),
// populated by a per-node reconciler. The modulo fallback below kicks in
// only during the pre-warmup window (reconciler has not run yet) or when
// the snapshot is missing a particular LP (a real bug post bootstrap-seed).
//
// The shard ids returned here are 1-indexed: shard 0 is reserved for the
// metadata Raft group (see internal/engine/cluster). When the cluster
// hosts N partition shards their ids are 1..N.
package routing

import (
	"hash/fnv"
	"sync/atomic"

	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Partitioner maps a logical key tuple to a partition shard id.
//
// NumShards is the partition count (RF agnostic; routing is by shard id,
// not by replica). It MUST be > 0 — callers are expected to read it from
// the persisted PartitionTable size.
//
// lpOwners, when non-nil, holds a snapshot of shard 0's LPOwnersTable
// (lp → shard_id). ShardForKey consults the snapshot first; the modulo
// fallback below kicks in only when (a) lpOwners is nil (Partitioner
// constructed without NewPartitioner, e.g. by FromPartitionTable for
// non-Host callers), (b) the snapshot has not been populated yet
// (pre-first-reconciler-tick warm-up), or (c) the LP is missing from
// the snapshot (a real bug post bootstrap-seed — not logged on this
// hot path).
//
// All value-copies of a Partitioner produced by NewPartitioner share the
// same atomic-pointer slot, so a single SetLPOwnersSnapshot call is
// visible to every reader.
type Partitioner struct {
	NumShards uint64
	lpOwners  *atomic.Pointer[map[uint32]uint64]
}

// NewPartitioner constructs a Partitioner with a fresh atomic-pointer
// snapshot slot. Reconcilers swap via SetLPOwnersSnapshot on the returned
// pointer; every value-copy of *p observes the swap.
func NewPartitioner(numShards uint64) *Partitioner {
	return &Partitioner{
		NumShards: numShards,
		lpOwners:  &atomic.Pointer[map[uint32]uint64]{},
	}
}

// SetLPOwnersSnapshot atomically swaps the routing snapshot. The
// reconciler calls this on each TableNotifier wake after a SyncRead.
// Passing nil clears the snapshot (subsequent ShardForKey calls fall
// back to the modulo).
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

// PartitionKey returns the canonical 64-bit partition key for a (service,
// object_key) tuple. The tuple is hashed with FNV-1a so the result is
// platform-independent and identical across nodes. Empty object_key (used
// for unkeyed services) hashes consistently — every invocation of the same
// unkeyed service routes to the same shard.
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
//   - lp-modulo fallback (lp(pk) % NumShards) + 1 — covers the
//     pre-warmup window and the snapshot-miss case. This is the SAME
//     formula the metadata-leader bootstrap uses to seed the table, so
//     the warm-up window routes to the same shard the post-seed table
//     will. Non-power-of-2 NumShards diverges from the legacy
//     pk-modulo (pk(8B) % N) ≠ (pk & 0xFFF) % N, hence the formula
//     change vs the pre-PR-1 routing.
//
// Returns 1 when NumShards is 0 — defensive only; tests guard against it.
func (p Partitioner) ShardForKey(partitionKey uint64) uint64 {
	if p.lpOwners != nil {
		if mp := p.lpOwners.Load(); mp != nil {
			if shard, ok := (*mp)[keys.LPFromPartitionKey(partitionKey)]; ok {
				return shard
			}
		}
	}
	if p.NumShards == 0 {
		return 1
	}
	return (uint64(keys.LPFromPartitionKey(partitionKey)) % p.NumShards) + 1
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
// ShardForKey treats as "fall back to shard 1") when pt is nil or empty.
// The returned Partitioner has no LPOwners snapshot slot; callers that
// need the routing table should use NewPartitioner and let a reconciler
// populate it. This constructor is retained for non-Host callers
// (currently tests).
func FromPartitionTable(pt *enginev1.PartitionTable) Partitioner {
	if pt == nil {
		return Partitioner{}
	}
	return Partitioner{NumShards: uint64(len(pt.GetShards()))}
}
