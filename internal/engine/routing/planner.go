package routing

import (
	"cmp"
	"slices"
	"strconv"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"

	"github.com/twinfer/reflow/internal/storage/keys"
)

// Tunables for the underlying consistent.Consistent ring.
//
//   - plannerReplicationFactor is the number of virtual nodes each shard
//     gets on the ring. Higher = better distribution, lower = cheaper ring
//     construction. 20 is what the upstream README example uses; for the
//     small shard counts Reflow runs (typically <= ~32) it gives sub-1%
//     deviation from the bounded-load ceiling.
//   - plannerLoad is the bounded-load ceiling: max owned partitions /
//     average. 1.25 matches the Google blog post's example and keeps the
//     post-rebalance "moves needed" count low while still bounding skew.
const (
	plannerReplicationFactor = 20
	plannerLoad              = 1.25
)

// Planner computes lp → shard ownership using a consistent-hash ring with
// bounded loads (https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
// — buraksezer/consistent is the Go implementation).
//
// The ring is built once at construction; ShardForLP / PlanAll are
// allocation-free reads against the pre-computed partition table.
//
// Determinism: the same (shardIDs) input produces the same ring across
// processes and architectures (xxhash is platform-neutral and the
// constructor sorts shard ids before building the ring, so the order
// in which the caller passes them is irrelevant).
//
// Bounded-load guarantee: each shard owns at most ⌈avg * plannerLoad⌉
// partitions. With LPCount=4096 and N shards, that is roughly
// ⌈4096/N · 1.25⌉ — comfortably bounded skew even for small N.
type Planner struct {
	c *consistent.Consistent
}

// NewPlanner builds a ring with the given shard ids. Returns nil when
// shardIDs is empty — callers must handle that case (it covers a
// freshly-bootstrapped Host whose PartitionTable has not yet committed).
func NewPlanner(shardIDs []uint64) *Planner {
	if len(shardIDs) == 0 {
		return nil
	}
	ids := append([]uint64(nil), shardIDs...)
	slices.Sort(ids)
	members := make([]consistent.Member, 0, len(ids))
	for _, id := range ids {
		members = append(members, shardMember(id))
	}
	c := consistent.New(members, consistent.Config{
		PartitionCount:    int(keys.LPCount),
		ReplicationFactor: plannerReplicationFactor,
		Load:              plannerLoad,
		Hasher:            xxhasher{},
	})
	return &Planner{c: c}
}

// ShardForLP returns the shard id that owns lp. Returns 0 on a nil
// receiver — callers treat 0 as "no owner" (shard 0 is the metadata
// group; partition shards are 1-indexed).
func (p *Planner) ShardForLP(lp uint32) uint64 {
	if p == nil || p.c == nil {
		return 0
	}
	m := p.c.GetPartitionOwner(int(lp))
	if m == nil {
		return 0
	}
	return uint64(m.(shardMember))
}

// PlanAll returns the lp → shard_id map for every lp ∈ [0, LPCount).
// Used by the metadata-leader bootstrap to seed shard 0's LPOwnersTable.
// Returns nil on a nil receiver.
func (p *Planner) PlanAll() map[uint32]uint64 {
	if p == nil || p.c == nil {
		return nil
	}
	out := make(map[uint32]uint64, keys.LPCount)
	for lp := range keys.LPCount {
		out[lp] = uint64(p.c.GetPartitionOwner(int(lp)).(shardMember))
	}
	return out
}

// LPMove is one element of a transfer plan: lp must move from FromShard
// to ToShard. FromShard==0 means "no current owner" (the lp is absent
// from current — used when seeding a fresh table). Consumed by PR 3's
// transfer protocol.
type LPMove struct {
	LP        uint32
	FromShard uint64
	ToShard   uint64
}

// Diff returns the LPs whose ownership differs between current and
// desired, in ascending LP order. LPs present only in desired emit a
// move with FromShard==0; LPs present only in current are NOT emitted
// (the planner is the source of truth — leaving an unmapped LP in place
// is a no-op for routing). desired==nil yields no moves; current==nil
// is treated as the empty mapping.
func Diff(current, desired map[uint32]uint64) []LPMove {
	if len(desired) == 0 {
		return nil
	}
	out := make([]LPMove, 0, len(desired))
	for lp, want := range desired {
		have := current[lp]
		if have != want {
			out = append(out, LPMove{LP: lp, FromShard: have, ToShard: want})
		}
	}
	slices.SortFunc(out, func(a, b LPMove) int { return cmp.Compare(a.LP, b.LP) })
	return out
}

// shardMember adapts a Reflow shard id to consistent.Member. The string
// form is the decimal shard id so log lines and LoadDistribution() output
// are readable.
type shardMember uint64

func (s shardMember) String() string { return strconv.FormatUint(uint64(s), 10) }

// xxhasher implements consistent.Hasher with xxhash/v2 (already a
// transitive dep via prometheus + dragonboat). xxhash is platform-neutral,
// so the ring is deterministic across darwin/linux/arm64/amd64.
type xxhasher struct{}

func (xxhasher) Sum64(data []byte) uint64 { return xxhash.Sum64(data) }
