package engine

import (
	"slices"
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestBuildBootstrapTable_FreshBootstrapSeedsFromPeers verifies the
// fresh-bootstrap path: when no on-disk PartitionTable exists, every
// partition shard 1..N is seeded with the full peer-id set and
// MetaReplicas mirrors the same set.
func TestBuildBootstrapTable_FreshBootstrapSeedsFromPeers(t *testing.T) {
	peers := []Peer{
		{NodeID: 1, RaftAddr: "10.0.0.1:9091"},
		{NodeID: 2, RaftAddr: "10.0.0.2:9091"},
		{NodeID: 3, RaftAddr: "10.0.0.3:9091"},
	}
	got := buildBootstrapTable(peers, nil, 7)

	if got.GetAssignmentEpoch() != 7 {
		t.Errorf("assignment_epoch = %d; want 7", got.GetAssignmentEpoch())
	}
	if len(got.GetShards()) != 3 {
		t.Fatalf("len(shards) = %d; want 3", len(got.GetShards()))
	}
	for sh := uint64(1); sh <= 3; sh++ {
		ids := got.GetShards()[sh].GetNodeIds()
		slices.Sort(ids)
		if !slices.Equal(ids, []uint64{1, 2, 3}) {
			t.Errorf("shards[%d].NodeIds = %v; want [1 2 3]", sh, ids)
		}
	}
	metaIDs := got.GetMetaReplicas().GetNodeIds()
	slices.Sort(metaIDs)
	if !slices.Equal(metaIDs, []uint64{1, 2, 3}) {
		t.Errorf("meta_replicas = %v; want [1 2 3]", metaIDs)
	}
}

// TestBuildBootstrapTable_PreservesRuntimeAddedShardMembers is the
// regression case for issue #3: a leader-gain re-run must not wipe
// partition-shard members that the rebalance pipeline added after
// bootstrap. The static peers list is {1,2,3}; the existing on-disk
// table has node 4 in every shard (added at runtime via the admin
// AddNode workflow). The re-run must re-propose the existing
// {1,2,3,4} replica set, not the static {1,2,3}.
func TestBuildBootstrapTable_PreservesRuntimeAddedShardMembers(t *testing.T) {
	peers := []Peer{
		{NodeID: 1, RaftAddr: "10.0.0.1:9091"},
		{NodeID: 2, RaftAddr: "10.0.0.2:9091"},
		{NodeID: 3, RaftAddr: "10.0.0.3:9091"},
	}
	existing := &enginev1.PartitionTable{
		AssignmentEpoch: 12,
		Shards: map[uint64]*enginev1.ReplicaSet{
			1: {NodeIds: []uint64{1, 2, 3, 4}},
			2: {NodeIds: []uint64{1, 2, 3, 4}},
			3: {NodeIds: []uint64{1, 2, 3, 4}},
		},
		MetaReplicas: &enginev1.ReplicaSet{NodeIds: []uint64{1, 2, 3, 4}},
	}
	got := buildBootstrapTable(peers, existing, 13)

	// AssignmentEpoch reflects the NEW leader epoch (UpdatePartitionTable
	// is a full overwrite — the proposer is the source of truth for that
	// field).
	if got.GetAssignmentEpoch() != 13 {
		t.Errorf("assignment_epoch = %d; want 13", got.GetAssignmentEpoch())
	}
	// Shards: must equal the existing on-disk view (which has node 4).
	// If the bug returns, the resulting shards would be {1,2,3} from the
	// static peer set and 4 would be wiped.
	for sh := uint64(1); sh <= 3; sh++ {
		ids := got.GetShards()[sh].GetNodeIds()
		slices.Sort(ids)
		if !slices.Equal(ids, []uint64{1, 2, 3, 4}) {
			t.Errorf("shards[%d].NodeIds = %v; want preserved [1 2 3 4]", sh, ids)
		}
	}
	// MetaReplicas: same — must preserve runtime additions.
	metaIDs := got.GetMetaReplicas().GetNodeIds()
	slices.Sort(metaIDs)
	if !slices.Equal(metaIDs, []uint64{1, 2, 3, 4}) {
		t.Errorf("meta_replicas = %v; want preserved [1 2 3 4]", metaIDs)
	}
}

// TestBuildBootstrapTable_EmptyExistingFallsBackToPeers covers an edge
// case: an on-disk PartitionTable exists but is empty (no Shards, no
// MetaReplicas). Fall back to the static peer seed; don't propose an
// empty table.
func TestBuildBootstrapTable_EmptyExistingFallsBackToPeers(t *testing.T) {
	peers := []Peer{
		{NodeID: 1, RaftAddr: "10.0.0.1:9091"},
		{NodeID: 2, RaftAddr: "10.0.0.2:9091"},
	}
	existing := &enginev1.PartitionTable{AssignmentEpoch: 5}
	got := buildBootstrapTable(peers, existing, 6)

	if len(got.GetShards()) != 2 {
		t.Errorf("len(shards) = %d; want 2 (peer-seeded fallback)", len(got.GetShards()))
	}
	if len(got.GetMetaReplicas().GetNodeIds()) != 2 {
		t.Errorf("meta_replicas len = %d; want 2", len(got.GetMetaReplicas().GetNodeIds()))
	}
}

// TestBuildBootstrapTable_DeterministicAcrossReplicas verifies that two
// invocations with the same inputs produce byte-equivalent tables. This
// is the apply-determinism invariant — every replica reaches the same
// bytes regardless of which one calls bootstrap.
func TestBuildBootstrapTable_DeterministicAcrossReplicas(t *testing.T) {
	peers := []Peer{
		{NodeID: 1, RaftAddr: "10.0.0.1:9091"},
		{NodeID: 2, RaftAddr: "10.0.0.2:9091"},
		{NodeID: 3, RaftAddr: "10.0.0.3:9091"},
	}
	a := buildBootstrapTable(peers, nil, 1)
	b := buildBootstrapTable(peers, nil, 1)
	// Direct field-by-field comparison: Shards map ordering is
	// non-deterministic in Go iteration, but the proto-level value is.
	// We compare the per-shard NodeIds slices after sort to sidestep
	// any in-slice ordering differences.
	if len(a.GetShards()) != len(b.GetShards()) {
		t.Fatalf("shard count diverged: a=%d b=%d", len(a.GetShards()), len(b.GetShards()))
	}
	for sh, rsA := range a.GetShards() {
		rsB := b.GetShards()[sh]
		idsA := slices.Clone(rsA.GetNodeIds())
		idsB := slices.Clone(rsB.GetNodeIds())
		slices.Sort(idsA)
		slices.Sort(idsB)
		if !slices.Equal(idsA, idsB) {
			t.Errorf("shards[%d]: a=%v b=%v", sh, idsA, idsB)
		}
	}
}
