package cluster

// Property-based state-machine test for the metadata-shard FSM, using
// pgregory.net/rapid.
//
// Surfaces covered:
//
//   - RegisterNode, EvictNode, UpdatePartitionTable
//   - BeginRebalanceStep (idempotent on duplicate) — partition shards AND shard 0
//   - CompleteRebalanceStep (delete / promote / add-non-voting) — partitions AND shard 0
//   - OnPartitionTable hook fires exactly when an Update produced a fresh table
//
// The model is a plain-Go mirror of the FSM's state (applied_index,
// members map, partition table) and the Check method asserts SUT and
// model agree after every step. Subtle correctness points we model
// faithfully:
//
//   - EvictNode walks shards in sorted shardId order when appending
//     DELETE_REPLICA steps; the shard-0 step (if the evicted node is a
//     metadata voter) is appended LAST so the resulting byte sequence
//     stays deterministic across replicas. (See applyEvictNode in fsm.go.)
//   - Re-applying EvictNode on an already-evicted node (last_seen=0) is
//     a no-op.
//   - BeginRebalanceStep on a duplicate (shard_id, step_id) is dropped.
//   - CompleteRebalanceStep is idempotent: if no entry matches, no-op
//     and no assignment_epoch bump.
//   - shard_id=0 CompleteRebalanceStep mutates pt.MetaReplicas, NOT
//     pt.Shards, and does NOT bump assignment_epoch (routing decisions
//     don't depend on metadata-shard membership).
//   - UpdatePartitionTable is a full overwrite — proposers send the
//     desired MetaReplicas every time (the metadata-runner bootstrap
//     reads the existing value off disk before proposing).
//
// The PBT does not exercise SaveSnapshot/RecoverFromSnapshot — stubSnapshotter
// does not back the snapshot stream. Snapshot round-trip is covered
// elsewhere; this PBT focuses on apply-path invariants.

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	"github.com/twinfer/reflow/internal/storage"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

const (
	pbtMaxNode  = 5 // node pool: {1..5}
	pbtMaxShard = 3 // shard pool: {1..3}
)

type fsmMachine struct {
	t *testing.T

	fsm *FSM

	// Captured hook calls for assertions.
	hookCalls []*enginev1.PartitionTable

	// Model state.
	appliedIdx uint64
	members    map[uint64]*enginev1.NodeMembership
	pt         *enginev1.PartitionTable

	raftIx uint64
}

func (m *fsmMachine) init(t *rapid.T) {
	dir := filepath.Join(m.t.TempDir(), "meta", "state")
	st, err := storage.OpenPebble(dir, nil)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	m.t.Cleanup(func() { _ = st.Close() })

	lead := &stubLeadership{}
	lead.leader.Store(true)
	m.fsm = New(0, 1, Config{
		Snapshotter: &stubSnapshotter{store: st},
		Leadership:  lead,
		OnPartitionTable: func(pt *enginev1.PartitionTable) {
			m.hookCalls = append(m.hookCalls, pt)
		},
	})

	m.members = map[uint64]*enginev1.NodeMembership{}
	m.pt = nil
	m.appliedIdx = 0
	m.raftIx = 0
	m.hookCalls = nil
}

func (m *fsmMachine) apply(t *rapid.T, cmd *enginev1.Command) {
	m.raftIx++
	if _, err := m.fsm.Update([]statemachine.Entry{{Index: m.raftIx, Cmd: envelope(m.t, cmd)}}); err != nil {
		t.Fatalf("Update idx=%d: %v", m.raftIx, err)
	}
	m.appliedIdx = m.raftIx
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

func (m *fsmMachine) RegisterNode(t *rapid.T) {
	nodeID := uint64(rapid.IntRange(1, pbtMaxNode).Draw(t, "node_id"))
	lastSeen := rapid.Int64Range(1, 1_000_000).Draw(t, "last_seen")
	raftAddr := rapid.SampledFrom([]string{"a:1", "b:2", "c:3"}).Draw(t, "raft_addr")
	nodeHostID := rapid.SampledFrom([]string{"nh-a", "nh-b", "nh-c"}).Draw(t, "nh_id")

	mem := &enginev1.NodeMembership{
		NodeId:     nodeID,
		RaftAddr:   raftAddr,
		NodeHostId: nodeHostID,
		LastSeenMs: lastSeen,
	}
	m.members[nodeID] = mem
	m.apply(t, &enginev1.Command{
		Kind: &enginev1.Command_RegisterNode{RegisterNode: &enginev1.RegisterNode{Member: mem}},
	})
}

func (m *fsmMachine) UpdatePartitionTable(t *rapid.T) {
	nShards := rapid.IntRange(1, pbtMaxShard).Draw(t, "n_shards")
	epoch := rapid.Uint64Range(1, 100).Draw(t, "epoch")
	shards := map[uint64]*enginev1.ReplicaSet{}
	for s := 1; s <= nShards; s++ {
		// 1..3 replicas per shard, drawn from node pool.
		nReplicas := rapid.IntRange(1, 3).Draw(t, "n_replicas")
		ids := uniqueSampledNodeIDs(t, nReplicas)
		shards[uint64(s)] = &enginev1.ReplicaSet{NodeIds: ids}
	}
	pt := &enginev1.PartitionTable{
		AssignmentEpoch: epoch,
		Shards:          shards,
	}
	// Sometimes include MetaReplicas, sometimes leave nil. UpdatePartitionTable
	// is a full overwrite — nil input means the persisted MetaReplicas
	// goes back to nil. The model just clones the input verbatim.
	if rapid.Bool().Draw(t, "include_meta_replicas") {
		nMeta := rapid.IntRange(1, 3).Draw(t, "n_meta_replicas")
		pt.MetaReplicas = &enginev1.ReplicaSet{NodeIds: uniqueSampledNodeIDs(t, nMeta)}
	}
	m.pt = proto.Clone(pt).(*enginev1.PartitionTable)
	m.apply(t, &enginev1.Command{
		Kind: &enginev1.Command_UpdatePartitionTable{
			UpdatePartitionTable: &enginev1.UpdatePartitionTable{Table: pt},
		},
	})
}

func (m *fsmMachine) EvictNode(t *rapid.T) {
	nodeID := uint64(rapid.IntRange(1, pbtMaxNode).Draw(t, "node_id"))
	m.applyEvictModel(nodeID)
	m.apply(t, &enginev1.Command{
		Kind: &enginev1.Command_EvictNode{EvictNode: &enginev1.EvictNode{NodeId: nodeID}},
	})
}

func (m *fsmMachine) BeginRebalanceStep(t *rapid.T) {
	if m.pt == nil || len(m.pt.GetShards()) == 0 {
		return
	}
	// Pool: partition shards in pt.Shards plus shard 0 (the metadata
	// Raft group itself). Shard 0 is included regardless of whether
	// MetaReplicas is populated — the FSM accepts the step either way
	// and CompleteRebalanceStep applies against MetaReplicas.
	shardID := pickShardIDIncludingZero(t, m.pt)
	stepID := uint64(rapid.IntRange(1, 10).Draw(t, "step_id"))
	kind := rapid.SampledFrom([]enginev1.RebalanceStep_Kind{
		enginev1.RebalanceStep_ADD_NON_VOTING,
		enginev1.RebalanceStep_PROMOTE_TO_VOTER,
		enginev1.RebalanceStep_DELETE_REPLICA,
	}).Draw(t, "kind")
	step := &enginev1.RebalanceStep{
		ShardId: shardID,
		StepId:  stepID,
		Kind:    kind,
	}
	switch kind {
	case enginev1.RebalanceStep_DELETE_REPLICA:
		step.RemoveNodeId = uint64(rapid.IntRange(1, pbtMaxNode).Draw(t, "remove_id"))
	default:
		step.AddNodeId = uint64(rapid.IntRange(1, pbtMaxNode).Draw(t, "add_id"))
	}

	m.applyBeginStepModel(step)
	m.apply(t, &enginev1.Command{
		Kind: &enginev1.Command_BeginRebalanceStep{
			BeginRebalanceStep: &enginev1.BeginRebalanceStep{Step: step},
		},
	})
}

func (m *fsmMachine) CompleteRebalanceStep(t *rapid.T) {
	if m.pt == nil || len(m.pt.GetPending()) == 0 {
		return
	}
	idx := rapid.IntRange(0, len(m.pt.GetPending())-1).Draw(t, "pending_idx")
	step := m.pt.GetPending()[idx]
	m.applyCompleteStepModel(step.GetShardId(), step.GetStepId())
	m.apply(t, &enginev1.Command{
		Kind: &enginev1.Command_CompleteRebalanceStep{
			CompleteRebalanceStep: &enginev1.CompleteRebalanceStep{
				ShardId: step.GetShardId(),
				StepId:  step.GetStepId(),
			},
		},
	})
}

func (m *fsmMachine) AnnounceLeader(t *rapid.T) {
	// Leadership state is observed only by Leadership.OnAnnounceLeader,
	// which the stub records but doesn't expose for invariants. Applying
	// keeps the FSM's apply-loop exercised but doesn't touch the model.
	m.apply(t, &enginev1.Command{
		Kind: &enginev1.Command_AnnounceLeader{
			AnnounceLeader: &enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 1},
		},
	})
}

// ---------------------------------------------------------------------------
// Model-side transitions (mirror fsm.go semantics)
// ---------------------------------------------------------------------------

func (m *fsmMachine) applyEvictModel(nodeID uint64) {
	mem, ok := m.members[nodeID]
	if !ok || mem.GetLastSeenMs() == 0 {
		// Either unknown or already evicted — no-op (mirrors fsm.go).
		return
	}
	mem.LastSeenMs = 0
	if m.pt == nil {
		return
	}
	// Walk partition shards in sorted shardId order to mirror fsm.go's
	// determinism, then append shard 0 LAST when the evicted node is a
	// metadata voter — same canonical byte sequence applyEvictNode
	// produces.
	shardIDs := sortedShardIDs(m.pt.GetShards())
	for _, sh := range shardIDs {
		rs := m.pt.GetShards()[sh]
		if !slices.Contains(rs.GetNodeIds(), nodeID) {
			continue
		}
		step := &enginev1.RebalanceStep{
			ShardId:      sh,
			Kind:         enginev1.RebalanceStep_DELETE_REPLICA,
			RemoveNodeId: nodeID,
			StepId:       nextStepIDModel(m.pt.GetPending(), sh),
		}
		m.pt.Pending = append(m.pt.Pending, step)
	}
	if slices.Contains(m.pt.GetMetaReplicas().GetNodeIds(), nodeID) {
		m.pt.Pending = append(m.pt.Pending, &enginev1.RebalanceStep{
			ShardId:      0,
			Kind:         enginev1.RebalanceStep_DELETE_REPLICA,
			RemoveNodeId: nodeID,
			StepId:       nextStepIDModel(m.pt.GetPending(), 0),
		})
	}
}

func (m *fsmMachine) applyBeginStepModel(step *enginev1.RebalanceStep) {
	if step.GetStepId() == 0 {
		return
	}
	if m.pt == nil {
		return
	}
	for _, p := range m.pt.GetPending() {
		if p.GetShardId() == step.GetShardId() && p.GetStepId() == step.GetStepId() {
			return // duplicate; FSM drops silently
		}
	}
	m.pt.Pending = append(m.pt.Pending, proto.Clone(step).(*enginev1.RebalanceStep))
}

func (m *fsmMachine) applyCompleteStepModel(shardID, stepID uint64) {
	if stepID == 0 {
		return
	}
	if m.pt == nil {
		return
	}
	var matched *enginev1.RebalanceStep
	kept := m.pt.Pending[:0]
	for _, p := range m.pt.GetPending() {
		if matched == nil && p.GetShardId() == shardID && p.GetStepId() == stepID {
			matched = p
			continue
		}
		kept = append(kept, p)
	}
	if matched == nil {
		return
	}
	m.pt.Pending = kept

	// Pick the ReplicaSet to mutate: shard 0 -> MetaReplicas, partitions
	// -> pt.Shards[shardID]. Mirrors pickRebalanceTarget in fsm.go.
	var targetRS *enginev1.ReplicaSet
	var setTargetRS func(*enginev1.ReplicaSet)
	if shardID == 0 {
		targetRS = m.pt.GetMetaReplicas()
		setTargetRS = func(rs *enginev1.ReplicaSet) { m.pt.MetaReplicas = rs }
	} else {
		targetRS = m.pt.GetShards()[shardID]
		setTargetRS = func(rs *enginev1.ReplicaSet) {
			if m.pt.Shards == nil {
				m.pt.Shards = map[uint64]*enginev1.ReplicaSet{}
			}
			m.pt.Shards[shardID] = rs
		}
	}
	switch matched.GetKind() {
	case enginev1.RebalanceStep_DELETE_REPLICA:
		if targetRS != nil {
			targetRS.NodeIds = removeUint64(targetRS.NodeIds, matched.GetRemoveNodeId())
		}
	case enginev1.RebalanceStep_PROMOTE_TO_VOTER:
		if targetRS == nil {
			targetRS = &enginev1.ReplicaSet{}
			setTargetRS(targetRS)
		}
		if !slices.Contains(targetRS.NodeIds, matched.GetAddNodeId()) {
			targetRS.NodeIds = append(targetRS.NodeIds, matched.GetAddNodeId())
		}
	case enginev1.RebalanceStep_ADD_NON_VOTING:
		// No replica-set mutation (mirrors fsm.go).
	}
	// AssignmentEpoch tracks partition-ownership generation for routing;
	// shard-0 changes are invisible to routing and don't bump.
	if shardID != 0 {
		m.pt.AssignmentEpoch++
	}
}

// ---------------------------------------------------------------------------
// Check (invariants)
// ---------------------------------------------------------------------------

func (m *fsmMachine) Check(t *rapid.T) {
	// 1. Applied index parity.
	gotIdxAny, err := m.fsm.Lookup(LookupAppliedIndex{})
	if err != nil {
		t.Fatalf("LookupAppliedIndex: %v", err)
	}
	if got := gotIdxAny.(uint64); got != m.appliedIdx {
		t.Fatalf("applied_index: SUT=%d model=%d", got, m.appliedIdx)
	}

	// 2. Membership parity.
	gotMembersAny, err := m.fsm.Lookup(LookupMembership{})
	if err != nil {
		t.Fatalf("LookupMembership: %v", err)
	}
	gotMembers := gotMembersAny.([]*enginev1.NodeMembership)
	if len(gotMembers) != len(m.members) {
		t.Fatalf("members: SUT len=%d model len=%d", len(gotMembers), len(m.members))
	}
	for _, gm := range gotMembers {
		wm, ok := m.members[gm.GetNodeId()]
		if !ok {
			t.Fatalf("members: SUT has node_id=%d not in model", gm.GetNodeId())
		}
		if !proto.Equal(gm, wm) {
			t.Fatalf("members[%d]: SUT=%+v model=%+v", gm.GetNodeId(), gm, wm)
		}
	}

	// 3. PartitionTable parity.
	gotPTAny, err := m.fsm.Lookup(LookupPartitionTable{})
	if err != nil {
		t.Fatalf("LookupPartitionTable: %v", err)
	}
	gotPT, _ := gotPTAny.(*enginev1.PartitionTable)
	if m.pt == nil {
		if gotPT != nil {
			t.Fatalf("partition_table: SUT non-nil but model nil: %+v", gotPT)
		}
	} else {
		if !proto.Equal(gotPT, m.pt) {
			t.Fatalf("partition_table mismatch:\n SUT=%v\n model=%v", gotPT, m.pt)
		}
	}

	// 4. Hook tail matches the current SUT partition table. The FSM
	//    fires OnPartitionTable exactly when an Update batch produced a
	//    newTable; the last hook value is a clone of what was just
	//    committed, so it must equal the current SUT view.
	if len(m.hookCalls) > 0 {
		last := m.hookCalls[len(m.hookCalls)-1]
		if !proto.Equal(last, gotPT) {
			t.Fatalf("hook tail mismatches current table:\n hook=%v\n SUT=%v", last, gotPT)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sortedShardIDs(shards map[uint64]*enginev1.ReplicaSet) []uint64 {
	out := make([]uint64, 0, len(shards))
	for k := range shards {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func removeUint64(ids []uint64, x uint64) []uint64 {
	for i, v := range ids {
		if v == x {
			return append(ids[:i], ids[i+1:]...)
		}
	}
	return ids
}

func nextStepIDModel(pending []*enginev1.RebalanceStep, shardID uint64) uint64 {
	var max uint64
	for _, p := range pending {
		if p.GetShardId() == shardID && p.GetStepId() > max {
			max = p.GetStepId()
		}
	}
	return max + 1
}

func uniqueSampledNodeIDs(t *rapid.T, n int) []uint64 {
	seen := map[uint64]struct{}{}
	out := make([]uint64, 0, n)
	for len(out) < n {
		id := uint64(rapid.IntRange(1, pbtMaxNode).Draw(t, "replica_id"))
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func pickShardID(t *rapid.T, pt *enginev1.PartitionTable) uint64 {
	ids := sortedShardIDs(pt.GetShards())
	return rapid.SampledFrom(ids).Draw(t, "shard_id")
}

// pickShardIDIncludingZero draws from the partition shards plus shard 0
// (the metadata Raft group). Used by BeginRebalanceStep so the
// generator exercises shard-0 add/remove/non-voting paths uniformly.
func pickShardIDIncludingZero(t *rapid.T, pt *enginev1.PartitionTable) uint64 {
	ids := sortedShardIDs(pt.GetShards())
	ids = append([]uint64{0}, ids...)
	return rapid.SampledFrom(ids).Draw(t, "shard_id")
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

func TestClusterFSM_PBT(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := &fsmMachine{t: t}
		m.init(rt)
		rt.Repeat(rapid.StateMachineActions(m))
	})
}
