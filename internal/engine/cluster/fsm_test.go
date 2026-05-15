package cluster

import (
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

type stubLeadership struct {
	leader atomic.Bool
	last   *enginev1.AnnounceLeader
}

func (s *stubLeadership) IsLeader() bool { return s.leader.Load() }
func (s *stubLeadership) OnAnnounceLeader(cmd *enginev1.AnnounceLeader) {
	s.last = cmd
}

// stubSnapshotter wraps an in-process storage.Store for FSM tests. We don't
// exercise SaveSnapshot/RecoverFromSnapshot in this unit test — those are
// covered indirectly by the dragonboat integration tests.
type stubSnapshotter struct{ store storage.Store }

func (s *stubSnapshotter) Store() storage.Store                  { return s.store }
func (s *stubSnapshotter) SaveSnapshot(_ io.Writer) error        { return nil }
func (s *stubSnapshotter) RecoverFromSnapshot(_ io.Reader) error { return nil }
func (s *stubSnapshotter) Close() error                          { return nil }

func newTestFSM(t *testing.T) (*FSM, *stubLeadership, storage.Store) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "meta", "state")
	st, err := storage.OpenPebble(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	lead := &stubLeadership{}
	lead.leader.Store(true)
	f := New(0, 1, Config{
		Snapshotter: &stubSnapshotter{store: st},
		Leadership:  lead,
	})
	return f, lead, st
}

func envelope(t *testing.T, cmd *enginev1.Command) []byte {
	t.Helper()
	// Stamp Header.CreatedAtMs so the metadata-shard FSM mirrors the
	// production envelope shape (proposer-stamped) even though shard 0
	// itself does not consume the timestamp today.
	buf, err := proto.Marshal(&enginev1.Envelope{
		Header:  &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: cmd,
	})
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestCluster_AnnounceLeaderForwarded(t *testing.T) {
	f, lead, _ := newTestFSM(t)
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_AnnounceLeader{
			AnnounceLeader: &enginev1.AnnounceLeader{NodeId: 2, LeaderEpoch: 7},
		},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	if lead.last == nil || lead.last.GetLeaderEpoch() != 7 || lead.last.GetNodeId() != 2 {
		t.Fatalf("expected leadership to observe AnnounceLeader{2,7}; got %+v", lead.last)
	}
}

func TestCluster_RegisterNodePersists(t *testing.T) {
	f, _, st := newTestFSM(t)
	mem := &enginev1.NodeMembership{
		NodeId:     3,
		RaftAddr:   "10.0.0.3:9091",
		NodeHostId: "reflowd-node-3",
		LastSeenMs: 1700000000,
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_RegisterNode{
			RegisterNode: &enginev1.RegisterNode{Member: mem},
		},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	got, err := (MembershipTable{S: st}).Get(3)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected membership row for node 3")
	}
	if got.GetRaftAddr() != mem.RaftAddr || got.GetNodeHostId() != mem.NodeHostId {
		t.Fatalf("membership mismatch: got %+v want %+v", got, mem)
	}
}

func TestCluster_UpdatePartitionTablePersistsAndHooks(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "meta", "state")
	st, err := storage.OpenPebble(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	lead := &stubLeadership{}
	lead.leader.Store(true)
	var hookCalls []*enginev1.PartitionTable
	f := New(0, 1, Config{
		Snapshotter:      &stubSnapshotter{store: st},
		Leadership:       lead,
		OnPartitionTable: func(pt *enginev1.PartitionTable) { hookCalls = append(hookCalls, pt) },
	})
	pt := &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards: map[uint64]*enginev1.ReplicaSet{
			1: {NodeIds: []uint64{1, 2, 3}},
			2: {NodeIds: []uint64{1, 2, 3}},
			3: {NodeIds: []uint64{1, 2, 3}},
		},
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpdatePartitionTable{
			UpdatePartitionTable: &enginev1.UpdatePartitionTable{Table: pt},
		},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: 5, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	got, err := (PartitionTableTable{S: st}).Get()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected persisted partition table")
	}
	if len(got.GetShards()) != 3 || got.GetAssignmentEpoch() != 1 {
		t.Fatalf("partition table mismatch: %+v", got)
	}
	if len(hookCalls) != 1 || len(hookCalls[0].GetShards()) != 3 {
		t.Fatalf("expected OnPartitionTable hook to fire with 3 shards; got %d call(s)", len(hookCalls))
	}
	// Applied index must reflect the entry's raft index.
	m, err := (MetaTable{S: st}).Get()
	if err != nil {
		t.Fatal(err)
	}
	if m.GetAppliedIndex() != 5 {
		t.Fatalf("applied index = %d; want 5", m.GetAppliedIndex())
	}
}

func TestCluster_RegisterDeploymentPersists(t *testing.T) {
	f, _, st := newTestFSM(t)
	rec := &enginev1.DeploymentRecord{
		Id:        "inproc-abc",
		Url:       "inproc://",
		Transport: "inproc",
		Handlers: []*enginev1.DeploymentHandler{
			{Service: "Greeter", Handler: "hello", Kind: 1},
		},
		RegisteredAtMs: 1700000000,
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_RegisterDeployment{
			RegisterDeployment: &enginev1.RegisterDeployment{Record: rec},
		},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: 9, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	got, err := (DeploymentTable{S: st}).Get("inproc-abc")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetUrl() != "inproc://" || got.GetTransport() != "inproc" {
		t.Fatalf("deployment row mismatch: %+v", got)
	}
	if len(got.GetHandlers()) != 1 || got.GetHandlers()[0].GetService() != "Greeter" {
		t.Fatalf("deployment handlers mismatch: %+v", got.GetHandlers())
	}

	// Re-applying with the same id is an upsert — overwrites, no error.
	rec2 := proto.Clone(rec).(*enginev1.DeploymentRecord)
	rec2.RegisteredAtMs = 1700001234
	cmd2 := &enginev1.Command{
		Kind: &enginev1.Command_RegisterDeployment{
			RegisterDeployment: &enginev1.RegisterDeployment{Record: rec2},
		},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: envelope(t, cmd2)}}); err != nil {
		t.Fatal(err)
	}
	got2, err := (DeploymentTable{S: st}).Get("inproc-abc")
	if err != nil {
		t.Fatal(err)
	}
	if got2.GetRegisteredAtMs() != 1700001234 {
		t.Errorf("upsert did not overwrite registered_at_ms: got %d", got2.GetRegisteredAtMs())
	}

	// Lookup variants.
	any1, err := f.Lookup(LookupDeployment{ID: "inproc-abc"})
	if err != nil {
		t.Fatal(err)
	}
	if any1.(*enginev1.DeploymentRecord).GetId() != "inproc-abc" {
		t.Errorf("LookupDeployment returned %+v", any1)
	}
	any2, err := f.Lookup(LookupDeployments{})
	if err != nil {
		t.Fatal(err)
	}
	if list := any2.([]*enginev1.DeploymentRecord); len(list) != 1 {
		t.Errorf("LookupDeployments len = %d; want 1", len(list))
	}
}

func TestCluster_RegisterDeployment_MissingRecord(t *testing.T) {
	f, _, _ := newTestFSM(t)
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_RegisterDeployment{
			RegisterDeployment: &enginev1.RegisterDeployment{},
		},
	}
	// Missing record is a warn-and-drop; Update must still succeed and
	// advance applied_index.
	if _, err := f.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
}

func TestCluster_LookupPartitionTable_Empty(t *testing.T) {
	f, _, _ := newTestFSM(t)
	got, err := f.Lookup(LookupPartitionTable{})
	if err != nil {
		t.Fatal(err)
	}
	// Returns (*enginev1.PartitionTable)(nil) when absent.
	pt, ok := got.(*enginev1.PartitionTable)
	if !ok {
		t.Fatalf("unexpected lookup type %T", got)
	}
	if pt != nil {
		t.Fatalf("expected nil PartitionTable for absent row; got %+v", pt)
	}
}

func TestCluster_OpenReturnsAppliedIndex(t *testing.T) {
	f, _, _ := newTestFSM(t)
	// Apply some entry to bump applied_index.
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_RegisterNode{
			RegisterNode: &enginev1.RegisterNode{Member: &enginev1.NodeMembership{NodeId: 1, RaftAddr: "x"}},
		},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: 42, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	idx, err := f.Open(nil)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 42 {
		t.Fatalf("Open() = %d; want 42", idx)
	}
}

// applyPartitionTable is a test helper that proposes UpdatePartitionTable
// and runs Update so subsequent tests can start from a seeded table.
func applyPartitionTable(t *testing.T, f *FSM, idx uint64, pt *enginev1.PartitionTable) {
	t.Helper()
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpdatePartitionTable{
			UpdatePartitionTable: &enginev1.UpdatePartitionTable{Table: pt},
		},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
}

func applyRegisterNode(t *testing.T, f *FSM, idx uint64, mem *enginev1.NodeMembership) {
	t.Helper()
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_RegisterNode{RegisterNode: &enginev1.RegisterNode{Member: mem}},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
}

func TestCluster_EvictNodeZeroesLastSeenAndEnqueuesDeleteSteps(t *testing.T) {
	f, _, st := newTestFSM(t)
	applyRegisterNode(t, f, 1, &enginev1.NodeMembership{NodeId: 3, RaftAddr: "10.0.0.3:9091", LastSeenMs: 1700})
	applyPartitionTable(t, f, 2, &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards: map[uint64]*enginev1.ReplicaSet{
			1: {NodeIds: []uint64{1, 2, 3}},
			2: {NodeIds: []uint64{1, 2}}, // node 3 absent
			3: {NodeIds: []uint64{1, 2, 3}},
		},
	})

	cmd := &enginev1.Command{Kind: &enginev1.Command_EvictNode{EvictNode: &enginev1.EvictNode{NodeId: 3}}}
	if _, err := f.Update([]statemachine.Entry{{Index: 3, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}

	m, _ := (MembershipTable{S: st}).Get(3)
	if m == nil || m.GetLastSeenMs() != 0 {
		t.Fatalf("expected node 3 last_seen_ms zeroed; got %+v", m)
	}
	pt, _ := (PartitionTableTable{S: st}).Get()
	if pt == nil {
		t.Fatal("expected partition table")
	}
	// Shards 1 and 3 had node 3; shard 2 did not. Expect exactly two pending steps.
	if len(pt.GetPending()) != 2 {
		t.Fatalf("pending = %d; want 2 (shards 1+3)", len(pt.GetPending()))
	}
	for _, p := range pt.GetPending() {
		if p.GetKind() != enginev1.RebalanceStep_DELETE_REPLICA || p.GetRemoveNodeId() != 3 {
			t.Errorf("unexpected pending step: %+v", p)
		}
	}

	// Re-applying EvictNode is a no-op (last_seen already 0).
	if _, err := f.Update([]statemachine.Entry{{Index: 4, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	pt2, _ := (PartitionTableTable{S: st}).Get()
	if len(pt2.GetPending()) != 2 {
		t.Fatalf("re-apply EvictNode bumped pending to %d; want 2", len(pt2.GetPending()))
	}
}

func TestCluster_BeginRebalanceStep_IdempotentOnStepID(t *testing.T) {
	f, _, st := newTestFSM(t)
	applyPartitionTable(t, f, 1, &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards:          map[uint64]*enginev1.ReplicaSet{1: {NodeIds: []uint64{1, 2, 3}}},
	})
	step := &enginev1.RebalanceStep{
		ShardId: 1, Kind: enginev1.RebalanceStep_ADD_NON_VOTING, AddNodeId: 4, StepId: 1,
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_BeginRebalanceStep{BeginRebalanceStep: &enginev1.BeginRebalanceStep{Step: step}}}
	if _, err := f.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Update([]statemachine.Entry{{Index: 3, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	pt, _ := (PartitionTableTable{S: st}).Get()
	if len(pt.GetPending()) != 1 {
		t.Fatalf("duplicate BeginRebalanceStep produced %d pending; want 1", len(pt.GetPending()))
	}
}

func TestCluster_CompleteRebalanceStep_PromoteAddsToReplicaSet(t *testing.T) {
	f, _, st := newTestFSM(t)
	applyPartitionTable(t, f, 1, &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards:          map[uint64]*enginev1.ReplicaSet{1: {NodeIds: []uint64{1, 2, 3}}},
		Pending: []*enginev1.RebalanceStep{{
			ShardId: 1, Kind: enginev1.RebalanceStep_PROMOTE_TO_VOTER, AddNodeId: 4, StepId: 1,
		}},
	})
	cmd := &enginev1.Command{Kind: &enginev1.Command_CompleteRebalanceStep{
		CompleteRebalanceStep: &enginev1.CompleteRebalanceStep{ShardId: 1, StepId: 1},
	}}
	if _, err := f.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	pt, _ := (PartitionTableTable{S: st}).Get()
	if len(pt.GetPending()) != 0 {
		t.Fatalf("pending should be empty; got %d", len(pt.GetPending()))
	}
	if !replicaSetContains(pt.GetShards()[1], 4) {
		t.Fatalf("node 4 not promoted into shard 1 replica set: %+v", pt.GetShards()[1].GetNodeIds())
	}
	if pt.GetAssignmentEpoch() != 2 {
		t.Fatalf("assignment_epoch = %d; want 2", pt.GetAssignmentEpoch())
	}
}

func TestCluster_CompleteRebalanceStep_DeleteRemovesFromReplicaSet(t *testing.T) {
	f, _, st := newTestFSM(t)
	applyPartitionTable(t, f, 1, &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards:          map[uint64]*enginev1.ReplicaSet{1: {NodeIds: []uint64{1, 2, 3}}},
		Pending: []*enginev1.RebalanceStep{{
			ShardId: 1, Kind: enginev1.RebalanceStep_DELETE_REPLICA, RemoveNodeId: 3, StepId: 1,
		}},
	})
	cmd := &enginev1.Command{Kind: &enginev1.Command_CompleteRebalanceStep{
		CompleteRebalanceStep: &enginev1.CompleteRebalanceStep{ShardId: 1, StepId: 1},
	}}
	if _, err := f.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	pt, _ := (PartitionTableTable{S: st}).Get()
	if replicaSetContains(pt.GetShards()[1], 3) {
		t.Fatalf("node 3 still in replica set: %+v", pt.GetShards()[1].GetNodeIds())
	}
}

func TestCluster_BeginRebalanceStep_AcceptsShardZero(t *testing.T) {
	// Shard 0 is the metadata Raft group itself — the rebalance pipeline
	// now carries its membership changes uniformly.
	f, _, st := newTestFSM(t)
	applyPartitionTable(t, f, 1, &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards:          map[uint64]*enginev1.ReplicaSet{1: {NodeIds: []uint64{1, 2, 3}}},
		MetaReplicas:    &enginev1.ReplicaSet{NodeIds: []uint64{1, 2, 3}},
	})
	step := &enginev1.RebalanceStep{
		ShardId: 0, Kind: enginev1.RebalanceStep_PROMOTE_TO_VOTER, AddNodeId: 4, StepId: 1,
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_BeginRebalanceStep{
		BeginRebalanceStep: &enginev1.BeginRebalanceStep{Step: step},
	}}
	if _, err := f.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	pt, _ := (PartitionTableTable{S: st}).Get()
	if len(pt.GetPending()) != 1 || pt.GetPending()[0].GetShardId() != 0 {
		t.Fatalf("expected one shard-0 pending step; got %+v", pt.GetPending())
	}
}

func TestCluster_CompleteRebalanceStep_ShardZeroPromoteUpdatesMetaReplicas(t *testing.T) {
	f, _, st := newTestFSM(t)
	applyPartitionTable(t, f, 1, &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards:          map[uint64]*enginev1.ReplicaSet{1: {NodeIds: []uint64{1, 2, 3}}},
		MetaReplicas:    &enginev1.ReplicaSet{NodeIds: []uint64{1, 2, 3}},
		Pending: []*enginev1.RebalanceStep{{
			ShardId: 0, Kind: enginev1.RebalanceStep_PROMOTE_TO_VOTER, AddNodeId: 4, StepId: 1,
		}},
	})
	cmd := &enginev1.Command{Kind: &enginev1.Command_CompleteRebalanceStep{
		CompleteRebalanceStep: &enginev1.CompleteRebalanceStep{ShardId: 0, StepId: 1},
	}}
	if _, err := f.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	pt, _ := (PartitionTableTable{S: st}).Get()
	if !replicaSetContains(pt.GetMetaReplicas(), 4) {
		t.Fatalf("node 4 not added to MetaReplicas: %+v", pt.GetMetaReplicas().GetNodeIds())
	}
	if len(pt.GetPending()) != 0 {
		t.Fatalf("pending not drained: %+v", pt.GetPending())
	}
	// assignment_epoch must NOT bump for shard-0 changes — routing
	// doesn't depend on metadata-shard membership.
	if pt.GetAssignmentEpoch() != 1 {
		t.Fatalf("assignment_epoch = %d; want 1 (shard-0 changes don't bump)", pt.GetAssignmentEpoch())
	}
}

func TestCluster_CompleteRebalanceStep_ShardZeroDeleteRemovesFromMetaReplicas(t *testing.T) {
	f, _, st := newTestFSM(t)
	applyPartitionTable(t, f, 1, &enginev1.PartitionTable{
		AssignmentEpoch: 5,
		Shards:          map[uint64]*enginev1.ReplicaSet{1: {NodeIds: []uint64{1, 2, 3}}},
		MetaReplicas:    &enginev1.ReplicaSet{NodeIds: []uint64{1, 2, 3, 4}},
		Pending: []*enginev1.RebalanceStep{{
			ShardId: 0, Kind: enginev1.RebalanceStep_DELETE_REPLICA, RemoveNodeId: 4, StepId: 1,
		}},
	})
	cmd := &enginev1.Command{Kind: &enginev1.Command_CompleteRebalanceStep{
		CompleteRebalanceStep: &enginev1.CompleteRebalanceStep{ShardId: 0, StepId: 1},
	}}
	if _, err := f.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	pt, _ := (PartitionTableTable{S: st}).Get()
	if replicaSetContains(pt.GetMetaReplicas(), 4) {
		t.Fatalf("node 4 still in MetaReplicas: %+v", pt.GetMetaReplicas().GetNodeIds())
	}
	if pt.GetAssignmentEpoch() != 5 {
		t.Fatalf("assignment_epoch = %d; want 5 (shard-0 changes don't bump)", pt.GetAssignmentEpoch())
	}
}

func TestCluster_EvictNodeEnqueuesShardZeroDeleteWhenMetaVoter(t *testing.T) {
	f, _, st := newTestFSM(t)
	applyRegisterNode(t, f, 1, &enginev1.NodeMembership{NodeId: 3, RaftAddr: "10.0.0.3:9091", LastSeenMs: 1700})
	applyPartitionTable(t, f, 2, &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards: map[uint64]*enginev1.ReplicaSet{
			1: {NodeIds: []uint64{1, 2, 3}},
			2: {NodeIds: []uint64{1, 2}},
			3: {NodeIds: []uint64{1, 2, 3}},
		},
		MetaReplicas: &enginev1.ReplicaSet{NodeIds: []uint64{1, 2, 3}},
	})

	cmd := &enginev1.Command{Kind: &enginev1.Command_EvictNode{EvictNode: &enginev1.EvictNode{NodeId: 3}}}
	if _, err := f.Update([]statemachine.Entry{{Index: 3, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}
	pt, _ := (PartitionTableTable{S: st}).Get()
	// Expect 3 pending: shard 1 + shard 3 (partition) + shard 0 (meta).
	if len(pt.GetPending()) != 3 {
		t.Fatalf("pending = %d; want 3 (1+3+0)", len(pt.GetPending()))
	}
	// shard 0 step must appear last for byte-deterministic ordering.
	if pt.GetPending()[2].GetShardId() != 0 ||
		pt.GetPending()[2].GetKind() != enginev1.RebalanceStep_DELETE_REPLICA ||
		pt.GetPending()[2].GetRemoveNodeId() != 3 {
		t.Fatalf("last pending step = %+v; want shard 0 DELETE_REPLICA node 3", pt.GetPending()[2])
	}
}

func TestCluster_UnknownCommandIsDropped(t *testing.T) {
	f, _, _ := newTestFSM(t)
	cmd := &enginev1.Command{
		// Invoke is a partition-shard variant; shard 0 must drop it without
		// halting.
		Kind: &enginev1.Command_Invoke{
			Invoke: &enginev1.InvokeCommand{},
		},
	}
	if _, err := f.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatalf("Update should not error on unknown command kind; got %v", err)
	}
}
