package cluster

import (
	"path/filepath"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func setDrainEnvelope(t *testing.T, shardID uint64, drain bool, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_SetRebalanceDrain{
				SetRebalanceDrain: &enginev1.SetRebalanceDrain{
					ShardId: shardID,
					Drain:   drain,
				},
			},
		},
	}
	if ifRev != 0 {
		env.Precondition = &enginev1.Precondition{IfTableRevisionEq: ifRev}
	}
	buf, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestCluster_SetRebalanceDrain_AddRemove(t *testing.T) {
	f, _, _ := newTestFSM(t)
	// Add drain for shard 3.
	res, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: setDrainEnvelope(t, 3, true, 0)}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value == ResultValueFailedPrecondition {
		t.Fatalf("first SetRebalanceDrain should not trip CAS")
	}
	store := f.cfg.Snapshotter.Store()
	rec, err := (RebalanceDrainTable{S: store}).Get(3)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("drain row missing after add")
	}
	if rec.GetShardId() != 3 {
		t.Fatalf("rec.shard_id=%d; want 3", rec.GetShardId())
	}
	if rec.GetAddedAtMs() != 1_700_000_000_000 {
		t.Fatalf("rec.added_at_ms=%d; want 1_700_000_000_000", rec.GetAddedAtMs())
	}
	rev, _ := (RevisionTable{S: store}).Get(RevisionTableRebalanceDrain)
	if rev != 1 {
		t.Fatalf("rev after add=%d; want 1", rev)
	}

	// Remove the same drain (drain=false).
	if _, err := f.Update([]statemachine.Entry{{Index: 11, Cmd: setDrainEnvelope(t, 3, false, 1)}}); err != nil {
		t.Fatal(err)
	}
	rec, _ = (RebalanceDrainTable{S: store}).Get(3)
	if rec != nil {
		t.Fatal("drain row still present after remove")
	}
	rev, _ = (RevisionTable{S: store}).Get(RevisionTableRebalanceDrain)
	if rev != 2 {
		t.Fatalf("rev after remove=%d; want 2", rev)
	}
}

func TestCluster_SetRebalanceDrain_CASMismatch(t *testing.T) {
	f, _, _ := newTestFSM(t)
	if _, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: setDrainEnvelope(t, 3, true, 0)}}); err != nil {
		t.Fatal(err)
	}
	// Stale ifRev — current is 1, this expects 99 → must trip CAS.
	res, err := f.Update([]statemachine.Entry{{Index: 11, Cmd: setDrainEnvelope(t, 4, true, 99)}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("expected CAS sentinel; got result.value=%d", res[0].Result.Value)
	}
	store := f.cfg.Snapshotter.Store()
	// Shard 4 must not be present.
	rec, _ := (RebalanceDrainTable{S: store}).Get(4)
	if rec != nil {
		t.Fatal("shard 4 drain row written despite CAS failure")
	}
}

func TestCluster_SetRebalanceDrain_NotifierFires(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "meta", "state")
	st, err := storage.OpenPebble(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	n := NewTableNotifier()
	lead := &stubLeadership{}
	lead.leader.Store(true)
	f := New(0, 1, Config{
		Snapshotter: &stubSnapshotter{store: st},
		Leadership:  lead,
		Notifiers:   Notifiers{RebalanceDrainTable: n},
	})
	if _, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: setDrainEnvelope(t, 3, true, 0)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-n.Subscribe():
	default:
		t.Fatal("RebalanceDrainTable notifier did not fire after SetRebalanceDrain")
	}
}

func TestCluster_SetRebalanceDrain_ZeroShardIgnored(t *testing.T) {
	f, _, _ := newTestFSM(t)
	// shard_id=0 is the metadata group — never a valid drain target.
	if _, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: setDrainEnvelope(t, 0, true, 0)}}); err != nil {
		t.Fatal(err)
	}
	store := f.cfg.Snapshotter.Store()
	rec, _ := (RebalanceDrainTable{S: store}).Get(0)
	if rec != nil {
		t.Fatal("shard 0 drain row written; should have been ignored")
	}
	rev, _ := (RevisionTable{S: store}).Get(RevisionTableRebalanceDrain)
	if rev != 0 {
		t.Fatalf("rev=%d after zero-shard ignore; want 0", rev)
	}
}
