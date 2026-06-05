package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func bulkUpsertLPOwnersEnvelope(t *testing.T, records []*enginev1.LPOwnerRecord, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_BulkUpsertLpOwners{
				BulkUpsertLpOwners: &enginev1.BulkUpsertLPOwners{Records: records},
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

func upsertLPOwnerEnvelope(t *testing.T, lp uint32, shard uint64, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_001},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_UpsertLpOwner{
				UpsertLpOwner: &enginev1.UpsertLPOwner{
					Record: &enginev1.LPOwnerRecord{Lp: lp, ShardId: shard},
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

func deleteLPOwnerEnvelope(t *testing.T, lp uint32, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_002},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_DeleteLpOwner{
				DeleteLpOwner: &enginev1.DeleteLPOwner{Lp: lp},
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

func newFSMWithLPOwnersNotifier(t *testing.T) (*FSM, *TableNotifier) {
	t.Helper()
	f, _, _ := newTestFSM(t)
	notifier := NewTableNotifier()
	f.cfg.Notifiers.LPOwnersTable = notifier
	return f, notifier
}

func TestCluster_BulkUpsertLPOwners_SeedsAllRowsOneRevBump(t *testing.T) {
	f, notifier := newFSMWithLPOwnersNotifier(t)
	records := []*enginev1.LPOwnerRecord{
		{Lp: 0, ShardId: 1},
		{Lp: 1, ShardId: 2},
		{Lp: 2, ShardId: 3},
		{Lp: 4095, ShardId: 1},
	}
	entries := []statemachine.Entry{{Index: 1, Cmd: bulkUpsertLPOwnersEnvelope(t, records, 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatal(err)
	}
	// Notifier should fire exactly once for the whole batch.
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("expected LPOwners notifier to fire after BulkUpsert")
	}
	// Revision is 1, not len(records).
	store := f.cfg.Snapshotter.Store()
	rev, err := (RevisionTable{S: store}).Get(RevisionTableLPOwners)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1 (single bump per batch)", rev)
	}
	// Every row persisted.
	tab := LPOwnersTable{S: store}
	for _, want := range records {
		got, err := tab.Get(want.GetLp())
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.GetShardId() != want.GetShardId() {
			t.Errorf("lp=%d: got %+v; want shard=%d", want.GetLp(), got, want.GetShardId())
		}
	}
	// Snapshot returns the full map.
	snap, err := tab.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap) != len(records) {
		t.Fatalf("snapshot size=%d; want %d", len(snap), len(records))
	}
}

func TestCluster_BulkUpsertLPOwners_CASOnFreshTable(t *testing.T) {
	f, _ := newFSMWithLPOwnersNotifier(t)
	rec := []*enginev1.LPOwnerRecord{{Lp: 0, ShardId: 1}}
	// First seed at CAS=0 lands; second seed at CAS=999 (table is now rev 1) fails.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: bulkUpsertLPOwnersEnvelope(t, rec, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: bulkUpsertLPOwnersEnvelope(t, rec, 999)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("stale CAS should fail; result.value=%d want=%d",
			res[0].Result.Value, ResultValueFailedPrecondition)
	}
}

func TestCluster_UpsertLPOwner_FlipsOneRow(t *testing.T) {
	f, notifier := newFSMWithLPOwnersNotifier(t)
	// Seed two rows.
	seed := []*enginev1.LPOwnerRecord{
		{Lp: 10, ShardId: 1},
		{Lp: 20, ShardId: 2},
	}
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: bulkUpsertLPOwnersEnvelope(t, seed, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// Drain the first notifier signal so we can detect a second.
	<-notifier.Subscribe()

	// Flip lp=10 from shard 1 to shard 3.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: upsertLPOwnerEnvelope(t, 10, 3, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("Upsert should fire the notifier")
	}
	store := f.cfg.Snapshotter.Store()
	got, _ := (LPOwnersTable{S: store}).Get(10)
	if got == nil || got.GetShardId() != 3 {
		t.Fatalf("lp=10 = %+v; want shard=3", got)
	}
	// Other rows are untouched.
	other, _ := (LPOwnersTable{S: store}).Get(20)
	if other.GetShardId() != 2 {
		t.Fatalf("lp=20 = %d; want untouched 2", other.GetShardId())
	}
}

func TestCluster_DeleteLPOwner_RemovesRowAndBumps(t *testing.T) {
	f, notifier := newFSMWithLPOwnersNotifier(t)
	seed := []*enginev1.LPOwnerRecord{{Lp: 7, ShardId: 1}}
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: bulkUpsertLPOwnersEnvelope(t, seed, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	<-notifier.Subscribe()
	if _, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: deleteLPOwnerEnvelope(t, 7, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("Delete should fire the notifier")
	}
	got, _ := (LPOwnersTable{S: f.cfg.Snapshotter.Store()}).Get(7)
	if got != nil {
		t.Fatalf("lp=7 should be deleted; got %+v", got)
	}
	rev, _ := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableLPOwners)
	if rev != 2 {
		t.Fatalf("rev=%d; want 2 (seed + delete)", rev)
	}
}

func TestCluster_LookupLPOwners_ReturnsListAndRevision(t *testing.T) {
	f, _ := newFSMWithLPOwnersNotifier(t)
	seed := []*enginev1.LPOwnerRecord{
		{Lp: 5, ShardId: 1},
		{Lp: 6, ShardId: 2},
	}
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: bulkUpsertLPOwnersEnvelope(t, seed, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := f.Lookup(LookupLPOwners{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := res.(*LPOwnersList)
	if !ok {
		t.Fatalf("Lookup result type %T; want *LPOwnersList", res)
	}
	if list.TableRevision != 1 {
		t.Errorf("TableRevision = %d; want 1", list.TableRevision)
	}
	if len(list.Records) != len(seed) {
		t.Errorf("Records len = %d; want %d", len(list.Records), len(seed))
	}
}
