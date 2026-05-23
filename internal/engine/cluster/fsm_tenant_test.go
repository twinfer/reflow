package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func upsertTenantEnvelope(t *testing.T, rec *enginev1.TenantRecord, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_UpsertTenant{
				UpsertTenant: &enginev1.UpsertTenant{Record: rec},
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

func deleteTenantEnvelope(t *testing.T, id uint32, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_001},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_DeleteTenant{
				DeleteTenant: &enginev1.DeleteTenant{Id: id},
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

func newFSMWithTenantNotifier(t *testing.T) (*FSM, *TableNotifier, *stubLeadership) {
	t.Helper()
	f, lead, _ := newTestFSM(t)
	notifier := NewTableNotifier()
	f.cfg.Notifiers.TenantTable = notifier
	return f, notifier, lead
}

func TestCluster_UpsertTenant_BumpAndNotify(t *testing.T) {
	f, notifier, _ := newFSMWithTenantNotifier(t)
	rec := &enginev1.TenantRecord{
		Id:                       7,
		Name:                     "acme",
		MaxConcurrentInvocations: 100,
	}
	entries := []statemachine.Entry{{Index: 10, Cmd: upsertTenantEnvelope(t, rec, 0)}}
	res, err := f.Update(entries)
	if err != nil {
		t.Fatal(err)
	}
	if got := res[0].Result.Value; got == ResultValueFailedPrecondition {
		t.Fatalf("first upsert should not fail precondition; result.value=%d", got)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("expected notifier to fire after Upsert")
	}
	store := f.cfg.Snapshotter.Store()
	rev, err := (RevisionTable{S: store}).Get(RevisionTableTenant)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
	got, err := (TenantTable{S: store}).Get(7)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetName() != "acme" || got.GetMaxConcurrentInvocations() != 100 {
		t.Fatalf("row missing or mismatched: %+v", got)
	}
	// Name index should resolve back to the id.
	id, err := (TenantNameIndexTable{S: store}).Get("acme")
	if err != nil {
		t.Fatal(err)
	}
	if id != 7 {
		t.Fatalf("name index=%d; want 7", id)
	}
}

func TestCluster_UpsertTenant_RejectsZeroID(t *testing.T) {
	f, notifier, _ := newFSMWithTenantNotifier(t)
	rec := &enginev1.TenantRecord{Id: 0, Name: "acme"}
	entries := []statemachine.Entry{{Index: 1, Cmd: upsertTenantEnvelope(t, rec, 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatal(err)
	}
	// Notifier should NOT have fired (apply arm ignored the bad record).
	select {
	case <-notifier.Subscribe():
		t.Fatal("notifier fired on rejected upsert (id=0)")
	default:
	}
	rev, _ := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableTenant)
	if rev != 0 {
		t.Fatalf("rev=%d; want 0 (apply arm should have ignored id=0)", rev)
	}
}

func TestCluster_UpsertTenant_Rename_EvictsOldNameIndex(t *testing.T) {
	f, _, _ := newFSMWithTenantNotifier(t)
	// First upsert: id=3 name=old.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertTenantEnvelope(t, &enginev1.TenantRecord{Id: 3, Name: "old"}, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// Rename: same id, new name.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: upsertTenantEnvelope(t, &enginev1.TenantRecord{Id: 3, Name: "new"}, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	store := f.cfg.Snapshotter.Store()
	idx := TenantNameIndexTable{S: store}
	// New name resolves.
	gotNew, err := idx.Get("new")
	if err != nil {
		t.Fatal(err)
	}
	if gotNew != 3 {
		t.Fatalf("name new → id=%d; want 3", gotNew)
	}
	// Old name no longer resolves (must NOT linger as a dangling index).
	gotOld, err := idx.Get("old")
	if err != nil {
		t.Fatal(err)
	}
	if gotOld != 0 {
		t.Fatalf("name old → id=%d; want 0 (stale index entry leaked)", gotOld)
	}
}

func TestCluster_TenantCAS_RoundTrip(t *testing.T) {
	f, _, _ := newFSMWithTenantNotifier(t)
	rec := &enginev1.TenantRecord{Id: 1, Name: "acme", MaxConcurrentInvocations: 50}
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertTenantEnvelope(t, rec, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// Stale CAS: table is at 1, we expect 999.
	rec2 := proto.Clone(rec).(*enginev1.TenantRecord)
	rec2.MaxConcurrentInvocations = 100
	res, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: upsertTenantEnvelope(t, rec2, 999)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("expected failed-precondition sentinel; got %d", res[0].Result.Value)
	}
	store := f.cfg.Snapshotter.Store()
	got, _ := (TenantTable{S: store}).Get(1)
	if got.GetMaxConcurrentInvocations() != 50 {
		t.Fatalf("CAS-failed upsert leaked through; max=%d want 50", got.GetMaxConcurrentInvocations())
	}
	rev, _ := (RevisionTable{S: store}).Get(RevisionTableTenant)
	if rev != 1 {
		t.Fatalf("rev=%d; want 1 (CAS-failed apply must not bump)", rev)
	}
	// Fresh CAS=1 succeeds.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 3, Cmd: upsertTenantEnvelope(t, rec2, 1)},
	}); err != nil {
		t.Fatal(err)
	}
	rev, _ = (RevisionTable{S: store}).Get(RevisionTableTenant)
	if rev != 2 {
		t.Fatalf("rev=%d; want 2", rev)
	}
	got, _ = (TenantTable{S: store}).Get(1)
	if got.GetMaxConcurrentInvocations() != 100 {
		t.Fatalf("post-CAS row max=%d; want 100", got.GetMaxConcurrentInvocations())
	}
}

func TestCluster_DeleteTenant_BumpsRevisionEvenIfAbsent(t *testing.T) {
	f, notifier, _ := newFSMWithTenantNotifier(t)
	entries := []statemachine.Entry{{Index: 1, Cmd: deleteTenantEnvelope(t, 42, 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("delete-of-absent should still fire the notifier (table revision changed)")
	}
	rev, _ := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableTenant)
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
}

func TestCluster_DeleteTenant_RemovesNameIndex(t *testing.T) {
	f, _, _ := newFSMWithTenantNotifier(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertTenantEnvelope(t, &enginev1.TenantRecord{Id: 5, Name: "acme"}, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: deleteTenantEnvelope(t, 5, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	store := f.cfg.Snapshotter.Store()
	got, _ := (TenantTable{S: store}).Get(5)
	if got != nil {
		t.Fatalf("tenant row still present after delete: %+v", got)
	}
	id, _ := (TenantNameIndexTable{S: store}).Get("acme")
	if id != 0 {
		t.Fatalf("name index still resolves to id=%d after delete; want 0", id)
	}
}

func TestCluster_LookupTenantByName(t *testing.T) {
	f, _, _ := newFSMWithTenantNotifier(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertTenantEnvelope(t, &enginev1.TenantRecord{Id: 9, Name: "acme", MaxConcurrentInvocations: 42}, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := f.Lookup(LookupTenantByName{Name: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := got.(*enginev1.TenantRecord)
	if !ok {
		t.Fatalf("Lookup type=%T; want *enginev1.TenantRecord", got)
	}
	if rec == nil || rec.GetId() != 9 || rec.GetMaxConcurrentInvocations() != 42 {
		t.Fatalf("Lookup result=%+v; want id=9 max=42", rec)
	}
	// Absent name → nil.
	missing, err := f.Lookup(LookupTenantByName{Name: "no-such"})
	if err != nil {
		t.Fatal(err)
	}
	rec2, ok := missing.(*enginev1.TenantRecord)
	if !ok {
		t.Fatalf("Lookup miss type=%T; want *enginev1.TenantRecord (typed nil)", missing)
	}
	if rec2 != nil {
		t.Fatalf("Lookup miss returned %+v; want nil", rec2)
	}
}

func TestCluster_LookupTenants_ReturnsListAndRevision(t *testing.T) {
	f, _, _ := newFSMWithTenantNotifier(t)
	for i, name := range []string{"a", "b", "c"} {
		if _, err := f.Update([]statemachine.Entry{
			{Index: uint64(i + 1), Cmd: upsertTenantEnvelope(t,
				&enginev1.TenantRecord{Id: uint32(i + 1), Name: name}, 0)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := f.Lookup(LookupTenants{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := got.(*TenantList)
	if !ok {
		t.Fatalf("Lookup type=%T; want *TenantList", got)
	}
	if len(list.Tenants) != 3 {
		t.Fatalf("len(tenants)=%d; want 3", len(list.Tenants))
	}
	if list.TableRevision != 3 {
		t.Fatalf("table_revision=%d; want 3", list.TableRevision)
	}
	// Rows sort by id ascending (4-byte BE keys).
	for i, want := range []uint32{1, 2, 3} {
		if list.Tenants[i].GetId() != want {
			t.Fatalf("tenants[%d].id=%d; want %d", i, list.Tenants[i].GetId(), want)
		}
	}
}
