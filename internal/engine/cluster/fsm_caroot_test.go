package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func upsertCARootEnvelope(t *testing.T, rec *enginev1.CARootRecord, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_UpsertCaRoot{
				UpsertCaRoot: &enginev1.UpsertCARoot{Record: rec},
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

func deleteCARootEnvelope(t *testing.T, name string, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_001},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_DeleteCaRoot{
				DeleteCaRoot: &enginev1.DeleteCARoot{Name: name},
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

func newFSMWithCARootNotifier(t *testing.T) (*FSM, *TableNotifier) {
	t.Helper()
	f, _, _ := newTestFSM(t)
	notifier := NewTableNotifier()
	f.cfg.Notifiers.CARootTable = notifier
	return f, notifier
}

func caRootRec(name, keySecret, fingerprint string) *enginev1.CARootRecord {
	return &enginev1.CARootRecord{
		Name:          name,
		CertPem:       []byte("-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"),
		KeySecretName: keySecret,
		Fingerprint:   fingerprint,
		RotationEpoch: 1,
		CreatedAtMs:   1_700_000_000_000,
	}
}

func TestCluster_UpsertCARoot_BumpAndNotify(t *testing.T) {
	f, notifier := newFSMWithCARootNotifier(t)
	rec := caRootRec("active", "ca/root/active", "sha256:abc")
	entries := []statemachine.Entry{{Index: 10, Cmd: upsertCARootEnvelope(t, rec, 0)}}
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
	rev, err := (RevisionTable{S: store}).Get(RevisionTableCARoot)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
	got, err := (CARootTable{S: store}).Get("active")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetKeySecretName() != "ca/root/active" {
		t.Fatalf("row missing or mismatched: %+v", got)
	}
	if got.GetFingerprint() != "sha256:abc" {
		t.Errorf("fingerprint not persisted: got %q", got.GetFingerprint())
	}
}

func TestCluster_CARootCAS_RoundTrip(t *testing.T) {
	f, _ := newFSMWithCARootNotifier(t)
	rec := caRootRec("active", "ca/root/active", "sha256:1")
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertCARootEnvelope(t, rec, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// Stale CAS — expecting 0, table is now at 1.
	rec2 := proto.Clone(rec).(*enginev1.CARootRecord)
	rec2.Fingerprint = "sha256:2"
	res, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: upsertCARootEnvelope(t, rec2, 999)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("expected failed-precondition sentinel; got %d", res[0].Result.Value)
	}
	store := f.cfg.Snapshotter.Store()
	got, _ := (CARootTable{S: store}).Get("active")
	if got.GetFingerprint() != "sha256:1" {
		t.Fatalf("CAS-failed upsert leaked through; fingerprint=%q", got.GetFingerprint())
	}
	rev, _ := (RevisionTable{S: store}).Get(RevisionTableCARoot)
	if rev != 1 {
		t.Fatalf("rev=%d; want 1 (CAS-fail must not bump)", rev)
	}
	// Correct CAS=1 succeeds and bumps to 2.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 3, Cmd: upsertCARootEnvelope(t, rec2, 1)},
	}); err != nil {
		t.Fatal(err)
	}
	rev, _ = (RevisionTable{S: store}).Get(RevisionTableCARoot)
	if rev != 2 {
		t.Fatalf("rev=%d; want 2", rev)
	}
}

func TestCluster_DeleteCARoot_BumpsEvenIfAbsent(t *testing.T) {
	f, notifier := newFSMWithCARootNotifier(t)
	entries := []statemachine.Entry{{Index: 1, Cmd: deleteCARootEnvelope(t, "no-such", 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("delete-of-absent should still fire the notifier")
	}
	rev, _ := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableCARoot)
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
}

func TestCluster_CARootLookupReturnsListAndRevision(t *testing.T) {
	f, _ := newFSMWithCARootNotifier(t)
	for i, name := range []string{"a", "b", "c"} {
		entries := []statemachine.Entry{{
			Index: uint64(i + 1),
			Cmd:   upsertCARootEnvelope(t, caRootRec(name, "ca/key/"+name, "sha256:"+name), 0),
		}}
		if _, err := f.Update(entries); err != nil {
			t.Fatal(err)
		}
	}
	res, err := f.Lookup(LookupCARoots{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := res.(*CARootList)
	if !ok {
		t.Fatalf("Lookup type = %T; want *CARootList", res)
	}
	if len(list.Records) != 3 {
		t.Fatalf("len=%d; want 3", len(list.Records))
	}
	if list.TableRevision != 3 {
		t.Fatalf("rev=%d; want 3", list.TableRevision)
	}
}
