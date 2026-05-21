package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func upsertWebhookEnvelope(t *testing.T, rec *enginev1.WebhookSourceRecord, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_UpsertWebhookSource{
				UpsertWebhookSource: &enginev1.UpsertWebhookSource{Record: rec},
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

func deleteWebhookEnvelope(t *testing.T, name string, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_001},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_DeleteWebhookSource{
				DeleteWebhookSource: &enginev1.DeleteWebhookSource{Name: name},
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

func newFSMWithWebhookNotifier(t *testing.T) (*FSM, *TableNotifier) {
	t.Helper()
	f, _, _ := newTestFSM(t)
	notifier := NewTableNotifier()
	f.cfg.Notifiers.WebhookSourceTable = notifier
	return f, notifier
}

func webhookRec(name, path string) *enginev1.WebhookSourceRecord {
	return &enginev1.WebhookSourceRecord{
		Name:     name,
		Path:     path,
		Verifier: "github",
		SecretRef: &enginev1.SecretRef{
			Source: &enginev1.SecretRef_EnvVarName{EnvVarName: "TEST_SECRET"},
		},
		Service: "svc",
		Handler: "on",
	}
}

func TestCluster_UpsertWebhookSource_BumpAndNotify(t *testing.T) {
	f, notifier := newFSMWithWebhookNotifier(t)
	rec := webhookRec("github-prod", "/webhooks/github")
	entries := []statemachine.Entry{{Index: 10, Cmd: upsertWebhookEnvelope(t, rec, 0)}}
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
	rev, err := (RevisionTable{S: store}).Get(RevisionTableWebhookSource)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
	got, err := (WebhookSourceTable{S: store}).Get("github-prod")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetPath() != "/webhooks/github" {
		t.Fatalf("row missing or mismatched: %+v", got)
	}
	if got.GetSecretRef().GetEnvVarName() != "TEST_SECRET" {
		t.Errorf("SecretRef not persisted: %+v", got.GetSecretRef())
	}
}

func TestCluster_WebhookCAS_RoundTrip(t *testing.T) {
	f, _ := newFSMWithWebhookNotifier(t)
	rec := webhookRec("src", "/webhooks/a")
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertWebhookEnvelope(t, rec, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// Stale CAS — expecting 0, table is now at 1.
	rec2 := proto.Clone(rec).(*enginev1.WebhookSourceRecord)
	rec2.Path = "/webhooks/different"
	res, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: upsertWebhookEnvelope(t, rec2, 999)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("expected failed-precondition sentinel; got %d", res[0].Result.Value)
	}
	store := f.cfg.Snapshotter.Store()
	got, _ := (WebhookSourceTable{S: store}).Get("src")
	if got.GetPath() != "/webhooks/a" {
		t.Fatalf("CAS-failed upsert leaked through; path=%q", got.GetPath())
	}
	rev, _ := (RevisionTable{S: store}).Get(RevisionTableWebhookSource)
	if rev != 1 {
		t.Fatalf("rev=%d; want 1 (CAS-fail must not bump)", rev)
	}
	// Correct CAS=1 succeeds and bumps to 2.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 3, Cmd: upsertWebhookEnvelope(t, rec2, 1)},
	}); err != nil {
		t.Fatal(err)
	}
	rev, _ = (RevisionTable{S: store}).Get(RevisionTableWebhookSource)
	if rev != 2 {
		t.Fatalf("rev=%d; want 2", rev)
	}
}

func TestCluster_DeleteWebhookSource_BumpsEvenIfAbsent(t *testing.T) {
	f, notifier := newFSMWithWebhookNotifier(t)
	entries := []statemachine.Entry{{Index: 1, Cmd: deleteWebhookEnvelope(t, "no-such", 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("delete-of-absent should still fire the notifier")
	}
	rev, _ := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableWebhookSource)
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
}

func TestCluster_WebhookLookupReturnsListAndRevision(t *testing.T) {
	f, _ := newFSMWithWebhookNotifier(t)
	for i, name := range []string{"a", "b", "c"} {
		entries := []statemachine.Entry{{
			Index: uint64(i + 1),
			Cmd:   upsertWebhookEnvelope(t, webhookRec(name, "/webhooks/"+name), 0),
		}}
		if _, err := f.Update(entries); err != nil {
			t.Fatal(err)
		}
	}
	res, err := f.Lookup(LookupWebhookSources{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := res.(*WebhookSourceList)
	if !ok {
		t.Fatalf("Lookup type = %T; want *WebhookSourceList", res)
	}
	if len(list.Sources) != 3 {
		t.Fatalf("len=%d; want 3", len(list.Sources))
	}
	if list.TableRevision != 3 {
		t.Fatalf("rev=%d; want 3", list.TableRevision)
	}
}
