package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func upsertEnvelope(t *testing.T, rec *enginev1.EventSourceRecord, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_UpsertEventSource{
				UpsertEventSource: &enginev1.UpsertEventSource{Record: rec},
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

func deleteEnvelope(t *testing.T, name string, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_001},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_DeleteEventSource{
				DeleteEventSource: &enginev1.DeleteEventSource{Name: name},
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

func newFSMWithNotifier(t *testing.T) (*FSM, *TableNotifier, *stubLeadership) {
	t.Helper()
	f, lead, _ := newTestFSM(t)
	notifier := NewTableNotifier()
	// Re-wire the FSM's config to attach the notifier. The constructor
	// captured Config by value, so we mutate the field via a helper —
	// or rebuild. Rebuilding is cleaner:
	f.cfg.Notifiers.EventSourceTable = notifier
	return f, notifier, lead
}

func TestCluster_UpsertEventSource_BumpAndNotify(t *testing.T) {
	f, notifier, _ := newFSMWithNotifier(t)
	rec := &enginev1.EventSourceRecord{
		Name:    "orders-kafka",
		Type:    "kafka",
		Topic:   "orders.created",
		Service: "Billing",
		Handler: "OnOrder",
	}
	entries := []statemachine.Entry{{Index: 10, Cmd: upsertEnvelope(t, rec, 0)}}
	res, err := f.Update(entries)
	if err != nil {
		t.Fatal(err)
	}
	if got := res[0].Result.Value; got == ResultValueFailedPrecondition {
		t.Fatalf("first upsert should not fail precondition; result.value=%d", got)
	}
	// Notifier should have fired exactly once.
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("expected notifier to fire after Upsert")
	}
	// Revision should be 1.
	store := f.cfg.Snapshotter.Store()
	rev, err := (RevisionTable{S: store}).Get(RevisionTableEventSource)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
	// Row should be persisted.
	got, err := (EventSourceTable{S: store}).Get("orders-kafka")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetTopic() != "orders.created" {
		t.Fatalf("row missing or mismatched: %+v", got)
	}
}

func TestCluster_CAS_RoundTrip(t *testing.T) {
	f, _, _ := newFSMWithNotifier(t)
	rec := &enginev1.EventSourceRecord{
		Name: "src", Type: "kafka", Topic: "t",
		Service: "S", Handler: "H",
	}
	// First upsert: no CAS, lands.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertEnvelope(t, rec, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// Second upsert with stale CAS (expecting 0, table is now at 1).
	rec2 := proto.Clone(rec).(*enginev1.EventSourceRecord)
	rec2.Topic = "different"
	entries := []statemachine.Entry{{Index: 2, Cmd: upsertEnvelope(t, rec2, 999)}}
	res, err := f.Update(entries)
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("expected failed-precondition sentinel; got %d", res[0].Result.Value)
	}
	// Row should NOT have been mutated.
	got, err := (EventSourceTable{S: f.cfg.Snapshotter.Store()}).Get("src")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetTopic() != "t" {
		t.Fatalf("CAS-failed upsert leaked through; topic=%q want %q", got.GetTopic(), "t")
	}
	// Revision should still be 1.
	rev, err := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableEventSource)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1 (CAS-failed apply must not bump)", rev)
	}
	// Third upsert with correct CAS=1 succeeds and bumps to 2.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 3, Cmd: upsertEnvelope(t, rec2, 1)},
	}); err != nil {
		t.Fatal(err)
	}
	rev, _ = (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableEventSource)
	if rev != 2 {
		t.Fatalf("rev=%d; want 2", rev)
	}
}

func TestCluster_BatchInCoherence_CASLosesToFirstWriter(t *testing.T) {
	// Two CAS upserts in the same Update batch: first wins (rev N→N+1),
	// second sees N+1 via in-batch read coherence and fails. Mirrors
	// the partition-heal stranding pattern documented in fsm.go.
	f, _, _ := newFSMWithNotifier(t)
	rec1 := &enginev1.EventSourceRecord{Name: "a", Type: "kafka", Topic: "t", Service: "S", Handler: "H"}
	rec2 := &enginev1.EventSourceRecord{Name: "b", Type: "kafka", Topic: "t", Service: "S", Handler: "H"}
	entries := []statemachine.Entry{
		{Index: 1, Cmd: upsertEnvelope(t, rec1, 0)}, // no CAS — lands, rev 0→1
		{Index: 2, Cmd: upsertEnvelope(t, rec2, 0)}, // no CAS either — lands, rev 1→2
		{Index: 3, Cmd: upsertEnvelope(t, rec1, 1)}, // CAS-expects 1 — but in-batch rev is now 2 → fails
	}
	res, err := f.Update(entries)
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value == ResultValueFailedPrecondition {
		t.Fatal("entry 0 should land (no CAS)")
	}
	if res[1].Result.Value == ResultValueFailedPrecondition {
		t.Fatal("entry 1 should land (no CAS)")
	}
	if res[2].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("entry 2 should fail CAS (batch-bumped rev to 2); got value=%d", res[2].Result.Value)
	}
	rev, _ := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableEventSource)
	if rev != 2 {
		t.Fatalf("final rev=%d; want 2", rev)
	}
}

func TestCluster_DeleteEventSource_BumpsRevisionEvenIfAbsent(t *testing.T) {
	f, notifier, _ := newFSMWithNotifier(t)
	// Delete-of-absent: no row exists; the CAS-on-zero precondition
	// applies and the revision still bumps.
	entries := []statemachine.Entry{{Index: 1, Cmd: deleteEnvelope(t, "no-such", 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("delete-of-absent should still fire the notifier (table revision changed)")
	}
	rev, err := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableEventSource)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
}
