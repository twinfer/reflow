package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func TestCluster_UpsertModelPersistsBumpsAndDeletes(t *testing.T) {
	f, _, st := newTestFSM(t)
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "Order", Version: "v1"}
	cmd := &enginev1.Command{Kind: &enginev1.Command_UpsertModel{
		UpsertModel: &enginev1.UpsertModel{Record: &enginev1.ModelRecord{ModelRef: ref, Xml: []byte("<definitions/>")}},
	}}
	if _, err := f.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, cmd)}}); err != nil {
		t.Fatal(err)
	}

	got, err := (ModelTable{S: st}).Get("bpmn", "Order", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || string(got.GetXml()) != "<definitions/>" {
		t.Fatalf("model row mismatch: %+v", got)
	}
	// registered_at_ms is stamped from the envelope header in the apply arm.
	if got.GetRegisteredAtMs() != 1_700_000_000_000 {
		t.Errorf("registered_at_ms = %d, want header stamp", got.GetRegisteredAtMs())
	}

	rev, err := (RevisionTable{S: st}).Get(RevisionTableModel)
	if err != nil {
		t.Fatal(err)
	}
	if rev == 0 {
		t.Error("model table revision not bumped")
	}

	ml := mustLookup[*ModelList](t, f, LookupModels{})
	if len(ml.Records) != 1 || ml.TableRevision != rev {
		t.Errorf("LookupModels = %d records, rev %d (table rev %d)", len(ml.Records), ml.TableRevision, rev)
	}

	// Delete removes the row and bumps the revision again.
	del := &enginev1.Command{Kind: &enginev1.Command_DeleteModel{DeleteModel: &enginev1.DeleteModel{ModelRef: ref}}}
	if _, err := f.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, del)}}); err != nil {
		t.Fatal(err)
	}
	gone, err := (ModelTable{S: st}).Get("bpmn", "Order", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if gone != nil {
		t.Fatalf("model not deleted: %+v", gone)
	}
	rev2, _ := (RevisionTable{S: st}).Get(RevisionTableModel)
	if rev2 <= rev {
		t.Errorf("delete did not bump revision: %d <= %d", rev2, rev)
	}
}

func TestCluster_UpsertModelCASMismatch(t *testing.T) {
	f, _, st := newTestFSM(t)
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "Order", Version: "v1"}
	cmd := &enginev1.Command{Kind: &enginev1.Command_UpsertModel{
		UpsertModel: &enginev1.UpsertModel{Record: &enginev1.ModelRecord{ModelRef: ref, Xml: []byte("<definitions/>")}},
	}}
	// Precondition revision=99 while the current model revision is 0 → CAS fails:
	// no row written, ResultValueFailedPrecondition stamped.
	buf, err := proto.Marshal(&enginev1.Envelope{
		Header:       &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command:      cmd,
		Precondition: &enginev1.Precondition{IfTableRevisionEq: 99},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.Update([]statemachine.Entry{{Index: 1, Cmd: buf}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Errorf("CAS-mismatch result.Value = %d, want %d", res[0].Result.Value, ResultValueFailedPrecondition)
	}
	got, err := (ModelTable{S: st}).Get("bpmn", "Order", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("CAS-failed upsert wrote a row: %+v", got)
	}
}

func mustLookup[T any](t *testing.T, f *FSM, q any) T {
	t.Helper()
	res, err := f.Lookup(q)
	if err != nil {
		t.Fatal(err)
	}
	out, ok := res.(T)
	if !ok {
		t.Fatalf("lookup returned %T, want %T", res, *new(T))
	}
	return out
}
