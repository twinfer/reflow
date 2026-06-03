package invoker

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// stubProcessEngine is a ProcessEngine test double: it records the input it
// saw and returns a canned ProcessAdvanced (or an error).
type stubProcessEngine struct {
	gotInput ProcessAdvanceInput
	out      *enginev1.ProcessAdvanced
	err      error
}

func (s *stubProcessEngine) Advance(_ context.Context, in ProcessAdvanceInput) (*enginev1.ProcessAdvanced, error) {
	s.gotInput = in
	return s.out, s.err
}

// extEntry builds a ProcessInboxEntry carrying an opaque (external) payload.
func extEntry(event []byte, logicalMs uint64) *enginev1.ProcessInboxEntry {
	return &enginev1.ProcessInboxEntry{
		Payload:       &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_External{External: event}},
		LogicalTimeMs: logicalMs,
	}
}

func seedInstance(t *testing.T, store storage.Store, lp uint32, service, key string, rec *enginev1.ProcessInstanceRecord) {
	t.Helper()
	b := store.NewBatch()
	defer b.Close()
	if err := (tables.ProcessInstanceTable{S: store}).Put(b, lp, service, key, rec); err != nil {
		t.Fatalf("seed instance: %v", err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
}

func waitDone(t *testing.T, s *processSession) {
	t.Helper()
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("processSession did not finish")
	}
}

func TestProcessSession_RunsAdvanceAndProposes(t *testing.T) {
	store := storage.NewMemStore()
	procs := tables.ProcessInstanceTable{S: store}

	pk := routing.PartitionKey(0, "OrderProc", "order-42") // band 0
	lp := keys.LPFromPartitionKey(pk)
	seedInstance(t, store, lp, "OrderProc", "order-42", &enginev1.ProcessInstanceRecord{
		ModelRef:  &enginev1.ModelRef{Kind: "bpmn", Name: "OrderProc", Version: "v1"},
		Kind:      enginev1.ProcessKind_PROCESS_KIND_BPMN,
		StateBlob: []byte(`{"tokens":{}}`),
		Status:    enginev1.ProcessStatus_PROCESS_STATUS_RUNNING,
	})

	want := &enginev1.ProcessAdvanced{
		NewState: []byte(`{"tokens":{"t1":{}}}`),
		ArmTimer: []*enginev1.TimerArm{{NodeId: "Timer1", FireAtMs: 123, Slot: 1}},
	}
	eng := &stubProcessEngine{out: want}
	prop := &fakeProposer{}

	ref := processRef{pk: pk, service: "OrderProc", instanceKey: "order-42"}
	s := newProcessSession(context.Background(), ref, extEntry([]byte("evt-bytes"), 1000), eng, procs, prop, slog.Default())
	s.start()
	waitDone(t, s)

	// The engine saw the loaded record, the event, and the stamped time.
	if eng.gotInput.Entry.GetLogicalTimeMs() != 1000 || string(eng.gotInput.Entry.GetPayload().GetExternal()) != "evt-bytes" {
		t.Fatalf("engine input mismatch: %+v", eng.gotInput)
	}
	if eng.gotInput.Record == nil || string(eng.gotInput.Record.GetStateBlob()) != `{"tokens":{}}` {
		t.Fatalf("engine did not get the loaded record: %+v", eng.gotInput.Record)
	}

	// Exactly one ProcessAdvanced proposed, carrying the engine output + the
	// stamped addressing.
	if len(prop.cmds) != 1 {
		t.Fatalf("want 1 proposed command, got %d", len(prop.cmds))
	}
	adv := prop.cmds[0].GetProcessAdvanced()
	if adv == nil {
		t.Fatalf("proposed command is not ProcessAdvanced: %T", prop.cmds[0].GetKind())
	}
	if adv.GetPk() != pk || adv.GetService() != "OrderProc" || adv.GetInstanceKey() != "order-42" {
		t.Fatalf("addressing not stamped: %+v", adv)
	}
	if string(adv.GetNewState()) != `{"tokens":{"t1":{}}}` || len(adv.GetArmTimer()) != 1 {
		t.Fatalf("advance output not carried: %+v", adv)
	}
}

func TestProcessSession_AdvanceErrorFailsInstance(t *testing.T) {
	store := storage.NewMemStore()
	procs := tables.ProcessInstanceTable{S: store}

	pk := routing.PartitionKey(0, "S", "k")
	lp := keys.LPFromPartitionKey(pk)
	seedInstance(t, store, lp, "S", "k", &enginev1.ProcessInstanceRecord{StateBlob: []byte("blob")})

	eng := &stubProcessEngine{err: errors.New("feel boom")}
	prop := &fakeProposer{}
	s := newProcessSession(context.Background(), processRef{pk: pk, service: "S", instanceKey: "k"}, extEntry(nil, 5), eng, procs, prop, slog.Default())
	s.start()
	waitDone(t, s)

	if len(prop.cmds) != 1 {
		t.Fatalf("want 1 proposed command, got %d", len(prop.cmds))
	}
	adv := prop.cmds[0].GetProcessAdvanced()
	if adv == nil || adv.GetTerminal() == nil || !adv.GetTerminal().GetFailed() {
		t.Fatalf("want failed terminal, got %+v", adv)
	}
	if adv.GetTerminal().GetFailureMessage() != "feel boom" {
		t.Fatalf("want failure message carried, got %q", adv.GetTerminal().GetFailureMessage())
	}
	// On failure the blob is carried through unchanged (no state mutation).
	if string(adv.GetNewState()) != "blob" {
		t.Fatalf("want unchanged blob on failure, got %q", adv.GetNewState())
	}
}

func TestProcessSession_MissingInstanceDropsTurn(t *testing.T) {
	store := storage.NewMemStore()
	procs := tables.ProcessInstanceTable{S: store}

	eng := &stubProcessEngine{out: &enginev1.ProcessAdvanced{}}
	prop := &fakeProposer{}
	pk := routing.PartitionKey(0, "S", "gone")
	s := newProcessSession(context.Background(), processRef{pk: pk, service: "S", instanceKey: "gone"}, extEntry(nil, 1), eng, procs, prop, slog.Default())
	s.start()
	waitDone(t, s)

	// No record → no Advance, no propose.
	if eng.gotInput.Entry != nil {
		t.Fatalf("engine should not have been called: %+v", eng.gotInput)
	}
	if len(prop.cmds) != 0 {
		t.Fatalf("want 0 proposed commands, got %d", len(prop.cmds))
	}
}
