package processengine

import (
	"testing"

	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflwos/bpmn"
	"github.com/twinfer/reflwos/cmmn"
)

func topLevelInput() invoker.ProcessAdvanceInput {
	return invoker.ProcessAdvanceInput{
		Pk: 1, Service: "P", InstanceKey: "k",
		Record: &enginev1.ProcessInstanceRecord{ModelRef: &enginev1.ModelRef{Kind: "bpmn", Name: "P"}},
	}
}

func childInput() invoker.ProcessAdvanceInput {
	in := topLevelInput()
	in.Record.ParentLink = &enginev1.ParentLink{ProcessParent: &enginev1.ProcessParent{
		Pk: 2, Service: "Parent", InstanceKey: "pk", NodeId: "call1",
	}}
	return in
}

// TestTranslateBPMN_IncidentTaxonomy pins the adapter's incident-vs-terminal split:
// a top-level genuine uncaught failure parks as an incident; an escalation cause
// stays terminal (it must deliver to a parent CallActivity boundary); a child's
// genuine failure stays terminal (it delivers to its parent).
func TestTranslateBPMN_IncidentTaxonomy(t *testing.T) {
	a := New(NewMapResolver())

	// Top-level genuine failure → incident.
	adv, err := a.translateBPMN(topLevelInput(), nil, []bpmn.Command{bpmn.ProcessFailed{NodeID: "Task1", Cause: "boom"}}, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if adv.GetIncident() == nil || adv.GetTerminal() != nil {
		t.Fatalf("top-level genuine failure must be an incident: terminal=%v incident=%v", adv.GetTerminal(), adv.GetIncident())
	}
	if adv.GetIncident().GetNodeId() != "Task1" || adv.GetIncident().GetCause() != "boom" {
		t.Fatalf("incident = %+v", adv.GetIncident())
	}
	if string(adv.GetNewState()) != "s" {
		t.Fatalf("incident must carry new_state, got %q", adv.GetNewState())
	}

	// Top-level escalation → terminal (cross-process; ends / delivers).
	adv, err = a.translateBPMN(topLevelInput(), nil, []bpmn.Command{bpmn.ProcessFailed{NodeID: "Esc1", Cause: "escalation:CODE"}}, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if adv.GetTerminal() == nil || !adv.GetTerminal().GetFailed() || adv.GetIncident() != nil {
		t.Fatalf("escalation must stay terminal: terminal=%v incident=%v", adv.GetTerminal(), adv.GetIncident())
	}

	// Child genuine failure → terminal (delivers to parent).
	adv, err = a.translateBPMN(childInput(), nil, []bpmn.Command{bpmn.ProcessFailed{NodeID: "Task1", Cause: "boom"}}, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if adv.GetTerminal() == nil || adv.GetIncident() != nil {
		t.Fatalf("child failure must stay terminal (deliver to parent): terminal=%v incident=%v", adv.GetTerminal(), adv.GetIncident())
	}
}

// TestEventForBPMN_Retry maps a ProcessRetry inbox payload to the reflwos
// RetryIncident resume event, decoding the operator variable patch.
func TestEventForBPMN_Retry(t *testing.T) {
	ev, err := eventForBPMN(&enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_Retry{
		Retry: &enginev1.ProcessRetry{NodeId: "gw", VarPatch: []byte(`{"status":"approved"}`)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ri, ok := ev.(bpmn.RetryIncident)
	if !ok {
		t.Fatalf("event = %T, want bpmn.RetryIncident", ev)
	}
	if ri.NodeID != "gw" {
		t.Fatalf("NodeID = %q, want gw", ri.NodeID)
	}
	if ri.Vars["status"] != "approved" {
		t.Fatalf("Vars = %+v, want status=approved", ri.Vars)
	}
}

// TestEventForCMMN_Retry maps a ProcessRetry inbox payload to the reflwos
// ManualReactivate resume event (CMMN reactivate: Failed → Active), decoding the
// operator variable patch.
func TestEventForCMMN_Retry(t *testing.T) {
	ev, err := eventForCMMN(&enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_Retry{
		Retry: &enginev1.ProcessRetry{NodeId: "pi1", VarPatch: []byte(`{"fixed":true}`)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	mr, ok := ev.(cmmn.ManualReactivate)
	if !ok {
		t.Fatalf("event = %T, want cmmn.ManualReactivate", ev)
	}
	if mr.PlanItemID != "pi1" {
		t.Fatalf("PlanItemID = %q, want pi1", mr.PlanItemID)
	}
	if mr.Vars["fixed"] != true {
		t.Fatalf("Vars = %+v, want fixed=true", mr.Vars)
	}
}

// TestCMMNOpenIncident: the helper picks the lowest-sorted PIFailed item and its
// cause, and reports none when no item is Failed.
func TestCMMNOpenIncident(t *testing.T) {
	none := &cmmn.CaseState{Items: map[string]cmmn.PlanItemState{"a": cmmn.PIActive, "b": cmmn.PICompleted}}
	if _, _, ok := cmmnOpenIncident(none); ok {
		t.Fatalf("no Failed item must report no incident")
	}
	open := &cmmn.CaseState{
		Items:        map[string]cmmn.PlanItemState{"z": cmmn.PIActive, "m": cmmn.PIFailed, "x": cmmn.PIFailed},
		FailureCause: "boom",
	}
	node, cause, ok := cmmnOpenIncident(open)
	if !ok || node != "m" || cause != "boom" {
		t.Fatalf("open incident = (%q,%q,%v), want (m,boom,true)", node, cause, ok)
	}
}

// TestTranslateCMMN_IncidentTaxonomy pins the spec-aligned split: a CaseFailed is a
// case-level hard error → terminal (not an incident); a runtime plan-item fault (a
// PIFailed item in the case state) → a non-terminal incident on a top-level case,
// or a terminal delivery on a child case (the parent's case-task faults in turn).
func TestTranslateCMMN_IncidentTaxonomy(t *testing.T) {
	a := New(NewMapResolver())

	// CaseFailed (hard case-level error) → terminal, even top-level.
	adv, err := a.translateCMMN(topLevelInput(), []cmmn.Command{cmmn.CaseFailed{PlanItemID: "Item1", Cause: "bad binding"}}, &cmmn.CaseState{}, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if adv.GetTerminal() == nil || !adv.GetTerminal().GetFailed() || adv.GetIncident() != nil {
		t.Fatalf("CaseFailed must be terminal: terminal=%v incident=%v", adv.GetTerminal(), adv.GetIncident())
	}

	// A Failed plan item on a top-level case → non-terminal incident.
	failedState := &cmmn.CaseState{
		Items:        map[string]cmmn.PlanItemState{"Item1": cmmn.PIFailed},
		FailureCause: "boom",
	}
	adv, err = a.translateCMMN(topLevelInput(), nil, failedState, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if adv.GetIncident() == nil || adv.GetTerminal() != nil {
		t.Fatalf("top-level fault must be an incident: terminal=%v incident=%v", adv.GetTerminal(), adv.GetIncident())
	}
	if adv.GetIncident().GetNodeId() != "Item1" || adv.GetIncident().GetCause() != "boom" {
		t.Fatalf("incident = %+v", adv.GetIncident())
	}

	// The same fault on a child case → terminal (delivers to the parent).
	adv, err = a.translateCMMN(childInput(), nil, failedState, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if adv.GetTerminal() == nil || !adv.GetTerminal().GetFailed() || adv.GetIncident() != nil {
		t.Fatalf("child fault must stay terminal (deliver to parent): terminal=%v incident=%v", adv.GetTerminal(), adv.GetIncident())
	}

	// PlanItemFaulted as a command must not trip the unsupported-command default.
	if _, err := a.translateCMMN(topLevelInput(), []cmmn.Command{cmmn.PlanItemFaulted{PlanItemID: "Item1", Cause: "boom"}}, failedState, []byte("s")); err != nil {
		t.Fatalf("PlanItemFaulted command must be tolerated: %v", err)
	}
}
