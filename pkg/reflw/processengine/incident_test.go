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

// TestTranslateCMMN_IncidentTaxonomy: a top-level CaseFailed parks as an incident;
// a child case failure stays terminal so it delivers to its parent. (CMMN has no
// escalation channel, so there is no escalation carve-out.)
func TestTranslateCMMN_IncidentTaxonomy(t *testing.T) {
	a := New(NewMapResolver())

	adv, err := a.translateCMMN(topLevelInput(), []cmmn.Command{cmmn.CaseFailed{PlanItemID: "Item1", Cause: "boom"}}, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if adv.GetIncident() == nil || adv.GetTerminal() != nil {
		t.Fatalf("top-level case failure must be an incident: terminal=%v incident=%v", adv.GetTerminal(), adv.GetIncident())
	}
	if adv.GetIncident().GetNodeId() != "Item1" || adv.GetIncident().GetCause() != "boom" {
		t.Fatalf("incident = %+v", adv.GetIncident())
	}

	adv, err = a.translateCMMN(childInput(), []cmmn.Command{cmmn.CaseFailed{PlanItemID: "Item1", Cause: "boom"}}, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if adv.GetTerminal() == nil || adv.GetIncident() != nil {
		t.Fatalf("child case failure must stay terminal (deliver to parent): terminal=%v incident=%v", adv.GetTerminal(), adv.GetIncident())
	}
}
