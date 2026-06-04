package iflowengine

import (
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// validDMN is a minimal decision file (one literal-expression decision).
// dmn.NewRuntime compiles it; the resolver only needs it to resolve to a
// non-nil runtime, so the decision id is not exercised here.
const validDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" name="test" id="_test">
  <decision name="d1" id="_d1">
    <variable name="d1"/>
    <literalExpression><text>1 + 1</text></literalExpression>
  </decision>
</definitions>`

// TestTableResolver_Bundle drives the bundle-resolution path: a BPMN model whose
// bundle pins a DMN decision and a child-ref override reconciles into a resolver
// that serves both, and a now-missing DMN preserves the previously-resolved
// model rather than dropping it.
func TestTableResolver_Bundle(t *testing.T) {
	r := NewTableResolver(nil)
	dmnRef := &enginev1.ModelRef{Kind: "dmn", Name: "CreditCheck", Version: "v1"}
	bpmnRef := &enginev1.ModelRef{Kind: "bpmn", Name: "Order", Version: "v1"}
	childRef := &enginev1.ModelRef{Kind: "bpmn", Name: "Shipping", Version: "v1"}
	bpmnRec := &enginev1.ModelRecord{
		ModelRef: bpmnRef,
		Xml:      []byte(bpmnWithTTL),
		Bundle: &enginev1.ModelBundle{
			Decisions: map[string]*enginev1.ModelRef{"checkCredit": dmnRef},
			Children:  map[string]*enginev1.ModelRef{"ship": childRef},
		},
	}
	r.Reconcile([]*enginev1.ModelRecord{
		{ModelRef: dmnRef, Xml: []byte(validDMN)},
		bpmnRec,
	})

	if _, err := r.BPMN(bpmnRef); err != nil {
		t.Fatalf("BPMN: %v", err)
	}
	// The bundle decision resolves to a runtime; an unlisted ref errors.
	if rt, _, err := r.BPMNDecisions(bpmnRef)("checkCredit"); err != nil || rt == nil {
		t.Fatalf("decision checkCredit: rt=%v err=%v", rt, err)
	}
	if _, _, err := r.BPMNDecisions(bpmnRef)("missing"); err == nil {
		t.Fatal("unlisted decision: want error")
	}
	// Child override wins; an unlisted calledElement falls back to convention.
	if got, err := r.ChildRef(bpmnRef, "bpmn", "ship"); err != nil || got.GetName() != "Shipping" {
		t.Fatalf("ChildRef override = %v, %v; want Shipping", got, err)
	}
	if got, _ := r.ChildRef(bpmnRef, "bpmn", "other"); got.GetName() != "other" || got.GetVersion() != "v1" {
		t.Fatalf("ChildRef convention = %v; want {bpmn,other,v1}", got)
	}

	// Drop the DMN row: Order's decision can no longer resolve, so the second
	// reconcile must preserve the previously-materialized model.
	r.Reconcile([]*enginev1.ModelRecord{bpmnRec})
	if rt, _, err := r.BPMNDecisions(bpmnRef)("checkCredit"); err != nil || rt == nil {
		t.Fatalf("after DMN removed, expected preserve-prev decision; rt=%v err=%v", rt, err)
	}
}
