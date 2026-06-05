package processengine

import (
	"errors"
	"strings"
	"testing"
	"time"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflwos/dmn"
)

// libDMN defines namespace urn:lib with one decision.
const libDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="lib" namespace="urn:lib" id="lib">
  <decision name="libDecision" id="libDecision">
    <variable name="libDecision" typeRef="number"/>
    <literalExpression><text>42</text></literalExpression>
  </decision>
</definitions>`

// mainImportsLibDMN imports urn:lib.
const mainImportsLibDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="main" namespace="urn:main" id="main">
  <import namespace="urn:lib" name="lib" importType="https://www.omg.org/spec/DMN/20191111/MODEL/"/>
  <decision name="mainDecision" id="mainDecision">
    <variable name="mainDecision" typeRef="number"/>
    <literalExpression><text>1 + 1</text></literalExpression>
  </decision>
</definitions>`

// cycleA imports urn:cycleb; cycleB imports urn:cyclea — a cyclic import graph.
const cycleADMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="a" namespace="urn:cyclea" id="a">
  <import namespace="urn:cycleb" name="b" importType="https://www.omg.org/spec/DMN/20191111/MODEL/"/>
  <decision name="aDecision" id="aDecision"><variable name="aDecision" typeRef="number"/><literalExpression><text>1</text></literalExpression></decision>
</definitions>`

const cycleBDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="b" namespace="urn:cycleb" id="b">
  <import namespace="urn:cyclea" name="a" importType="https://www.omg.org/spec/DMN/20191111/MODEL/"/>
  <decision name="bDecision" id="bDecision"><variable name="bDecision" typeRef="number"/><literalExpression><text>2</text></literalExpression></decision>
</definitions>`

// discountDMNForPlan provides a decision named "discount".
const discountDMNForPlan = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="disc" namespace="urn:disc" id="disc">
  <decision name="discount" id="discount">
    <variable name="discount" typeRef="number"/>
    <literalExpression><text>0.1</text></literalExpression>
  </decision>
</definitions>`

// orderBPMN has a BusinessRuleTask referencing decision "discount" and a
// CallActivity calling child process "Shipping".
const orderBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="order" isExecutable="true">
    <startEvent id="s"><outgoing>f1</outgoing></startEvent>
    <businessRuleTask id="brt" name="discount"><incoming>f1</incoming><outgoing>f2</outgoing></businessRuleTask>
    <callActivity id="ca" name="ship" calledElement="Shipping"><incoming>f2</incoming><outgoing>f3</outgoing></callActivity>
    <endEvent id="e"><incoming>f3</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="s" targetRef="brt"/>
    <sequenceFlow id="f2" sourceRef="brt" targetRef="ca"/>
    <sequenceFlow id="f3" sourceRef="ca" targetRef="e"/>
  </process>
</definitions>`

// shippingBPMN is the child process, registered under model name "Shipping".
const shippingBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="Shipping" isExecutable="true">
    <startEvent id="s"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="s" targetRef="e"/>
    <endEvent id="e"><incoming>f1</incoming></endEvent>
  </process>
</definitions>`

func rec(kind, name, version, xml string) *enginev1.ModelRecord {
	return &enginev1.ModelRecord{
		ModelRef: &enginev1.ModelRef{Kind: kind, Name: name, Version: version},
		Xml:      []byte(xml),
	}
}

func findRec(t *testing.T, recs []*enginev1.ModelRecord, name string) *enginev1.ModelRecord {
	t.Helper()
	for _, r := range recs {
		if r.GetModelRef().GetName() == name {
			return r
		}
	}
	t.Fatalf("no record named %q in result", name)
	return nil
}

func TestPlanModelSet_DMNImportClosure(t *testing.T) {
	out, err := PlanModelSet([]*enginev1.ModelRecord{
		rec("dmn", "Main", "v1", mainImportsLibDMN),
		rec("dmn", "Lib", "v1", libDMN),
	}, nil)
	if err != nil {
		t.Fatalf("PlanModelSet: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("got %d records, want 2", len(out))
	}
	main := findRec(t, out, "Main")
	pin := main.GetBundle().GetImports()["urn:lib"]
	if pin == nil || pin.GetName() != "Lib" || pin.GetKind() != "dmn" {
		t.Fatalf("Main.bundle.imports[urn:lib] = %v, want dmn/Lib", pin)
	}
}

func TestPlanModelSet_ImportFromExistingTable(t *testing.T) {
	out, err := PlanModelSet(
		[]*enginev1.ModelRecord{rec("dmn", "Main", "v1", mainImportsLibDMN)},
		[]*enginev1.ModelRecord{rec("dmn", "Lib", "v1", libDMN)},
	)
	if err != nil {
		t.Fatalf("PlanModelSet: %v", err)
	}
	pin := findRec(t, out, "Main").GetBundle().GetImports()["urn:lib"]
	if pin == nil || pin.GetName() != "Lib" {
		t.Fatalf("Main import not pinned to existing Lib: %v", pin)
	}
}

func TestPlanModelSet_MissingImport(t *testing.T) {
	_, err := PlanModelSet(
		[]*enginev1.ModelRecord{rec("dmn", "Main", "v1", mainImportsLibDMN)},
		nil, // Lib is registered nowhere
	)
	if err == nil {
		t.Fatal("PlanModelSet accepted a set with an unresolved import; want error")
	}
	if !strings.Contains(err.Error(), "urn:lib") {
		t.Fatalf("error %q does not name the missing namespace urn:lib", err)
	}
}

func TestPlanModelSet_ImportCycle(t *testing.T) {
	_, err := PlanModelSet([]*enginev1.ModelRecord{
		rec("dmn", "A", "v1", cycleADMN),
		rec("dmn", "B", "v1", cycleBDMN),
	}, nil)
	if err == nil {
		t.Fatal("PlanModelSet accepted a cyclic import graph; want error")
	}
	if !errors.Is(err, dmn.ErrCyclicDependency) {
		t.Fatalf("error %q is not ErrCyclicDependency", err)
	}
}

// impBaseLibDMN provides decision "base" = 40 under namespace urn:implib.
const impBaseLibDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="implib" namespace="urn:implib" id="implib">
  <decision name="base" id="base"><variable name="base" typeRef="number"/><literalExpression><text>40</text></literalExpression></decision>
</definitions>`

// impTotalMainDMN imports urn:implib and computes total = lib.base + 2 = 42 — but
// only if the import resolves. Without the resolver, lib.base is unbound and total
// evaluates to null.
const impTotalMainDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" name="impmain" namespace="urn:impmain" id="impmain">
  <import namespace="urn:implib" name="lib" importType="https://www.omg.org/spec/DMN/20191111/MODEL/"/>
  <decision name="total" id="total">
    <variable name="total" typeRef="number"/>
    <informationRequirement><requiredDecision href="urn:implib#base"/></informationRequirement>
    <literalExpression><text>lib.base + 2</text></literalExpression>
  </decision>
</definitions>`

// impOrderBPMN's BusinessRuleTask references decision "total".
const impOrderBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="order" isExecutable="true">
    <startEvent id="s"><outgoing>f1</outgoing></startEvent>
    <businessRuleTask id="brt" name="total"><incoming>f1</incoming><outgoing>f2</outgoing></businessRuleTask>
    <endEvent id="e"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="s" targetRef="brt"/>
    <sequenceFlow id="f2" sourceRef="brt" targetRef="e"/>
  </process>
</definitions>`

// TestTableResolver_DMNImportResolvesAtEval is the end-to-end regression: plan a
// set whose BPMN BusinessRuleTask "total" resolves to a DMN that imports another
// DMN, reconcile it into a TableResolver, and evaluate the decision. total =
// lib.base + 2 = 42 proves the cross-file import resolved at eval — the exact
// path that silently produced null before the pins-only resolver was wired.
func TestTableResolver_DMNImportResolvesAtEval(t *testing.T) {
	out, err := PlanModelSet([]*enginev1.ModelRecord{
		rec("dmn", "Lib", "v1", impBaseLibDMN),
		rec("dmn", "Main", "v1", impTotalMainDMN),
		rec("bpmn", "Order", "v1", impOrderBPMN),
	}, nil)
	if err != nil {
		t.Fatalf("PlanModelSet: %v", err)
	}
	// The importer must carry the pin RegisterModelSet computes.
	if pin := findRec(t, out, "Main").GetBundle().GetImports()["urn:implib"]; pin == nil || pin.GetName() != "Lib" {
		t.Fatalf("Main import pin = %v, want dmn/Lib", pin)
	}

	tr := NewTableResolver(nil)
	tr.Reconcile(out)

	orderRef := &enginev1.ModelRef{Kind: "bpmn", Name: "Order", Version: "v1"}
	rt, evalRef, err := tr.BPMNDecisions(orderRef)("total")
	if err != nil {
		t.Fatalf("decision resolver: %v", err)
	}
	val, err := rt.Evaluate(time.Now().UTC(), evalRef, map[string]any{})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	var got float64
	switch v := val.(type) {
	case int64:
		got = float64(v)
	case float64:
		got = v
	default:
		t.Fatalf("total = %v (%T), want numeric 42 — imported lib.base did not resolve", val, val)
	}
	if got != 42 {
		t.Fatalf("total = %v, want 42 — imported lib.base did not resolve", got)
	}
}

// bpmnEndEventWithOutgoing is well-formed and parses, but an endEvent with an
// outgoing flow is a static defect (reflwos BPM001).
const bpmnEndEventWithOutgoing = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <endEvent id="mid"><incoming>f1</incoming><outgoing>f2</outgoing></endEvent>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="mid"/>
    <sequenceFlow id="f2" sourceRef="mid" targetRef="end"/>
  </process>
</definitions>`

// cmmnNoPlanItems parses but fails cmmn.Validate (root stage has no plan items).
const cmmnNoPlanItems = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/CMMN/20151109/MODEL">
  <case id="c"><casePlanModel id="stage0"/></case>
</definitions>`

// TestPlanModelSet_RejectsInvalidModels ports the per-kind validation coverage:
// the planner rejects parse failures and static defects for each kind (the checks
// that previously lived in ValidateModel, now exercised through the planner).
func TestPlanModelSet_RejectsInvalidModels(t *testing.T) {
	cases := []struct {
		name, kind, xml string
	}{
		{"bpmn static error", "bpmn", bpmnEndEventWithOutgoing},
		{"cmmn no plan items", "cmmn", cmmnNoPlanItems},
		{"malformed dmn", "dmn", `<definitions`},
		{"malformed xml", "bpmn", `<definitions><process id="p"`},
		{"unknown kind", "xslt", orderBPMN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := PlanModelSet([]*enginev1.ModelRecord{rec(tc.kind, "M", "v1", tc.xml)}, nil); err == nil {
				t.Fatalf("PlanModelSet(%s) = nil, want rejection", tc.name)
			}
		})
	}
}

func TestPlanModelSet_BPMNDecisionAndChild(t *testing.T) {
	out, err := PlanModelSet([]*enginev1.ModelRecord{
		rec("bpmn", "Order", "v1", orderBPMN),
		rec("bpmn", "Shipping", "v1", shippingBPMN),
		rec("dmn", "Discount", "v1", discountDMNForPlan),
	}, nil)
	if err != nil {
		t.Fatalf("PlanModelSet: %v", err)
	}
	b := findRec(t, out, "Order").GetBundle()
	if d := b.GetDecisions()["discount"]; d == nil || d.GetName() != "Discount" || d.GetKind() != "dmn" {
		t.Errorf("Order.bundle.decisions[discount] = %v, want dmn/Discount", d)
	}
	if c := b.GetChildren()["Shipping"]; c == nil || c.GetName() != "Shipping" || c.GetKind() != "bpmn" {
		t.Errorf("Order.bundle.children[Shipping] = %v, want bpmn/Shipping", c)
	}
}
