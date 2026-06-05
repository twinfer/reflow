package processengine

import (
	"context"
	"testing"

	"github.com/twinfer/reflwos/dmn"
)

// Start -> BusinessRuleTask(name="discount") -> End. The task name is the
// decision ref; the engine evaluates it inline via the wired DecisionResolver.
const businessRuleBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://test">
  <process id="dmnProc" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <businessRuleTask id="decide" name="discount">
      <incoming>f1</incoming><outgoing>f2</outgoing>
    </businessRuleTask>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="decide"/>
    <sequenceFlow id="f2" sourceRef="decide" targetRef="end"/>
  </process>
</definitions>`

const discountDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" name="m" id="_m">
  <decision name="discount" id="_d">
    <variable name="discount"/>
    <literalExpression><text>if tier = "gold" then 0.2 else 0.05</text></literalExpression>
  </decision>
</definitions>`

func TestAdvanceBPMN_BusinessRuleTaskInlineDMN(t *testing.T) {
	r := NewMapResolver()
	if err := r.ParseBPMN("dmnproc", "v1", []byte(businessRuleBPMN)); err != nil {
		t.Fatalf("parse bpmn: %v", err)
	}
	rt, err := dmn.NewRuntime([]byte(discountDMN))
	if err != nil {
		t.Fatalf("dmn runtime: %v", err)
	}
	r.AddDecision("dmnproc", "v1", "discount", rt)
	a := New(r)

	adv, err := a.Advance(context.Background(), startInput("dmnproc", []byte(`{"tier":"gold"}`), 1000))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if adv.GetTerminal() == nil || adv.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal (decision evaluated inline), got %+v", adv.GetTerminal())
	}
	out, err := decodeVars(adv.GetTerminal().GetOutput())
	if err != nil {
		t.Fatalf("decode outputs: %v", err)
	}
	if out["discount"] != 0.2 {
		t.Errorf("discount = %v (%T), want 0.2", out["discount"], out["discount"])
	}
}
