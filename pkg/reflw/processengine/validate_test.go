package processengine

import "testing"

// bpmnEndEventWithOutgoing is well-formed XML and parses cleanly, but is
// structurally invalid: an endEvent must not have an outgoing flow (reflwos
// static rule BPM001). It is the case the config-layer well-formed-XML check
// cannot catch and ValidateModel must.
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

// cmmnNoPlanItems parses but fails cmmn.Validate — a case whose root stage has
// no plan items.
const cmmnNoPlanItems = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/CMMN/20151109/MODEL">
  <case id="c"><casePlanModel id="stage0"/></case>
</definitions>`

func TestValidateModel(t *testing.T) {
	cases := []struct {
		name    string
		kind    string
		xml     string
		wantErr bool
	}{
		{"valid bpmn", "bpmn", bpmnWithTTL, false},
		{"valid cmmn", "cmmn", echoCaseCMMN, false},
		{"valid dmn", "dmn", validDMN, false},
		{"malformed dmn", "dmn", `<definitions`, true},
		// Well-formed + parseable but statically invalid: the gap a shallow XML
		// check misses. bpmn endEvent-with-outgoing → BPM001; empty cmmn case →
		// cmmn.Validate "root stage has no plan items".
		{"bpmn static error", "bpmn", bpmnEndEventWithOutgoing, true},
		{"cmmn no plan items", "cmmn", cmmnNoPlanItems, true},
		// Not well-formed XML at all.
		{"malformed xml", "bpmn", `<definitions><process id="p"`, true},
		// Input guards.
		{"empty xml", "bpmn", "", true},
		{"unknown kind", "xslt", bpmnWithTTL, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateModel(tc.kind, []byte(tc.xml))
			if tc.wantErr && err == nil {
				t.Fatalf("ValidateModel(%q) = nil, want error", tc.kind)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateModel(%q) = %v, want nil", tc.kind, err)
			}
		})
	}
}
