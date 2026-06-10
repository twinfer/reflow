package processengine

import (
	"context"
	"encoding/json"
	"testing"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// Start -> UserTask(with a data output) -> End. The data output is the task's
// completion submission, so schemagen emits a completeTask op for node "u".
const userTaskOutputBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
             xmlns:xsd="http://www.w3.org/2001/XMLSchema" targetNamespace="test">
  <itemDefinition id="Item_Decision" structureRef="xsd:string"/>
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="u"/>
    <userTask id="u" name="Approve"><incoming>f1</incoming><outgoing>f2</outgoing>
      <ioSpecification><dataOutput id="o0" name="decision" itemSubjectRef="Item_Decision"/></ioSpecification>
    </userTask>
    <sequenceFlow id="f2" sourceRef="u" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
  </process>
</definitions>`

// TestTaskSchema_BPMNUserTask: TaskSchema resolves a parked user task's submission
// schema from the reconciled model — the completeTask op's input object, with the
// model's $defs grafted so its $ref stays resolvable standalone.
func TestTaskSchema_BPMNUserTask(t *testing.T) {
	r := NewTableResolver(nil)
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "approval", Version: "v1"}
	r.Reconcile([]*enginev1.ModelRecord{{ModelRef: ref, Xml: []byte(userTaskOutputBPMN)}})

	b, err := r.TaskSchema(context.Background(), ref, "u")
	if err != nil {
		t.Fatalf("TaskSchema: %v", err)
	}
	if b == nil {
		t.Fatal("TaskSchema returned nil for a user task with output data")
	}
	var got struct {
		Type       string `json:"type"`
		Properties map[string]struct {
			Ref string `json:"$ref"`
		} `json:"properties"`
		Required []string                   `json:"required"`
		Defs     map[string]json.RawMessage `json:"$defs"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal schema %q: %v", b, err)
	}
	if got.Type != "object" {
		t.Errorf("type = %q, want object", got.Type)
	}
	if gotRef := got.Properties["decision"].Ref; gotRef != "#/$defs/Item_Decision" {
		t.Errorf("properties.decision.$ref = %q, want #/$defs/Item_Decision", gotRef)
	}
	if len(got.Required) != 1 || got.Required[0] != "decision" {
		t.Errorf("required = %v, want [decision]", got.Required)
	}
	if _, ok := got.Defs["Item_Decision"]; !ok {
		t.Errorf("$defs missing Item_Decision: %q", b)
	}
}

// TestTaskSchema_EdgeCases: a node with no typed completion contract resolves to
// (nil, nil) — the read surfaces the descriptor without a schema; an unknown model
// is an error.
func TestTaskSchema_EdgeCases(t *testing.T) {
	r := NewTableResolver(nil)
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "approval", Version: "v1"}
	r.Reconcile([]*enginev1.ModelRecord{{ModelRef: ref, Xml: []byte(userTaskOutputBPMN)}})

	// "start" is a real node but not a completeTask op → no typed contract.
	b, err := r.TaskSchema(context.Background(), ref, "start")
	if err != nil {
		t.Fatalf("TaskSchema(start): unexpected error %v", err)
	}
	if b != nil {
		t.Fatalf("TaskSchema(start) = %q, want nil (no typed completion contract)", b)
	}

	// Unknown model → error (the read maps it to a logged, omitted schema).
	if _, err := r.TaskSchema(context.Background(), &enginev1.ModelRef{Kind: "bpmn", Name: "nope", Version: "v1"}, "u"); err == nil {
		t.Fatal("TaskSchema(unknown model) = nil error, want ErrModelNotFound")
	}
}
