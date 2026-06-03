package iflowengine

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/twinfer/reflow/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Start -> ServiceTask(echo:noop) -> End.
const echoServiceTaskBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="work"/>
    <serviceTask id="work" operationRef="echo:noop"><incoming>f1</incoming><outgoing>f2</outgoing></serviceTask>
    <sequenceFlow id="f2" sourceRef="work" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
  </process>
</definitions>`

// Start -> End (no work; completes immediately).
const emptyBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="end"/>
    <endEvent id="end"><incoming>f1</incoming></endEvent>
  </process>
</definitions>`

// Start -> UserTask -> End (RunUserTask is unsupported this iteration).
const userTaskBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="u"/>
    <userTask id="u"><incoming>f1</incoming><outgoing>f2</outgoing></userTask>
    <sequenceFlow id="f2" sourceRef="u" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
  </process>
</definitions>`

func mustResolver(t *testing.T, name, xml string) *MapResolver {
	t.Helper()
	r := NewMapResolver()
	if err := r.ParseBPMN(name, "v1", []byte(xml)); err != nil {
		t.Fatalf("parse model %q: %v", name, err)
	}
	return r
}

func bpmnRecord(name string, stateBlob []byte) *enginev1.ProcessInstanceRecord {
	return &enginev1.ProcessInstanceRecord{
		Kind:      enginev1.ProcessKind_PROCESS_KIND_BPMN,
		ModelRef:  &enginev1.ModelRef{Kind: "bpmn", Name: name, Version: "v1"},
		StateBlob: stateBlob,
	}
}

func extPayload(b []byte) *enginev1.ProcessEventPayload {
	return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_External{External: b}}
}

func taskCompletedPayload(node string, out []byte) *enginev1.ProcessEventPayload {
	return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
		TaskCompleted: &enginev1.ProcessTaskCompleted{NodeId: node, Output: out},
	}}
}

func startInput(name string, vars []byte, logical uint64) invoker.ProcessAdvanceInput {
	return invoker.ProcessAdvanceInput{
		Pk: 0, Service: name, InstanceKey: "i1",
		Record: bpmnRecord(name, nil),
		Entry:  &enginev1.ProcessInboxEntry{Payload: extPayload(vars), LogicalTimeMs: logical},
	}
}

func TestAdvanceBPMN_StartEmitsServiceInvoke(t *testing.T) {
	a := New(mustResolver(t, "echo", echoServiceTaskBPMN))

	adv, err := a.Advance(context.Background(), startInput("echo", nil, 1000))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if adv.GetTerminal() != nil {
		t.Fatalf("unexpected terminal on start: %+v", adv.GetTerminal())
	}
	if len(adv.GetInvoke()) != 1 {
		t.Fatalf("want 1 invoke, got %d", len(adv.GetInvoke()))
	}
	inv := adv.GetInvoke()[0]
	if inv.GetNodeId() != "work" {
		t.Errorf("invoke node = %q, want work", inv.GetNodeId())
	}
	if inv.GetTarget().GetServiceName() != BridgeService || inv.GetTarget().GetHandlerName() != BridgeHandler {
		t.Errorf("target = %v, want %s/%s", inv.GetTarget(), BridgeService, BridgeHandler)
	}
	var bi BridgeInput
	if err := json.Unmarshal(inv.GetInput(), &bi); err != nil {
		t.Fatalf("decode bridge input: %v", err)
	}
	if bi.Ref != "echo:noop" {
		t.Errorf("bridge ref = %q, want echo:noop", bi.Ref)
	}
	if len(adv.GetNewState()) == 0 {
		t.Error("NewState is empty after start")
	}
}

func TestAdvanceBPMN_TaskCompletionCompletesProcess(t *testing.T) {
	a := New(mustResolver(t, "echo", echoServiceTaskBPMN))

	start, err := a.Advance(context.Background(), startInput("echo", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "echo", InstanceKey: "i1",
		Record: bpmnRecord("echo", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: taskCompletedPayload("work", []byte(`{"ok":true}`)), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	if adv.GetTerminal() == nil {
		t.Fatalf("want terminal after task completion, got none (invoke=%d)", len(adv.GetInvoke()))
	}
	if adv.GetTerminal().GetFailed() {
		t.Errorf("terminal failed = true, want false: %s", adv.GetTerminal().GetFailureMessage())
	}
}

func TestAdvanceBPMN_EmptyProcessCompletesOnStart(t *testing.T) {
	a := New(mustResolver(t, "empty", emptyBPMN))

	adv, err := a.Advance(context.Background(), startInput("empty", nil, 1000))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if adv.GetTerminal() == nil || adv.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal on start, got %+v", adv.GetTerminal())
	}
}

func TestAdvanceBPMN_StartIsDeterministic(t *testing.T) {
	a := New(mustResolver(t, "echo", echoServiceTaskBPMN))

	adv1, err := a.Advance(context.Background(), startInput("echo", []byte(`{"x":1}`), 1000))
	if err != nil {
		t.Fatalf("run 1: %v", err)
	}
	adv2, err := a.Advance(context.Background(), startInput("echo", []byte(`{"x":1}`), 1000))
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if !bytes.Equal(adv1.GetNewState(), adv2.GetNewState()) {
		t.Errorf("NewState not byte-identical across identical turns:\n 1=%s\n 2=%s", adv1.GetNewState(), adv2.GetNewState())
	}
}

func TestAdvanceBPMN_UserTaskUnsupported(t *testing.T) {
	a := New(mustResolver(t, "ut", userTaskBPMN))

	_, err := a.Advance(context.Background(), startInput("ut", nil, 1000))
	if err == nil {
		t.Fatal("want error for unsupported RunUserTask, got nil")
	}
}

func TestAdvanceBPMN_UnknownModelErrors(t *testing.T) {
	a := New(NewMapResolver()) // no models registered

	_, err := a.Advance(context.Background(), startInput("missing", nil, 1000))
	if err == nil {
		t.Fatal("want error for missing model, got nil")
	}
}
