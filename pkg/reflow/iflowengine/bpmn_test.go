package iflowengine

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/twinfer/reflwos/bpmn"
	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"google.golang.org/protobuf/proto"
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
	// The replay contract covers the WHOLE ProcessAdvanced, not just NewState —
	// most importantly the bridge Input bytes the engine later hashes for
	// divergence detection. The echo model emits a service-task invoke, so the
	// full-message comparison actually exercises those bytes.
	if len(adv1.GetInvoke()) == 0 {
		t.Fatal("expected a service-task invoke to exercise the bridge input bytes")
	}
	m := proto.MarshalOptions{Deterministic: true}
	b1, err := m.Marshal(adv1)
	if err != nil {
		t.Fatalf("marshal adv1: %v", err)
	}
	b2, err := m.Marshal(adv2)
	if err != nil {
		t.Fatalf("marshal adv2: %v", err)
	}
	if !bytes.Equal(b1, b2) {
		t.Errorf("ProcessAdvanced not byte-identical across identical turns:\n 1=%x\n 2=%x", b1, b2)
	}
}

// TestEventForBPMN_CodedTaskFailureCarriesErrorCode pins the coded-fault path: a
// failed service task whose message is a bridgeFault envelope decodes to a
// bpmn.TaskFailed with ErrorCode set (so a matching error boundary catches it),
// while a plain message stays catch-all with a clean cause.
func TestEventForBPMN_CodedTaskFailureCarriesErrorCode(t *testing.T) {
	coded := &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
		TaskCompleted: &enginev1.ProcessTaskCompleted{
			NodeId: "work", Failed: true, FailureMessage: encodeBridgeFault("E_DECLINED", "card declined"),
		},
	}}
	ev, err := eventForBPMN(coded)
	if err != nil {
		t.Fatalf("eventForBPMN (coded): %v", err)
	}
	tf, ok := ev.(bpmn.TaskFailed)
	if !ok {
		t.Fatalf("event = %T, want bpmn.TaskFailed", ev)
	}
	if tf.ErrorCode != "E_DECLINED" {
		t.Errorf("ErrorCode = %q, want E_DECLINED", tf.ErrorCode)
	}
	if tf.Cause != "card declined" {
		t.Errorf("Cause = %q, want the clean human message", tf.Cause)
	}

	plain := &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
		TaskCompleted: &enginev1.ProcessTaskCompleted{NodeId: "work", Failed: true, FailureMessage: "boom"},
	}}
	ev2, err := eventForBPMN(plain)
	if err != nil {
		t.Fatalf("eventForBPMN (plain): %v", err)
	}
	if tf2 := ev2.(bpmn.TaskFailed); tf2.ErrorCode != "" || tf2.Cause != "boom" {
		t.Errorf("plain failure = (code %q, cause %q), want (\"\", boom)", tf2.ErrorCode, tf2.Cause)
	}
}

// TestEventForBPMN_ChildEscalationCarriesErrorCode pins the cross-process
// escalation surface: a child process that throws an uncaught escalation fails
// with ProcessFailed{Cause:"escalation:CODE"}, which encodeProcessFailure
// promotes into the child terminal's bridgeFault envelope. The calling process
// then sees child_completed{Failed} and eventForBPMN must recover ErrorCode as
// the full "escalation:CODE" so advanceTaskFailed's CutPrefix fires the
// CallActivity's escalation boundary. A plain child failure stays catch-all.
func TestEventForBPMN_ChildEscalationCarriesErrorCode(t *testing.T) {
	// Child side → parent side round-trip for an escalation cause.
	esc := &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_ChildCompleted{
		ChildCompleted: &enginev1.ProcessChildCompleted{
			NodeId: "CallActivity_1", Failed: true,
			FailureMessage: encodeProcessFailure("EndEvent_2", "escalation:ESC_1"),
		},
	}}
	ev, err := eventForBPMN(esc)
	if err != nil {
		t.Fatalf("eventForBPMN (escalation child): %v", err)
	}
	tf, ok := ev.(bpmn.TaskFailed)
	if !ok {
		t.Fatalf("event = %T, want bpmn.TaskFailed", ev)
	}
	if tf.NodeID != "CallActivity_1" {
		t.Errorf("NodeID = %q, want CallActivity_1", tf.NodeID)
	}
	// The full "escalation:CODE" must survive so advanceTaskFailed's CutPrefix matches.
	if tf.ErrorCode != "escalation:ESC_1" {
		t.Errorf("ErrorCode = %q, want escalation:ESC_1", tf.ErrorCode)
	}

	// A plain (non-escalation) child failure stays catch-all: encodeProcessFailure
	// leaves it a bare message, so ErrorCode is "" (catch-all error boundary).
	plain := &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_ChildCompleted{
		ChildCompleted: &enginev1.ProcessChildCompleted{
			NodeId: "CallActivity_1", Failed: true,
			FailureMessage: encodeProcessFailure("EndEvent_9", "boom"),
		},
	}}
	ev2, err := eventForBPMN(plain)
	if err != nil {
		t.Fatalf("eventForBPMN (plain child): %v", err)
	}
	if tf2 := ev2.(bpmn.TaskFailed); tf2.ErrorCode != "" {
		t.Errorf("plain child failure ErrorCode = %q, want \"\" (catch-all)", tf2.ErrorCode)
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
