package processengine

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflwos/bpmn"
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

// Start -> UserTask -> End. The user task parks (passive, like cmmn.RunHumanTask);
// a person completes it later as an external event the engine handles like a
// service-task completion.
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

// Start -> work(serviceTask, interrupting boundary timer bt) -> End; the boundary
// flows to recover(serviceTask) -> End2. Firing bt cancels host "work" mid-flow
// (CancelActivity) and dispatches "recover" — so the turn does NOT terminate,
// letting a test observe the CancelInvoke alongside the recovery invoke.
const interruptingBoundaryToTaskBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="work"/>
    <serviceTask id="work" operationRef="echo:noop"><incoming>f1</incoming><outgoing>f2</outgoing></serviceTask>
    <boundaryEvent id="bt" attachedToRef="work" cancelActivity="true"><outgoing>f3</outgoing>
      <timerEventDefinition><timeDuration>PT5M</timeDuration></timerEventDefinition>
    </boundaryEvent>
    <sequenceFlow id="f2" sourceRef="work" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f3" sourceRef="bt" targetRef="recover"/>
    <serviceTask id="recover" operationRef="echo:noop"><incoming>f3</incoming><outgoing>f4</outgoing></serviceTask>
    <sequenceFlow id="f4" sourceRef="recover" targetRef="end2"/>
    <endEvent id="end2"><incoming>f4</incoming></endEvent>
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

// bpmnExtPayload wraps a typed bpmn engine event in the external-event envelope
// (kind + JSON payload) the adapter decodes via eventForBPMN → UnmarshalEvent —
// the exact bytes DeliverProcessEvent constructs on the wire.
func bpmnExtPayload(t *testing.T, kind string, ev any) *enginev1.ProcessEventPayload {
	t.Helper()
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	env, err := json.Marshal(externalEvent{Kind: kind, Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	return extPayload(env)
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

// TestAdvanceBPMN_UserTaskParksThenCompletes: RunUserTask used to hard-error the
// instance into an incident. Now it parks (no invoke, no terminal, no error) and
// a later external completion — which the reflwos engine handles identically to a
// service-task completion — finishes the process. The real wire path delivers a
// UserTaskCompleted via DeliverProcessEvent; at the adapter level the internal
// task_completed payload exercises the same advanceCompletion path.
func TestAdvanceBPMN_UserTaskParksThenCompletes(t *testing.T) {
	a := New(mustResolver(t, "ut", userTaskBPMN))

	start, err := a.Advance(context.Background(), startInput("ut", nil, 1000))
	if err != nil {
		t.Fatalf("start (user task should park, not error): %v", err)
	}
	if len(start.GetInvoke()) != 0 {
		t.Errorf("user task must not dispatch an invoke; got %d", len(start.GetInvoke()))
	}
	if start.GetTerminal() != nil {
		t.Errorf("user task should park, not terminate; got %+v", start.GetTerminal())
	}
	if start.GetIncident() != nil {
		t.Errorf("user task should park clean, not raise an incident; got %+v", start.GetIncident())
	}
	if aw := start.GetAwaiting(); len(aw) != 1 || aw[0].GetNodeId() != "u" {
		t.Errorf("parked user task should surface as awaiting [u]; got %+v", aw)
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "ut", InstanceKey: "i1",
		Record: bpmnRecord("ut", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: taskCompletedPayload("u", []byte(`{"approved":true}`)), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("user task completion: %v", err)
	}
	if adv.GetTerminal() == nil || adv.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal after user task completion, got %+v", adv.GetTerminal())
	}
	if len(adv.GetAwaiting()) != 0 {
		t.Errorf("completed instance should have no awaiting tasks; got %+v", adv.GetAwaiting())
	}
}

func hasCancelInvoke(cancels []*enginev1.InvokeCancel, node string) bool {
	for _, c := range cancels {
		if c.GetNodeId() == node {
			return true
		}
	}
	return false
}

// TestAdvanceBPMN_CancelActivityEmitsCancelInvoke pins Gap 4: an interrupting
// boundary event firing on a running service task must tear down that task's
// in-flight invocation (bpmn.CancelActivity -> InvokeCancel), mirroring CMMN's
// CancelTask. The model routes the boundary to another service task so the turn
// does not terminate (terminal-wins would otherwise drop the cleanup), proving
// the CancelInvoke is emitted alongside the recovery dispatch.
func TestAdvanceBPMN_CancelActivityEmitsCancelInvoke(t *testing.T) {
	a := New(mustResolver(t, "ib", interruptingBoundaryToTaskBPMN))

	start, err := a.Advance(context.Background(), startInput("ib", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if !hasInvoke(start.GetInvoke(), "work") {
		t.Fatalf("want Invoke for work on start, got %v", start.GetInvoke())
	}
	if findArm(start.GetArmTimer(), "bt") == nil {
		t.Fatalf("want ArmTimer for boundary bt on start, got %v", start.GetArmTimer())
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "ib", InstanceKey: "i1",
		Record: bpmnRecord("ib", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: timerFiredPayload("bt"), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("boundary fire: %v", err)
	}
	if adv.GetTerminal() != nil {
		t.Fatalf("unexpected terminal mid-flow: %+v", adv.GetTerminal())
	}
	if !hasCancelInvoke(adv.GetCancelInvoke(), "work") {
		t.Errorf("want CancelInvoke for cancelled host work, got %v", adv.GetCancelInvoke())
	}
	if !hasInvoke(adv.GetInvoke(), "recover") {
		t.Errorf("want Invoke for recover after boundary fire, got %v", adv.GetInvoke())
	}
}

// TestEventForBPMN_ExternalVariablesUpdated pins Gap 6 on the BPMN plane: a
// VariablesUpdated event delivered through the external-event envelope (the
// DeliverProcessEvent path) decodes to bpmn.VariablesUpdated, so the engine merges
// the write and re-evaluates parked conditional catches. The reflwos engine owns
// the merge+re-eval; this guards the adapter's decode of the wire envelope.
func TestEventForBPMN_ExternalVariablesUpdated(t *testing.T) {
	ev, err := eventForBPMN(bpmnExtPayload(t, "VariablesUpdated", bpmn.VariablesUpdated{
		Vars: map[string]any{"approved": true},
	}))
	if err != nil {
		t.Fatalf("eventForBPMN: %v", err)
	}
	vu, ok := ev.(bpmn.VariablesUpdated)
	if !ok {
		t.Fatalf("event = %T, want bpmn.VariablesUpdated", ev)
	}
	if vu.Vars["approved"] != true {
		t.Fatalf("Vars = %v, want approved=true", vu.Vars)
	}
}

func TestAdvanceBPMN_UnknownModelErrors(t *testing.T) {
	a := New(NewMapResolver()) // no models registered

	_, err := a.Advance(context.Background(), startInput("missing", nil, 1000))
	if err == nil {
		t.Fatal("want error for missing model, got nil")
	}
}
