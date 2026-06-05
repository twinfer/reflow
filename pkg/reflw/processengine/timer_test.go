package processengine

import (
	"context"
	"testing"

	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// Start -> IntermediateTimerCatch(PT5M) -> End.
const timerCatchBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="wait"/>
    <intermediateCatchEvent id="wait">
      <incoming>f1</incoming><outgoing>f2</outgoing>
      <timerEventDefinition><timeDuration>PT5M</timeDuration></timerEventDefinition>
    </intermediateCatchEvent>
    <sequenceFlow id="f2" sourceRef="wait" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
  </process>
</definitions>`

// Start -> work(serviceTask, interrupting boundary timer bt) -> work2(serviceTask) -> End.
// Completing work cancels the boundary timer mid-flow (no terminal in that batch).
const boundaryTimerBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="work"/>
    <serviceTask id="work" operationRef="echo:noop"><incoming>f1</incoming><outgoing>f2</outgoing></serviceTask>
    <boundaryEvent id="bt" attachedToRef="work">
      <timerEventDefinition><timeDuration>PT10M</timeDuration></timerEventDefinition>
    </boundaryEvent>
    <sequenceFlow id="f2" sourceRef="work" targetRef="work2"/>
    <serviceTask id="work2" operationRef="echo:noop"><incoming>f2</incoming><outgoing>f3</outgoing></serviceTask>
    <sequenceFlow id="f3" sourceRef="work2" targetRef="end"/>
    <endEvent id="end"><incoming>f3</incoming></endEvent>
  </process>
</definitions>`

// Start -> IntermediateCatch(timeCycle R3/PT1S) -> ServiceTask(echo:noop) -> End.
// Each fire dispatches "beat" and re-arms the next repetition until R3 is spent.
const intermediateCycleBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="cycle"/>
    <intermediateCatchEvent id="cycle">
      <incoming>f1</incoming><outgoing>f2</outgoing>
      <timerEventDefinition><timeCycle>R3/PT1S</timeCycle></timerEventDefinition>
    </intermediateCatchEvent>
    <sequenceFlow id="f2" sourceRef="cycle" targetRef="beat"/>
    <serviceTask id="beat" operationRef="echo:noop"><incoming>f2</incoming><outgoing>f3</outgoing></serviceTask>
    <sequenceFlow id="f3" sourceRef="beat" targetRef="end"/>
    <endEvent id="end"><incoming>f3</incoming></endEvent>
  </process>
</definitions>`

func timerFiredPayload(node string) *enginev1.ProcessEventPayload {
	return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TimerFired{
		TimerFired: &enginev1.ProcessTimerFired{NodeId: node},
	}}
}

func findArm(arms []*enginev1.TimerArm, node string) *enginev1.TimerArm {
	for _, a := range arms {
		if a.GetNodeId() == node {
			return a
		}
	}
	return nil
}

func hasInvoke(invs []*enginev1.TaskInvoke, node string) bool {
	for _, i := range invs {
		if i.GetNodeId() == node {
			return true
		}
	}
	return false
}

func hasCancelTimer(cancels []*enginev1.TimerCancel, node string) bool {
	for _, c := range cancels {
		if c.GetNodeId() == node {
			return true
		}
	}
	return false
}

func TestAdvanceBPMN_TimerCatchArmsAndFires(t *testing.T) {
	a := New(mustResolver(t, "timer", timerCatchBPMN))

	start, err := a.Advance(context.Background(), startInput("timer", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	arm := findArm(start.GetArmTimer(), "wait")
	if arm == nil {
		t.Fatalf("want ArmTimer for node wait, got %v", start.GetArmTimer())
	}
	if got, want := arm.GetFireAtMs(), uint64(1000+5*60*1000); got != want {
		t.Errorf("FireAtMs = %d, want %d (logical + PT5M)", got, want)
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "timer", InstanceKey: "i1",
		Record: bpmnRecord("timer", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: timerFiredPayload("wait"), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("timer fire: %v", err)
	}
	if adv.GetTerminal() == nil || adv.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal after timer fire, got %+v", adv.GetTerminal())
	}
}

func TestAdvanceBPMN_BoundaryTimerArmedThenCancelled(t *testing.T) {
	a := New(mustResolver(t, "bt", boundaryTimerBPMN))

	start, err := a.Advance(context.Background(), startInput("bt", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if !hasInvoke(start.GetInvoke(), "work") {
		t.Errorf("want Invoke for work, got %v", start.GetInvoke())
	}
	if findArm(start.GetArmTimer(), "bt") == nil {
		t.Errorf("want ArmTimer for boundary bt, got %v", start.GetArmTimer())
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "bt", InstanceKey: "i1",
		Record: bpmnRecord("bt", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: taskCompletedPayload("work", nil), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("work completion: %v", err)
	}
	if adv.GetTerminal() != nil {
		t.Fatalf("unexpected terminal mid-flow: %+v", adv.GetTerminal())
	}
	if !hasCancelTimer(adv.GetCancelTimer(), "bt") {
		t.Errorf("want CancelTimer for boundary bt after work completion, got %v", adv.GetCancelTimer())
	}
	if !hasInvoke(adv.GetInvoke(), "work2") {
		t.Errorf("want Invoke for work2 after work completion, got %v", adv.GetInvoke())
	}
}

// TestAdvanceBPMN_TimeCycleArmsAndReArms pins durable cyclic-timer support. A
// TimeCycle (R<n>/…) catch arms a timer on Start; because reflwos re-emits a fresh
// WaitForTimer (count decremented in its durable state) on every fire, each
// TimerFired re-arms the next repetition while dispatching the outgoing flow,
// until the count is exhausted. The adapter owns no cycle bookkeeping — it just
// translates each WaitForTimer to a one-shot ArmTimer. Rejecting Repeat != 0 (as
// an earlier revision did) failed the whole class on Start.
func TestAdvanceBPMN_TimeCycleArmsAndReArms(t *testing.T) {
	a := New(mustResolver(t, "cycle", intermediateCycleBPMN))

	// Start: the token parks at the catch and arms the first repetition (no fire
	// yet → no dispatch). FireAtMs = logical(1000) + PT1S.
	start, err := a.Advance(context.Background(), startInput("cycle", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if start.GetTerminal() != nil {
		t.Fatalf("unexpected terminal on start: %+v", start.GetTerminal())
	}
	if len(start.GetInvoke()) != 0 {
		t.Errorf("want no invoke before the first fire, got %d", len(start.GetInvoke()))
	}
	arm := findArm(start.GetArmTimer(), "cycle")
	if arm == nil {
		t.Fatalf("want ArmTimer for node cycle on start, got %v", start.GetArmTimer())
	}
	if got, want := arm.GetFireAtMs(), uint64(2000); got != want {
		t.Errorf("FireAtMs = %d, want %d (logical + PT1S)", got, want)
	}

	// Fire 1/3: dispatch "beat" AND re-arm the next repetition at fireAt(2000)+PT1S,
	// since the count is not yet exhausted.
	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "cycle", InstanceKey: "i1",
		Record: bpmnRecord("cycle", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: timerFiredPayload("cycle"), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("fire 1: %v", err)
	}
	if adv.GetTerminal() != nil {
		t.Fatalf("unexpected terminal on fire 1: %+v", adv.GetTerminal())
	}
	if !hasInvoke(adv.GetInvoke(), "beat") {
		t.Errorf("want Invoke for beat on fire 1, got %v", adv.GetInvoke())
	}
	rearm := findArm(adv.GetArmTimer(), "cycle")
	if rearm == nil {
		t.Fatalf("want re-arm for cycle on fire 1 (count not exhausted), got %v", adv.GetArmTimer())
	}
	if got, want := rearm.GetFireAtMs(), uint64(3000); got != want {
		t.Errorf("re-armed FireAtMs = %d, want %d (fire + PT1S)", got, want)
	}
}
