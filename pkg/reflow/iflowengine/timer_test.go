package iflowengine

import (
	"context"
	"testing"

	"github.com/twinfer/reflow/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
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
