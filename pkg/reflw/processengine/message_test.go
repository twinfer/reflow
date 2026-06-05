package processengine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// Start -> IntermediateCatch(message "shipped", correlate orderId) -> End. A
// message catch parks at WaitForSignal; the correlation key is the FEEL value of
// <correlate var="orderId"/> against the start vars.
const messageCatchBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <message id="shipped" name="shipped"/>
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <intermediateCatchEvent id="wait">
      <extensionElements><correlate var="orderId"/></extensionElements>
      <incoming>f1</incoming><outgoing>f2</outgoing>
      <messageEventDefinition messageRef="shipped"/>
    </intermediateCatchEvent>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="wait"/>
    <sequenceFlow id="f2" sourceRef="wait" targetRef="end"/>
  </process>
</definitions>`

func messageReceivedPayload(nodeID string, payload []byte) *enginev1.ProcessEventPayload {
	return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_MessageReceived{
		MessageReceived: &enginev1.ProcessMessageReceived{NodeId: nodeID, Payload: payload},
	}}
}

// TestAdvanceBPMN_MessageCatchEmitsSubscribe: a parked message catch translates
// WaitForSignal → a SignalSubscribe carrying the messageRef and the resolved
// correlation key, addressed to the catch node.
func TestAdvanceBPMN_MessageCatchEmitsSubscribe(t *testing.T) {
	a := New(mustResolver(t, "msgproc", messageCatchBPMN))

	adv, err := a.Advance(context.Background(), startInput("msgproc", []byte(`{"orderId":"A-1"}`), 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if adv.GetTerminal() != nil {
		t.Fatalf("unexpected terminal on start: %+v", adv.GetTerminal())
	}
	if len(adv.GetSubscribe()) != 1 {
		t.Fatalf("want 1 subscribe, got %d", len(adv.GetSubscribe()))
	}
	sub := adv.GetSubscribe()[0]
	if sub.GetNodeId() != "wait" {
		t.Errorf("subscribe node = %q, want wait", sub.GetNodeId())
	}
	if sub.GetMessageName() != "shipped" {
		t.Errorf("subscribe message = %q, want shipped", sub.GetMessageName())
	}
	if sub.GetCorrelationKey() != "A-1" {
		t.Errorf("subscribe correlation = %q, want A-1 (FEEL of orderId)", sub.GetCorrelationKey())
	}
}

// TestAdvanceBPMN_MessageDeliveryResumesAndCompletes: feeding the parked instance
// a ProcessMessageReceived (the read path's delivery) decodes to a
// bpmn.SignalReceived, resumes the token, and runs the process to a successful
// terminal — with the message payload merged into the instance variables.
func TestAdvanceBPMN_MessageDeliveryResumesAndCompletes(t *testing.T) {
	a := New(mustResolver(t, "msgproc", messageCatchBPMN))

	start, err := a.Advance(context.Background(), startInput("msgproc", []byte(`{"orderId":"A-1"}`), 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "msgproc", InstanceKey: "i1",
		Record: bpmnRecord("msgproc", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: messageReceivedPayload("wait", []byte(`{"tracking":"Z9"}`)), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("message delivery: %v", err)
	}
	if adv.GetTerminal() == nil || adv.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal after message, got %+v (subscribe=%d)", adv.GetTerminal(), len(adv.GetSubscribe()))
	}
	var out map[string]any
	if err := json.Unmarshal(adv.GetTerminal().GetOutput(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if out["tracking"] != "Z9" {
		t.Errorf("output tracking = %v, want Z9 (delivered message payload merged)", out["tracking"])
	}
}
