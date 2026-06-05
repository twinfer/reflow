package iflowengine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// A case whose single blocking task names a capability; the autoComplete stage
// completes the case once the task completes.
const echoCaseCMMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/CMMN/20151109/MODEL">
  <case id="echocase">
    <casePlanModel id="stage0" name="Root" autoComplete="true">
      <planItem id="pi1" definitionRef="t1"/>
      <task id="t1" name="Work" isBlocking="true">
        <extensionElements>
          <capability ref="echo:noop"/>
        </extensionElements>
      </task>
    </casePlanModel>
  </case>
</definitions>`

func cmmnRecord(name string, stateBlob []byte) *enginev1.ProcessInstanceRecord {
	return &enginev1.ProcessInstanceRecord{
		Kind:      enginev1.ProcessKind_PROCESS_KIND_CMMN,
		ModelRef:  &enginev1.ModelRef{Kind: "cmmn", Name: name, Version: "v1"},
		StateBlob: stateBlob,
	}
}

func cmmnStartInput(name string, vars []byte, logical uint64) invoker.ProcessAdvanceInput {
	return invoker.ProcessAdvanceInput{
		Pk: 0, Service: name, InstanceKey: "i1",
		Record: cmmnRecord(name, nil),
		Entry:  &enginev1.ProcessInboxEntry{Payload: extPayload(vars), LogicalTimeMs: logical},
	}
}

func mustCMMNResolver(t *testing.T, name, xml string) *MapResolver {
	t.Helper()
	r := NewMapResolver()
	if err := r.ParseCMMN(name, "v1", []byte(xml)); err != nil {
		t.Fatalf("parse cmmn %q: %v", name, err)
	}
	return r
}

func TestAdvanceCMMN_TaskStartEmitsInvoke(t *testing.T) {
	a := New(mustCMMNResolver(t, "echocase", echoCaseCMMN))

	adv, err := a.Advance(context.Background(), cmmnStartInput("echocase", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if len(adv.GetInvoke()) != 1 {
		t.Fatalf("want 1 invoke, got %d", len(adv.GetInvoke()))
	}
	inv := adv.GetInvoke()[0]
	if inv.GetNodeId() != "pi1" {
		t.Errorf("invoke node = %q, want pi1 (plan item)", inv.GetNodeId())
	}
	var bi BridgeInput
	if err := json.Unmarshal(inv.GetInput(), &bi); err != nil {
		t.Fatalf("decode bridge input: %v", err)
	}
	if bi.Ref != "echo:noop" {
		t.Errorf("bridge ref = %q, want echo:noop", bi.Ref)
	}
}

func TestAdvanceCMMN_TaskCompletionCompletesCase(t *testing.T) {
	a := New(mustCMMNResolver(t, "echocase", echoCaseCMMN))

	start, err := a.Advance(context.Background(), cmmnStartInput("echocase", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "echocase", InstanceKey: "i1",
		Record: cmmnRecord("echocase", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: taskCompletedPayload("pi1", []byte(`{"ok":true}`)), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("task completion: %v", err)
	}
	if adv.GetTerminal() == nil || adv.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal after task completion, got %+v (invoke=%d)", adv.GetTerminal(), len(adv.GetInvoke()))
	}
}
