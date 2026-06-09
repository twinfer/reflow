package processengine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflwos/cmmn"
	"github.com/twinfer/reflwos/dmn"
)

// A case whose single plan item is a decisionTask; the engine evaluates the DMN
// inline (via the wired CMMNDecisions resolver) and the autoComplete stage
// completes the case in the same turn. <decisionRefExpression> evaluates against
// case vars to the registered decision ref.
const decisionCaseCMMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/CMMN/20151109/MODEL">
  <case id="deccase">
    <casePlanModel id="stage0" name="Root" autoComplete="true">
      <planItem id="pi1" definitionRef="dt1"/>
      <decisionTask id="dt1" name="Evaluate">
        <decisionRefExpression>targetDecision</decisionRefExpression>
      </decisionTask>
    </casePlanModel>
  </case>
</definitions>`

const approvalDMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" name="m" id="_m">
  <decision name="approval" id="_d">
    <variable name="approval"/>
    <literalExpression><text>amount &gt; 1000</text></literalExpression>
  </decision>
</definitions>`

// A case whose single plan item is a (blocking) humanTask. Start parks the task
// — no command to actuate; a person completes it later as an external
// TaskCompleted, after which the autoComplete stage finishes the case.
const humanCaseCMMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/CMMN/20151109/MODEL">
  <case id="humancase">
    <casePlanModel id="stage0" name="Root" autoComplete="true">
      <planItem id="pi1" definitionRef="h1"/>
      <humanTask id="h1" name="Approve"/>
    </casePlanModel>
  </case>
</definitions>`

// An exit criterion on mainItem fires when trigItem completes, terminating
// mainItem mid-case (CancelTask). keepItem stays active so the case does NOT
// auto-complete in the same turn — proving the adapter emits CancelInvoke rather
// than being short-circuited by the "terminal wins" early return.
const exitCriterionCaseCMMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/CMMN/20151109/MODEL">
  <case id="caseExit" name="Exit">
    <casePlanModel id="stageRoot" name="Root" autoComplete="true">
      <planItem id="mainItem" definitionRef="main">
        <exitCriterion id="ex1" sentryRef="sExit"/>
      </planItem>
      <planItem id="trigItem" definitionRef="trig"/>
      <planItem id="keepItem" definitionRef="keep"/>
      <sentry id="sExit"><planItemOnPart sourceRef="trigItem"/></sentry>
      <task id="main" name="Main" isBlocking="true">
        <extensionElements><capability ref="echo:noop"/></extensionElements>
      </task>
      <task id="trig" name="Trigger" isBlocking="true">
        <extensionElements><capability ref="echo:noop"/></extensionElements>
      </task>
      <task id="keep" name="Keep" isBlocking="true">
        <extensionElements><capability ref="echo:noop"/></extensionElements>
      </task>
    </casePlanModel>
  </case>
</definitions>`

// cmmnExtPayload wraps a typed CMMN engine event in the external-event envelope
// (kind + JSON payload) the adapter decodes via eventForCMMN → UnmarshalEvent.
func cmmnExtPayload(t *testing.T, kind string, ev any) *enginev1.ProcessEventPayload {
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

// TestAdvanceCMMN_DecisionTaskInline: a DecisionTask used to CaseFail (no
// resolver wired); with CMMNDecisions threaded into advanceCMMN it evaluates
// inline and the case completes.
func TestAdvanceCMMN_DecisionTaskInline(t *testing.T) {
	r := mustCMMNResolver(t, "deccase", decisionCaseCMMN)
	rt, err := dmn.NewRuntime([]byte(approvalDMN))
	if err != nil {
		t.Fatalf("dmn runtime: %v", err)
	}
	r.AddCMMNDecision("deccase", "v1", "approval", rt)
	a := New(r)

	adv, err := a.Advance(context.Background(), cmmnStartInput("deccase", []byte(`{"targetDecision":"approval","amount":5000}`), 1000))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if adv.GetTerminal() == nil || adv.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal (decision evaluated inline), got %+v", adv.GetTerminal())
	}
}

// TestAdvanceCMMN_DecisionTaskNoResolverStillFails guards the wiring is the only
// reason it works: a resolver that knows no decisions surfaces CaseFailed (the
// engine's own "no decision" path), not a panic or a silent success.
func TestAdvanceCMMN_DecisionTaskUnknownRefFails(t *testing.T) {
	r := mustCMMNResolver(t, "deccase", decisionCaseCMMN)
	a := New(r) // no AddCMMNDecision → resolver errors on "approval"

	adv, err := a.Advance(context.Background(), cmmnStartInput("deccase", []byte(`{"targetDecision":"approval","amount":5000}`), 1000))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if adv.GetTerminal() == nil || !adv.GetTerminal().GetFailed() {
		t.Fatalf("want failed terminal for unresolvable decision, got %+v", adv.GetTerminal())
	}
}

// TestAdvanceCMMN_HumanTaskParksThenCompletes: RunHumanTask used to hit the
// outer switch default → CaseFailed. Now it parks (no invoke, no terminal, no
// error) and a later external TaskCompleted finishes the case.
func TestAdvanceCMMN_HumanTaskParksThenCompletes(t *testing.T) {
	a := New(mustCMMNResolver(t, "humancase", humanCaseCMMN))

	start, err := a.Advance(context.Background(), cmmnStartInput("humancase", nil, 1000))
	if err != nil {
		t.Fatalf("start (human task should park, not error): %v", err)
	}
	if len(start.GetInvoke()) != 0 {
		t.Errorf("human task must not dispatch an invoke; got %d", len(start.GetInvoke()))
	}
	if start.GetTerminal() != nil {
		t.Errorf("human task should park, not terminate; got %+v", start.GetTerminal())
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "humancase", InstanceKey: "i1",
		Record: cmmnRecord("humancase", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: taskCompletedPayload("pi1", []byte(`{"ok":true}`)), LogicalTimeMs: 2000},
	}
	done, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("human task completion: %v", err)
	}
	if done.GetTerminal() == nil || done.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal after human completes, got %+v", done.GetTerminal())
	}
}

// TestEventForCMMN_ExternalUserEventOccurrence pins Gap 6 on the CMMN plane: an
// OccurUserEvent delivered through the external-event envelope (the
// DeliverProcessEvent path) decodes to cmmn.OccurUserEvent, so the engine fires
// the parked user-event listener. Closes the "parked but un-triggerable" gap for
// CMMN user-event listeners, the same way DeliverProcessEvent completes a human task.
func TestEventForCMMN_ExternalUserEventOccurrence(t *testing.T) {
	ev, err := eventForCMMN(cmmnExtPayload(t, "OccurUserEvent", cmmn.OccurUserEvent{PlanItemID: "evt1"}))
	if err != nil {
		t.Fatalf("eventForCMMN: %v", err)
	}
	ue, ok := ev.(cmmn.OccurUserEvent)
	if !ok {
		t.Fatalf("event = %T, want cmmn.OccurUserEvent", ev)
	}
	if ue.PlanItemID != "evt1" {
		t.Fatalf("PlanItemID = %q, want evt1", ue.PlanItemID)
	}
}

// TestAdvanceCMMN_ExitCriterionEmitsCancelInvoke: an exit criterion firing on an
// active task mid-case used to hit the outer switch default (CancelTask) →
// CaseFailed. Now the adapter emits a cancel_invoke for the exited node and the
// case keeps running its other items.
func TestAdvanceCMMN_ExitCriterionEmitsCancelInvoke(t *testing.T) {
	a := New(mustCMMNResolver(t, "caseExit", exitCriterionCaseCMMN))

	start, err := a.Advance(context.Background(), cmmnStartInput("caseExit", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if len(start.GetInvoke()) != 3 {
		t.Fatalf("want 3 dispatched tasks, got %d", len(start.GetInvoke()))
	}
	if len(start.GetCancelInvoke()) != 0 {
		t.Fatalf("no cancel expected at start, got %d", len(start.GetCancelInvoke()))
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "caseExit", InstanceKey: "i1",
		Record: cmmnRecord("caseExit", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: taskCompletedPayload("trigItem", []byte(`{}`)), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("exit-criterion turn must not error (was CaseFailed): %v", err)
	}
	found := false
	for _, ci := range adv.GetCancelInvoke() {
		if ci.GetNodeId() == "mainItem" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want CancelInvoke for exited mainItem, got %+v", adv.GetCancelInvoke())
	}
	if adv.GetTerminal() != nil {
		t.Fatalf("case must stay alive (keepItem still active), got terminal %+v", adv.GetTerminal())
	}
}

// TestAdvanceCMMN_SuspendBuffersCompletionUntilResume: a blocking task suspended
// mid-flight then completing used to crash the case (PISuspended rejects
// triggerComplete → CaseFailed). The adapter now honors the CMMN §7.6.1 host
// contract: a completion for a Suspended item is held (HoldEventNode, no advance,
// state unchanged), resume emits ReleaseHeldNode, and replaying the completion
// then finishes the case.
func TestAdvanceCMMN_SuspendBuffersCompletionUntilResume(t *testing.T) {
	a := New(mustCMMNResolver(t, "echocase", echoCaseCMMN))

	start, err := a.Advance(context.Background(), cmmnStartInput("echocase", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if len(start.GetInvoke()) != 1 {
		t.Fatalf("want 1 dispatched task, got %d", len(start.GetInvoke()))
	}

	// Suspend the in-flight task: pi1 → PISuspended. SuspendTask itself emits no
	// hold/release — the buffering keys off the engine's PISuspended state.
	sAdv, err := a.Advance(context.Background(), cmmnContinue("echocase", start.GetNewState(),
		cmmnExtPayload(t, "ManualSuspend", map[string]any{"PlanItemID": "pi1"}), 2000))
	if err != nil {
		t.Fatalf("ManualSuspend must not error: %v", err)
	}
	if sAdv.GetHoldEventNode() != "" || len(sAdv.GetReleaseHeldNode()) != 0 {
		t.Fatalf("suspend emits no hold/release; got hold=%q release=%v", sAdv.GetHoldEventNode(), sAdv.GetReleaseHeldNode())
	}

	// The task completes WHILE suspended: buffer it, don't advance.
	hAdv, err := a.Advance(context.Background(), cmmnContinue("echocase", sAdv.GetNewState(),
		taskCompletedPayload("pi1", []byte(`{"ok":true}`)), 3000))
	if err != nil {
		t.Fatalf("completion-while-suspended must not error (was CaseFailed): %v", err)
	}
	if hAdv.GetHoldEventNode() != "pi1" {
		t.Fatalf("want HoldEventNode=pi1, got %q", hAdv.GetHoldEventNode())
	}
	if hAdv.GetTerminal() != nil {
		t.Fatalf("held completion must not terminate the case, got %+v", hAdv.GetTerminal())
	}
	if len(hAdv.GetInvoke()) != 0 {
		t.Fatalf("held completion must not dispatch, got %d invokes", len(hAdv.GetInvoke()))
	}
	if string(hAdv.GetNewState()) != string(sAdv.GetNewState()) {
		t.Fatal("hold must leave case state unchanged (engine not advanced)")
	}

	// Resume: pi1 → PIActive, ResumeTask → ReleaseHeldNode.
	rAdv, err := a.Advance(context.Background(), cmmnContinue("echocase", sAdv.GetNewState(),
		cmmnExtPayload(t, "ManualResume", map[string]any{"PlanItemID": "pi1"}), 4000))
	if err != nil {
		t.Fatalf("ManualResume must not error: %v", err)
	}
	released := false
	for _, n := range rAdv.GetReleaseHeldNode() {
		if n == "pi1" {
			released = true
		}
	}
	if !released {
		t.Fatalf("resume must release the buffered node; got %v", rAdv.GetReleaseHeldNode())
	}

	// Replay the buffered completion now that pi1 is Active again → case completes.
	done, err := a.Advance(context.Background(), cmmnContinue("echocase", rAdv.GetNewState(),
		taskCompletedPayload("pi1", []byte(`{"ok":true}`)), 5000))
	if err != nil {
		t.Fatalf("replayed completion: %v", err)
	}
	if done.GetTerminal() == nil || done.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal after resume+replay, got %+v", done.GetTerminal())
	}
}
