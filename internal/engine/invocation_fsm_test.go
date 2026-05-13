package engine

import (
	"errors"
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func mkID() *enginev1.InvocationId {
	return &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
}

func freeStatus() *enginev1.InvocationStatus {
	return &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Free{Free: &enginev1.Free{}},
	}
}

func scheduledStatus(target *enginev1.InvocationTarget) *enginev1.InvocationStatus {
	return &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Scheduled{
			Scheduled: &enginev1.Scheduled{Target: target, CreatedAtMs: 100},
		},
	}
}

func invokedStatus(target *enginev1.InvocationTarget) *enginev1.InvocationStatus {
	return &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Invoked{
			Invoked: &enginev1.Invoked{Target: target, CreatedAtMs: 100, InvokedAtMs: 200},
		},
	}
}

func suspendedStatus(target *enginev1.InvocationTarget) *enginev1.InvocationStatus {
	return &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Suspended{
			Suspended: &enginev1.Suspended{Target: target, SuspendedAtMs: 300},
		},
	}
}

func completedStatus(target *enginev1.InvocationTarget) *enginev1.InvocationStatus {
	return &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Completed{
			Completed: &enginev1.Completed{Target: target, Output: []byte("ok"), CompletedAtMs: 400},
		},
	}
}

func TestInvoke_FromFree(t *testing.T) {
	id := mkID()
	cmd := &enginev1.InvokeCommand{
		InvocationId: id,
		Target:       &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"},
		Input:        []byte("in"),
	}
	next, actions, err := transitionOnInvoke(id, freeStatus(), cmd, 100)
	if err != nil {
		t.Fatal(err)
	}
	sched, ok := next.GetStatus().(*enginev1.InvocationStatus_Scheduled)
	if !ok {
		t.Fatalf("got %T; want Scheduled", next.GetStatus())
	}
	if sched.Scheduled.GetTarget().GetHandlerName() != "h" {
		t.Errorf("target not preserved")
	}
	if len(actions) != 1 {
		t.Fatalf("actions = %d; want 1", len(actions))
	}
	inv, ok := actions[0].(ActInvoke)
	if !ok {
		t.Fatalf("action = %T; want ActInvoke", actions[0])
	}
	if inv.ID != id {
		t.Errorf("action id mismatch")
	}
}

func TestInvoke_IdempotentFromScheduledAndInvoked(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cmd := &enginev1.InvokeCommand{InvocationId: id, Target: target}

	cur := scheduledStatus(target)
	next, actions, err := transitionOnInvoke(id, cur, cmd, 100)
	if err != nil || len(actions) != 0 {
		t.Errorf("Scheduled+Invoke: err=%v actions=%d; want nil, 0", err, len(actions))
	}
	if next != cur {
		t.Errorf("expected same status pointer on no-op")
	}

	cur = invokedStatus(target)
	next, actions, err = transitionOnInvoke(id, cur, cmd, 100)
	if err != nil || len(actions) != 0 || next != cur {
		t.Errorf("Invoked+Invoke: err=%v actions=%d; want nil, 0, same", err, len(actions))
	}
}

func TestInvoke_InvalidFromCompleted(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cmd := &enginev1.InvokeCommand{InvocationId: id, Target: target}
	_, _, err := transitionOnInvoke(id, completedStatus(target), cmd, 100)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestJournalAppend_InputPromotesScheduled(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := scheduledStatus(target)
	app := &enginev1.JournalEntryAppended{
		Entry: &enginev1.JournalEntry{
			Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("x")}},
		},
	}
	next, _, err := transitionOnJournalAppend(id, cur, app, 250)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.GetStatus().(*enginev1.InvocationStatus_Invoked); !ok {
		t.Fatalf("Scheduled+Input: got %T; want Invoked", next.GetStatus())
	}
}

func TestJournalAppend_FromInvokedNoop(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := invokedStatus(target)
	app := &enginev1.JournalEntryAppended{Entry: &enginev1.JournalEntry{
		Entry: &enginev1.JournalEntry_Sleep{Sleep: &enginev1.JESleep{FireAtMs: 500}},
	}}
	next, actions, err := transitionOnJournalAppend(id, cur, app, 250)
	if err != nil || len(actions) != 0 || next != cur {
		t.Errorf("Invoked+JournalAppend: err=%v actions=%d", err, len(actions))
	}
}

func TestJournalAppend_FromSuspendedResumes(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := suspendedStatus(target)
	app := &enginev1.JournalEntryAppended{Entry: &enginev1.JournalEntry{
		Entry: &enginev1.JournalEntry_CallResult{CallResult: &enginev1.JECallResult{}},
	}}
	next, actions, err := transitionOnJournalAppend(id, cur, app, 350)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.GetStatus().(*enginev1.InvocationStatus_Invoked); !ok {
		t.Errorf("Suspended+JournalAppend(*Result): got %T; want Invoked", next.GetStatus())
	}
	if len(actions) != 1 {
		t.Errorf("expected ActInvoke on resume, got %d actions", len(actions))
	}
}

func TestComplete_FromInvoked(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	next, _, err := transitionOnComplete(id, invokedStatus(target), &enginev1.InvocationCompleted{Output: []byte("o")}, 400)
	if err != nil {
		t.Fatal(err)
	}
	cmp, ok := next.GetStatus().(*enginev1.InvocationStatus_Completed)
	if !ok {
		t.Fatalf("Invoked+Complete: got %T", next.GetStatus())
	}
	if string(cmp.Completed.GetOutput()) != "o" {
		t.Errorf("output not preserved")
	}
}

// TestComplete_FromSuspended documents Phase 2.5's race tolerance:
// a wake-event (TimerFired / AwakeableResolved / CallResult) can land
// between the SDK's in-flight Suspended propose and the next ActInvoke
// commit, causing the new session that completes the handler to send
// InvokerEffect.Completed while status is Suspended. Rejecting this
// would strand the invocation forever.
func TestComplete_FromSuspended(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := suspendedStatus(target)
	eff := &enginev1.InvocationCompleted{Output: []byte("done")}
	next, _, err := transitionOnComplete(id, cur, eff, 700)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	completed, ok := next.GetStatus().(*enginev1.InvocationStatus_Completed)
	if !ok {
		t.Fatalf("status = %T; want Completed", next.GetStatus())
	}
	if string(completed.Completed.GetOutput()) != "done" {
		t.Errorf("Output = %q; want done", completed.Completed.GetOutput())
	}
}

func TestComplete_Idempotent(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := completedStatus(target)
	next, _, err := transitionOnComplete(id, cur, &enginev1.InvocationCompleted{}, 400)
	if err != nil || next != cur {
		t.Errorf("Completed+Complete must be idempotent")
	}
}

func TestSuspend_FromInvoked(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	next, _, err := transitionOnSuspend(id, invokedStatus(target), &enginev1.InvocationSuspended{AwaitingOn: []string{"sleep:0"}}, 300)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := next.GetStatus().(*enginev1.InvocationStatus_Suspended)
	if !ok {
		t.Fatalf("got %T; want Suspended", next.GetStatus())
	}
	if len(s.Suspended.GetAwaitingOn()) != 1 {
		t.Errorf("awaiting_on not preserved")
	}
}

func TestTimerFired_ResumesSuspended(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	next, actions, err := transitionOnTimerFired(id, suspendedStatus(target), &enginev1.TimerFired{SleepIndex: 0}, 500)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.GetStatus().(*enginev1.InvocationStatus_Invoked); !ok {
		t.Fatalf("got %T; want Invoked", next.GetStatus())
	}
	if len(actions) != 1 {
		t.Errorf("expected ActInvoke")
	}
}

// TestTimerFired_OnInvokedEmitsActInvoke locks in the resume-race fix:
// a TimerFired that lands while the FSM still reports Invoked (because
// a resumed session's Suspended propose hasn't applied yet) must still
// emit ActInvoke. Otherwise the wake is swallowed: the SleepResult is
// journaled but nothing re-spawns a session once the existing one
// exits via Suspended. The Invoker dedupes idempotently against any
// currently-running session via its pendingRespawn queue.
func TestTimerFired_OnInvokedEmitsActInvoke(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := invokedStatus(target)
	next, actions, err := transitionOnTimerFired(id, cur, &enginev1.TimerFired{}, 500)
	if err != nil {
		t.Fatalf("err = %v; want nil", err)
	}
	if next != cur {
		t.Errorf("status changed; want Invoked → Invoked (status preserved)")
	}
	if len(actions) != 1 {
		t.Fatalf("actions = %d; want 1 (ActInvoke)", len(actions))
	}
	inv, ok := actions[0].(ActInvoke)
	if !ok {
		t.Fatalf("action[0] = %T; want ActInvoke", actions[0])
	}
	if inv.Target.GetServiceName() != "S" {
		t.Errorf("ActInvoke target = %q; want S", inv.Target.GetServiceName())
	}
}

func TestPurge_FromCompleted(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	next, _, err := transitionOnPurge(id, completedStatus(target))
	if err != nil {
		t.Fatal(err)
	}
	if _, free := next.GetStatus().(*enginev1.InvocationStatus_Free); !free {
		t.Errorf("expected Free, got %T", next.GetStatus())
	}
}

func TestPurge_FromInvokedInvalid(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	_, _, err := transitionOnPurge(id, invokedStatus(target))
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}

// Phase 2: new JournalEntry kinds are accepted in Invoked state as no-op
// state transitions. The wildcard logic in transitionOnJournalAppend
// covers them — these tests pin that behavior down.
func TestJournalAppend_Phase2EntryTypesNoOpFromInvoked(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cases := []struct {
		name  string
		entry *enginev1.JournalEntry
	}{
		{"JERun", &enginev1.JournalEntry{
			Entry: &enginev1.JournalEntry_Run{Run: &enginev1.JERun{Value: []byte("v")}},
		}},
		{"JEAwakeable", &enginev1.JournalEntry{
			Entry: &enginev1.JournalEntry_Awakeable{Awakeable: &enginev1.JEAwakeable{AwakeableId: "awk_AAAAAAAAAAAAAAAAAAAAAA"}},
		}},
		{"JESignal", &enginev1.JournalEntry{
			Entry: &enginev1.JournalEntry_Signal{Signal: &enginev1.JESignal{SignalName: "ping"}},
		}},
		{"JEClearState", &enginev1.JournalEntry{
			Entry: &enginev1.JournalEntry_ClearState{ClearState: &enginev1.JEClearState{Key: "k"}},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cur := invokedStatus(target)
			app := &enginev1.JournalEntryAppended{Entry: c.entry}
			next, actions, err := transitionOnJournalAppend(id, cur, app, 250)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if next != cur {
				t.Errorf("Invoked must remain Invoked (same pointer)")
			}
			if len(actions) != 0 {
				t.Errorf("expected no FSM actions; got %d", len(actions))
			}
		})
	}
}

func TestJournalAppend_AwakeableResultWakesSuspended(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := suspendedStatus(target)
	app := &enginev1.JournalEntryAppended{Entry: &enginev1.JournalEntry{
		Entry: &enginev1.JournalEntry_AwakeableResult{
			AwakeableResult: &enginev1.JEAwakeableResult{AwakeableId: "awk_AAAAAAAAAAAAAAAAAAAAAA", Value: []byte("v")},
		},
	}}
	next, actions, err := transitionOnJournalAppend(id, cur, app, 350)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.GetStatus().(*enginev1.InvocationStatus_Invoked); !ok {
		t.Errorf("got %T; want Invoked", next.GetStatus())
	}
	if len(actions) != 1 {
		t.Fatalf("expected ActInvoke, got %d actions", len(actions))
	}
	if _, ok := actions[0].(ActInvoke); !ok {
		t.Errorf("action[0] = %T; want ActInvoke", actions[0])
	}
}

func TestAwakeableResolved_FromSuspended(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	next, actions, err := transitionOnAwakeableResolved(id, suspendedStatus(target), 7, []byte("v"), "", 400)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.GetStatus().(*enginev1.InvocationStatus_Invoked); !ok {
		t.Fatalf("got %T; want Invoked", next.GetStatus())
	}
	if len(actions) != 2 {
		t.Fatalf("expected 2 actions (ActInvoke + ActDeliverNotification); got %d", len(actions))
	}
	if _, ok := actions[0].(ActInvoke); !ok {
		t.Errorf("action[0] = %T; want ActInvoke", actions[0])
	}
	notify, ok := actions[1].(ActDeliverNotification)
	if !ok {
		t.Fatalf("action[1] = %T; want ActDeliverNotification", actions[1])
	}
	if notify.CompletionID != 7 || string(notify.Value) != "v" {
		t.Errorf("notification fields: %+v", notify)
	}
}

func TestAwakeableResolved_FromInvokedLiveDelivery(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := invokedStatus(target)
	next, actions, err := transitionOnAwakeableResolved(id, cur, 9, nil, "boom", 500)
	if err != nil {
		t.Fatal(err)
	}
	if next != cur {
		t.Errorf("Invoked must remain Invoked (same pointer)")
	}
	if len(actions) != 1 {
		t.Fatalf("expected 1 action; got %d", len(actions))
	}
	notify, ok := actions[0].(ActDeliverNotification)
	if !ok {
		t.Fatalf("action[0] = %T; want ActDeliverNotification", actions[0])
	}
	if notify.Failure != "boom" || notify.CompletionID != 9 {
		t.Errorf("failure delivery fields: %+v", notify)
	}
}

func TestAwakeableResolved_FromCompletedNoop(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := completedStatus(target)
	next, actions, err := transitionOnAwakeableResolved(id, cur, 1, []byte("late"), "", 600)
	if err != nil || len(actions) != 0 || next != cur {
		t.Errorf("Completed late arrival: err=%v actions=%d", err, len(actions))
	}
}

func TestAwakeableResolved_FromFreeInvalid(t *testing.T) {
	id := mkID()
	_, _, err := transitionOnAwakeableResolved(id, freeStatus(), 1, nil, "", 0)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestSignalDelivered_FromSuspended(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	next, actions, err := transitionOnSignalDelivered(id, suspendedStatus(target), 4, "ping", []byte("payload"), 700)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.GetStatus().(*enginev1.InvocationStatus_Invoked); !ok {
		t.Fatalf("got %T; want Invoked", next.GetStatus())
	}
	if len(actions) != 2 {
		t.Fatalf("expected 2 actions; got %d", len(actions))
	}
	notify := actions[1].(ActDeliverNotification)
	if notify.CompletionID != 4 || string(notify.Value) != "payload" {
		t.Errorf("notify fields: %+v", notify)
	}
}

func TestSignalDelivered_FromInvokedLiveDelivery(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := invokedStatus(target)
	next, actions, err := transitionOnSignalDelivered(id, cur, 8, "shutdown", []byte("now"), 800)
	if err != nil {
		t.Fatal(err)
	}
	if next != cur || len(actions) != 1 {
		t.Errorf("Invoked live delivery: actions=%d next==cur=%v", len(actions), next == cur)
	}
}

func TestSignalDelivered_FromCompletedNoop(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := completedStatus(target)
	next, actions, err := transitionOnSignalDelivered(id, cur, 1, "late", nil, 900)
	if err != nil || len(actions) != 0 || next != cur {
		t.Errorf("late signal on Completed: err=%v actions=%d", err, len(actions))
	}
}

// ---- Phase 2.5: ParentLink propagation ----

func mkParentLink() *enginev1.ParentLink {
	return &enginev1.ParentLink{
		ParentId:  &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("parentuuid-aabb1")},
		CallIndex: 7,
	}
}

func parentLinkOf(s *enginev1.InvocationStatus) *enginev1.ParentLink {
	switch st := s.GetStatus().(type) {
	case *enginev1.InvocationStatus_Scheduled:
		return st.Scheduled.GetParentLink()
	case *enginev1.InvocationStatus_Invoked:
		return st.Invoked.GetParentLink()
	case *enginev1.InvocationStatus_Suspended:
		return st.Suspended.GetParentLink()
	default:
		return nil
	}
}

func assertParentLink(t *testing.T, got, want *enginev1.ParentLink) {
	t.Helper()
	if got.GetCallIndex() != want.GetCallIndex() {
		t.Fatalf("parent_link call_index: got %d, want %d", got.GetCallIndex(), want.GetCallIndex())
	}
	if string(got.GetParentId().GetUuid()) != string(want.GetParentId().GetUuid()) {
		t.Fatalf("parent_link parent_id mismatch")
	}
}

func TestInvoke_PropagatesParentLink(t *testing.T) {
	id := mkID()
	pl := mkParentLink()
	cmd := &enginev1.InvokeCommand{
		InvocationId: id,
		Target:       &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"},
		Input:        []byte("in"),
		ParentLink:   pl,
	}
	next, _, err := transitionOnInvoke(id, freeStatus(), cmd, 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assertParentLink(t, parentLinkOf(next), pl)
}

func TestJournalAppend_PropagatesParentLinkScheduledToInvoked(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	pl := mkParentLink()
	cur := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Scheduled{
			Scheduled: &enginev1.Scheduled{Target: target, CreatedAtMs: 100, ParentLink: pl},
		},
	}
	appended := &enginev1.JournalEntryAppended{
		Entry: &enginev1.JournalEntry{
			Index: 0,
			Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("x")}},
		},
	}
	next, _, err := transitionOnJournalAppend(id, cur, appended, 200)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assertParentLink(t, parentLinkOf(next), pl)
}

func TestJournalAppend_PropagatesParentLinkSuspendedToInvoked(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	pl := mkParentLink()
	cur := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Suspended{
			Suspended: &enginev1.Suspended{Target: target, SuspendedAtMs: 300, ParentLink: pl},
		},
	}
	appended := &enginev1.JournalEntryAppended{
		Entry: &enginev1.JournalEntry{
			Index: 5,
			Entry: &enginev1.JournalEntry_SleepResult{SleepResult: &enginev1.JESleepResult{SleepIndex: 3}},
		},
	}
	next, _, err := transitionOnJournalAppend(id, cur, appended, 400)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assertParentLink(t, parentLinkOf(next), pl)
}

func TestSuspend_PropagatesParentLink(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	pl := mkParentLink()
	cur := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Invoked{
			Invoked: &enginev1.Invoked{Target: target, InvokedAtMs: 200, ParentLink: pl},
		},
	}
	eff := &enginev1.InvocationSuspended{AwaitingOn: []string{"call:5"}}
	next, _, err := transitionOnSuspend(id, cur, eff, 500)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assertParentLink(t, parentLinkOf(next), pl)
}

func TestTimerFired_PropagatesParentLink(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	pl := mkParentLink()
	cur := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Suspended{
			Suspended: &enginev1.Suspended{Target: target, SuspendedAtMs: 300, ParentLink: pl},
		},
	}
	next, _, err := transitionOnTimerFired(id, cur, &enginev1.TimerFired{}, 400)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assertParentLink(t, parentLinkOf(next), pl)
}

func TestAwakeableResolved_PropagatesParentLink(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	pl := mkParentLink()
	cur := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Suspended{
			Suspended: &enginev1.Suspended{Target: target, SuspendedAtMs: 300, ParentLink: pl},
		},
	}
	next, _, err := transitionOnAwakeableResolved(id, cur, 4, []byte("v"), "", 400)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assertParentLink(t, parentLinkOf(next), pl)
}

func TestSignalDelivered_PropagatesParentLink(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	pl := mkParentLink()
	cur := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Suspended{
			Suspended: &enginev1.Suspended{Target: target, SuspendedAtMs: 300, ParentLink: pl},
		},
	}
	next, _, err := transitionOnSignalDelivered(id, cur, 4, "sig", []byte("p"), 400)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assertParentLink(t, parentLinkOf(next), pl)
}

// ---- Phase 2.5: transitionOnCallResultDelivered ----

func TestCallResultDelivered_WakesSuspendedParent(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "Caller"}
	cur := suspendedStatus(target)
	next, actions, err := transitionOnCallResultDelivered(id, cur, 8, []byte("pong"), "", 500)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, ok := next.GetStatus().(*enginev1.InvocationStatus_Invoked); !ok {
		t.Fatalf("expected Invoked, got %T", next.GetStatus())
	}
	if len(actions) != 2 {
		t.Fatalf("expected ActInvoke + ActDeliverNotification, got %d actions", len(actions))
	}
	if _, ok := actions[0].(ActInvoke); !ok {
		t.Errorf("actions[0] = %T; want ActInvoke", actions[0])
	}
	notify, ok := actions[1].(ActDeliverNotification)
	if !ok {
		t.Fatalf("actions[1] = %T; want ActDeliverNotification", actions[1])
	}
	if notify.CompletionID != 8 {
		t.Errorf("notify.CompletionID = %d; want 8", notify.CompletionID)
	}
	if string(notify.Value) != "pong" {
		t.Errorf("notify.Value = %q; want pong", notify.Value)
	}
}

func TestCallResultDelivered_OnInvokedJustNotifies(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "Caller"}
	cur := invokedStatus(target)
	next, actions, err := transitionOnCallResultDelivered(id, cur, 8, []byte("pong"), "", 500)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if next != cur {
		t.Errorf("status changed; expected no-op on Invoked")
	}
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if _, ok := actions[0].(ActDeliverNotification); !ok {
		t.Errorf("actions[0] = %T; want ActDeliverNotification", actions[0])
	}
}

func TestCallResultDelivered_OnCompletedNoop(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "Caller"}
	cur := completedStatus(target)
	next, actions, err := transitionOnCallResultDelivered(id, cur, 8, []byte("late"), "", 600)
	if err != nil || len(actions) != 0 || next != cur {
		t.Errorf("late CallResult on Completed: err=%v actions=%d", err, len(actions))
	}
}

func TestCallResultDelivered_FromFreeInvalid(t *testing.T) {
	id := mkID()
	_, _, err := transitionOnCallResultDelivered(id, freeStatus(), 1, nil, "", 100)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestCallResultDelivered_PropagatesParentLink(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "Caller"}
	pl := mkParentLink()
	cur := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Suspended{
			Suspended: &enginev1.Suspended{Target: target, SuspendedAtMs: 300, ParentLink: pl},
		},
	}
	next, _, err := transitionOnCallResultDelivered(id, cur, 8, []byte("pong"), "", 500)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assertParentLink(t, parentLinkOf(next), pl)
}
