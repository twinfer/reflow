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

func TestTimerFired_LateOnInvokedNoop(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cur := invokedStatus(target)
	next, actions, err := transitionOnTimerFired(id, cur, &enginev1.TimerFired{}, 500)
	if err != nil || len(actions) != 0 || next != cur {
		t.Errorf("late timer: err=%v actions=%d", err, len(actions))
	}
}

func TestPurge_FromCompleted(t *testing.T) {
	id := mkID()
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	next, _, err := transitionOnPurge(id, completedStatus(target), 0)
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
	_, _, err := transitionOnPurge(id, invokedStatus(target), 0)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}
