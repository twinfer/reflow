package invoker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// sessionFixture wires a session against an in-memory store with a fake
// proposer that mirrors the FSM's journal-append behaviour. Tests build
// one of these per scenario and call runAndWait to drive the session to
// natural completion.
type sessionFixture struct {
	s      *session
	fp     *fakeProposer
	store  storage.Store
	id     *enginev1.InvocationId
	target *enginev1.InvocationTarget
}

func newSessionFixture(t *testing.T, handler sdk.Handler) *sessionFixture {
	t.Helper()
	s := storage.NewMemStore()
	t.Cleanup(func() { s.Close() })
	fp := &fakeProposer{store: s}
	id := newID(1, "test-inv")
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}
	transport, _ := NewChanTransport()
	sess := newSession(
		context.Background(),
		id,
		target,
		handler,
		fp,
		NewJournalReader(tables.JournalTable{S: s}),
		tables.InvocationTable{S: s},
		tables.StateTable{S: s},
		transport,
		discardLogger(),
	)
	return &sessionFixture{s: sess, fp: fp, store: s, id: id, target: target}
}

func (f *sessionFixture) runAndWait(t *testing.T) {
	t.Helper()
	f.s.start()
	select {
	case <-f.s.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("session did not exit within 3s")
	}
}

func (f *sessionFixture) putStatus(t *testing.T, status *enginev1.InvocationStatus) {
	t.Helper()
	b := f.store.NewBatch()
	defer b.Close()
	if err := (tables.InvocationTable{S: f.store}).Put(b, f.id, status); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
}

func (f *sessionFixture) seedScheduled(t *testing.T, input []byte) {
	t.Helper()
	f.putStatus(t, &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Scheduled{
			Scheduled: &enginev1.Scheduled{Target: f.target, Input: input},
		},
	})
}

func (f *sessionFixture) seedInvoked(t *testing.T) {
	t.Helper()
	f.putStatus(t, &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Invoked{
			Invoked: &enginev1.Invoked{Target: f.target},
		},
	})
}

func (f *sessionFixture) seedCompleted(t *testing.T) {
	t.Helper()
	f.putStatus(t, &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Completed{
			Completed: &enginev1.Completed{Target: f.target},
		},
	})
}

func (f *sessionFixture) appendJournal(t *testing.T, entries ...*enginev1.JournalEntry) {
	t.Helper()
	b := f.store.NewBatch()
	defer b.Close()
	jt := tables.JournalTable{S: f.store}
	for _, e := range entries {
		if err := jt.Append(b, f.id, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// completedEffect returns the InvocationCompleted from the fixture's
// proposed effects, or nil if none was proposed.
func (f *sessionFixture) completedEffect() *enginev1.InvocationCompleted {
	for _, eff := range f.fp.effects() {
		if c, ok := eff.GetKind().(*enginev1.InvokerEffect_Completed); ok {
			return c.Completed
		}
	}
	return nil
}

// suspendedEffect returns the InvocationSuspended from the fixture's
// proposed effects, or nil if none was proposed.
func (f *sessionFixture) suspendedEffect() *enginev1.InvocationSuspended {
	for _, eff := range f.fp.effects() {
		if s, ok := eff.GetKind().(*enginev1.InvokerEffect_Suspended); ok {
			return s.Suspended
		}
	}
	return nil
}

// journalAppendCount returns how many JournalAppended effects landed on
// the proposer for journalEntryName tag.
func (f *sessionFixture) journalAppendsFor(checker func(*enginev1.JournalEntry) bool) int {
	count := 0
	for _, eff := range f.fp.effects() {
		if app, ok := eff.GetKind().(*enginev1.InvokerEffect_JournalAppended); ok {
			if checker(app.JournalAppended.GetEntry()) {
				count++
			}
		}
	}
	return count
}

// --- session-state-machine tests ---

func TestSession_ScheduledProposesInputAndCompletes(t *testing.T) {
	got := make(chan []byte, 1)
	handler := func(_ sdk.Context, input []byte) ([]byte, error) {
		got <- append([]byte(nil), input...)
		return []byte("done"), nil
	}
	f := newSessionFixture(t, handler)
	f.seedScheduled(t, []byte("hello"))

	f.runAndWait(t)

	select {
	case in := <-got:
		if string(in) != "hello" {
			t.Errorf("handler input = %q; want hello", in)
		}
	default:
		t.Fatal("handler did not observe input")
	}

	if n := f.journalAppendsFor(func(e *enginev1.JournalEntry) bool {
		_, ok := e.GetEntry().(*enginev1.JournalEntry_Input)
		return ok
	}); n != 1 {
		t.Errorf("JEInput proposals = %d; want 1", n)
	}

	cmp := f.completedEffect()
	if cmp == nil {
		t.Fatal("no Completed effect proposed")
	}
	if string(cmp.GetOutput()) != "done" {
		t.Errorf("Completed.output = %q; want done", cmp.GetOutput())
	}
	if cmp.GetFailureMessage() != "" {
		t.Errorf("Completed.failure_message = %q; want empty", cmp.GetFailureMessage())
	}
}

func TestSession_InvokedStatusSkipsInputPropose(t *testing.T) {
	handler := func(_ sdk.Context, _ []byte) ([]byte, error) { return []byte("ok"), nil }
	f := newSessionFixture(t, handler)
	f.seedInvoked(t)

	f.runAndWait(t)

	if n := f.journalAppendsFor(func(e *enginev1.JournalEntry) bool {
		_, ok := e.GetEntry().(*enginev1.JournalEntry_Input)
		return ok
	}); n != 0 {
		t.Errorf("JEInput proposals = %d; want 0 (already Invoked)", n)
	}
	if f.completedEffect() == nil {
		t.Error("expected Completed effect")
	}
}

func TestSession_CompletedStatusExitsEarly(t *testing.T) {
	called := false
	handler := func(_ sdk.Context, _ []byte) ([]byte, error) {
		called = true
		return nil, nil
	}
	f := newSessionFixture(t, handler)
	f.seedCompleted(t)

	f.runAndWait(t)

	if called {
		t.Error("handler should not run for Completed status")
	}
	if len(f.fp.effects()) != 0 {
		t.Errorf("effects after Completed-status exit = %v; want none", f.fp.effects())
	}
}

func TestSession_HandlerFailureRecordedAsCompleted(t *testing.T) {
	handler := func(_ sdk.Context, _ []byte) ([]byte, error) {
		return nil, sdk.NewFailure(7, "explode")
	}
	f := newSessionFixture(t, handler)
	f.seedInvoked(t)

	f.runAndWait(t)

	cmp := f.completedEffect()
	if cmp == nil {
		t.Fatal("no Completed effect")
	}
	if cmp.GetFailureMessage() != "explode" {
		t.Errorf("failure_message = %q; want explode", cmp.GetFailureMessage())
	}
}

func TestSession_HandlerGenericErrorRecordedAsCompleted(t *testing.T) {
	handler := func(_ sdk.Context, _ []byte) ([]byte, error) {
		return nil, errors.New("boom")
	}
	f := newSessionFixture(t, handler)
	f.seedInvoked(t)

	f.runAndWait(t)

	cmp := f.completedEffect()
	if cmp == nil {
		t.Fatal("no Completed effect")
	}
	if cmp.GetFailureMessage() != "boom" {
		t.Errorf("failure_message = %q; want boom", cmp.GetFailureMessage())
	}
}

func TestSession_HandlerPanicCaughtAsCompletedFailure(t *testing.T) {
	handler := func(_ sdk.Context, _ []byte) ([]byte, error) {
		panic("kapow")
	}
	f := newSessionFixture(t, handler)
	f.seedInvoked(t)

	f.runAndWait(t)

	cmp := f.completedEffect()
	if cmp == nil {
		t.Fatal("no Completed effect")
	}
	if !strings.Contains(cmp.GetFailureMessage(), "kapow") {
		t.Errorf("failure_message = %q; want it to mention kapow", cmp.GetFailureMessage())
	}
}

func TestSession_HandlerSuspendsProposesSuspended(t *testing.T) {
	handler := func(c sdk.Context, _ []byte) ([]byte, error) {
		_, err := c.Sleep(100 * time.Millisecond).Result()
		return nil, err
	}
	f := newSessionFixture(t, handler)
	f.seedInvoked(t)

	f.runAndWait(t)

	susp := f.suspendedEffect()
	if susp == nil {
		t.Fatal("no Suspended effect")
	}
	if len(susp.GetAwaitingOn()) != 1 || !strings.HasPrefix(susp.GetAwaitingOn()[0], "sleep:") {
		t.Errorf("awaiting_on = %v; want one sleep token", susp.GetAwaitingOn())
	}
	// Sleep itself proposed a JESleep before suspending.
	if n := f.journalAppendsFor(func(e *enginev1.JournalEntry) bool {
		_, ok := e.GetEntry().(*enginev1.JournalEntry_Sleep)
		return ok
	}); n != 1 {
		t.Errorf("JESleep proposals = %d; want 1", n)
	}
	// No Completed should have fired on the suspension path.
	if f.completedEffect() != nil {
		t.Errorf("Suspended path also proposed Completed: %v", f.completedEffect())
	}
}

func TestSession_AbortCancelsHandler(t *testing.T) {
	f := newSessionFixture(t, blockingHandler)
	f.seedInvoked(t)

	f.s.start()
	// Give the handler a moment to start.
	time.Sleep(20 * time.Millisecond)

	f.s.abort()
	select {
	case <-f.s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not exit after abort within 2s")
	}
	// Abort path: no terminal effect proposed.
	if len(f.fp.effects()) != 0 {
		t.Errorf("effects after abort = %v; want none", f.fp.effects())
	}
}

// --- invocationContext tests ---

// ctxFixture binds a fresh invocationContext to a real session backed by
// a fakeProposer. Use when the unit under test is one of the ctx
// methods rather than the run() lifecycle.
type ctxFixture struct {
	ictx *invocationContext
	fp   *fakeProposer
	s    *session
}

func newCtxFixture(t *testing.T, journalEntries ...*enginev1.JournalEntry) *ctxFixture {
	t.Helper()
	store := storage.NewMemStore()
	t.Cleanup(func() { store.Close() })
	fp := &fakeProposer{store: store}
	transport, _ := NewChanTransport()
	sess := newSession(
		context.Background(),
		newID(1, "ctx-inv"),
		&enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"},
		nil,
		fp,
		NewJournalReader(tables.JournalTable{S: store}),
		tables.InvocationTable{S: store},
		tables.StateTable{S: store},
		transport,
		discardLogger(),
	)
	idx := make(map[uint32]*enginev1.JournalEntry, len(journalEntries))
	for _, e := range journalEntries {
		idx[e.GetIndex()] = e
	}
	return &ctxFixture{
		ictx: newInvocationContext(sess, nil, idx, nil),
		fp:   fp,
		s:    sess,
	}
}

func makeSleepEntry(idx uint32, fireAtMs uint64) *enginev1.JournalEntry {
	return &enginev1.JournalEntry{
		Index: idx,
		Entry: &enginev1.JournalEntry_Sleep{Sleep: &enginev1.JESleep{FireAtMs: fireAtMs}},
	}
}

func makeSleepResultEntry(idx uint32, sleepIdx uint32) *enginev1.JournalEntry {
	return &enginev1.JournalEntry{
		Index: idx,
		Entry: &enginev1.JournalEntry_SleepResult{
			SleepResult: &enginev1.JESleepResult{SleepIndex: sleepIdx},
		},
	}
}

func makeRunEntry(idx uint32, value []byte, failure string) *enginev1.JournalEntry {
	return &enginev1.JournalEntry{
		Index: idx,
		Entry: &enginev1.JournalEntry_Run{Run: &enginev1.JERun{
			Value:          value,
			FailureMessage: failure,
		}},
	}
}

func TestContext_SleepFirstCallSuspends(t *testing.T) {
	cf := newCtxFixture(t)

	_, err := cf.ictx.Sleep(50 * time.Millisecond).Result()
	if !errors.Is(err, sdk.ErrSuspended) {
		t.Fatalf("Sleep err = %v; want ErrSuspended", err)
	}
	if cf.ictx.suspendedOn[0] != "sleep:1" {
		t.Errorf("suspendedOn = %v; want [sleep:1]", cf.ictx.suspendedOn)
	}
	// Verify JESleep was proposed.
	props := cf.fp.effects()
	if len(props) != 1 {
		t.Fatalf("proposals = %d; want 1", len(props))
	}
	app, ok := props[0].GetKind().(*enginev1.InvokerEffect_JournalAppended)
	if !ok {
		t.Fatalf("proposal kind = %T; want JournalAppended", props[0].GetKind())
	}
	if _, isSleep := app.JournalAppended.GetEntry().GetEntry().(*enginev1.JournalEntry_Sleep); !isSleep {
		t.Errorf("proposed entry is not JESleep")
	}
}

func TestContext_SleepReplayWithResultReturnsImmediately(t *testing.T) {
	cf := newCtxFixture(t,
		makeSleepEntry(1, 12345),
		makeSleepResultEntry(2, 1),
	)
	if _, err := cf.ictx.Sleep(50 * time.Millisecond).Result(); err != nil {
		t.Fatalf("Sleep on replay = %v; want nil", err)
	}
	if len(cf.fp.effects()) != 0 {
		t.Errorf("replay proposed effects: %v", cf.fp.effects())
	}
}

func TestContext_SleepReplayMissingResultSuspends(t *testing.T) {
	cf := newCtxFixture(t,
		makeSleepEntry(1, 12345),
		// no SleepResult — timer not yet fired
	)
	if _, err := cf.ictx.Sleep(50 * time.Millisecond).Result(); !errors.Is(err, sdk.ErrSuspended) {
		t.Fatalf("Sleep replay-no-result err = %v; want ErrSuspended", err)
	}
	if len(cf.fp.effects()) != 0 {
		t.Errorf("replay-no-result should not re-propose: %v", cf.fp.effects())
	}
}

func TestContext_SleepJournalDivergence(t *testing.T) {
	cf := newCtxFixture(t,
		makeRunEntry(1, []byte("oops"), ""), // wrong type at idx 1
	)
	_, err := cf.ictx.Sleep(50 * time.Millisecond).Result()
	if err == nil || !strings.Contains(err.Error(), "divergence") {
		t.Errorf("Sleep with wrong entry type err = %v; want divergence", err)
	}
}

func TestContext_RunFirstCallExecutesFn(t *testing.T) {
	cf := newCtxFixture(t)
	called := 0
	value, err := cf.ictx.Run("compute", func() ([]byte, error) {
		called++
		return []byte("answer"), nil
	})
	if err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if string(value) != "answer" {
		t.Errorf("value = %q; want answer", value)
	}
	if called != 1 {
		t.Errorf("fn called %d times; want 1", called)
	}
	// fakeProposer turns RunProposal into a JERun journal entry.
	props := cf.fp.effects()
	if len(props) != 1 {
		t.Fatalf("proposals = %d; want 1", len(props))
	}
	if _, ok := props[0].GetKind().(*enginev1.InvokerEffect_RunProposal); !ok {
		t.Errorf("proposal kind = %T; want RunProposal", props[0].GetKind())
	}
}

func TestContext_RunReplayUsesStoredValue(t *testing.T) {
	cf := newCtxFixture(t, makeRunEntry(1, []byte("cached"), ""))
	called := 0
	value, err := cf.ictx.Run("compute", func() ([]byte, error) {
		called++
		return []byte("WRONG"), nil
	})
	if err != nil {
		t.Fatalf("Run replay err = %v", err)
	}
	if string(value) != "cached" {
		t.Errorf("value = %q; want cached", value)
	}
	if called != 0 {
		t.Errorf("fn called %d times on replay; want 0", called)
	}
}

func TestContext_RunReplayWithStoredFailureReturnsFailure(t *testing.T) {
	cf := newCtxFixture(t, makeRunEntry(1, nil, "previous run failed"))
	_, err := cf.ictx.Run("compute", func() ([]byte, error) {
		t.Fatal("fn should not be called on replay-with-failure")
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error on replay-with-failure")
	}
	f, ok := sdk.AsFailure(err)
	if !ok {
		t.Errorf("err type = %T; want *sdk.Failure", err)
	} else if f.Message != "previous run failed" {
		t.Errorf("failure message = %q; want 'previous run failed'", f.Message)
	}
}

func TestContext_RunFnFailureProposedAndReturned(t *testing.T) {
	cf := newCtxFixture(t)
	_, err := cf.ictx.Run("compute", func() ([]byte, error) {
		return nil, errors.New("transient")
	})
	if err == nil {
		t.Fatal("expected error from Run when fn failed")
	}
	// Should have proposed RunProposal with a failure_message.
	props := cf.fp.effects()
	if len(props) != 1 {
		t.Fatalf("proposals = %d; want 1", len(props))
	}
	rp, ok := props[0].GetKind().(*enginev1.InvokerEffect_RunProposal)
	if !ok {
		t.Fatalf("proposal kind = %T; want RunProposal", props[0].GetKind())
	}
	if rp.RunProposal.GetFailureMessage() != "transient" {
		t.Errorf("RunProposal.failure_message = %q; want transient", rp.RunProposal.GetFailureMessage())
	}
}

func TestContext_RunNilFnRejected(t *testing.T) {
	cf := newCtxFixture(t)
	if _, err := cf.ictx.Run("name", nil); err == nil {
		t.Error("expected error from Run(nil)")
	}
}

func TestContext_SetStateLiveJournals(t *testing.T) {
	cf := newCtxFixture(t)
	if err := cf.ictx.SetState("key1", []byte("v1")); err != nil {
		t.Fatalf("SetState err = %v", err)
	}
	props := cf.fp.effects()
	if len(props) != 1 {
		t.Fatalf("proposals = %d; want 1", len(props))
	}
	app, ok := props[0].GetKind().(*enginev1.InvokerEffect_JournalAppended)
	if !ok {
		t.Fatalf("kind = %T; want JournalAppended", props[0].GetKind())
	}
	set, ok := app.JournalAppended.GetEntry().GetEntry().(*enginev1.JournalEntry_SetState)
	if !ok {
		t.Fatalf("entry kind = %T; want SetState", app.JournalAppended.GetEntry().GetEntry())
	}
	if set.SetState.GetKey() != "key1" || string(set.SetState.GetValue()) != "v1" {
		t.Errorf("entry = %+v; want key=key1 value=v1", set.SetState)
	}
}

func TestContext_SetStateReplayNoOp(t *testing.T) {
	existing := &enginev1.JournalEntry{
		Index: 1,
		Entry: &enginev1.JournalEntry_SetState{
			SetState: &enginev1.JESetState{Key: "key1", Value: []byte("v1")},
		},
	}
	cf := newCtxFixture(t, existing)
	if err := cf.ictx.SetState("key1", []byte("DIFFERENT")); err != nil {
		t.Fatalf("SetState replay err = %v", err)
	}
	if len(cf.fp.effects()) != 0 {
		t.Errorf("replay proposed effects: %v", cf.fp.effects())
	}
}

func TestContext_ClearStateLiveJournals(t *testing.T) {
	cf := newCtxFixture(t)
	if err := cf.ictx.ClearState("key1"); err != nil {
		t.Fatal(err)
	}
	props := cf.fp.effects()
	if len(props) != 1 {
		t.Fatalf("proposals = %d; want 1", len(props))
	}
	app, ok := props[0].GetKind().(*enginev1.InvokerEffect_JournalAppended)
	if !ok || app.JournalAppended.GetEntry().GetEntry() == nil {
		t.Fatalf("kind = %T; want JournalAppended", props[0].GetKind())
	}
	if _, isClear := app.JournalAppended.GetEntry().GetEntry().(*enginev1.JournalEntry_ClearState); !isClear {
		t.Errorf("entry kind != ClearState")
	}
}

func TestContext_CallFirstCallSuspends(t *testing.T) {
	cf := newCtxFixture(t)
	_, err := cf.ictx.Call(sdk.Target{Service: "T", Handler: "h"}, []byte("input")).Result()
	if !errors.Is(err, sdk.ErrSuspended) {
		t.Fatalf("Call err = %v; want ErrSuspended", err)
	}
	if len(cf.ictx.suspendedOn) != 1 || !strings.HasPrefix(cf.ictx.suspendedOn[0], "call:") {
		t.Errorf("suspendedOn = %v; want one call token", cf.ictx.suspendedOn)
	}
	props := cf.fp.effects()
	if len(props) != 1 {
		t.Fatalf("proposals = %d; want 1", len(props))
	}
	app, ok := props[0].GetKind().(*enginev1.InvokerEffect_JournalAppended)
	if !ok {
		t.Fatalf("kind = %T; want JournalAppended", props[0].GetKind())
	}
	call, ok := app.JournalAppended.GetEntry().GetEntry().(*enginev1.JournalEntry_Call)
	if !ok {
		t.Fatalf("entry kind = %T; want Call", app.JournalAppended.GetEntry().GetEntry())
	}
	if call.Call.GetTarget().GetServiceName() != "T" {
		t.Errorf("target service = %q; want T", call.Call.GetTarget().GetServiceName())
	}
	if string(call.Call.GetInput()) != "input" {
		t.Errorf("input = %q; want input", call.Call.GetInput())
	}
}

func TestContext_CallReplayWithResult(t *testing.T) {
	callEntry := &enginev1.JournalEntry{
		Index: 1,
		Entry: &enginev1.JournalEntry_Call{
			Call: &enginev1.JECall{Target: &enginev1.InvocationTarget{ServiceName: "T", HandlerName: "h"}},
		},
	}
	resultEntry := &enginev1.JournalEntry{
		Index: 2,
		Entry: &enginev1.JournalEntry_CallResult{
			CallResult: &enginev1.JECallResult{CallIndex: 1, Result: []byte("from-callee")},
		},
	}
	cf := newCtxFixture(t, callEntry, resultEntry)
	out, err := cf.ictx.Call(sdk.Target{Service: "T", Handler: "h"}, nil).Result()
	if err != nil {
		t.Fatalf("Call replay err = %v", err)
	}
	if string(out) != "from-callee" {
		t.Errorf("out = %q; want from-callee", out)
	}
	if len(cf.fp.effects()) != 0 {
		t.Errorf("replay proposed effects: %v", cf.fp.effects())
	}
}

func TestContext_AwakeableFirstCallMintsID(t *testing.T) {
	cf := newCtxFixture(t)
	id, fut := cf.ictx.Awakeable()
	if !strings.HasPrefix(id, "awk_") || len(id) != 26 {
		t.Errorf("awakeable id = %q; want awk_<22>", id)
	}
	if fut == nil {
		t.Fatal("future is nil")
	}
	// future.Result should suspend.
	if _, err := fut.Result(); !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("future.Result err = %v; want ErrSuspended", err)
	}
	props := cf.fp.effects()
	if len(props) != 1 {
		t.Fatalf("proposals = %d; want 1", len(props))
	}
	app := props[0].GetKind().(*enginev1.InvokerEffect_JournalAppended)
	ak := app.JournalAppended.GetEntry().GetEntry().(*enginev1.JournalEntry_Awakeable)
	if ak.Awakeable.GetAwakeableId() != id {
		t.Errorf("journaled id = %q; want %q", ak.Awakeable.GetAwakeableId(), id)
	}
}

func TestContext_AwakeableReplayWithResult(t *testing.T) {
	akEntry := &enginev1.JournalEntry{
		Index: 1,
		Entry: &enginev1.JournalEntry_Awakeable{Awakeable: &enginev1.JEAwakeable{AwakeableId: "awk_AAAAAAAAAAAAAAAAAAAAAA"}},
	}
	resEntry := &enginev1.JournalEntry{
		Index: 2,
		Entry: &enginev1.JournalEntry_AwakeableResult{
			AwakeableResult: &enginev1.JEAwakeableResult{
				AwakeableId: "awk_AAAAAAAAAAAAAAAAAAAAAA",
				Value:       []byte("resolved"),
			},
		},
	}
	cf := newCtxFixture(t, akEntry, resEntry)
	id, fut := cf.ictx.Awakeable()
	if id != "awk_AAAAAAAAAAAAAAAAAAAAAA" {
		t.Errorf("replay id = %q; want awk_AAAA...", id)
	}
	value, err := fut.Result()
	if err != nil {
		t.Fatalf("future.Result err = %v", err)
	}
	if string(value) != "resolved" {
		t.Errorf("value = %q; want resolved", value)
	}
	if len(cf.fp.effects()) != 0 {
		t.Errorf("replay proposed effects: %v", cf.fp.effects())
	}
}

func TestContext_AwakeableResultPropagatesFailure(t *testing.T) {
	akEntry := &enginev1.JournalEntry{
		Index: 1,
		Entry: &enginev1.JournalEntry_Awakeable{Awakeable: &enginev1.JEAwakeable{AwakeableId: "awk_BBBBBBBBBBBBBBBBBBBBBB"}},
	}
	resEntry := &enginev1.JournalEntry{
		Index: 2,
		Entry: &enginev1.JournalEntry_AwakeableResult{
			AwakeableResult: &enginev1.JEAwakeableResult{
				AwakeableId:    "awk_BBBBBBBBBBBBBBBBBBBBBB",
				FailureMessage: "callee rejected",
			},
		},
	}
	cf := newCtxFixture(t, akEntry, resEntry)
	_, fut := cf.ictx.Awakeable()
	_, err := fut.Result()
	if err == nil {
		t.Fatal("expected failure")
	}
	if _, ok := sdk.AsFailure(err); !ok {
		t.Errorf("err type = %T; want *sdk.Failure", err)
	}
}

func TestContext_SuspendIsSticky(t *testing.T) {
	cf := newCtxFixture(t)
	// First Sleep suspends.
	if _, err := cf.ictx.Sleep(time.Millisecond).Result(); !errors.Is(err, sdk.ErrSuspended) {
		t.Fatal("first Sleep did not suspend")
	}
	// Second ctx call must also return ErrSuspended without journaling.
	if err := cf.ictx.SetState("k", []byte("v")); !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("SetState after suspend err = %v; want ErrSuspended", err)
	}
	if _, err := cf.ictx.Run("r", func() ([]byte, error) { return nil, nil }); !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("Run after suspend err = %v; want ErrSuspended", err)
	}
	if _, err := cf.ictx.Call(sdk.Target{Service: "T", Handler: "h"}, nil).Result(); !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("Call after suspend err = %v; want ErrSuspended", err)
	}
	// Only the first Sleep should have proposed an effect.
	if len(cf.fp.effects()) != 1 {
		t.Errorf("propose count after suspend = %d; want 1", len(cf.fp.effects()))
	}
}

func TestContext_OneWayCallStub(t *testing.T) {
	cf := newCtxFixture(t)
	err := cf.ictx.OneWayCall(sdk.Target{Service: "T", Handler: "h"}, nil)
	if !errors.Is(err, errNotImplemented) {
		t.Errorf("OneWayCall err = %v; want errNotImplemented", err)
	}
}

func TestContext_GetStateStub(t *testing.T) {
	cf := newCtxFixture(t)
	_, _, err := cf.ictx.GetState("k")
	if !errors.Is(err, errNotImplemented) {
		t.Errorf("GetState err = %v; want errNotImplemented", err)
	}
}

func TestContext_SendSignalStub(t *testing.T) {
	cf := newCtxFixture(t)
	err := cf.ictx.SendSignal(sdk.Target{Service: "T", Handler: "h"}, "boom", nil)
	if !errors.Is(err, errNotImplemented) {
		t.Errorf("SendSignal err = %v; want errNotImplemented", err)
	}
}

func TestContext_InputAndIDExposed(t *testing.T) {
	store := storage.NewMemStore()
	defer store.Close()
	fp := &fakeProposer{store: store}
	id := newID(7, "abc")
	transport, _ := NewChanTransport()
	sess := newSession(
		context.Background(),
		id,
		&enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"},
		nil,
		fp,
		NewJournalReader(tables.JournalTable{S: store}),
		tables.InvocationTable{S: store},
		tables.StateTable{S: store},
		transport,
		discardLogger(),
	)
	ictx := newInvocationContext(sess, []byte("payload"), nil, nil)

	if string(ictx.Input()) != "payload" {
		t.Errorf("Input = %q; want payload", ictx.Input())
	}
	if ictx.InvocationID() == nil || ictx.InvocationID().GetPartitionKey() != 7 {
		t.Errorf("InvocationID = %v; want id with pk=7", ictx.InvocationID())
	}
	if ictx.Context() == nil {
		t.Error("Context() returned nil")
	}
}

// --- end-to-end style: replay-then-resume on subsequent session run ---

func TestSession_SleepResumeAfterTimerFires(t *testing.T) {
	// Handler: Sleep then return.
	handler := func(c sdk.Context, _ []byte) ([]byte, error) {
		if _, err := c.Sleep(50 * time.Millisecond).Result(); err != nil {
			return nil, err
		}
		return []byte("woke"), nil
	}

	f := newSessionFixture(t, handler)
	f.seedInvoked(t)

	// First session run — handler suspends.
	f.runAndWait(t)
	susp := f.suspendedEffect()
	if susp == nil {
		t.Fatal("first run: expected Suspended")
	}

	// Simulate timer firing: append JESleepResult at index 2 (JESleep
	// was at index 1, written by fakeProposer during first run).
	f.appendJournal(t, makeSleepResultEntry(2, 1))

	// Second session run — fresh session, same id. Handler should
	// fast-replay through Sleep and complete.
	fp2 := &fakeProposer{store: f.store}
	transport, _ := NewChanTransport()
	s2 := newSession(
		context.Background(),
		f.id,
		f.target,
		handler,
		fp2,
		NewJournalReader(tables.JournalTable{S: f.store}),
		tables.InvocationTable{S: f.store},
		tables.StateTable{S: f.store},
		transport,
		discardLogger(),
	)
	s2.start()
	select {
	case <-s2.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("second session did not exit")
	}

	// The second run should have proposed Completed{output: "woke"}.
	var completed *enginev1.InvocationCompleted
	for _, eff := range fp2.effects() {
		if c, ok := eff.GetKind().(*enginev1.InvokerEffect_Completed); ok {
			completed = c.Completed
		}
	}
	if completed == nil {
		t.Fatalf("second run: no Completed; effects=%v", fp2.effects())
	}
	if string(completed.GetOutput()) != "woke" {
		t.Errorf("output = %q; want woke", completed.GetOutput())
	}
}

// TestSession_PreloadState_HydratesCache exercises the eager-state path:
// rows present in StateTable for the invocation's (service, object_key)
// are reflected in GetState via the in-memory cache, without any apply
// round-trip.
func TestSession_PreloadState_HydratesCache(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	target := &enginev1.InvocationTarget{ServiceName: "Counter", HandlerName: "incr", ObjectKey: "user-1"}

	// Seed StateTable with three rows.
	st := tables.StateTable{S: s}
	b := s.NewBatch()
	for _, kv := range [][2]string{{"a", "1"}, {"b", "two"}, {"c", "three!"}} {
		if err := st.Set(b, target, kv[0], []byte(kv[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	sess := &session{
		id:         newID(1, "preload-inv"),
		target:     target,
		stateTable: st,
		log:        discardLogger(),
	}
	cache := sess.preloadState()
	if cache == nil {
		t.Fatalf("preloadState returned nil; want hydrated cache")
	}
	if string(cache["a"]) != "1" || string(cache["b"]) != "two" || string(cache["c"]) != "three!" {
		t.Errorf("cache mismatch: %+v", cache)
	}

	ictx := newInvocationContext(sess, nil, map[uint32]*enginev1.JournalEntry{}, cache)
	val, present, err := ictx.GetState("b")
	if err != nil {
		t.Fatalf("GetState err = %v", err)
	}
	if !present || string(val) != "two" {
		t.Errorf("GetState(b): %q present=%v; want two/true", val, present)
	}
	val, present, err = ictx.GetState("absent")
	if err != nil || present {
		t.Errorf("GetState(absent): %q present=%v err=%v; want nil/false/nil", val, present, err)
	}
}

// TestSession_PreloadState_OverflowReturnsNil verifies the 64 KiB cap.
func TestSession_PreloadState_OverflowReturnsNil(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	target := &enginev1.InvocationTarget{ServiceName: "Big", HandlerName: "h", ObjectKey: "obj"}
	st := tables.StateTable{S: s}

	// One row whose value alone exceeds the cap.
	big := make([]byte, 64*1024+1)
	b := s.NewBatch()
	if err := st.Set(b, target, "huge", big); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	sess := &session{
		id:         newID(1, "big-inv"),
		target:     target,
		stateTable: st,
		log:        discardLogger(),
	}
	if cache := sess.preloadState(); cache != nil {
		t.Errorf("expected nil cache on overflow; got %d entries", len(cache))
	}
}

// TestSession_PreloadState_UnkeyedReturnsNil — unkeyed services have no
// per-object state contract, so preload short-circuits to nil and the
// existing not-implemented GetState path remains.
func TestSession_PreloadState_UnkeyedReturnsNil(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	sess := &session{
		id:         newID(1, "unkeyed"),
		target:     &enginev1.InvocationTarget{ServiceName: "Echo", HandlerName: "h"},
		stateTable: tables.StateTable{S: s},
		log:        discardLogger(),
	}
	if cache := sess.preloadState(); cache != nil {
		t.Errorf("unkeyed target should have nil cache; got %+v", cache)
	}
}

// TestContext_SetStateUpdatesCache verifies cache stays coherent with
// in-handler SetState calls.
func TestContext_SetStateUpdatesCache(t *testing.T) {
	cf := newCtxFixture(t)
	cf.ictx.stateCache = map[string][]byte{"existing": []byte("v0")}
	if err := cf.ictx.SetState("k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if got := cf.ictx.stateCache["k1"]; string(got) != "v1" {
		t.Errorf("cache[k1] = %q; want v1", got)
	}
	if err := cf.ictx.ClearState("existing"); err != nil {
		t.Fatal(err)
	}
	if _, present := cf.ictx.stateCache["existing"]; present {
		t.Errorf("cache[existing] still present after ClearState")
	}
	if err := cf.ictx.ClearAllState(); err != nil {
		t.Fatal(err)
	}
	if len(cf.ictx.stateCache) != 0 {
		t.Errorf("cache not empty after ClearAllState: %+v", cf.ictx.stateCache)
	}
}
