package engine

import (
	"bytes"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

type stubLeadership struct {
	leader atomic.Bool
	last   *enginev1.AnnounceLeader
}

func (s *stubLeadership) IsLeader() bool { return s.leader.Load() }
func (s *stubLeadership) OnAnnounceLeader(cmd *enginev1.AnnounceLeader) {
	s.last = cmd
}

func newTestPartition(t *testing.T) (*Partition, *stubLeadership, *ActionCollector) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "p", "state")
	snap, err := NewSnapshotter(dir, func(path string) (storage.Store, error) {
		return storage.OpenPebble(path, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	lead := &stubLeadership{}
	lead.leader.Store(true)
	col := &ActionCollector{}
	p := NewPartition(1, 1, PartitionConfig{
		Snapshotter: snap,
		Leadership:  lead,
		Collector:   col,
	})
	t.Cleanup(func() { _ = p.Close() })
	return p, lead, col
}

// testEnvelopeNowMs is the wall-clock value stamped onto the envelope
// Header by the test helpers below. Tests don't care about its
// absolute value — only that it is non-zero so the apply path reads a
// definite "now" instead of relying on a fallback that no longer
// exists in production (see partition.applyCommand).
const testEnvelopeNowMs uint64 = 1_700_000_000_000

func envelope(t *testing.T, cmd *enginev1.Command) []byte {
	t.Helper()
	buf, err := proto.Marshal(&enginev1.Envelope{
		Header:  &enginev1.Header{CreatedAtMs: testEnvelopeNowMs},
		Command: cmd,
	})
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func envelopeWithDedup(t *testing.T, d *enginev1.Dedup, cmd *enginev1.Command) []byte {
	t.Helper()
	buf, err := proto.Marshal(&enginev1.Envelope{
		Header:  &enginev1.Header{CreatedAtMs: testEnvelopeNowMs, Dedup: d},
		Command: cmd,
	})
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestPartition_ApplyInvokeAndJournal(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	// 1. Invoke
	invCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{
			Invoke: &enginev1.InvokeCommand{
				InvocationId: id, Target: target, Input: []byte("in"),
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: invCmd}}); err != nil {
		t.Fatal(err)
	}
	// Should produce ActInvoke (leader).
	actions := col.Drain()
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if _, ok := actions[0].(ActInvoke); !ok {
		t.Errorf("expected ActInvoke, got %T", actions[0])
	}

	// 2. JournalAppended(Input) -> Invoked
	jApp := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{
			InvokerEffect: &enginev1.InvokerEffect{
				InvocationId: id,
				Kind: &enginev1.InvokerEffect_JournalAppended{
					JournalAppended: &enginev1.JournalEntryAppended{
						Entry: &enginev1.JournalEntry{
							Index: 0,
							Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
						},
					},
				},
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: jApp}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// 3. Completed
	cmpCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{
			InvokerEffect: &enginev1.InvokerEffect{
				InvocationId: id,
				Kind: &enginev1.InvokerEffect_Completed{
					Completed: &enginev1.InvocationCompleted{Output: []byte("ok")},
				},
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: cmpCmd}}); err != nil {
		t.Fatal(err)
	}

	// Verify final status via Lookup.
	got, err := p.Lookup(LookupInvocation{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	st := got.(*enginev1.InvocationStatus)
	cmp, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed)
	if !ok {
		t.Fatalf("status = %T; want Completed", st.GetStatus())
	}
	if !bytes.Equal(cmp.Completed.GetOutput(), []byte("ok")) {
		t.Errorf("output mismatch: %x", cmp.Completed.GetOutput())
	}

	// applied_index must be 3.
	idx, err := p.Lookup(LookupAppliedIndex{})
	if err != nil {
		t.Fatal(err)
	}
	if idx.(uint64) != 3 {
		t.Errorf("applied_index = %v; want 3", idx)
	}
}

func TestPartition_FollowerDropsActions(t *testing.T) {
	p, lead, col := newTestPartition(t)
	lead.leader.Store(false)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatal(err)
	}
	if col.Len() != 0 {
		t.Errorf("follower must not buffer actions; got %d", col.Len())
	}
}

func TestPartition_DedupRejectsDuplicate(t *testing.T) {
	p, _, _ := newTestPartition(t)

	dedup := &enginev1.Dedup{Kind: &enginev1.Dedup_Arbitrary{
		Arbitrary: &enginev1.ArbitraryDedup{ProducerId: "ingress-1", Seq: 1},
	}}
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	invokeCmd := &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target,
	}}}

	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelopeWithDedup(t, dedup, invokeCmd)}}); err != nil {
		t.Fatal(err)
	}
	// Re-applying the same dedup should be a no-op (no new action).
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelopeWithDedup(t, dedup, invokeCmd)}}); err != nil {
		t.Fatal(err)
	}

	// applied_index advances to 2.
	idx, _ := p.Lookup(LookupAppliedIndex{})
	if idx.(uint64) != 2 {
		t.Errorf("applied_index = %v; want 2", idx)
	}
}

func TestPartition_IdempotencyKey_FirstInvokeWinsSecondDropped(t *testing.T) {
	p, _, col := newTestPartition(t)

	target := &enginev1.InvocationTarget{ServiceName: "Counter", HandlerName: "incr", ObjectKey: ""}
	idA := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("aaaaaaaaaaaaaaaa")}
	idB := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("bbbbbbbbbbbbbbbb")}

	mkInvoke := func(id *enginev1.InvocationId) []byte {
		return envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId:   id,
			Target:         target,
			Input:          []byte("in"),
			IdempotencyKey: "req-1",
		}}})
	}

	// First Invoke wins: status registered, ActInvoke emitted.
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: mkInvoke(idA)}}); err != nil {
		t.Fatal(err)
	}
	acts := col.Drain()
	if len(acts) != 1 {
		t.Fatalf("first invoke: got %d actions, want 1", len(acts))
	}
	if a, ok := acts[0].(ActInvoke); !ok || !bytes.Equal(a.ID.GetUuid(), idA.GetUuid()) {
		t.Errorf("first action: %+v want ActInvoke for idA", acts[0])
	}

	// Second Invoke with same idempotency_key but new id: dropped, no action,
	// and idB never appears in InvocationTable.
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: mkInvoke(idB)}}); err != nil {
		t.Fatal(err)
	}
	if n := col.Len(); n != 0 {
		t.Errorf("second invoke: got %d actions, want 0", n)
	}
	got, err := p.Lookup(LookupInvocation{ID: idB})
	if err != nil {
		t.Fatal(err)
	}
	st := got.(*enginev1.InvocationStatus)
	if _, free := st.GetStatus().(*enginev1.InvocationStatus_Free); !free && st.GetStatus() != nil {
		t.Errorf("idB status = %T; want Free/absent", st.GetStatus())
	}

	// LookupIdempotency returns idA.
	res, err := p.Lookup(LookupIdempotency{Service: "Counter", Handler: "incr", IdempotencyKey: "req-1"})
	if err != nil {
		t.Fatal(err)
	}
	prior, ok := res.(*enginev1.InvocationId)
	if !ok || prior == nil {
		t.Fatalf("LookupIdempotency: %v %T", res, res)
	}
	if !bytes.Equal(prior.GetUuid(), idA.GetUuid()) {
		t.Errorf("LookupIdempotency uuid = %x; want %x", prior.GetUuid(), idA.GetUuid())
	}

	// LookupIdempotency for an absent key returns a typed-nil *InvocationId.
	res2, err := p.Lookup(LookupIdempotency{Service: "Counter", Handler: "incr", IdempotencyKey: "req-999"})
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := res2.(*enginev1.InvocationId); id != nil {
		t.Errorf("absent key returned %+v; want nil", id)
	}
}

func TestPartition_ClearAllState_WipesAllRowsForObject(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "Counter", HandlerName: "incr", ObjectKey: "user-1"}
	otherTarget := &enginev1.InvocationTarget{ServiceName: "Counter", HandlerName: "incr", ObjectKey: "user-2"}

	// Seed StateTable with rows on two objects so we can confirm only the
	// invocation's own object is wiped.
	store := p.cfg.Snapshotter.Store()
	st := tables.StateTable{S: store}
	b := store.NewBatch()
	for _, k := range []string{"a", "b", "c"} {
		if err := st.Set(b, target, k, []byte(k+"-val")); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Set(b, otherTarget, "z", []byte("z-val")); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	// Move the invocation to Invoked so JEClearAllState's status-target
	// extraction succeeds.
	invCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target, Input: []byte("in"),
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: invCmd}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	jApp := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{
				Index: 0,
				Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
			},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: jApp}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Fire JEClearAllState at journal index 1.
	clearAll := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{
				Index: 1,
				Entry: &enginev1.JournalEntry_ClearAllState{ClearAllState: &enginev1.JEClearAllState{}},
			},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: clearAll}}); err != nil {
		t.Fatal(err)
	}

	// All rows on the invocation's object are gone.
	for _, k := range []string{"a", "b", "c"} {
		_, present, err := st.Get(target, k)
		if err != nil {
			t.Fatal(err)
		}
		if present {
			t.Errorf("state[%s] still present after ClearAllState", k)
		}
	}
	// Rows on a different object_key are untouched.
	v, present, err := st.Get(otherTarget, "z")
	if err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Errorf("user-2/z was wiped by ClearAllState on user-1")
	}
	if !bytes.Equal(v, []byte("z-val")) {
		t.Errorf("user-2/z value drift: %q", v)
	}
}

func TestPartition_RunProposal_TerminalWritesJERun(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	// Drive to Invoked first.
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{InvocationId: id, Target: target, Input: []byte("in")}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 0,
					Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
				},
			}},
		}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Terminal Run proposal — retryable=false.
	terminal := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_RunProposal{RunProposal: &enginev1.JERunProposal{
			EntryIndex: 1, Value: []byte("ok"), Retryable: false, Attempt: 0,
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: terminal}}); err != nil {
		t.Fatal(err)
	}
	// No timer scheduled for terminal proposal.
	for _, a := range col.Drain() {
		if _, isTimer := a.(ActRegisterTimer); isTimer {
			t.Errorf("terminal proposal must not schedule a timer; got %T", a)
		}
	}

	// Journal entry at index 1 has retryable=false + value=ok.
	journal := tables.JournalTable{S: p.cfg.Snapshotter.Store()}
	got, err := journal.Read(id, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := got.GetEntry().(*enginev1.JournalEntry_Run)
	if !ok {
		t.Fatalf("entry at idx 1 is %T; want JERun", got.GetEntry())
	}
	if run.Run.GetRetryable() {
		t.Errorf("retryable=true for terminal proposal")
	}
	if !bytes.Equal(run.Run.GetValue(), []byte("ok")) {
		t.Errorf("value mismatch: %q", run.Run.GetValue())
	}
}

func TestPartition_RunProposal_RetryableSchedulesTimer(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("aaaaaaaaaaaaaaaa")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{InvocationId: id, Target: target}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 0,
					Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
				},
			}},
		}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Retryable proposal — apply must (a) write JERun{retryable=true},
	// (b) insert a timer, (c) push ActRegisterTimer.
	retryable := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_RunProposal{RunProposal: &enginev1.JERunProposal{
			EntryIndex:     1,
			FailureMessage: "transient",
			Retryable:      true,
			Attempt:        0,
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: retryable}}); err != nil {
		t.Fatal(err)
	}
	acts := col.Drain()
	var timerAct *ActRegisterTimer
	for i := range acts {
		if rt, ok := acts[i].(ActRegisterTimer); ok {
			timerAct = &rt
		}
	}
	if timerAct == nil {
		t.Fatalf("retryable proposal must emit ActRegisterTimer; got %v", acts)
	}
	if timerAct.SleepIdx != 1 {
		t.Errorf("timer sleep_idx = %d; want 1 (JERun journal index)", timerAct.SleepIdx)
	}

	// Journal entry is JERun{retryable=true}.
	journal := tables.JournalTable{S: p.cfg.Snapshotter.Store()}
	got, err := journal.Read(id, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := got.GetEntry().(*enginev1.JournalEntry_Run)
	if !ok {
		t.Fatalf("entry at idx 1 is %T; want JERun", got.GetEntry())
	}
	if !run.Run.GetRetryable() {
		t.Errorf("retryable=false; want true")
	}
	if run.Run.GetFailureMessage() != "transient" {
		t.Errorf("failure_message = %q", run.Run.GetFailureMessage())
	}
}

func TestPartition_RunProposal_ExhaustedPolicyDemotesToTerminal(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("bbbbbbbbbbbbbbbb")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{InvocationId: id, Target: target}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 0,
					Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
				},
			}},
		}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// max_attempts=2; this is attempt=2 → exhausted → demoted to terminal.
	proposal := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_RunProposal{RunProposal: &enginev1.JERunProposal{
			EntryIndex:     1,
			FailureMessage: "boom",
			Retryable:      true,
			Attempt:        2,
			RetryPolicy:    &enginev1.RunRetryPolicy{MaxAttempts: 2},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: proposal}}); err != nil {
		t.Fatal(err)
	}
	for _, a := range col.Drain() {
		if _, isTimer := a.(ActRegisterTimer); isTimer {
			t.Errorf("exhausted policy must not schedule a timer; got %T", a)
		}
	}

	journal := tables.JournalTable{S: p.cfg.Snapshotter.Store()}
	got, err := journal.Read(id, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := got.GetEntry().(*enginev1.JournalEntry_Run)
	if !ok {
		t.Fatalf("entry at idx 1 is %T; want JERun", got.GetEntry())
	}
	if run.Run.GetRetryable() {
		t.Errorf("exhausted policy must demote to retryable=false; got true")
	}
	if run.Run.GetFailureMessage() != "boom" {
		t.Errorf("failure_message lost on demotion: %q", run.Run.GetFailureMessage())
	}
}

func TestPartition_AnnounceLeaderNotifiesObserver(t *testing.T) {
	p, lead, _ := newTestPartition(t)
	cmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_AnnounceLeader{
			AnnounceLeader: &enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 5},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatal(err)
	}
	if lead.last == nil || lead.last.GetLeaderEpoch() != 5 {
		t.Fatalf("OnAnnounceLeader not called or wrong epoch: %+v", lead.last)
	}
}

func TestPartition_UnknownCommandIsNoop(t *testing.T) {
	p, _, _ := newTestPartition(t)
	// Empty Envelope (no command kind set) — must not error.
	buf, _ := proto.Marshal(&enginev1.Envelope{
		Header:  &enginev1.Header{CreatedAtMs: testEnvelopeNowMs},
		Command: &enginev1.Command{},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: buf}}); err != nil {
		t.Fatalf("unknown command must not return error; got %v", err)
	}
	idx, _ := p.Lookup(LookupAppliedIndex{})
	if idx.(uint64) != 1 {
		t.Errorf("applied_index = %v; want 1 (advance even on no-op)", idx)
	}
}

func TestPartition_MalformedEnvelopeIsNoop(t *testing.T) {
	p, _, _ := newTestPartition(t)
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: []byte("\xff\xff garbage \xff\xff")}}); err != nil {
		t.Fatalf("malformed envelope must not return error; got %v", err)
	}
	idx, _ := p.Lookup(LookupAppliedIndex{})
	if idx.(uint64) != 1 {
		t.Errorf("applied_index = %v; want 1", idx)
	}
}

func TestPartition_SleepInsertsTimerAndSurvives(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}

	// Invoke
	invokeCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target,
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: invokeCmd}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Journal Input (Scheduled -> Invoked)
	input := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 0, Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{}}},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: input}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Journal Sleep — should insert a timer and emit ActRegisterTimer.
	sleep := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 1, Entry: &enginev1.JournalEntry_Sleep{Sleep: &enginev1.JESleep{FireAtMs: 9999}}},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: sleep}}); err != nil {
		t.Fatal(err)
	}
	actions := col.Drain()
	var registered *ActRegisterTimer
	for _, a := range actions {
		if r, ok := a.(ActRegisterTimer); ok {
			registered = &r
		}
	}
	if registered == nil {
		t.Fatalf("expected ActRegisterTimer; got %+v", actions)
	}
	if registered.FireAtMs != 9999 {
		t.Errorf("ActRegisterTimer FireAtMs = %d; want 9999", registered.FireAtMs)
	}

	// Verify timer row persists.
	t2 := tables.TimerTable{S: p.cfg.Snapshotter.Store()}
	var found bool
	_ = t2.ScanAll(func(e tables.TimerEntry) error {
		if e.FireAtMs == 9999 {
			found = true
		}
		return nil
	})
	if !found {
		t.Errorf("timer row not persisted")
	}
}

func TestPartition_PurgeReapsPendingTimers(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}

	// Invoke → Input → Sleep (registers timer) → Complete → Purge.
	mustApply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	mustApply(1, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target,
	}}})
	mustApply(2, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 0, Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{}}},
		}},
	}}})
	mustApply(3, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 1, Entry: &enginev1.JournalEntry_Sleep{Sleep: &enginev1.JESleep{FireAtMs: 9999}}},
		}},
	}}})
	mustApply(4, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind:         &enginev1.InvokerEffect_Completed{Completed: &enginev1.InvocationCompleted{Output: []byte("ok")}},
	}}})
	col.Drain()

	mustApply(5, &enginev1.Command{Kind: &enginev1.Command_Purge{Purge: &enginev1.PurgeInvocation{
		InvocationId: id,
	}}})

	// Timer row must be gone.
	store := p.cfg.Snapshotter.Store()
	timersT := tables.TimerTable{S: store}
	var remaining int
	_ = timersT.ScanAll(func(e tables.TimerEntry) error {
		if e.FireAtMs == 9999 && bytes.Equal(e.ID.GetUuid(), id.GetUuid()) {
			remaining++
		}
		return nil
	})
	if remaining != 0 {
		t.Errorf("expected 0 pending timer rows after purge; got %d", remaining)
	}

	// ActDeleteTimer must have been emitted.
	var deleted *ActDeleteTimer
	for _, a := range col.Drain() {
		if d, ok := a.(ActDeleteTimer); ok {
			deleted = &d
		}
	}
	if deleted == nil {
		t.Fatal("expected ActDeleteTimer; got none")
	}
	if deleted.FireAtMs != 9999 {
		t.Errorf("ActDeleteTimer.FireAtMs = %d; want 9999", deleted.FireAtMs)
	}
}

func TestPartition_SnapshotRoundTrip(t *testing.T) {
	p, _, _ := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	invokeCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target,
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 42, Cmd: invokeCmd}}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := p.SaveSnapshot(nil, &buf, nil); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Build a fresh partition in a separate dir, then recover into it.
	dirB := filepath.Join(t.TempDir(), "p", "state")
	snapB, err := NewSnapshotter(dirB, func(path string) (storage.Store, error) {
		return storage.OpenPebble(path, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	leadB := &stubLeadership{}
	leadB.leader.Store(true)
	pB := NewPartition(1, 1, PartitionConfig{
		Snapshotter: snapB,
		Leadership:  leadB,
		Collector:   &ActionCollector{},
	})
	defer pB.Close()

	if err := pB.RecoverFromSnapshot(&buf, nil); err != nil {
		t.Fatalf("RecoverFromSnapshot: %v", err)
	}

	idx, err := pB.Open(nil)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 42 {
		t.Errorf("post-recover Open returned %d; want 42", idx)
	}
	gotStatus, _ := pB.Lookup(LookupInvocation{ID: id})
	if _, ok := gotStatus.(*enginev1.InvocationStatus).GetStatus().(*enginev1.InvocationStatus_Scheduled); !ok {
		t.Errorf("post-recover status = %T; want Scheduled", gotStatus)
	}
}
