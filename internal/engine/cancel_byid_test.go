package engine

import (
	"bytes"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/storage/tables"
	"github.com/twinfer/reflw/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func cancelByIDCmd(id *enginev1.InvocationId) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		Kind: &enginev1.InvokerEffect_CancelById{CancelById: &enginev1.CancelById{Id: id}},
	}}}
}

func hasAbort(acts []Action, id *enginev1.InvocationId) bool {
	for i := range acts {
		if a, ok := acts[i].(ActAbortInvocation); ok &&
			a.ID.GetPartitionKey() == id.GetPartitionKey() && bytes.Equal(a.ID.GetUuid(), id.GetUuid()) {
			return true
		}
	}
	return false
}

func invStatus(t *testing.T, p *Partition, id *enginev1.InvocationId) *enginev1.InvocationStatus {
	t.Helper()
	st, err := (tables.InvocationTable{S: p.cfg.Snapshotter.Store()}).Get(id)
	if err != nil {
		t.Fatalf("load status: %v", err)
	}
	return st
}

// driveToInvoked applies Invoke then JournalAppended(Input) so id lands in
// the Invoked state, draining actions. startIdx is the first Raft index used.
func driveToInvoked(t *testing.T, p *Partition, col *ActionCollector, id *enginev1.InvocationId, target *enginev1.InvocationTarget, startIdx uint64) {
	t.Helper()
	invCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target, Input: []byte("in"),
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: startIdx, Cmd: invCmd}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	jApp := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 0, Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}}},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: startIdx + 1, Cmd: jApp}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	if st := invStatus(t, p, id); st.GetInvoked() == nil {
		t.Fatalf("precondition: want Invoked, got %T", st.GetStatus())
	}
}

func assertCancelled(t *testing.T, p *Partition, id *enginev1.InvocationId) {
	t.Helper()
	st := invStatus(t, p, id)
	cmp, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed)
	if !ok {
		t.Fatalf("status = %T; want Completed", st.GetStatus())
	}
	if cmp.Completed.GetFailureCode() != wire.CancelledCode {
		t.Fatalf("failure code = %d; want CancelledCode %d", cmp.Completed.GetFailureCode(), wire.CancelledCode)
	}
}

// TestPartition_CancelByIdFromInvoked: a by-id cancel of a running (Invoked)
// unkeyed invocation synthesizes a terminal Completed{CancelledCode} and emits
// ActAbortInvocation to interrupt the in-flight handler request.
func TestPartition_CancelByIdFromInvoked(t *testing.T) {
	p, _, col := newTestPartition(t)
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("cancel-invoked-16")[:16]}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"} // unkeyed

	driveToInvoked(t, p, col, id, target, 1)

	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: envelope(t, cancelByIDCmd(id))}}); err != nil {
		t.Fatal(err)
	}
	acts := col.Drain()
	assertCancelled(t, p, id)
	if !hasAbort(acts, id) {
		t.Fatalf("want ActAbortInvocation for running session; actions=%+v", acts)
	}
}

// TestPartition_CancelByIdFromScheduled: cancelling a Scheduled (not yet
// started) invocation terminates it but emits NO abort — there is no session.
func TestPartition_CancelByIdFromScheduled(t *testing.T) {
	p, _, col := newTestPartition(t)
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("cancel-sched-x16")[:16]}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	invCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target, Input: []byte("in"),
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: invCmd}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	if st := invStatus(t, p, id); st.GetScheduled() == nil {
		t.Fatalf("precondition: want Scheduled, got %T", st.GetStatus())
	}

	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, cancelByIDCmd(id))}}); err != nil {
		t.Fatal(err)
	}
	acts := col.Drain()
	assertCancelled(t, p, id)
	if hasAbort(acts, id) {
		t.Fatalf("Scheduled invocation has no session; abort must not be emitted; actions=%+v", acts)
	}
}

// TestPartition_CancelByIdIdempotentReplay: cancelling an already-terminal
// invocation is a clean no-op — the original output is preserved and no abort
// is emitted.
func TestPartition_CancelByIdIdempotentReplay(t *testing.T) {
	p, _, col := newTestPartition(t)
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("cancel-idem-x-16")[:16]}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	driveToInvoked(t, p, col, id, target, 1)
	cmpCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind:         &enginev1.InvokerEffect_Completed{Completed: &enginev1.InvocationCompleted{Output: []byte("ok")}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: cmpCmd}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	if _, err := p.Update([]statemachine.Entry{{Index: 4, Cmd: envelope(t, cancelByIDCmd(id))}}); err != nil {
		t.Fatal(err)
	}
	acts := col.Drain()
	st := invStatus(t, p, id)
	cmp := st.GetStatus().(*enginev1.InvocationStatus_Completed)
	if !bytes.Equal(cmp.Completed.GetOutput(), []byte("ok")) {
		t.Fatalf("idempotent replay clobbered output: %q", cmp.Completed.GetOutput())
	}
	if cmp.Completed.GetFailureCode() == wire.CancelledCode {
		t.Fatalf("idempotent replay overwrote terminal with CancelledCode")
	}
	if hasAbort(acts, id) {
		t.Fatalf("terminal invocation must not emit abort; actions=%+v", acts)
	}
}

// TestPartition_CancelByIdUnknownNoop: a by-id cancel for an id that was never
// seen is a no-op — no status row, no actions, no shard halt.
func TestPartition_CancelByIdUnknownNoop(t *testing.T) {
	p, _, col := newTestPartition(t)
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("cancel-ghost-x16")[:16]}

	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, cancelByIDCmd(id))}}); err != nil {
		t.Fatal(err)
	}
	if acts := col.Drain(); len(acts) != 0 {
		t.Fatalf("unknown-id cancel emitted actions: %+v", acts)
	}
	// inv.Get synthesizes Free for an absent row; the cancel must leave it
	// Free (no synthesized terminal).
	if st := invStatus(t, p, id); st.GetFree() == nil {
		t.Fatalf("unknown-id cancel wrote a non-Free status: %T", st.GetStatus())
	}
}
