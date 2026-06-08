package engine

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// holdAdvance is a no-advance turn: the adapter found the active event targets a
// Suspended node and wants its completion buffered (CMMN §7.6.1). new_state is
// carried unchanged.
func holdAdvance(pk uint64, svc, key, node string, state []byte) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: state, HoldEventNode: node,
	}}}
}

// releaseAdvance is the resume turn: the node is Active again, so its buffered
// completion is replayed into the inbox.
func releaseAdvance(pk uint64, svc, key string, nodes ...string) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s-resume"), ReleaseHeldNode: nodes,
	}}}
}

func taskCompletedEvent(pk uint64, svc, key, node string, taskID *enginev1.InvocationId) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: svc, InstanceKey: key,
		Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
			TaskCompleted: &enginev1.ProcessTaskCompleted{NodeId: node, Output: []byte("ok"), TaskInvocationId: taskID},
		}},
	}}}
}

// TestProcess_HoldBuffersCompletionThenReleaseReplays drives the apply-path
// mechanics of CMMN suspend completion-buffering with synthetic ProcessAdvanced
// turns: a dispatched task completes (feedback balances outstanding, clears the
// invoke index), a hold_event_node turn moves that completion out of the live
// inbox into proc_held without touching outstanding, and a release_held_node turn
// replays it back into the inbox as a fresh turn and drops the buffer.
func TestProcess_HoldBuffersCompletionThenReleaseReplays(t *testing.T) {
	p, col := newProcPartition(t, routing.NewPartitioner(4))
	const svc, key = "Proc", "i1"
	ipk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(ipk)
	root := processRootID(ipk, svc, key)
	procs, inbox := procStore(p)
	heldT := tables.ProcessHeldTable{S: p.cfg.Snapshotter.Store()}
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(ipk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	col.Drain()
	must(2, taskAdvance(ipk, svc, key, "T1", &enginev1.InvocationTarget{ServiceName: "bridge", HandlerName: "run"}))
	col.Drain()
	callee := firstInvokeID(t, p, root)

	rec, _, _ := procs.Get(lp, svc, key)
	if rec.GetOutstanding() != 1 {
		t.Fatalf("after dispatch: outstanding = %d, want 1", rec.GetOutstanding())
	}

	// The task completes: feedback decrements outstanding and clears the invoke
	// index; the TaskCompleted becomes the active inbox turn.
	must(3, taskCompletedEvent(ipk, svc, key, "T1", callee))
	col.Drain()
	rec, _, _ = procs.Get(lp, svc, key)
	if rec.GetOutstanding() != 0 {
		t.Fatalf("after feedback: outstanding = %d, want 0 (balanced at enqueue)", rec.GetOutstanding())
	}
	active := rec.GetActiveSeq()
	if active == 0 {
		t.Fatal("completion should be the active turn")
	}
	if n, _ := invokeIndexCount(p, root); n != 0 {
		t.Fatalf("invoke index = %d, want 0 (delete-on-complete)", n)
	}

	// Hold the completion (the adapter found T1 Suspended). It moves to proc_held,
	// leaves outstanding untouched, and the cursor goes idle.
	must(4, holdAdvance(ipk, svc, key, "T1", rec.GetStateBlob()))
	rec, _, _ = procs.Get(lp, svc, key)
	if rec.GetActiveSeq() != 0 {
		t.Fatalf("after hold: active_seq = %d, want 0", rec.GetActiveSeq())
	}
	if rec.GetOutstanding() != 0 {
		t.Fatalf("after hold: outstanding = %d, want 0 (hold must not touch it)", rec.GetOutstanding())
	}
	if _, present, _ := inbox.Get(lp, svc, key, active); present {
		t.Fatal("held completion must leave the live inbox")
	}
	if _, ok, err := heldT.Get(root, "T1"); err != nil || !ok {
		t.Fatalf("completion not buffered in proc_held: ok=%v err=%v", ok, err)
	}

	// Resume needs an active turn before the release advance can apply.
	must(5, procExtCmd(ipk, svc, key, []byte("resume")))
	col.Drain()
	must(6, releaseAdvance(ipk, svc, key, "T1"))

	if _, ok, _ := heldT.Get(root, "T1"); ok {
		t.Fatal("proc_held row must be deleted after release")
	}
	rec, _, _ = procs.Get(lp, svc, key)
	replaySeq := rec.GetActiveSeq()
	if replaySeq == 0 {
		t.Fatal("replayed completion should be the active turn after release")
	}
	entry, ok, err := inbox.Get(lp, svc, key, replaySeq)
	if err != nil || !ok {
		t.Fatalf("replayed inbox entry missing: ok=%v err=%v", ok, err)
	}
	if tc := entry.GetPayload().GetTaskCompleted(); tc == nil || tc.GetNodeId() != "T1" {
		t.Fatalf("replayed entry is not the buffered TaskCompleted for T1, got %+v", entry.GetPayload())
	}
}

// TestProcess_TerminateClearsBufferedCompletion: an instance terminated while a
// completion is buffered (exit criterion / TERMINATE on a still-suspended item)
// must drop the proc_held row so nothing leaks past the record.
func TestProcess_TerminateClearsBufferedCompletion(t *testing.T) {
	p, col := newProcPartition(t, routing.NewPartitioner(4))
	const svc, key = "Proc", "i1"
	ipk := routing.PartitionKey(svc, key)
	root := processRootID(ipk, svc, key)
	heldT := tables.ProcessHeldTable{S: p.cfg.Snapshotter.Store()}
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(ipk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	col.Drain()
	must(2, taskAdvance(ipk, svc, key, "T1", &enginev1.InvocationTarget{ServiceName: "bridge", HandlerName: "run"}))
	col.Drain()
	callee := firstInvokeID(t, p, root)
	must(3, taskCompletedEvent(ipk, svc, key, "T1", callee))
	col.Drain()
	procs, _ := procStore(p)
	rec, _, _ := procs.Get(keys.LPFromPartitionKey(ipk), svc, key)
	must(4, holdAdvance(ipk, svc, key, "T1", rec.GetStateBlob()))
	if _, ok, _ := heldT.Get(root, "T1"); !ok {
		t.Fatal("precondition: completion should be buffered")
	}

	// Terminate the instance with the completion still buffered.
	must(5, procExtCmd(ipk, svc, key, []byte("kill")))
	col.Drain()
	must(6, terminalAdvance(ipk, svc, key))

	if _, ok, _ := heldT.Get(root, "T1"); ok {
		t.Fatal("proc_held row must be range-deleted on terminate")
	}
}
