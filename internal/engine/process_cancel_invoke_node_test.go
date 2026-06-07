package engine

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// cancelInvokeAdvance is a synthetic turn carrying a CancelTask: the engine
// exited node mid-case and wants its in-flight work torn down silently.
func cancelInvokeAdvance(pk uint64, svc, key, node string) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s-cancel"),
		CancelInvoke: []*enginev1.InvokeCancel{{NodeId: node}},
	}}}
}

// TestProcess_InvokeIndexCarriesNodeId: the proc_invoke_idx value is the
// dispatching node id, so a CancelTask can match its tasks by node.
func TestProcess_InvokeIndexCarriesNodeId(t *testing.T) {
	p, col := newProcPartition(t, routing.NewPartitioner(4))
	const svc, key = "Proc", "i1"
	ipk := routing.PartitionKey(svc, key)
	root := processRootID(ipk, svc, key)
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

	var gotNode string
	tbl := tables.ProcessInvokeIndexTable{S: p.cfg.Snapshotter.Store()}
	if err := tbl.ScanByInstance(root, func(_ *enginev1.InvocationId, node string) error {
		gotNode = node
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if gotNode != "T1" {
		t.Fatalf("index node = %q, want T1", gotNode)
	}
}

// TestProcess_CancelTaskSilentlyCancelsServiceTask: a CancelTask on a node with
// an in-flight service task force-cancels the task by id AND suppresses the
// parent feedback — the instance already exited the node, so a stale
// ProcessTaskCompleted would crash the next turn. We prove silence three ways:
// the instance stays RUNNING, outstanding balances to 0, and no feedback turn
// was enqueued.
func TestProcess_CancelTaskSilentlyCancelsServiceTask(t *testing.T) {
	target := &enginev1.InvocationTarget{ServiceName: "bridge", HandlerName: "run"}
	taskLP := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	part := routing.NewPartitioner(4)
	part.SetLPOwnersSnapshot(map[uint32]uint64{taskLP: 1}) // task local → inline cancel
	p, col := newProcPartition(t, part)

	const svc, key = "Proc", "i1"
	ipk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(ipk)
	root := processRootID(ipk, svc, key)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(ipk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	col.Drain()
	must(2, taskAdvance(ipk, svc, key, "T1", target))
	col.Drain()
	callee := firstInvokeID(t, p, root)

	// Land the callee with its ProcessParent up-link (as the real outbox Invoke
	// does) so the feedback arm WOULD fire if not suppressed.
	must(3, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: callee, Target: target, Input: []byte("in"),
		ParentLink: &enginev1.ParentLink{ProcessParent: &enginev1.ProcessParent{
			Pk: ipk, Service: svc, InstanceKey: key, NodeId: "T1",
		}},
	}}})
	col.Drain()
	if invStatus(t, p, callee).GetScheduled() == nil {
		t.Fatalf("precondition: task not Scheduled, got %T", invStatus(t, p, callee).GetStatus())
	}

	// Exit-criterion turn: an event then the CancelTask advance.
	must(4, procExtCmd(ipk, svc, key, []byte("exit")))
	col.Drain()
	must(5, cancelInvokeAdvance(ipk, svc, key, "T1"))

	assertCancelled(t, p, callee)
	if n, err := invokeIndexCount(p, root); err != nil || n != 0 {
		t.Fatalf("invoke-index not cleared: count=%d err=%v", n, err)
	}

	procs, inbox := procStore(p)
	rec, ok, err := procs.Get(lp, svc, key)
	if err != nil || !ok {
		t.Fatalf("record load: ok=%v err=%v", ok, err)
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		t.Fatalf("instance must stay running, got %v", rec.GetStatus())
	}
	if rec.GetOutstanding() != 0 {
		t.Fatalf("outstanding = %d, want 0 (cancel balanced it)", rec.GetOutstanding())
	}
	if rec.GetActiveSeq() != 0 {
		t.Fatalf("active seq = %d, want 0 (a stale feedback turn would be queued)", rec.GetActiveSeq())
	}
	if _, fed, _ := inbox.Get(lp, svc, key, 3); fed {
		t.Fatal("a ProcessTaskCompleted feedback was enqueued (seq 3): cancel was not silent")
	}
}

// TestProcess_CancelTaskCrossShardSuppressesFeedback: when the task lives on
// another shard, the CancelTask ships an OutboxEnvelope.cancel_invocation
// carrying suppress_parent_feedback=true so the silent kill survives the hop.
func TestProcess_CancelTaskCrossShardSuppressesFeedback(t *testing.T) {
	target := &enginev1.InvocationTarget{ServiceName: "bridge", HandlerName: "run"}
	taskLP := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	part := routing.NewPartitioner(4)
	part.SetLPOwnersSnapshot(map[uint32]uint64{taskLP: 2}) // task owned by shard 2
	p, col := newProcPartition(t, part)

	const svc, key = "Proc", "i1"
	ipk := routing.PartitionKey(svc, key)
	root := processRootID(ipk, svc, key)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(ipk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	col.Drain()
	must(2, taskAdvance(ipk, svc, key, "T1", target))
	col.Drain()
	callee := firstInvokeID(t, p, root)

	must(3, procExtCmd(ipk, svc, key, []byte("exit")))
	col.Drain()
	must(4, cancelInvokeAdvance(ipk, svc, key, "T1"))

	ot := tables.OutboxTable{S: p.cfg.Snapshotter.Store()}
	found := false
	if err := ot.ScanFrom(0, func(row tables.OutboxRow) error {
		ci := row.Envelope.GetCancelInvocation()
		if ci == nil {
			return nil
		}
		found = true
		if row.Envelope.GetDestinationShardId() != 2 {
			t.Errorf("dest shard = %d, want 2", row.Envelope.GetDestinationShardId())
		}
		if ci.GetId().GetPartitionKey() != callee.GetPartitionKey() {
			t.Errorf("cancel id pk mismatch: %d vs %d", ci.GetId().GetPartitionKey(), callee.GetPartitionKey())
		}
		if !ci.GetSuppressParentFeedback() {
			t.Error("cross-shard CancelTask must set suppress_parent_feedback")
		}
		return nil
	}); err != nil {
		t.Fatalf("scan outbox: %v", err)
	}
	if !found {
		t.Fatal("cross-shard: no CancelInvocation outbox envelope found")
	}
}

// TestProcess_CancelTaskCancelsChildInstance: a CancelTask on a node holding a
// running child process tears the child down (reusing the already-silent
// cancelInstanceTree), drops the parent's child-index row, and balances the
// parent's outstanding — all while the parent keeps running.
func TestProcess_CancelTaskCancelsChildInstance(t *testing.T) {
	p, _, col := newTestPartition(t)
	const psvc, pkey = "Parent", "p1"
	const csvc, ckey = "Child", "c1"
	ppk := routing.PartitionKey(psvc, pkey)
	plp := keys.LPFromPartitionKey(ppk)
	cpk := routing.PartitionKey(csvc, ckey)
	procs, _ := procStore(p)
	parentRoot := processRootID(ppk, psvc, pkey)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(ppk, psvc, pkey, []byte("pv"), &enginev1.ModelRef{Name: psvc}))
	col.Drain()
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: ppk, Service: psvc, InstanceKey: pkey, NewState: []byte("ps1"),
		StartChild: []*enginev1.ChildStart{{
			NodeId: "CA1", ModelRef: &enginev1.ModelRef{Name: csvc},
			Kind: enginev1.ProcessKind_PROCESS_KIND_BPMN, InstanceKey: ckey,
		}},
	}}})
	col.Drain()
	if n, _ := childIndexCount(p, parentRoot); n != 1 {
		t.Fatalf("precondition: child-index = %d, want 1", n)
	}
	if _, ok, _ := procs.Get(keys.LPFromPartitionKey(cpk), csvc, ckey); !ok {
		t.Fatal("precondition: child must exist")
	}

	// Exit the node holding the child mid-case.
	must(3, procExtCmd(ppk, psvc, pkey, []byte("exit")))
	col.Drain()
	must(4, cancelInvokeAdvance(ppk, psvc, pkey, "CA1"))

	if _, ok, _ := procs.Get(keys.LPFromPartitionKey(cpk), csvc, ckey); ok {
		t.Fatal("child instance must be cancelled by the CancelTask")
	}
	if n, err := childIndexCount(p, parentRoot); err != nil || n != 0 {
		t.Fatalf("parent child-index = %d (err %v), want 0", n, err)
	}
	rec, ok, _ := procs.Get(plp, psvc, pkey)
	if !ok {
		t.Fatal("parent record gone")
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		t.Fatalf("parent must stay running, got %v", rec.GetStatus())
	}
	if rec.GetOutstanding() != 0 {
		t.Fatalf("parent outstanding = %d, want 0 (child cancel balanced it)", rec.GetOutstanding())
	}
}
