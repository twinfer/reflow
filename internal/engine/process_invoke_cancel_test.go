package engine

import (
	"path/filepath"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// newProcPartition builds a leader shard-1 Partition wired to part — the
// process-instruction tests need a Partitioner (actuate mints task ids and
// routes their outbox envelopes).
func newProcPartition(t *testing.T, part *routing.Partitioner) (*Partition, *ActionCollector) {
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
		Snapshotter: snap, Leadership: lead, Collector: col, Partitioner: *part,
	})
	t.Cleanup(func() { _ = p.Close() })
	return p, col
}

func invokeIndexCount(p *Partition, root *enginev1.InvocationId) (int, error) {
	tbl := tables.ProcessInvokeIndexTable{S: p.cfg.Snapshotter.Store()}
	n := 0
	err := tbl.ScanByInstance(root, func(*enginev1.InvocationId) error { n++; return nil })
	return n, err
}

func firstInvokeID(t *testing.T, p *Partition, root *enginev1.InvocationId) *enginev1.InvocationId {
	t.Helper()
	tbl := tables.ProcessInvokeIndexTable{S: p.cfg.Snapshotter.Store()}
	var got *enginev1.InvocationId
	if err := tbl.ScanByInstance(root, func(id *enginev1.InvocationId) error {
		if got == nil {
			got = id
		}
		return nil
	}); err != nil {
		t.Fatalf("scan invoke index: %v", err)
	}
	if got == nil {
		t.Fatal("no proc_invoke_idx row")
	}
	return got
}

func taskAdvance(pk uint64, svc, key, node string, target *enginev1.InvocationTarget) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
		Invoke: []*enginev1.TaskInvoke{{NodeId: node, Target: target, Input: []byte("in")}},
	}}}
}

func terminalAdvance(pk uint64, svc, key string) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s2"),
		Terminal: &enginev1.ProcessTerminal{},
	}}}
}

// TestProcess_InvokeIndexPutAndClear: dispatching a service task writes one
// proc_invoke_idx row; the task's TaskCompleted feedback (carrying
// task_invocation_id) clears it.
func TestProcess_InvokeIndexPutAndClear(t *testing.T) {
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

	if n, err := invokeIndexCount(p, root); err != nil || n != 1 {
		t.Fatalf("after dispatch: invoke-index count = %d err=%v; want 1", n, err)
	}
	callee := firstInvokeID(t, p, root)

	// Task feeds back; the delete-on-complete block clears the row.
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: ipk, Service: svc, InstanceKey: key,
		Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
			TaskCompleted: &enginev1.ProcessTaskCompleted{NodeId: "T1", Output: []byte("ok"), TaskInvocationId: callee},
		}},
	}}})
	col.Drain()

	if n, err := invokeIndexCount(p, root); err != nil || n != 0 {
		t.Fatalf("after feedback: invoke-index count = %d err=%v; want 0", n, err)
	}
}

// TestProcess_TerminateCancelsInFlightTaskSameShard: terminating an instance
// with a task still in flight force-cancels that task by id (same-shard inline),
// leaving it Completed{CancelledCode}, and clears the index.
func TestProcess_TerminateCancelsInFlightTaskSameShard(t *testing.T) {
	target := &enginev1.InvocationTarget{ServiceName: "bridge", HandlerName: "run"}
	taskLP := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	part := routing.NewPartitioner(4)
	part.SetLPOwnersSnapshot(map[uint32]uint64{taskLP: 1}) // task is local to shard 1 → inline cancel
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

	// Simulate the outbox Invoke landing locally: the task invocation now exists.
	must(3, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: callee, Target: target, Input: []byte("in"),
	}}})
	col.Drain()
	if invStatus(t, p, callee).GetScheduled() == nil {
		t.Fatalf("precondition: task not Scheduled, got %T", invStatus(t, p, callee).GetStatus())
	}

	// Terminate the instance: kill event then the terminal advance.
	must(4, procExtCmd(ipk, svc, key, []byte("kill")))
	col.Drain()
	must(5, terminalAdvance(ipk, svc, key))

	assertCancelled(t, p, callee) // the in-flight task was force-cancelled
	if n, err := invokeIndexCount(p, root); err != nil || n != 0 {
		t.Fatalf("invoke-index not cleared on terminate: count=%d err=%v", n, err)
	}
}

// TestProcess_TerminateCancelsInFlightTaskCrossShardUsesOutbox: when the task
// invocation lives on a different shard, terminate ships a CancelInvocation
// outbox envelope to that shard instead of cancelling inline.
func TestProcess_TerminateCancelsInFlightTaskCrossShardUsesOutbox(t *testing.T) {
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

	must(3, procExtCmd(ipk, svc, key, []byte("kill")))
	col.Drain()
	must(4, terminalAdvance(ipk, svc, key))

	// A CancelInvocation envelope addressed to shard 2 carries the task id.
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
		if ci.GetPartitionKey() != callee.GetPartitionKey() {
			t.Errorf("cancel id pk mismatch: %d vs %d", ci.GetPartitionKey(), callee.GetPartitionKey())
		}
		return nil
	}); err != nil {
		t.Fatalf("scan outbox: %v", err)
	}
	if !found {
		t.Fatal("cross-shard: no CancelInvocation outbox envelope found")
	}
}

// TestOutboxEnvelopeToCommand_CancelInvocation: the cross-shard vehicle lands as
// an InvokerEffect_CancelById command on the dest shard.
func TestOutboxEnvelopeToCommand_CancelInvocation(t *testing.T) {
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("cancel-env-id--16")[:16]}
	env := &enginev1.OutboxEnvelope{Kind: &enginev1.OutboxEnvelope_CancelInvocation{CancelInvocation: id}}
	cmd := outboxEnvelopeToCommand(env)
	cb := cmd.GetInvokerEffect().GetCancelById()
	if cb == nil {
		t.Fatalf("want InvokerEffect_CancelById, got %T", cmd.GetKind())
	}
	if cb.GetPartitionKey() != 7 {
		t.Fatalf("cancel id pk = %d, want 7", cb.GetPartitionKey())
	}
}
