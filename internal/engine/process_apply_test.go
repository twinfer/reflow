package engine

import (
	"bytes"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func extPayload(b []byte) *enginev1.ProcessEventPayload {
	return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_External{External: b}}
}

func procEventCmd(pk uint64, service, key string, event []byte, modelRef *enginev1.ModelRef) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: service, InstanceKey: key, Payload: extPayload(event), ModelRef: modelRef,
		Kind: enginev1.ProcessKind_PROCESS_KIND_BPMN,
	}}}
}

func procAdvancedCmd(pk uint64, service, key string, newState []byte, terminal *enginev1.ProcessTerminal) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: service, InstanceKey: key, NewState: newState, Terminal: terminal,
	}}}
}

// procExtCmd is a continuation (non-start) external event for an existing
// instance: no model_ref, so the apply path queues it as a turn rather than
// treating it as a start.
func procExtCmd(pk uint64, service, key string, event []byte) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: service, InstanceKey: key, Payload: extPayload(event),
	}}}
}

// childIndexCount returns how many live-child reverse-index rows parent owns.
func childIndexCount(p *Partition, parentRoot *enginev1.InvocationId) (int, error) {
	t := tables.ProcessChildIndexTable{S: p.cfg.Snapshotter.Store()}
	n := 0
	err := t.ScanByParent(parentRoot, func(*enginev1.ProcessCancel) error { n++; return nil })
	return n, err
}

// timerIndexCount returns how many armed-timer reverse-index rows root owns.
func timerIndexCount(p *Partition, root *enginev1.InvocationId) (int, error) {
	t := tables.ProcessTimerIndexTable{S: p.cfg.Snapshotter.Store()}
	n := 0
	err := t.ScanByInstance(root, func(*enginev1.InvocationId) error { n++; return nil })
	return n, err
}

// timerExists reports whether the TimerTable still holds a row for tid.
func timerExists(p *Partition, tid *enginev1.InvocationId) (bool, error) {
	t := tables.TimerTable{S: p.cfg.Snapshotter.Store()}
	found := false
	err := t.ScanByInvocation(tid, func(uint64) error { found = true; return nil })
	return found, err
}

// hasDeleteTimer reports whether acts contains an ActDeleteTimer for tid.
func hasDeleteTimer(acts []Action, tid *enginev1.InvocationId) bool {
	for i := range acts {
		if a, ok := acts[i].(ActDeleteTimer); ok && a.ID.GetPartitionKey() == tid.GetPartitionKey() &&
			bytes.Equal(a.ID.GetUuid(), tid.GetUuid()) {
			return true
		}
	}
	return false
}

func procStore(p *Partition) (tables.ProcessInstanceTable, tables.ProcessInboxTable) {
	s := p.cfg.Snapshotter.Store()
	return tables.ProcessInstanceTable{S: s}, tables.ProcessInboxTable{S: s}
}

// firstAdvance returns the first ActAdvanceProcess for service in acts.
func firstAdvance(acts []Action, service string) *ActAdvanceProcess {
	for i := range acts {
		if a, ok := acts[i].(ActAdvanceProcess); ok && a.Service == service {
			return &a
		}
	}
	return nil
}

func TestProcess_StartEnqueueActivate(t *testing.T) {
	p, _, col := newTestPartition(t)
	pk := routing.PartitionKey("OrderProc", "order-1")
	lp := keys.LPFromPartitionKey(pk)

	cmd := envelope(t, procEventCmd(pk, "OrderProc", "order-1", []byte("vars"),
		&enginev1.ModelRef{Kind: "bpmn", Name: "OrderProc", Version: "v1"}))
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatal(err)
	}

	acts := col.Drain()
	if len(acts) != 1 {
		t.Fatalf("want 1 action, got %d", len(acts))
	}
	adv, ok := acts[0].(ActAdvanceProcess)
	if !ok {
		t.Fatalf("want ActAdvanceProcess, got %T", acts[0])
	}
	if adv.Service != "OrderProc" || adv.InstanceKey != "order-1" || string(adv.Entry.GetPayload().GetExternal()) != "vars" {
		t.Fatalf("action mismatch: %+v", adv)
	}

	procs, inbox := procStore(p)
	rec, ok, err := procs.Get(lp, "OrderProc", "order-1")
	if err != nil || !ok {
		t.Fatalf("record load: ok=%v err=%v", ok, err)
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		t.Fatalf("status = %v", rec.GetStatus())
	}
	if rec.GetNextSeq() != 2 || rec.GetActiveSeq() != 1 {
		t.Fatalf("cursor: next=%d active=%d", rec.GetNextSeq(), rec.GetActiveSeq())
	}
	if rec.GetRootId().GetPartitionKey() != pk || len(rec.GetRootId().GetUuid()) != 16 {
		t.Fatalf("root_id: %+v", rec.GetRootId())
	}
	entry, ok, err := inbox.Get(lp, "OrderProc", "order-1", 1)
	if err != nil || !ok || string(entry.GetPayload().GetExternal()) != "vars" {
		t.Fatalf("inbox[1]: ok=%v err=%v entry=%+v", ok, err, entry)
	}
}

// TestProcess_SerializesConcurrentEvents is the core correctness property: two
// events for one instance arriving back-to-back (the parallel-gateway case)
// serialize through the inbox — the second queues behind the active turn and is
// activated only after the first turn's ProcessAdvanced commits, so the state
// blob is never raced. The terminal turn then reaps the instance.
func TestProcess_SerializesConcurrentEvents(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "P", "k1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, inbox := procStore(p)

	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
	recOf := func() *enginev1.ProcessInstanceRecord {
		t.Helper()
		r, ok, err := procs.Get(lp, svc, key)
		if err != nil || !ok {
			t.Fatalf("record load: ok=%v err=%v", ok, err)
		}
		return r
	}
	inboxHas := func(seq uint64) bool {
		t.Helper()
		_, ok, err := inbox.Get(lp, svc, key, seq)
		if err != nil {
			t.Fatalf("inbox get: %v", err)
		}
		return ok
	}

	// Start (seq 1) activates immediately.
	must(1, procEventCmd(pk, svc, key, []byte("start"), &enginev1.ModelRef{Name: svc}))
	if acts := col.Drain(); len(acts) != 1 {
		t.Fatalf("start: want 1 action, got %d", len(acts))
	}

	// Second event while turn 1 is active → queues, no activation.
	must(2, procEventCmd(pk, svc, key, []byte("e2"), nil))
	if acts := col.Drain(); len(acts) != 0 {
		t.Fatalf("queued event must not activate: got %d actions", len(acts))
	}
	if r := recOf(); r.GetActiveSeq() != 1 || r.GetNextSeq() != 3 {
		t.Fatalf("after queue: active=%d next=%d", r.GetActiveSeq(), r.GetNextSeq())
	}

	// Turn 1 completes (non-terminal) → dequeue seq 1, activate seq 2.
	must(3, procAdvancedCmd(pk, svc, key, []byte("s1"), nil))
	acts := col.Drain()
	if len(acts) != 1 {
		t.Fatalf("turn1 complete: want 1 action, got %d", len(acts))
	}
	if a := acts[0].(ActAdvanceProcess); string(a.Entry.GetPayload().GetExternal()) != "e2" {
		t.Fatalf("want activation for e2, got %q", a.Entry.GetPayload().GetExternal())
	}
	if r := recOf(); r.GetActiveSeq() != 2 || string(r.GetStateBlob()) != "s1" {
		t.Fatalf("after turn1: active=%d state=%q", r.GetActiveSeq(), r.GetStateBlob())
	}
	if inboxHas(1) {
		t.Fatal("inbox[1] should be dequeued")
	}

	// Turn 2 completes (terminal) → instance reaped (record + inbox gone), idle.
	must(4, procAdvancedCmd(pk, svc, key, []byte("s2"), &enginev1.ProcessTerminal{}))
	if acts := col.Drain(); len(acts) != 0 {
		t.Fatalf("terminal must not activate: got %d", len(acts))
	}
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatal("terminal instance should be reaped")
	}
	if inboxHas(2) {
		t.Fatal("inbox[2] should be dequeued")
	}
}

// TestProcess_ActuatesInstructions covers the non-terminal actuation arm: a
// service-task invoke becomes an outbox InvokeCommand carrying a process_parent
// link with a deterministic callee id; a timer arm becomes a persisted process
// timer + ActRegisterTimer; a later cancel deletes it.
func TestProcess_ActuatesInstructions(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Proc", "i1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Start → activate seq 1.
	must(1, procEventCmd(pk, svc, key, []byte("vars"), &enginev1.ModelRef{Name: svc}))
	col.Drain()

	// Turn 1: dispatch a service task + arm a boundary timer.
	target := &enginev1.InvocationTarget{ServiceName: "Cap", HandlerName: "do"}
	fireAt := testEnvelopeNowMs + 5000
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
		Invoke:   []*enginev1.TaskInvoke{{NodeId: "Task1", Target: target, Input: []byte("ti")}},
		ArmTimer: []*enginev1.TimerArm{{NodeId: "Boundary1", FireAtMs: fireAt, Slot: 1}},
	}}})

	var disp *ActDispatchOutbox
	var reg *ActRegisterTimer
	for _, a := range col.Drain() {
		switch act := a.(type) {
		case ActDispatchOutbox:
			disp = &act
		case ActRegisterTimer:
			reg = &act
		}
	}
	if disp == nil || reg == nil {
		t.Fatalf("want dispatch + register actions; disp=%v reg=%v", disp, reg)
	}

	// Task invoke carries the process_parent link + deterministic id.
	inv := disp.Envelope.GetInvoke()
	if inv == nil {
		t.Fatalf("outbox envelope is not an Invoke: %+v", disp.Envelope)
	}
	pp := inv.GetParentLink().GetProcessParent()
	if pp.GetPk() != pk || pp.GetService() != svc || pp.GetInstanceKey() != key || pp.GetNodeId() != "Task1" {
		t.Fatalf("process_parent link mismatch: %+v", pp)
	}
	// Turn 1 actuates at active_seq 1 (start activated seq 1).
	wantID := mintProcessTaskID(processRootID(pk, svc, key), "Task1", "", 1, 0, target)
	if inv.GetInvocationId().GetPartitionKey() != wantID.GetPartitionKey() ||
		!bytes.Equal(inv.GetInvocationId().GetUuid(), wantID.GetUuid()) {
		t.Fatalf("callee id not deterministic: %x", inv.GetInvocationId().GetUuid())
	}
	if string(inv.GetInput()) != "ti" || inv.GetTarget().GetServiceName() != "Cap" {
		t.Fatalf("task target/input mismatch: %+v", inv)
	}

	// Timer registered with a process descriptor + persisted row.
	if reg.Process.GetNodeId() != "Boundary1" || reg.Process.GetSlot() != 1 {
		t.Fatalf("timer process descriptor mismatch: %+v", reg.Process)
	}
	wantTID := processTimerID(pk, svc, key, "Boundary1", 1)
	if reg.ID.GetPartitionKey() != pk || !bytes.Equal(reg.ID.GetUuid(), wantTID.GetUuid()) {
		t.Fatalf("timer id mismatch: %x", reg.ID.GetUuid())
	}
	found := false
	if err := (tables.TimerTable{S: p.cfg.Snapshotter.Store()}).ScanByInvocation(wantTID, func(at uint64) error {
		if at == fireAt {
			found = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("process timer row not persisted")
	}
	if r, _, _ := procs.Get(lp, svc, key); r.GetActiveSeq() != 0 || string(r.GetStateBlob()) != "s1" {
		t.Fatalf("record after turn: %+v", r)
	}

	// Re-activate (seq 2) and cancel the boundary timer.
	must(3, procEventCmd(pk, svc, key, []byte("e2"), nil))
	col.Drain()
	must(4, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s2"),
		CancelTimer: []*enginev1.TimerCancel{{NodeId: "Boundary1", Slot: 1}},
	}}})
	var del *ActDeleteTimer
	for _, a := range col.Drain() {
		if d, ok := a.(ActDeleteTimer); ok {
			del = &d
		}
	}
	if del == nil || !bytes.Equal(del.ID.GetUuid(), wantTID.GetUuid()) {
		t.Fatalf("want ActDeleteTimer for the boundary timer, got %+v", del)
	}
	stillThere := false
	if err := (tables.TimerTable{S: p.cfg.Snapshotter.Store()}).ScanByInvocation(wantTID, func(uint64) error {
		stillThere = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if stillThere {
		t.Fatal("cancelled process timer row still present")
	}
}

// TestProcess_FiredTimerRowReclaimed pins the fire-side timer cleanup: when a
// process timer fires, its durable row (primary + secondaries) is deleted in the
// same apply, so a later leader-gain TimerService.Rebuild — which ScanAlls the
// timer table — cannot re-load the past-due row and re-fire a duplicate
// timer_fired. The cancel side is covered by TestProcess_ActuatesInstructions.
func TestProcess_FiredTimerRowReclaimed(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Proc", "tf"
	pk := routing.PartitionKey(svc, key)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
	// timerRows counts primary timer/ rows for the Boundary1 timer — exactly what
	// TimerService.Rebuild's ScanAll would re-load on leader gain.
	wantTID := processTimerID(pk, svc, key, "Boundary1", 1)
	timerRows := func() int {
		t.Helper()
		n := 0
		if err := (tables.TimerTable{S: p.cfg.Snapshotter.Store()}).ScanAll(func(e tables.TimerEntry) error {
			if bytes.Equal(e.ID.GetUuid(), wantTID.GetUuid()) {
				n++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Start, then arm a boundary timer.
	must(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	col.Drain()
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
		ArmTimer: []*enginev1.TimerArm{{NodeId: "Boundary1", FireAtMs: testEnvelopeNowMs + 5000, Slot: 1}},
	}}})
	col.Drain()
	if got := timerRows(); got != 1 {
		t.Fatalf("after arm: timer rows=%d, want 1", got)
	}

	// Fire the timer. The fire-apply must reclaim the durable row so a later
	// Rebuild (ScanAll) cannot re-load and re-fire it.
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: svc, InstanceKey: key,
		Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TimerFired{
			TimerFired: &enginev1.ProcessTimerFired{NodeId: "Boundary1", Slot: 1},
		}},
	}}})
	col.Drain()
	if got := timerRows(); got != 0 {
		t.Fatalf("after fire: timer rows=%d, want 0 (row must be reclaimed so Rebuild cannot re-fire)", got)
	}
}

// TestProcess_RepeatedNodeDispatchDistinctIDs is the regression for the
// loop/retry id collision: a node that dispatches a service task more than once
// over an instance's lifetime (a rework loop, an error-boundary retry) must mint
// a DISTINCT callee id each turn. If the two dispatches shared an id the
// receiving shard would dedup the second against the first's still-Completed
// invocation row (onInvoke → transitionOnInvoke: Completed → ErrInvalidTransition
// → dropped) and the instance would hang. The turn seq folded into
// mintProcessTaskID separates them; the same-turn (re-driven) stability is pinned
// by TestProcess_ActuatesInstructions.
func TestProcess_RepeatedNodeDispatchDistinctIDs(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "LoopProc", "i1"
	pk := routing.PartitionKey(svc, key)
	target := &enginev1.InvocationTarget{ServiceName: "Cap", HandlerName: "do"}
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
	// Drive one turn that dispatches the SAME node "Work"; return the minted id.
	dispatchWork := func(idx uint64, state string) *enginev1.InvocationId {
		t.Helper()
		must(idx, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
			Pk: pk, Service: svc, InstanceKey: key, NewState: []byte(state),
			Invoke: []*enginev1.TaskInvoke{{NodeId: "Work", Target: target, Input: []byte("ti")}},
		}}})
		var disp *ActDispatchOutbox
		for _, a := range col.Drain() {
			if d, ok := a.(ActDispatchOutbox); ok {
				disp = &d
			}
		}
		if disp == nil || disp.Envelope.GetInvoke() == nil {
			t.Fatalf("turn at index %d: want an Invoke outbox dispatch, got %+v", idx, disp)
		}
		return disp.Envelope.GetInvoke().GetInvocationId()
	}

	// Start activates seq 1; turn 1 dispatches "Work" at active_seq 1.
	must(1, procEventCmd(pk, svc, key, []byte("vars"), &enginev1.ModelRef{Name: svc}))
	col.Drain()
	id1 := dispatchWork(2, "s1")

	// A second event activates seq 2; re-dispatch the same node — the loop
	// iteration. active_seq is now 2, so the callee id must differ.
	must(3, procEventCmd(pk, svc, key, []byte("e2"), nil))
	col.Drain()
	id2 := dispatchWork(4, "s2")

	root := processRootID(pk, svc, key)
	if want := mintProcessTaskID(root, "Work", "", 1, 0, target); !bytes.Equal(id1.GetUuid(), want.GetUuid()) {
		t.Fatalf("turn 1 id = %x, want %x (seq 1)", id1.GetUuid(), want.GetUuid())
	}
	if want := mintProcessTaskID(root, "Work", "", 2, 0, target); !bytes.Equal(id2.GetUuid(), want.GetUuid()) {
		t.Fatalf("turn 2 id = %x, want %x (seq 2)", id2.GetUuid(), want.GetUuid())
	}
	if bytes.Equal(id1.GetUuid(), id2.GetUuid()) {
		t.Fatalf("re-dispatched node collided on id %x — the receiver would dedup-drop the retry and the instance would hang", id1.GetUuid())
	}
}

// TestProcess_FanOutSameNodeDistinctIDs is the regression for the
// completionQuantity (BPMN §10.2) id collision: a SINGLE turn can dispatch the
// same (nodeID, instanceIdx) more than once — N tokens leave a node whose
// completionQuantity is N, each entering and dispatching the next activity in
// that one turn. turnSeq is identical across a turn's fan-out, so without the
// fan-out index every dispatch would re-mint one id; the receiving shard would
// dedup all but the first and the extra tokens would vanish — a downstream
// startQuantity barrier would then never fill and the instance would park
// (betsy Token_Cardinality_Default/Explicit). The fan-out index in
// mintProcessTaskID separates them.
func TestProcess_FanOutSameNodeDistinctIDs(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "FanProc", "i1"
	pk := routing.PartitionKey(svc, key)
	target := &enginev1.InvocationTarget{ServiceName: "Cap", HandlerName: "do"}
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
	// Start activates seq 1.
	must(1, procEventCmd(pk, svc, key, []byte("vars"), &enginev1.ModelRef{Name: svc}))
	col.Drain()

	// One turn that dispatches node "Fwd" TWICE — a completionQuantity=2 fan-out.
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
		Invoke: []*enginev1.TaskInvoke{
			{NodeId: "Fwd", Target: target, Input: []byte("a")},
			{NodeId: "Fwd", Target: target, Input: []byte("b")},
		},
	}}})
	var ids []*enginev1.InvocationId
	for _, a := range col.Drain() {
		if d, ok := a.(ActDispatchOutbox); ok {
			if inv := d.Envelope.GetInvoke(); inv != nil {
				ids = append(ids, inv.GetInvocationId())
			}
		}
	}
	if len(ids) != 2 {
		t.Fatalf("want 2 distinct outbox Invoke dispatches for the fan-out, got %d", len(ids))
	}
	if bytes.Equal(ids[0].GetUuid(), ids[1].GetUuid()) {
		t.Fatalf("same-turn fan-out collided on id %x — the receiver would dedup the 2nd dispatch and the extra token would be lost", ids[0].GetUuid())
	}
	// Each must equal the deterministic mint at its fan-out index (seq 1).
	root := processRootID(pk, svc, key)
	if want := mintProcessTaskID(root, "Fwd", "", 1, 0, target); !bytes.Equal(ids[0].GetUuid(), want.GetUuid()) {
		t.Errorf("fan-out[0] id = %x, want %x (seq 1, idx 0)", ids[0].GetUuid(), want.GetUuid())
	}
	if want := mintProcessTaskID(root, "Fwd", "", 1, 1, target); !bytes.Equal(ids[1].GetUuid(), want.GetUuid()) {
		t.Errorf("fan-out[1] id = %x, want %x (seq 1, idx 1)", ids[1].GetUuid(), want.GetUuid())
	}
}

// TestProcess_ChildStartAndTerminalDelivery covers the call-activity loop: a
// ChildStart instruction starts a process-parented child instance; the child's
// terminal turn reaps the child and feeds child_completed back to the parent
// node, activating the parent.
func TestProcess_ChildStartAndTerminalDelivery(t *testing.T) {
	p, _, col := newTestPartition(t)
	const psvc, pkey = "Parent", "p1"
	ppk := routing.PartitionKey(psvc, pkey)
	plp := keys.LPFromPartitionKey(ppk)
	procs, inbox := procStore(p)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Start the parent, then advance turn 1 to start a child (call activity CA1).
	must(1, procEventCmd(ppk, psvc, pkey, []byte("pv"), &enginev1.ModelRef{Name: psvc}))
	col.Drain()
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: ppk, Service: psvc, InstanceKey: pkey, NewState: []byte("ps1"),
		StartChild: []*enginev1.ChildStart{{
			NodeId: "CA1", ModelRef: &enginev1.ModelRef{Name: "Child"},
			Kind: enginev1.ProcessKind_PROCESS_KIND_BPMN, InstanceKey: "c1", Vars: []byte("cv"),
		}},
	}}})

	const csvc, ckey = "Child", "c1"
	cpk := routing.PartitionKey(csvc, ckey)
	clp := keys.LPFromPartitionKey(cpk)

	// Child created, process-parented to CA1, and activated (its start vars).
	crec, ok, err := procs.Get(clp, csvc, ckey)
	if err != nil || !ok {
		t.Fatalf("child record: ok=%v err=%v", ok, err)
	}
	if cpp := crec.GetParentLink().GetProcessParent(); cpp.GetPk() != ppk || cpp.GetService() != psvc ||
		cpp.GetInstanceKey() != pkey || cpp.GetNodeId() != "CA1" {
		t.Fatalf("child process_parent mismatch: %+v", crec.GetParentLink())
	}
	if crec.GetActiveSeq() != 1 {
		t.Fatalf("child not activated: %+v", crec)
	}
	if ca := firstAdvance(col.Drain(), csvc); ca == nil || string(ca.Entry.GetPayload().GetExternal()) != "cv" {
		t.Fatalf("child start activation missing/wrong: %+v", ca)
	}

	// Child terminal (completed with output) → reap child, deliver to parent.
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: cpk, Service: csvc, InstanceKey: ckey, NewState: []byte("cs1"),
		Terminal: &enginev1.ProcessTerminal{Output: []byte("child-out")},
	}}})

	if _, ok, _ := procs.Get(clp, csvc, ckey); ok {
		t.Fatal("child record should be reaped on terminal")
	}
	// Parent was idle after turn 1, so child_completed activates it as seq 2.
	prec, ok, _ := procs.Get(plp, psvc, pkey)
	if !ok || prec.GetActiveSeq() != 2 || prec.GetNextSeq() != 3 {
		t.Fatalf("parent cursor after child completion: %+v", prec)
	}
	pentry, ok, err := inbox.Get(plp, psvc, pkey, 2)
	if err != nil || !ok {
		t.Fatalf("parent inbox[2]: ok=%v err=%v", ok, err)
	}
	cc := pentry.GetPayload().GetChildCompleted()
	if cc.GetNodeId() != "CA1" || string(cc.GetOutput()) != "child-out" || cc.GetFailed() {
		t.Fatalf("child_completed payload mismatch: %+v", cc)
	}
	if pa := firstAdvance(col.Drain(), psvc); pa == nil || pa.Entry.GetPayload().GetChildCompleted() == nil {
		t.Fatalf("parent not activated with child result: %+v", pa)
	}
}

// TestProcess_ChildIncidentParksAndBlocksParent is the core of "let children
// park": a child instance whose turn ends in a (BPMN) incident parks in place —
// PROCESS_STATUS_INCIDENT, record + state_blob retained, the deep node pinned —
// and delivers NO child_completed to its parent. The parent stays RUNNING and
// idle, still counting the outstanding child, blocked until the child eventually
// completes (after a ResolveProcessIncident on the child's own instance_key).
// Contrast TestProcess_ChildStartAndTerminalDelivery, where a terminal child does
// deliver; before this change the adapter forced a child's fault terminal so it
// propagated, now only an escalation does.
func TestProcess_ChildIncidentParksAndBlocksParent(t *testing.T) {
	p, _, col := newTestPartition(t)
	const psvc, pkey = "Parent", "p1"
	ppk := routing.PartitionKey(psvc, pkey)
	plp := keys.LPFromPartitionKey(ppk)
	procs, inbox := procStore(p)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Parent starts, then turn 1 starts a BPMN child (call activity CA1).
	must(1, procEventCmd(ppk, psvc, pkey, []byte("pv"), &enginev1.ModelRef{Name: psvc}))
	col.Drain()
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: ppk, Service: psvc, InstanceKey: pkey, NewState: []byte("ps1"),
		StartChild: []*enginev1.ChildStart{{
			NodeId: "CA1", ModelRef: &enginev1.ModelRef{Name: "Child"},
			Kind: enginev1.ProcessKind_PROCESS_KIND_BPMN, InstanceKey: "c1", Vars: []byte("cv"),
		}},
	}}})
	col.Drain() // clear the child's start activation

	const csvc, ckey = "Child", "c1"
	cpk := routing.PartitionKey(csvc, ckey)
	clp := keys.LPFromPartitionKey(cpk)

	// The child's turn ends in an incident (genuine uncaught fault on node "deep").
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: cpk, Service: csvc, InstanceKey: ckey, NewState: []byte("cs1"),
		Incident: &enginev1.ProcessIncident{NodeId: "deep", Cause: "boom"},
	}}})

	// Child parks: retained (not reaped), INCIDENT, deep node + cause pinned, the
	// failing state_blob kept so a RETRY can re-drive it.
	crec, ok, err := procs.Get(clp, csvc, ckey)
	if err != nil || !ok {
		t.Fatalf("child record must be retained on incident: ok=%v err=%v", ok, err)
	}
	if crec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_INCIDENT {
		t.Fatalf("child status = %v, want INCIDENT", crec.GetStatus())
	}
	if crec.GetIncident().GetNodeId() != "deep" || crec.GetIncident().GetCause() != "boom" {
		t.Fatalf("child incident = %+v, want node=deep cause=boom", crec.GetIncident())
	}
	if string(crec.GetStateBlob()) != "cs1" {
		t.Fatalf("child state_blob = %q, want retained cs1 for RETRY", crec.GetStateBlob())
	}

	// Parent is NOT activated: no child_completed delivered, parent stays RUNNING
	// and idle, still counting the outstanding child (blocked, not abandoned).
	prec, ok, _ := procs.Get(plp, psvc, pkey)
	if !ok {
		t.Fatal("parent record missing")
	}
	if prec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		t.Fatalf("parent status = %v, want RUNNING (blocked on child)", prec.GetStatus())
	}
	if prec.GetActiveSeq() != 0 {
		t.Fatalf("parent must stay idle (ActiveSeq 0), got %d", prec.GetActiveSeq())
	}
	if prec.GetOutstanding() != 1 {
		t.Fatalf("parent Outstanding = %d, want 1 (still awaiting the parked child)", prec.GetOutstanding())
	}
	if _, ok, _ := inbox.Get(plp, psvc, pkey, 2); ok {
		t.Fatal("parent must have no child_completed inbox row (child parked, did not deliver)")
	}
	if pa := firstAdvance(col.Drain(), psvc); pa != nil {
		t.Fatalf("parent must not be re-activated by a parked child: %+v", pa)
	}
}

// TestProcess_ParentTerminateCascadesToParkedChild is the headline of point (b):
// a parent that terminates while a child is incident-parked tears the child down
// instead of orphaning it. Parent starts a BPMN child, the child parks as an
// incident, then a concurrent branch drives the parent to a terminal turn — the
// finishProcessInstance cascade must cancel the parked child and clear the index.
func TestProcess_ParentTerminateCascadesToParkedChild(t *testing.T) {
	p, _, col := newTestPartition(t)
	const psvc, pkey = "Parent", "p1"
	const csvc, ckey = "Child", "c1"
	ppk := routing.PartitionKey(psvc, pkey)
	plp := keys.LPFromPartitionKey(ppk)
	cpk := routing.PartitionKey(csvc, ckey)
	clp := keys.LPFromPartitionKey(cpk)
	procs, _ := procStore(p)
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
			Kind: enginev1.ProcessKind_PROCESS_KIND_BPMN, InstanceKey: ckey, Vars: []byte("cv"),
		}},
	}}})
	col.Drain()
	// The index now records the live child.
	parentRoot := processRootID(ppk, psvc, pkey)
	if n, err := childIndexCount(p, parentRoot); err != nil || n != 1 {
		t.Fatalf("child index count = %d (err %v), want 1 after StartChild", n, err)
	}

	// Child parks as an incident.
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: cpk, Service: csvc, InstanceKey: ckey, NewState: []byte("cs1"),
		Incident: &enginev1.ProcessIncident{NodeId: "deep", Cause: "boom"},
	}}})
	if crec, ok, _ := procs.Get(clp, csvc, ckey); !ok || crec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_INCIDENT {
		t.Fatalf("precondition: child must be incident-parked, ok=%v rec=%+v", ok, crec)
	}
	col.Drain()

	// A concurrent branch wakes the parent and drives it to a terminal turn.
	must(4, procExtCmd(ppk, psvc, pkey, []byte("boom-elsewhere")))
	col.Drain()
	must(5, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: ppk, Service: psvc, InstanceKey: pkey, NewState: []byte("ps2"),
		Terminal: &enginev1.ProcessTerminal{Failed: true, FailureMessage: "parent died"},
	}}})

	// Parent gone (retention 0), and the parked child was cancelled — not orphaned.
	if _, ok, _ := procs.Get(plp, psvc, pkey); ok {
		t.Fatal("parent record must be deleted on terminal (retention 0)")
	}
	if _, ok, _ := procs.Get(clp, csvc, ckey); ok {
		t.Fatal("parked child must be cancelled (record deleted) when its parent terminates")
	}
	if n, err := childIndexCount(p, parentRoot); err != nil || n != 0 {
		t.Fatalf("child index count = %d (err %v), want 0 after cascade", n, err)
	}
}

// TestProcess_ParentTerminateCascadesRecursively proves the cascade recurses:
// parent → child → grandchild, all live; terminating the parent tears the whole
// subtree down (child cancels its own grandchild before deleting itself).
func TestProcess_ParentTerminateCascadesRecursively(t *testing.T) {
	p, _, col := newTestPartition(t)
	const psvc, pkey = "Parent", "p1"
	const csvc, ckey = "Child", "c1"
	const gsvc, gkey = "Grand", "g1"
	ppk := routing.PartitionKey(psvc, pkey)
	cpk := routing.PartitionKey(csvc, ckey)
	gpk := routing.PartitionKey(gsvc, gkey)
	procs, _ := procStore(p)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	// parent → child
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
	// child → grandchild (the child's start turn is active; advance it to start gc)
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: cpk, Service: csvc, InstanceKey: ckey, NewState: []byte("cs1"),
		StartChild: []*enginev1.ChildStart{{
			NodeId: "CA2", ModelRef: &enginev1.ModelRef{Name: gsvc},
			Kind: enginev1.ProcessKind_PROCESS_KIND_BPMN, InstanceKey: gkey,
		}},
	}}})
	col.Drain()
	if _, ok, _ := procs.Get(keys.LPFromPartitionKey(gpk), gsvc, gkey); !ok {
		t.Fatal("precondition: grandchild must exist")
	}

	// Terminate the parent.
	must(4, procExtCmd(ppk, psvc, pkey, []byte("kill")))
	col.Drain()
	must(5, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: ppk, Service: psvc, InstanceKey: pkey, NewState: []byte("ps2"),
		Terminal: &enginev1.ProcessTerminal{Failed: false},
	}}})

	// The entire subtree is gone.
	if _, ok, _ := procs.Get(keys.LPFromPartitionKey(cpk), csvc, ckey); ok {
		t.Fatal("child must be cancelled by the cascade")
	}
	if _, ok, _ := procs.Get(keys.LPFromPartitionKey(gpk), gsvc, gkey); ok {
		t.Fatal("grandchild must be cancelled recursively")
	}
	if n, err := childIndexCount(p, processRootID(cpk, csvc, ckey)); err != nil || n != 0 {
		t.Fatalf("child's child-index = %d (err %v), want 0", n, err)
	}
}

// TestProcess_ChildCompletionClearsIndex pins delete-on-complete: a normally
// completing child drops its parent→child index row (via the child_root it
// stamps on ChildCompleted), so a later parent terminate finds nothing to cancel.
func TestProcess_ChildCompletionClearsIndex(t *testing.T) {
	p, _, col := newTestPartition(t)
	const psvc, pkey = "Parent", "p1"
	const csvc, ckey = "Child", "c1"
	ppk := routing.PartitionKey(psvc, pkey)
	plp := keys.LPFromPartitionKey(ppk)
	cpk := routing.PartitionKey(csvc, ckey)
	procs, inbox := procStore(p)
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
	parentRoot := processRootID(ppk, psvc, pkey)
	if n, _ := childIndexCount(p, parentRoot); n != 1 {
		t.Fatalf("index count = %d, want 1", n)
	}

	// Child completes normally → ChildCompleted delivered to the parent, which
	// decrements outstanding and drops the index row (delete-on-complete).
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: cpk, Service: csvc, InstanceKey: ckey, NewState: []byte("cs1"),
		Terminal: &enginev1.ProcessTerminal{Failed: false, Output: []byte("done")},
	}}})

	if n, err := childIndexCount(p, parentRoot); err != nil || n != 0 {
		t.Fatalf("index count = %d (err %v), want 0 after child completion", n, err)
	}
	// The parent observed the completion: outstanding back to 0, a child_completed
	// inbox row enqueued (seq 2).
	prec, ok, _ := procs.Get(plp, psvc, pkey)
	if !ok || prec.GetOutstanding() != 0 {
		t.Fatalf("parent outstanding = %d (ok %v), want 0", prec.GetOutstanding(), ok)
	}
	if _, ok, _ := inbox.Get(plp, psvc, pkey, 2); !ok {
		t.Fatal("parent must have a child_completed inbox row at seq 2")
	}
}

// TestProcess_CancelTearsDownArmedTimer proves the per-instance timer index does
// its job: a child that armed a timer, when cancelled by its parent's terminate,
// has that timer deleted (TimerTable + index + ActDeleteTimer) instead of left to
// self-reclaim on fire.
func TestProcess_CancelTearsDownArmedTimer(t *testing.T) {
	p, _, col := newTestPartition(t)
	const psvc, pkey = "Parent", "p1"
	const csvc, ckey = "Child", "c1"
	ppk := routing.PartitionKey(psvc, pkey)
	cpk := routing.PartitionKey(csvc, ckey)
	clp := keys.LPFromPartitionKey(cpk)
	procs, _ := procStore(p)
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
	// Child's turn 1 arms a timer on node T1.
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: cpk, Service: csvc, InstanceKey: ckey, NewState: []byte("cs1"),
		ArmTimer: []*enginev1.TimerArm{{NodeId: "T1", FireAtMs: 5_000_000, Slot: 0}},
	}}})
	col.Drain()

	childRoot := processRootID(cpk, csvc, ckey)
	tid := processTimerID(cpk, csvc, ckey, "T1", 0)
	if ex, _ := timerExists(p, tid); !ex {
		t.Fatal("precondition: child's timer must be armed")
	}
	if n, _ := timerIndexCount(p, childRoot); n != 1 {
		t.Fatalf("precondition: timer index = %d, want 1", n)
	}

	// Terminate the parent → child cancelled → its timer torn down.
	must(4, procExtCmd(ppk, psvc, pkey, []byte("kill")))
	col.Drain()
	must(5, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: ppk, Service: psvc, InstanceKey: pkey, NewState: []byte("ps2"),
		Terminal: &enginev1.ProcessTerminal{Failed: false},
	}}})

	if _, ok, _ := procs.Get(clp, csvc, ckey); ok {
		t.Fatal("child must be cancelled")
	}
	if ex, _ := timerExists(p, tid); ex {
		t.Fatal("child's armed timer must be deleted on cancel, not left to self-reclaim")
	}
	if n, _ := timerIndexCount(p, childRoot); n != 0 {
		t.Fatalf("timer index = %d, want 0 after cancel", n)
	}
	if !hasDeleteTimer(col.Drain(), tid) {
		t.Fatal("cancel must emit ActDeleteTimer so the leader heap drops the timer too")
	}
}

// TestProcess_TimerIndexSyncedOnCancelAndFire pins the index↔TimerTable invariant
// at the three maintenance sites: arm (Put), CancelTimer (Delete), and fire
// (reclaimFiredProcessTimer Delete). A stale index would make teardown emit
// spurious ActDeleteTimers; a missed Put would leave a timer un-torn-down.
func TestProcess_TimerIndexSyncedOnCancelAndFire(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Proc", "i1"
	pk := routing.PartitionKey(svc, key)
	root := processRootID(pk, svc, key)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
	advance := func(idx uint64, adv *enginev1.ProcessAdvanced) {
		adv.Pk, adv.Service, adv.InstanceKey = pk, svc, key
		must(idx, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv}})
		col.Drain()
	}

	must(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	col.Drain()
	// Arm.
	advance(2, &enginev1.ProcessAdvanced{NewState: []byte("s1"), ArmTimer: []*enginev1.TimerArm{{NodeId: "T1", FireAtMs: 5_000_000, Slot: 0}}})
	tid := processTimerID(pk, svc, key, "T1", 0)
	if n, _ := timerIndexCount(p, root); n != 1 {
		t.Fatalf("after arm: index = %d, want 1", n)
	}

	// Cancel → index row + timer gone.
	must(3, procExtCmd(pk, svc, key, []byte("e1")))
	col.Drain()
	advance(4, &enginev1.ProcessAdvanced{NewState: []byte("s2"), CancelTimer: []*enginev1.TimerCancel{{NodeId: "T1", Slot: 0}}})
	if n, _ := timerIndexCount(p, root); n != 0 {
		t.Fatalf("after cancel: index = %d, want 0", n)
	}
	if ex, _ := timerExists(p, tid); ex {
		t.Fatal("after cancel: timer must be gone")
	}

	// Re-arm, then fire → reclaimFiredProcessTimer drops the index row + timer.
	must(5, procExtCmd(pk, svc, key, []byte("e2")))
	col.Drain()
	advance(6, &enginev1.ProcessAdvanced{NewState: []byte("s3"), ArmTimer: []*enginev1.TimerArm{{NodeId: "T1", FireAtMs: 6_000_000, Slot: 0}}})
	if n, _ := timerIndexCount(p, root); n != 1 {
		t.Fatalf("after re-arm: index = %d, want 1", n)
	}
	must(7, &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: svc, InstanceKey: key,
		Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TimerFired{
			TimerFired: &enginev1.ProcessTimerFired{NodeId: "T1", Slot: 0},
		}},
	}}})
	if n, _ := timerIndexCount(p, root); n != 0 {
		t.Fatalf("after fire: index = %d, want 0", n)
	}
	if ex, _ := timerExists(p, tid); ex {
		t.Fatal("after fire: timer must be reclaimed")
	}
}

// TestProcess_OnProcessCancelLanding drives a Command_ProcessCancel directly
// through Update — the path a cross-shard ProcessCancel takes when it lands on the
// child's owning shard — and asserts the instance is torn down.
func TestProcess_OnProcessCancelLanding(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Lonely", "x1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	col.Drain()
	if _, ok, _ := procs.Get(lp, svc, key); !ok {
		t.Fatal("precondition: instance must exist")
	}

	// The cross-shard landing: a ProcessCancel command applied locally.
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessCancel{ProcessCancel: &enginev1.ProcessCancel{
		Pk: pk, Service: svc, InstanceKey: key,
	}}})
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatal("onProcessCancel landing must tear the instance down")
	}

	// Idempotent: a second cancel for the now-absent instance is a no-op.
	must(3, &enginev1.Command{Kind: &enginev1.Command_ProcessCancel{ProcessCancel: &enginev1.ProcessCancel{
		Pk: pk, Service: svc, InstanceKey: key,
	}}})
}

// TestOutboxEnvelopeToCommand_ProcessCancel pins the cross-shard reshape: the
// outbox shuffler must turn a ProcessCancel envelope into a ProcessCancel command
// for the dest shard's apply path.
func TestOutboxEnvelopeToCommand_ProcessCancel(t *testing.T) {
	pc := &enginev1.ProcessCancel{Pk: 42, Service: "Child", InstanceKey: "c9"}
	cmd := outboxEnvelopeToCommand(&enginev1.OutboxEnvelope{
		DestinationShardId: 7,
		Kind:               &enginev1.OutboxEnvelope_ProcessCancel{ProcessCancel: pc},
	})
	got, ok := cmd.GetKind().(*enginev1.Command_ProcessCancel)
	if !ok {
		t.Fatalf("want Command_ProcessCancel, got %T", cmd.GetKind())
	}
	if got.ProcessCancel.GetPk() != 42 || got.ProcessCancel.GetService() != "Child" || got.ProcessCancel.GetInstanceKey() != "c9" {
		t.Fatalf("fields not carried through: %+v", got.ProcessCancel)
	}
}

// TestProcess_ServiceTaskResultFeedsBackToParent covers the task→process
// feedback: an invocation carrying a process_parent link, on terminal
// completion, delivers a task_completed ProcessEvent to its parent instance
// instead of a JECallResult.
func TestProcess_ServiceTaskResultFeedsBackToParent(t *testing.T) {
	p, _, col := newTestPartition(t)
	const psvc, pkey = "Proc", "ip"
	ppk := routing.PartitionKey(psvc, pkey)
	plp := keys.LPFromPartitionKey(ppk)
	procs, _ := procStore(p)
	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Start the parent and advance turn 1 to idle (so feedback activates at once).
	must(1, procEventCmd(ppk, psvc, pkey, []byte("v"), &enginev1.ModelRef{Name: psvc}))
	col.Drain()
	must(2, procAdvancedCmd(ppk, psvc, pkey, []byte("ps"), nil))
	col.Drain()

	// A service-task invocation parented to the process at node Task9.
	taskID := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("task-inv-uuid-16")}
	pp := &enginev1.ProcessParent{Pk: ppk, Service: psvc, InstanceKey: pkey, NodeId: "Task9"}
	must(3, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: taskID,
		Target:       &enginev1.InvocationTarget{ServiceName: "Cap", HandlerName: "do"},
		Input:        []byte("in"),
		ParentLink:   &enginev1.ParentLink{ProcessParent: pp},
	}}})
	col.Drain()
	must(4, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: taskID,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 0, Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}}},
		}},
	}}})
	col.Drain()
	// Complete the task → process_parent branch delivers task_completed.
	must(5, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: taskID,
		Kind:         &enginev1.InvokerEffect_Completed{Completed: &enginev1.InvocationCompleted{Output: []byte("task-out")}},
	}}})

	act := firstAdvance(col.Drain(), psvc)
	if act == nil {
		t.Fatal("parent not activated with task result")
	}
	tc := act.Entry.GetPayload().GetTaskCompleted()
	if tc.GetNodeId() != "Task9" || string(tc.GetOutput()) != "task-out" || tc.GetFailed() {
		t.Fatalf("task_completed payload mismatch: %+v", tc)
	}
	if r, ok, _ := procs.Get(plp, psvc, pkey); !ok || r.GetActiveSeq() == 0 {
		t.Fatalf("parent record/active after feedback: %+v", r)
	}
}

// TestProcess_TimerFireCommand asserts the TimerService builds a
// Command_ProcessEvent{timer_fired} for a process timer (and the plain
// Command_TimerFired otherwise).
func TestProcess_TimerFireCommand(t *testing.T) {
	pk := routing.PartitionKey("Proc", "i9")
	id := processTimerID(pk, "Proc", "i9", "Boundary", 2)
	cmd := timerFireCommand(timerHeapEntry{
		fireAtMs: 9999,
		id:       id,
		process:  &enginev1.ProcessTimer{Service: "Proc", InstanceKey: "i9", NodeId: "Boundary", Slot: 2},
	})
	ev := cmd.GetProcessEvent()
	if ev == nil {
		t.Fatalf("want ProcessEvent, got %T", cmd.GetKind())
	}
	if ev.GetPk() != pk || ev.GetService() != "Proc" || ev.GetInstanceKey() != "i9" || ev.GetLogicalTimeMs() != 9999 {
		t.Fatalf("addressing mismatch: %+v", ev)
	}
	if tf := ev.GetPayload().GetTimerFired(); tf.GetNodeId() != "Boundary" || tf.GetSlot() != 2 {
		t.Fatalf("timer_fired payload mismatch: %+v", ev.GetPayload())
	}

	plain := timerFireCommand(timerHeapEntry{fireAtMs: 5, id: id, sleepIdx: 7})
	if tf := plain.GetTimerFired(); tf == nil || tf.GetSleepIndex() != 7 {
		t.Fatalf("plain timer should fire TimerFired: %+v", plain.GetKind())
	}
}

// TestProcess_RetentionImmediateDelete: a terminal turn with retention_ms==0
// deletes the record immediately (opt-in retention; prior behavior).
func TestProcess_RetentionImmediateDelete(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "P", "k1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)

	apply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		col.Drain()
	}
	apply(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"}))
	apply(2, procAdvancedCmd(pk, svc, key, []byte("final"), &enginev1.ProcessTerminal{Output: []byte("out")}))

	if _, ok, err := procs.Get(lp, svc, key); err != nil || ok {
		t.Fatalf("record should be deleted (retention 0): ok=%v err=%v", ok, err)
	}
}

// TestProcess_RetentionRetainsAndReaps: a terminal turn with retention_ms>0
// retains the record (terminal status + output + timestamps), schedules a
// process reap, and the ReapProcessInstance arm deletes record + row. A
// duplicate fire (row already consumed) is a no-op.
func TestProcess_RetentionRetainsAndReaps(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "P", "k1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)
	const retention uint64 = 60_000
	wantFireAt := testEnvelopeNowMs + retention

	apply := func(idx uint64, cmd *enginev1.Command) []Action {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		return col.Drain()
	}
	apply(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"}))
	acts := apply(2, procAdvancedCmd(pk, svc, key, []byte("final"),
		&enginev1.ProcessTerminal{Output: []byte("out"), RetentionMs: retention}))

	rec, ok, err := procs.Get(lp, svc, key)
	if err != nil || !ok {
		t.Fatalf("record should be retained: ok=%v err=%v", ok, err)
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_COMPLETED {
		t.Fatalf("status = %v, want COMPLETED", rec.GetStatus())
	}
	if string(rec.GetOutput()) != "out" {
		t.Fatalf("output = %q", rec.GetOutput())
	}
	if rec.GetCreatedAtMs() != testEnvelopeNowMs || rec.GetEndedAtMs() != testEnvelopeNowMs {
		t.Fatalf("timestamps: created=%d ended=%d", rec.GetCreatedAtMs(), rec.GetEndedAtMs())
	}
	if rec.GetActiveSeq() != 0 || rec.GetOutstanding() != 0 {
		t.Fatalf("active=%d outstanding=%d", rec.GetActiveSeq(), rec.GetOutstanding())
	}

	store := p.cfg.Snapshotter.Store()
	root := processRootID(pk, svc, key)
	if present, err := (tables.ProcessReapTable{S: store}).Exists(wantFireAt, root); err != nil || !present {
		t.Fatalf("proc_reap row: present=%v err=%v", present, err)
	}
	var sched *ActScheduleProcessReap
	for i := range acts {
		if a, ok := acts[i].(ActScheduleProcessReap); ok {
			sched = &a
		}
	}
	if sched == nil || sched.FireAtMs != wantFireAt || sched.Service != svc || sched.InstanceKey != key {
		t.Fatalf("ActScheduleProcessReap = %+v", sched)
	}

	reapCmd := func() *enginev1.Command {
		return &enginev1.Command{Kind: &enginev1.Command_ReapProcessInstance{
			ReapProcessInstance: &enginev1.ReapProcessInstance{
				Pk: pk, Service: svc, InstanceKey: key, FireAtMs: wantFireAt,
			},
		}}
	}
	apply(3, reapCmd())
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatalf("record should be reaped")
	}
	if present, _ := (tables.ProcessReapTable{S: store}).Exists(wantFireAt, root); present {
		t.Fatalf("proc_reap row should be deleted")
	}

	// Duplicate / stale fire: row already consumed → no-op (no error, nothing recreated).
	apply(4, reapCmd())
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatalf("duplicate reap must not resurrect the record")
	}
}

// procSubFixture drives one instance through start + a subscribe turn and
// returns accessors for the forward MessageSubscription count and the reverse
// proc_sub_idx presence. The catch is (node, msg, corr); same-shard routing (the
// test Partitioner sends every key to shard 1) so the forward row lands locally.
func procSubFixture(t *testing.T, p *Partition, col *ActionCollector) (svc, key, node, msg, corr string, subCount func() int, idxPresent func() bool, apply func(uint64, *enginev1.Command)) {
	t.Helper()
	svc, key, node, msg, corr = "P", "k1", "Catch1", "PaymentDone", "ord-1"
	pk := routing.PartitionKey(svc, key)
	msgLp := keys.LPFromPartitionKey(routing.PartitionKey(msg, corr))
	store := p.cfg.Snapshotter.Store()
	root := processRootID(pk, svc, key)

	apply = func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		col.Drain()
	}
	subCount = func() int {
		n := 0
		if err := (tables.MessageSubscriptionTable{S: store}).ScanByCorrelation(msgLp, msg, corr,
			func(_ []byte, _ *enginev1.MessageSubscription) error { n++; return nil }); err != nil {
			t.Fatalf("scan subs: %v", err)
		}
		return n
	}
	idxPresent = func() bool {
		_, ok, err := (tables.ProcessSubIndexTable{S: store}).Get(root, node)
		if err != nil {
			t.Fatalf("idx get: %v", err)
		}
		return ok
	}

	apply(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"}))
	apply(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
		Subscribe: []*enginev1.SignalSubscribe{{NodeId: node, MessageName: msg, CorrelationKey: corr}},
	}}})
	if subCount() != 1 || !idxPresent() {
		t.Fatalf("after subscribe: forward=%d reverseIdx=%v, want 1, true", subCount(), idxPresent())
	}
	return svc, key, node, msg, corr, subCount, idxPresent, apply
}

// TestProcess_UnsubscribeTearsDownBoth: a SignalUnsubscribe instruction deletes
// the forward MessageSubscription and the reverse proc_sub_idx row (gateway-loser).
func TestProcess_UnsubscribeTearsDownBoth(t *testing.T) {
	p, _, col := newTestPartition(t)
	svc, key, node, _, _, subCount, idxPresent, apply := procSubFixture(t, p, col)
	pk := routing.PartitionKey(svc, key)

	// Open a second turn, then unsubscribe the catch.
	apply(3, procEventCmd(pk, svc, key, []byte("e2"), nil))
	apply(4, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s2"),
		Unsubscribe: []*enginev1.SignalUnsubscribe{{NodeId: node}},
	}}})
	if subCount() != 0 || idxPresent() {
		t.Fatalf("after unsubscribe: forward=%d reverseIdx=%v, want 0, false", subCount(), idxPresent())
	}

	// A repeat unsubscribe for the same node is a no-op (reverse row already gone).
	apply(5, procEventCmd(pk, svc, key, []byte("e3"), nil))
	apply(6, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s3"),
		Unsubscribe: []*enginev1.SignalUnsubscribe{{NodeId: node}},
	}}})
	if subCount() != 0 {
		t.Fatalf("repeat unsubscribe must stay a no-op: forward=%d", subCount())
	}
}

// TestProcess_TerminalSweepsSubscriptions: terminating while a catch is parked
// (terminate-while-parked) sweeps the forward MessageSubscription and the
// reverse index via finishProcessInstance, even with retention_ms==0.
func TestProcess_TerminalSweepsSubscriptions(t *testing.T) {
	p, _, col := newTestPartition(t)
	svc, key, _, _, _, subCount, idxPresent, apply := procSubFixture(t, p, col)
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)

	apply(3, procEventCmd(pk, svc, key, []byte("e2"), nil))
	apply(4, procAdvancedCmd(pk, svc, key, []byte("final"), &enginev1.ProcessTerminal{Output: []byte("out")}))

	if subCount() != 0 || idxPresent() {
		t.Fatalf("terminal sweep: forward=%d reverseIdx=%v, want 0, false", subCount(), idxPresent())
	}
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatalf("record should be reaped (retention 0)")
	}
}

// TestProcess_LookupProcessInstances exercises the shard-side scan that backs the
// ListProcessInstances fan-out: multi-LP scan, service + status filters, the
// limit cap, and the tenant-band defense.
func TestProcess_LookupProcessInstances(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc = "OrderProc"
	modelRef := &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"}
	idx := uint64(0)
	apply := func(cmd *enginev1.Command) {
		t.Helper()
		idx++
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		col.Drain()
	}
	mk := func(key string) uint64 { return routing.PartitionKey(svc, key) }

	// Three running instances in band 0; complete one with retention so it is
	// retained as COMPLETED.
	for _, k := range []string{"a", "b", "c"} {
		apply(procEventCmd(mk(k), svc, k, []byte("v"), modelRef))
	}
	apply(procAdvancedCmd(mk("a"), svc, "a", []byte("s"), &enginev1.ProcessTerminal{RetentionMs: 60_000}))

	list := func(q LookupProcessInstances) []ProcessInstanceSummary {
		t.Helper()
		res, err := p.Lookup(q)
		if err != nil {
			t.Fatal(err)
		}
		r, ok := res.(ProcessInstancesLookupResult)
		if !ok {
			t.Fatalf("unexpected lookup result type %T", res)
		}
		return r.Instances
	}

	if all := list(LookupProcessInstances{Service: svc}); len(all) != 3 {
		t.Fatalf("list all: got %d, want 3", len(all))
	}
	running := list(LookupProcessInstances{Service: svc,
		StatusFilter: []enginev1.ProcessStatus{enginev1.ProcessStatus_PROCESS_STATUS_RUNNING}})
	if len(running) != 2 {
		t.Fatalf("list running: got %d, want 2", len(running))
	}
	if none := list(LookupProcessInstances{Service: "Other"}); len(none) != 0 {
		t.Fatalf("list other service: got %d, want 0", len(none))
	}
	if capped := list(LookupProcessInstances{Service: svc, Limit: 1}); len(capped) != 1 {
		t.Fatalf("list limit 1: got %d, want 1", len(capped))
	}

	// created_at window: every instance is stamped testEnvelopeNowMs at creation.
	// A lower bound one past it excludes all; an upper bound one past it keeps all.
	if after := list(LookupProcessInstances{Service: svc, CreatedAfterMs: testEnvelopeNowMs + 1}); len(after) != 0 {
		t.Fatalf("created_after now+1: got %d, want 0", len(after))
	}
	if before := list(LookupProcessInstances{Service: svc, CreatedBeforeMs: testEnvelopeNowMs + 1}); len(before) != 3 {
		t.Fatalf("created_before now+1: got %d, want 3", len(before))
	}

	// Page cursor: After = the first row's key resumes strictly past it.
	all := list(LookupProcessInstances{Service: svc})
	first := all[0]
	lp := keys.LPFromPartitionKey(mk(first.InstanceKey))
	resumed := list(LookupProcessInstances{Service: svc,
		After: keys.ProcessInstanceKey(lp, first.Service, first.InstanceKey)})
	if len(resumed) != len(all)-1 {
		t.Fatalf("resume after first: got %d, want %d", len(resumed), len(all)-1)
	}
	for i := range resumed {
		if resumed[i].InstanceKey != all[i+1].InstanceKey {
			t.Fatalf("resume row %d: got %q, want %q", i, resumed[i].InstanceKey, all[i+1].InstanceKey)
		}
	}
}
