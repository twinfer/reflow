package engine

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// TestProcess_OutstandingCounter pins ProcessInstanceRecord.outstanding — the
// authoritative "dispatched work in flight" count GetProcessInstance exposes so a
// client can tell a working instance from a parked one without timing heuristics.
// Each dispatched unit (service task, armed timer, child) increments it in
// actuateProcessInstructions; its feedback event (or, for timers, a cancel)
// decrements it. Invokes/children pair 1:1; the timer decrement saturates at 0 to
// absorb the benign fire-vs-cancel race.
func TestProcess_OutstandingCounter(t *testing.T) {
	mustApply := func(t *testing.T, p *Partition, idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
	outstanding := func(t *testing.T, p *Partition, lp uint32, svc, key string) uint32 {
		t.Helper()
		procs, _ := procStore(p)
		r, ok, err := procs.Get(lp, svc, key)
		if err != nil || !ok {
			t.Fatalf("record load: ok=%v err=%v", ok, err)
		}
		return r.GetOutstanding()
	}
	feedback := func(pk uint64, svc, key string, pl *enginev1.ProcessEventPayload) *enginev1.Command {
		return &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
			Pk: pk, Service: svc, InstanceKey: key, Payload: pl,
		}}}
	}
	taskDone := func(node string) *enginev1.ProcessEventPayload {
		return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{TaskCompleted: &enginev1.ProcessTaskCompleted{NodeId: node}}}
	}
	timerFired := func(node string, slot uint32) *enginev1.ProcessEventPayload {
		return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TimerFired{TimerFired: &enginev1.ProcessTimerFired{NodeId: node, Slot: slot}}}
	}
	childDone := func(node string) *enginev1.ProcessEventPayload {
		return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_ChildCompleted{ChildCompleted: &enginev1.ProcessChildCompleted{NodeId: node}}}
	}

	t.Run("invoke fan-out increments by N, each feedback decrements", func(t *testing.T) {
		p, _, col := newTestPartition(t)
		const svc, key = "Proc", "i1"
		pk := routing.PartitionKey(svc, key)
		lp := keys.LPFromPartitionKey(pk)
		target := &enginev1.InvocationTarget{ServiceName: "Cap", HandlerName: "do"}

		mustApply(t, p, 1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
		col.Drain()
		// One turn dispatches three service tasks.
		mustApply(t, p, 2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
			Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
			Invoke: []*enginev1.TaskInvoke{
				{NodeId: "T1", Target: target}, {NodeId: "T2", Target: target}, {NodeId: "T3", Target: target},
			},
		}}})
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 3 {
			t.Fatalf("after dispatch: outstanding=%d, want 3", got)
		}
		// Each task_completed feedback decrements, including the ones that queue
		// behind the active turn.
		mustApply(t, p, 3, feedback(pk, svc, key, taskDone("T1")))
		mustApply(t, p, 4, feedback(pk, svc, key, taskDone("T2")))
		mustApply(t, p, 5, feedback(pk, svc, key, taskDone("T3")))
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 0 {
			t.Fatalf("after 3 feedbacks: outstanding=%d, want 0", got)
		}
	})

	t.Run("armed timer fires", func(t *testing.T) {
		p, _, col := newTestPartition(t)
		const svc, key = "Proc", "tf"
		pk := routing.PartitionKey(svc, key)
		lp := keys.LPFromPartitionKey(pk)

		mustApply(t, p, 1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
		col.Drain()
		mustApply(t, p, 2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
			Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
			ArmTimer: []*enginev1.TimerArm{{NodeId: "Boundary", FireAtMs: testEnvelopeNowMs + 5000, Slot: 1}},
		}}})
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 1 {
			t.Fatalf("after arm: outstanding=%d, want 1", got)
		}
		mustApply(t, p, 3, feedback(pk, svc, key, timerFired("Boundary", 1)))
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 0 {
			t.Fatalf("after fire: outstanding=%d, want 0", got)
		}
	})

	t.Run("armed timer cancelled", func(t *testing.T) {
		p, _, col := newTestPartition(t)
		const svc, key = "Proc", "tc"
		pk := routing.PartitionKey(svc, key)
		lp := keys.LPFromPartitionKey(pk)

		mustApply(t, p, 1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
		col.Drain()
		mustApply(t, p, 2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
			Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
			ArmTimer: []*enginev1.TimerArm{{NodeId: "Boundary", FireAtMs: testEnvelopeNowMs + 5000, Slot: 1}},
		}}})
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 1 {
			t.Fatalf("after arm: outstanding=%d, want 1", got)
		}
		// Re-activate, then a turn that cancels the boundary timer.
		mustApply(t, p, 3, procEventCmd(pk, svc, key, []byte("e2"), nil))
		col.Drain()
		mustApply(t, p, 4, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
			Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s2"),
			CancelTimer: []*enginev1.TimerCancel{{NodeId: "Boundary", Slot: 1}},
		}}})
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 0 {
			t.Fatalf("after cancel: outstanding=%d, want 0", got)
		}
	})

	t.Run("child start and completion", func(t *testing.T) {
		p, _, col := newTestPartition(t)
		const svc, key = "Parent", "p1"
		pk := routing.PartitionKey(svc, key)
		lp := keys.LPFromPartitionKey(pk)

		mustApply(t, p, 1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
		col.Drain()
		mustApply(t, p, 2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
			Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
			StartChild: []*enginev1.ChildStart{{
				NodeId: "CA1", ModelRef: &enginev1.ModelRef{Name: "Child"},
				Kind: enginev1.ProcessKind_PROCESS_KIND_BPMN, InstanceKey: "c1",
			}},
		}}})
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 1 {
			t.Fatalf("after child start: outstanding=%d, want 1", got)
		}
		mustApply(t, p, 3, feedback(pk, svc, key, childDone("CA1")))
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 0 {
			t.Fatalf("after child completion: outstanding=%d, want 0", got)
		}
	})

	t.Run("feedback at zero saturates (no underflow)", func(t *testing.T) {
		p, _, col := newTestPartition(t)
		const svc, key = "Proc", "z1"
		pk := routing.PartitionKey(svc, key)
		lp := keys.LPFromPartitionKey(pk)

		mustApply(t, p, 1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
		col.Drain()
		mustApply(t, p, 2, procAdvancedCmd(pk, svc, key, []byte("s1"), nil)) // idle, outstanding 0
		col.Drain()
		// A stray timer_fired (e.g. a fire that raced a cancel) must clamp, not wrap.
		mustApply(t, p, 3, feedback(pk, svc, key, timerFired("Ghost", 9)))
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 0 {
			t.Fatalf("stray feedback at zero: outstanding=%d, want 0 (saturated)", got)
		}
	})

	t.Run("external input does not decrement", func(t *testing.T) {
		p, _, col := newTestPartition(t)
		const svc, key = "Proc", "x1"
		pk := routing.PartitionKey(svc, key)
		lp := keys.LPFromPartitionKey(pk)
		target := &enginev1.InvocationTarget{ServiceName: "Cap", HandlerName: "do"}

		mustApply(t, p, 1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
		col.Drain()
		mustApply(t, p, 2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
			Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
			Invoke: []*enginev1.TaskInvoke{{NodeId: "T1", Target: target}},
		}}})
		col.Drain()
		// An external event (injected message / signal arm) is not feedback for any
		// dispatched unit, so it must leave the count untouched.
		mustApply(t, p, 3, procEventCmd(pk, svc, key, []byte("ext"), nil))
		col.Drain()
		if got := outstanding(t, p, lp, svc, key); got != 1 {
			t.Fatalf("external event changed the count: outstanding=%d, want 1", got)
		}
	})
}
