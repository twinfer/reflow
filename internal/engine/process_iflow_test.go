package engine

import (
	"context"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/invoker"
	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/pkg/reflw/processengine"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// echoModelXML: Start -> ServiceTask(echo:noop) -> End.
const echoModelXML = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="work"/>
    <serviceTask id="work" operationRef="echo:noop"><incoming>f1</incoming><outgoing>f2</outgoing></serviceTask>
    <sequenceFlow id="f2" sourceRef="work" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
  </process>
</definitions>`

func procTaskCompletedCmd(pk uint64, service, key, node string, out []byte) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: service, InstanceKey: key,
		Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
			TaskCompleted: &enginev1.ProcessTaskCompleted{NodeId: node, Output: out},
		}},
	}}}
}

func hasInvokeNode(invs []*enginev1.TaskInvoke, node string) bool {
	for _, i := range invs {
		if i.GetNodeId() == node {
			return true
		}
	}
	return false
}

// TestProcess_IflowAdapterLifecycle drives a real BPMN model through the real
// processengine.Adapter and the real partition state machine across a full
// lifecycle: start -> service-task invoke -> task completion -> terminal reap.
//
// The invoker is not run; the service task's completion is delivered as the
// ProcessEvent{task_completed} the invoker's process_parent branch would itself
// produce (see TestProcess_ServiceTaskResultFeedsBackToParent for that path).
// Capability execution is covered by the processengine bridge unit tests.
func TestProcess_IflowAdapterLifecycle(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "echo", "i1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)

	res := processengine.NewMapResolver()
	if err := res.ParseBPMN("echo", "v1", []byte(echoModelXML)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	adapter := processengine.New(res)

	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Drain to the activation action, load the record, and run the adapter turn.
	runTurn := func() *enginev1.ProcessAdvanced {
		t.Helper()
		act := firstAdvance(col.Drain(), svc)
		if act == nil {
			t.Fatal("no ActAdvanceProcess produced")
		}
		rec, ok, err := procs.Get(lp, svc, key)
		if err != nil || !ok {
			t.Fatalf("record load: ok=%v err=%v", ok, err)
		}
		adv, err := adapter.Advance(context.Background(), invoker.ProcessAdvanceInput{
			Pk: pk, Service: svc, InstanceKey: key, Record: rec, Entry: act.Entry,
		})
		if err != nil {
			t.Fatalf("adapter.Advance: %v", err)
		}
		return adv
	}

	// 1. Start: adapter must emit an Invoke for the service task.
	must(1, procEventCmd(pk, svc, key, []byte(`{}`),
		&enginev1.ModelRef{Kind: "bpmn", Name: "echo", Version: "v1"}))
	adv1 := runTurn()
	if !hasInvokeNode(adv1.GetInvoke(), "work") {
		t.Fatalf("start turn: want Invoke for work, got %v", adv1.GetInvoke())
	}
	if adv1.GetTerminal() != nil {
		t.Fatalf("start turn: unexpected terminal %+v", adv1.GetTerminal())
	}
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv1}})
	col.Drain() // discard the outbox dispatch for the invoke

	if r, ok, _ := procs.Get(lp, svc, key); !ok || r.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		t.Fatalf("after start: want RUNNING record, got ok=%v rec=%+v", ok, r)
	}

	// 2. Deliver the service-task completion (as the invoker would) → terminal.
	must(3, procTaskCompletedCmd(pk, svc, key, "work", []byte(`{"ok":true}`)))
	adv2 := runTurn()
	if adv2.GetTerminal() == nil || adv2.GetTerminal().GetFailed() {
		t.Fatalf("completion turn: want successful terminal, got %+v", adv2.GetTerminal())
	}
	must(4, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv2}})
	col.Drain()

	// 3. Terminal reaps the instance record.
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatalf("after terminal: want instance reaped, record still present")
	}
}
