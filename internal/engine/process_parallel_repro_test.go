package engine

import (
	"context"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflow/internal/engine/invoker"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage/keys"
	iflowengine "github.com/twinfer/reflow/pkg/reflow/iflowengine"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// These drive the parallel-join and MI models through the REAL partition state
// machine + REAL iflowengine adapter, hand-delivering each service-task
// completion the way the invoker's process_parent branch would — isolating
// adapter-translate + apply-path (inbox cursor, actuation, blob persist/reload)
// from the invoker/bridge.
//
// They were written to localize the durable conformance gaps (parallel join, MI)
// and PASS — confirming the adapter and apply path were always correct for these
// shapes. The pure-engine differential (iflow/bpmn/engine_reflow_roundtrip_test.go)
// likewise proved ExecutionState round-trips faithfully per turn. The real gap
// was a lost-wakeup race in invoker.StartProcessTurn — a cursor-driven activation
// for the next inbox seq was dropped while the prior turn's session was still
// being reclaimed, freezing any model with concurrent feedback at the join turn —
// fixed by invoker.redriveActiveTurn. These remain as apply-path regression
// guards; the race itself is exercised end-to-end by betsyconf's reflow suite.

// Start -> parallelGateway(split) -> a, b -> parallelGateway(join) -> End.
const reproParallelJoinXML = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f0</outgoing></startEvent>
    <parallelGateway id="split"><incoming>f0</incoming><outgoing>fA</outgoing><outgoing>fB</outgoing></parallelGateway>
    <serviceTask id="a" operationRef="svc"><incoming>fA</incoming><outgoing>f1</outgoing></serviceTask>
    <serviceTask id="b" operationRef="svc"><incoming>fB</incoming><outgoing>f2</outgoing></serviceTask>
    <parallelGateway id="join"><incoming>f1</incoming><incoming>f2</incoming><outgoing>f3</outgoing></parallelGateway>
    <endEvent id="end"><incoming>f3</incoming></endEvent>
    <sequenceFlow id="f0" sourceRef="start" targetRef="split"/>
    <sequenceFlow id="fA" sourceRef="split" targetRef="a"/>
    <sequenceFlow id="fB" sourceRef="split" targetRef="b"/>
    <sequenceFlow id="f1" sourceRef="a" targetRef="join"/>
    <sequenceFlow id="f2" sourceRef="b" targetRef="join"/>
    <sequenceFlow id="f3" sourceRef="join" targetRef="end"/>
  </process>
</definitions>`

func procTaskCompletedCmdMI(pk uint64, service, key, node, instanceIdx string, out []byte) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: service, InstanceKey: key,
		Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
			TaskCompleted: &enginev1.ProcessTaskCompleted{NodeId: node, InstanceIdx: instanceIdx, Output: out},
		}},
	}}}
}

func TestProcess_Iflow_ParallelJoin_ApplyPath(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "pj", "i1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)

	res := iflowengine.NewMapResolver()
	if err := res.ParseBPMN("pj", "v1", []byte(reproParallelJoinXML)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	adapter := iflowengine.New(res)

	var idx uint64
	must := func(cmd *enginev1.Command) {
		t.Helper()
		idx++
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
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
	applyAdv := func(adv *enginev1.ProcessAdvanced) {
		must(&enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv}})
		col.Drain()
	}

	// Start → split dispatches a and b.
	must(procEventCmd(pk, svc, key, []byte(`{}`),
		&enginev1.ModelRef{Kind: "bpmn", Name: "pj", Version: "v1"}))
	adv1 := runTurn()
	if !hasInvokeNode(adv1.GetInvoke(), "a") || !hasInvokeNode(adv1.GetInvoke(), "b") {
		t.Fatalf("start turn: want Invoke for a and b, got %v", adv1.GetInvoke())
	}
	applyAdv(adv1)

	// Branch a completes → join must NOT fire yet.
	must(procTaskCompletedCmd(pk, svc, key, "a", []byte(`{}`)))
	adv2 := runTurn()
	if adv2.GetTerminal() != nil {
		t.Fatalf("after branch a only: unexpected terminal %+v (join over-fired)", adv2.GetTerminal())
	}
	applyAdv(adv2)
	if _, ok, _ := procs.Get(lp, svc, key); !ok {
		t.Fatal("after branch a: record must still be RUNNING")
	}

	// Branch b completes → join fires → end → terminal → reap.
	must(procTaskCompletedCmd(pk, svc, key, "b", []byte(`{}`)))
	adv3 := runTurn()
	if adv3.GetTerminal() == nil || adv3.GetTerminal().GetFailed() {
		t.Fatalf("after both branches: want successful terminal, got %+v", adv3.GetTerminal())
	}
	applyAdv(adv3)
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatal("after terminal: instance should be reaped")
	}
}

// Start -> serviceTask(MI parallel, cardinality 3) -> End.
const reproMINumericXML = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <serviceTask id="approve" operationRef="svc">
      <multiInstanceLoopCharacteristics isSequential="false">
        <loopCardinality>3</loopCardinality>
      </multiInstanceLoopCharacteristics>
      <incoming>f1</incoming><outgoing>f2</outgoing>
    </serviceTask>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="approve"/>
    <sequenceFlow id="f2" sourceRef="approve" targetRef="end"/>
  </process>
</definitions>`

func TestProcess_Iflow_MINumeric_ApplyPath(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "mi", "i1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)

	res := iflowengine.NewMapResolver()
	if err := res.ParseBPMN("mi", "v1", []byte(reproMINumericXML)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	adapter := iflowengine.New(res)

	var idx uint64
	must := func(cmd *enginev1.Command) {
		t.Helper()
		idx++
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
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
	applyAdv := func(adv *enginev1.ProcessAdvanced) {
		must(&enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv}})
		col.Drain()
	}

	// Start → 3 MI dispatches of "approve".
	must(procEventCmd(pk, svc, key, []byte(`{}`),
		&enginev1.ModelRef{Kind: "bpmn", Name: "mi", Version: "v1"}))
	adv1 := runTurn()
	if n := len(adv1.GetInvoke()); n != 3 {
		t.Fatalf("start turn: want 3 MI invokes, got %d: %v", n, adv1.GetInvoke())
	}
	applyAdv(adv1)

	// Complete instances 0,1,2 → after the third, terminal.
	var last *enginev1.ProcessAdvanced
	for _, inst := range []string{"0", "1", "2"} {
		must(procTaskCompletedCmdMI(pk, svc, key, "approve", inst, []byte(`{}`)))
		last = runTurn()
		applyAdv(last)
	}
	if last.GetTerminal() == nil || last.GetTerminal().GetFailed() {
		t.Fatalf("after 3 MI completions: want successful terminal, got %+v", last.GetTerminal())
	}
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatal("after terminal: instance should be reaped")
	}
}
