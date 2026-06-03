package ingress_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/pkg/ingressclient"
	iflowengine "github.com/twinfer/reflow/pkg/reflow/iflowengine"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// e2eMessageCatchBPMN: Start -> IntermediateCatch(message "shipped", correlate
// orderId) -> End. No service task, so no capability bridge / handler deployment
// is needed — the instance parks on the message wait until DeliverMessage.
const e2eMessageCatchBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <message id="shipped" name="shipped"/>
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <intermediateCatchEvent id="wait">
      <extensionElements><correlate var="orderId"/></extensionElements>
      <incoming>f1</incoming><outgoing>f2</outgoing>
      <messageEventDefinition messageRef="shipped"/>
    </intermediateCatchEvent>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="wait"/>
    <sequenceFlow id="f2" sourceRef="wait" targetRef="end"/>
  </process>
</definitions>`

// bringUpHostWithProcessEngine boots a single-node host (shard 0 + shard 1) with
// the given ProcessEngine wired in and the ingress transport started. Mirrors
// bringUpHostWithIngress but for the process plane (no handler deployment).
func bringUpHostWithProcessEngine(t *testing.T, pe *iflowengine.Adapter) (*engine.Host, *ingressclient.Client) {
	t.Helper()
	dir := t.TempDir()
	h, err := engine.NewHost(context.Background(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeAddr(t),
		DataDir:            filepath.Join(dir, "node1"),
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		ProcessEngine:      pe,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.StartMetadataShard(); err != nil {
		t.Fatalf("StartMetadataShard: %v", err)
	}
	if _, err := h.StartPartition(1); err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.AwaitMetadataLeader(ctx); err != nil {
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}
	if err := h.AwaitLeader(ctx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		Addr:             "127.0.0.1:0",
		Middleware:       testIngressMiddleware(t),
		AuthzInterceptor: testAuthzInterceptor(t),
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	cli, err := ingressclient.Dial(ingressclient.Options{BaseURL: "http://" + rt.Addr()})
	if err != nil {
		t.Fatalf("ingressclient.Dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return h, cli
}

// TestIngress_StartProcessThenDeliverMessage is the end-to-end proof that a real
// BPMN model runs through the live host via the new ingress RPCs: StartProcess
// launches it, the leader's procSession runs the start turn and parks the
// instance on its message catch (writing a subscription), DeliverMessage
// correlates an inbound message to that subscription, and the resumed instance
// runs to completion (reaping its record). Exercises StartProcess, DeliverMessage,
// LookupProcessInstance, the subscribe actuation, and the delivery read path
// together against real Raft + the real adapter.
func TestIngress_StartProcessThenDeliverMessage(t *testing.T) {
	res := iflowengine.NewMapResolver()
	if err := res.ParseBPMN("msgproc", "v1", []byte(e2eMessageCatchBPMN)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	h, cli := bringUpHostWithProcessEngine(t, iflowengine.New(res))

	const svc, instKey = "msgproc", "o-1"
	shardID := h.Partitioner().ShardForKey(routing.PartitionKey(0, svc, instKey))

	lookup := func() (engine.ProcessInstanceLookupResult, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		v, err := h.NodeHost().SyncRead(ctx, shardID, engine.LookupProcessInstance{
			Service: svc, InstanceKey: instKey, Tenant: 0,
		})
		if err != nil {
			return engine.ProcessInstanceLookupResult{}, err
		}
		return v.(engine.ProcessInstanceLookupResult), nil
	}

	// 1. Start the process.
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()
	startResp, err := cli.StartProcess(startCtx, connect.NewRequest(&ingressv1.StartProcessRequest{
		ModelRef:    &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"},
		InstanceKey: instKey,
		Vars:        []byte(`{"orderId":"A-1"}`),
	}))
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if startResp.Msg.GetInstanceKey() != instKey {
		t.Fatalf("StartProcess instance key = %q, want %q", startResp.Msg.GetInstanceKey(), instKey)
	}

	// 2. Wait until the start turn has run and parked the instance on its message
	//    wait (active_seq 0 ⇒ idle ⇒ the subscription has been written in the same
	//    committed batch).
	deadline := time.Now().Add(10 * time.Second)
	parked := false
	for time.Now().Before(deadline) {
		r, err := lookup()
		if err == nil && r.Present && r.Record.GetActiveSeq() == 0 {
			parked = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !parked {
		t.Fatal("instance never parked on its message wait (start turn / subscribe did not run)")
	}

	// 2b. The public GetProcessInstance RPC observes the same parked state
	//     (present, idle) without reaching into the engine's internal Lookup.
	getInst := func() *ingressv1.GetProcessInstanceResponse {
		gctx, gcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer gcancel()
		resp, gerr := cli.GetProcessInstance(gctx, connect.NewRequest(&ingressv1.GetProcessInstanceRequest{
			ModelRef: &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"}, InstanceKey: instKey,
		}))
		if gerr != nil {
			t.Fatalf("GetProcessInstance: %v", gerr)
		}
		return resp.Msg
	}
	if g := getInst(); !g.GetPresent() || g.GetActiveSeq() != 0 {
		t.Fatalf("GetProcessInstance parked = {present:%v active_seq:%d}, want {true 0}", g.GetPresent(), g.GetActiveSeq())
	}

	// 3. Deliver the correlated message.
	delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer delCancel()
	if _, err := cli.DeliverMessage(delCtx, connect.NewRequest(&ingressv1.DeliverMessageRequest{
		MessageName:    "shipped",
		CorrelationKey: "A-1",
		Payload:        []byte(`{"tracking":"Z9"}`),
	})); err != nil {
		t.Fatalf("DeliverMessage: %v", err)
	}

	// 4. The resumed instance runs to completion and reaps its record.
	deadline = time.Now().Add(10 * time.Second)
	completed := false
	for time.Now().Before(deadline) {
		r, err := lookup()
		if err == nil && !r.Present {
			completed = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !completed {
		t.Fatal("instance did not complete after DeliverMessage (record never reaped)")
	}

	// 4b. GetProcessInstance now reports absent — terminal-and-reaped is the
	//     public completion signal the betsyconf e2e driver polls for.
	if g := getInst(); g.GetPresent() {
		t.Fatalf("GetProcessInstance after completion = present, want absent (reaped)")
	}
}
