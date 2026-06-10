package ingress_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/ingress"
	"github.com/twinfer/reflw/pkg/ingressclient"
	"github.com/twinfer/reflw/pkg/reflw/processengine"
	apiv1 "github.com/twinfer/reflw/proto/apiv1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
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
func bringUpHostWithProcessEngine(t *testing.T, pe *processengine.Adapter) (*engine.Host, *ingressclient.Client) {
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
	res := processengine.NewMapResolver()
	if err := res.ParseBPMN("msgproc", "v1", []byte(e2eMessageCatchBPMN)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	h, cli := bringUpHostWithProcessEngine(t, processengine.New(res))

	const svc, instKey = "msgproc", "o-1"
	shardID := h.Partitioner().ShardForKey(routing.PartitionKey(svc, instKey))

	lookup := func() (engine.ProcessInstanceLookupResult, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		v, err := h.NodeHost().SyncRead(ctx, shardID, engine.LookupProcessInstance{
			Service: svc, InstanceKey: instKey,
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
		Kind:        "bpmn",
		Name:        svc,
		Version:     "v1",
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
			Name: svc, InstanceKey: instKey,
		}))
		if gerr != nil {
			t.Fatalf("GetProcessInstance: %v", gerr)
		}
		return resp.Msg
	}
	if g := getInst(); !g.GetPresent() || g.GetInstance().GetActiveSeq() != 0 {
		t.Fatalf("GetProcessInstance parked = {present:%v active_seq:%d}, want {true 0}", g.GetPresent(), g.GetInstance().GetActiveSeq())
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

// TestIngress_GetProcessInstanceHistory proves the activity-timeline read path
// against the live host: a real BPMN instance parks on its message catch, and the
// public GetProcessInstanceHistory RPC returns its recorded timeline — the start
// event (STARTED) and the message subscription (SUBSCRIBED) — in seq order, with
// after_seq paging strictly past a cursor.
func TestIngress_GetProcessInstanceHistory(t *testing.T) {
	res := processengine.NewMapResolver()
	if err := res.ParseBPMN("msgproc", "v1", []byte(e2eMessageCatchBPMN)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	_, cli := bringUpHostWithProcessEngine(t, processengine.New(res))

	const svc, instKey = "msgproc", "h-1"
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()
	if _, err := cli.StartProcess(startCtx, connect.NewRequest(&ingressv1.StartProcessRequest{
		Kind:        "bpmn",
		Name:        svc,
		Version:     "v1",
		InstanceKey: instKey,
		Vars:        []byte(`{"orderId":"A-1"}`),
	})); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	hist := func(after uint64, limit uint32) *ingressv1.GetProcessInstanceHistoryResponse {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := cli.GetProcessInstanceHistory(ctx, connect.NewRequest(&ingressv1.GetProcessInstanceHistoryRequest{
			Name: svc, InstanceKey: instKey,
			AfterSeq: after, Limit: limit,
		}))
		if err != nil {
			t.Fatalf("GetProcessInstanceHistory: %v", err)
		}
		return resp.Msg
	}

	// Wait until the start turn recorded both the inbound start (STARTED) and the
	// outbound subscribe (SUBSCRIBED) in the same committed batch.
	deadline := time.Now().Add(10 * time.Second)
	var got *ingressv1.GetProcessInstanceHistoryResponse
	for time.Now().Before(deadline) {
		got = hist(0, 0)
		if got.GetPresent() && len(got.GetEvents()) >= 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !got.GetPresent() || len(got.GetEvents()) < 2 {
		t.Fatalf("history present=%v len=%d, want present with >=2 events", got.GetPresent(), len(got.GetEvents()))
	}
	evs := got.GetEvents()
	for i, ev := range evs {
		if ev.GetSeq() != uint64(i+1) {
			t.Fatalf("event[%d] seq=%d, want %d", i, ev.GetSeq(), i+1)
		}
	}
	if evs[0].GetKind() != apiv1.ProcessHistoryKind_PROCESS_HISTORY_KIND_STARTED {
		t.Fatalf("event[0] kind=%v, want STARTED", evs[0].GetKind())
	}
	subscribed := false
	for _, ev := range evs {
		if ev.GetKind() == apiv1.ProcessHistoryKind_PROCESS_HISTORY_KIND_SUBSCRIBED {
			subscribed = true
		}
	}
	if !subscribed {
		t.Fatalf("no SUBSCRIBED event in timeline: %+v", evs)
	}

	// after_seq=1 resumes strictly past the start event.
	page := hist(1, 0)
	if len(page.GetEvents()) == 0 || page.GetEvents()[0].GetSeq() != 2 {
		t.Fatalf("after_seq=1 first seq = %v, want 2", page.GetEvents())
	}

	// An unknown instance reports present=false (not an error).
	if r := hist(0, 0); !r.GetPresent() {
		t.Fatal("sanity: known instance should be present")
	}
	unknownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	unknown, err := cli.GetProcessInstanceHistory(unknownCtx, connect.NewRequest(&ingressv1.GetProcessInstanceHistoryRequest{
		Name: svc, InstanceKey: "no-such-instance",
	}))
	if err != nil {
		t.Fatalf("GetProcessInstanceHistory(unknown): %v", err)
	}
	if unknown.Msg.GetPresent() {
		t.Fatalf("unknown instance reported present")
	}
}

// TestIngress_ListProcessInstances proves the band-scoped fan-out: several parked
// instances of one model are enumerated by ListProcessInstances (each running,
// parked on its message catch), with service and status filters applied. Single
// node, so the fan-out resolves every band LP to the one partition shard.
func TestIngress_ListProcessInstances(t *testing.T) {
	res := processengine.NewMapResolver()
	if err := res.ParseBPMN("msgproc", "v1", []byte(e2eMessageCatchBPMN)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	_, cli := bringUpHostWithProcessEngine(t, processengine.New(res))

	const svc = "msgproc"
	instKeys := []string{"o-1", "o-2", "o-3"}
	for _, k := range instKeys {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := cli.StartProcess(ctx, connect.NewRequest(&ingressv1.StartProcessRequest{
			Kind:        "bpmn",
			Name:        svc,
			Version:     "v1",
			InstanceKey: k,
			Vars:        []byte(`{"orderId":"` + k + `"}`),
		}))
		cancel()
		if err != nil {
			t.Fatalf("StartProcess %s: %v", k, err)
		}
	}

	listReq := func(req *ingressv1.ListProcessInstancesRequest) []*apiv1.ProcessInstanceView {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err := cli.ListProcessInstances(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatalf("ListProcessInstances: %v", err)
		}
		return resp.Msg.GetInstances()
	}

	// Wait until all three records exist (each StartProcess commits its record).
	deadline := time.Now().Add(10 * time.Second)
	var got []*apiv1.ProcessInstanceView
	for time.Now().Before(deadline) {
		got = listReq(&ingressv1.ListProcessInstancesRequest{Name: svc})
		if len(got) == len(instKeys) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(got) != len(instKeys) {
		t.Fatalf("list returned %d instances, want %d", len(got), len(instKeys))
	}
	seen := map[string]bool{}
	for _, s := range got {
		if s.GetService() != svc {
			t.Fatalf("summary service = %q, want %q", s.GetService(), svc)
		}
		seen[s.GetInstanceKey()] = true
	}
	for _, k := range instKeys {
		if !seen[k] {
			t.Fatalf("instance %q missing from list", k)
		}
	}

	// A non-matching model name lists nothing.
	if other := listReq(&ingressv1.ListProcessInstancesRequest{Name: "nope"}); len(other) != 0 {
		t.Fatalf("list other model: got %d, want 0", len(other))
	}
	// limit caps the result.
	if capped := listReq(&ingressv1.ListProcessInstancesRequest{Name: svc, Limit: 1}); len(capped) != 1 {
		t.Fatalf("list limit 1: got %d, want 1", len(capped))
	}
}

// e2eGatewayFailBPMN: Start -> ExclusiveGateway with one conditioned outgoing flow
// and no default. With start vars that fail the condition, the gateway takes no
// flow and the engine emits an uncaught ProcessFailed on the start turn — a
// top-level genuine failure, which the adapter parks as an incident. Copied from
// reflwos bpmn/engine_gateway_test.go (TestEngine_ExclusiveGateway_NoMatchFails);
// FEEL dialect (bare `=`, no ${} wrapper).
const e2eGatewayFailBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <exclusiveGateway id="gw">
      <incoming>f1</incoming>
      <outgoing>fYes</outgoing>
    </exclusiveGateway>
    <endEvent id="end"><incoming>fYes</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="gw"/>
    <sequenceFlow id="fYes" sourceRef="gw" targetRef="end">
      <conditionExpression>status = "approved"</conditionExpression>
    </sequenceFlow>
  </process>
</definitions>`

// TestIngress_ProcessIncidentLifecycle drives a top-level BPMN instance into an
// uncaught failure (exclusive gateway, no matching flow), then exercises the
// incident operational surface: observe it via GetProcessInstance (status INCIDENT
// + incident details) and the history stream (INCIDENT_RAISED), and resolve with
// TERMINATE (the instance is reaped). The RETRY happy path lives in
// TestIngress_ProcessIncidentRetry.
func TestIngress_ProcessIncidentLifecycle(t *testing.T) {
	res := processengine.NewMapResolver()
	if err := res.ParseBPMN("failproc", "v1", []byte(e2eGatewayFailBPMN)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	_, cli := bringUpHostWithProcessEngine(t, processengine.New(res))

	const svc, instKey = "failproc", "f-1"
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()
	if _, err := cli.StartProcess(startCtx, connect.NewRequest(&ingressv1.StartProcessRequest{
		Kind:        "bpmn",
		Name:        svc,
		Version:     "v1",
		InstanceKey: instKey,
		Vars:        []byte(`{"status":"rejected"}`),
	})); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	getInst := func() *ingressv1.GetProcessInstanceResponse {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := cli.GetProcessInstance(ctx, connect.NewRequest(&ingressv1.GetProcessInstanceRequest{
			Name: svc, InstanceKey: instKey,
		}))
		if err != nil {
			t.Fatalf("GetProcessInstance: %v", err)
		}
		return resp.Msg
	}

	// Wait until the start turn fails the gateway and parks the instance as an
	// incident (non-terminal: present, status INCIDENT).
	deadline := time.Now().Add(10 * time.Second)
	var g *ingressv1.GetProcessInstanceResponse
	for time.Now().Before(deadline) {
		g = getInst()
		if g.GetPresent() && g.GetInstance().GetStatus() == apiv1.ProcessStatus_PROCESS_STATUS_INCIDENT {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !g.GetPresent() || g.GetInstance().GetStatus() != apiv1.ProcessStatus_PROCESS_STATUS_INCIDENT {
		t.Fatalf("instance not parked as incident: present=%v status=%v", g.GetPresent(), g.GetInstance().GetStatus())
	}
	if g.GetInstance().GetIncident().GetNodeId() != "gw" {
		t.Fatalf("incident node = %q, want gw (incident=%+v)", g.GetInstance().GetIncident().GetNodeId(), g.GetInstance().GetIncident())
	}

	// The activity timeline records the incident.
	hctx, hcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer hcancel()
	hresp, err := cli.GetProcessInstanceHistory(hctx, connect.NewRequest(&ingressv1.GetProcessInstanceHistoryRequest{
		Name: svc, InstanceKey: instKey,
	}))
	if err != nil {
		t.Fatalf("GetProcessInstanceHistory: %v", err)
	}
	sawIncident := false
	for _, ev := range hresp.Msg.GetEvents() {
		if ev.GetKind() == apiv1.ProcessHistoryKind_PROCESS_HISTORY_KIND_INCIDENT_RAISED {
			sawIncident = true
		}
	}
	if !sawIncident {
		t.Fatalf("history missing INCIDENT_RAISED: %+v", hresp.Msg.GetEvents())
	}

	// TERMINATE resolves the incident; the instance is reaped.
	tctx, tcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tcancel()
	if _, err := cli.ResolveProcessIncident(tctx, connect.NewRequest(&ingressv1.ResolveProcessIncidentRequest{
		Name: svc, InstanceKey: instKey,
		Resolution: apiv1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_TERMINATE,
	})); err != nil {
		t.Fatalf("ResolveProcessIncident TERMINATE: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	gone := false
	for time.Now().Before(deadline) {
		if !getInst().GetPresent() {
			gone = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !gone {
		t.Fatal("TERMINATE did not reap the incident instance")
	}
}

// e2eGatewayRetryBPMN is e2eGatewayFailBPMN plus a historyTimeToLive so the
// terminal record is retained (not reaped on completion) — letting the retry
// test observe the COMPLETED status directly rather than racing the reaper.
const e2eGatewayRetryBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://test">
  <process id="p" isExecutable="true" historyTimeToLive="P7D">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <exclusiveGateway id="gw">
      <incoming>f1</incoming>
      <outgoing>fYes</outgoing>
    </exclusiveGateway>
    <endEvent id="end"><incoming>fYes</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="gw"/>
    <sequenceFlow id="fYes" sourceRef="gw" targetRef="end">
      <conditionExpression>status = "approved"</conditionExpression>
    </sequenceFlow>
  </process>
</definitions>`

// TestIngress_ProcessIncidentRetry is the end-to-end proof of incident RETRY: a
// top-level BPMN instance parks on an unroutable gateway, the operator resolves
// the incident with RETRY and a variable patch that fixes the routing condition,
// and the instance re-drives the failed node and runs to completion. This is the
// data-fix path — the canonical Camunda/Zeebe incident-management flow.
func TestIngress_ProcessIncidentRetry(t *testing.T) {
	res := processengine.NewMapResolver()
	if err := res.ParseBPMN("retryproc", "v1", []byte(e2eGatewayRetryBPMN)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	_, cli := bringUpHostWithProcessEngine(t, processengine.New(res))

	const svc, instKey = "retryproc", "r-1"
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()
	if _, err := cli.StartProcess(startCtx, connect.NewRequest(&ingressv1.StartProcessRequest{
		Kind:        "bpmn",
		Name:        svc,
		Version:     "v1",
		InstanceKey: instKey,
		Vars:        []byte(`{"status":"rejected"}`),
	})); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	getInst := func() *ingressv1.GetProcessInstanceResponse {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := cli.GetProcessInstance(ctx, connect.NewRequest(&ingressv1.GetProcessInstanceRequest{
			Name: svc, InstanceKey: instKey,
		}))
		if err != nil {
			t.Fatalf("GetProcessInstance: %v", err)
		}
		return resp.Msg
	}

	// Wait for the instance to park as an incident at the gateway.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if g := getInst(); g.GetPresent() && g.GetInstance().GetStatus() == apiv1.ProcessStatus_PROCESS_STATUS_INCIDENT {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if g := getInst(); g.GetInstance().GetStatus() != apiv1.ProcessStatus_PROCESS_STATUS_INCIDENT {
		t.Fatalf("instance not parked as incident: status=%v", g.GetInstance().GetStatus())
	}

	// Resolve with RETRY, patching the variable that fixes the gateway condition.
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	rresp, rerr := cli.ResolveProcessIncident(rctx, connect.NewRequest(&ingressv1.ResolveProcessIncidentRequest{
		Name: svc, InstanceKey: instKey,
		Resolution: apiv1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_RETRY,
		VarPatch:   []byte(`{"status":"approved"}`),
	}))
	if rerr != nil {
		t.Fatalf("ResolveProcessIncident RETRY: %v", rerr)
	}
	if !rresp.Msg.GetAccepted() {
		t.Fatal("RETRY not accepted")
	}

	// The retried instance re-drives the gateway and completes (retained, not
	// reaped, thanks to historyTimeToLive).
	deadline = time.Now().Add(10 * time.Second)
	var g *ingressv1.GetProcessInstanceResponse
	for time.Now().Before(deadline) {
		g = getInst()
		if g.GetPresent() && g.GetInstance().GetStatus() == apiv1.ProcessStatus_PROCESS_STATUS_COMPLETED {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if g.GetInstance().GetStatus() != apiv1.ProcessStatus_PROCESS_STATUS_COMPLETED {
		t.Fatalf("after RETRY, status = %v, want COMPLETED (incident=%+v)", g.GetInstance().GetStatus(), g.GetInstance().GetIncident())
	}
	if g.GetInstance().GetIncident() != nil {
		t.Fatalf("completed instance still carries an incident: %+v", g.GetInstance().GetIncident())
	}

	// The timeline records the resolution and the eventual completion.
	hctx, hcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer hcancel()
	hresp, err := cli.GetProcessInstanceHistory(hctx, connect.NewRequest(&ingressv1.GetProcessInstanceHistoryRequest{
		Name: svc, InstanceKey: instKey,
	}))
	if err != nil {
		t.Fatalf("GetProcessInstanceHistory: %v", err)
	}
	var sawResolved, sawCompleted bool
	for _, ev := range hresp.Msg.GetEvents() {
		switch ev.GetKind() {
		case apiv1.ProcessHistoryKind_PROCESS_HISTORY_KIND_INCIDENT_RESOLVED:
			sawResolved = true
		case apiv1.ProcessHistoryKind_PROCESS_HISTORY_KIND_COMPLETED:
			sawCompleted = true
		}
	}
	if !sawResolved || !sawCompleted {
		t.Fatalf("history missing INCIDENT_RESOLVED(%v) / COMPLETED(%v): %+v", sawResolved, sawCompleted, hresp.Msg.GetEvents())
	}
}
