package reflw_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/admin"
	"github.com/twinfer/reflw/internal/connectserver"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/pkg/ingressclient"
	"github.com/twinfer/reflw/pkg/reflw"
	"github.com/twinfer/reflw/pkg/reflw/processengine"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	"github.com/twinfer/reflw/proto/adminv1/adminv1connect"
	apiv1 "github.com/twinfer/reflw/proto/apiv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// e2eStartEndBPMN is a trivial executable process (start → end) carrying a
// historyTimeToLive so its terminal record is retained and observable via
// GetProcessInstance.
const e2eStartEndBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:camunda="http://camunda.org/schema/1.0/bpmn" targetNamespace="test">
  <process id="p" isExecutable="true" camunda:historyTimeToLive="P1D">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="end"/>
    <endEvent id="end"><incoming>f1</incoming></endEvent>
  </process>
</definitions>`

// newLoopbackAdminClient mounts srv's Config handler on a loopback
// connectserver and returns a client + cleanup. No authz interceptor — the
// admin listener's mTLS/Cedar gating is exercised by the auth tests; this drives
// the handler logic directly, as the engine config integration tests do.
func newLoopbackAdminClient(t *testing.T, ctx context.Context, srv *admin.Server) (adminv1connect.AdminClient, func()) {
	t.Helper()
	path, h := srv.NewHandler()
	cs, err := connectserver.New(ctx, connectserver.Config{Addr: "127.0.0.1:0"},
		connectserver.Route{Path: path, Handler: h})
	if err != nil {
		t.Fatalf("connectserver.New: %v", err)
	}
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	tr.Protocols.SetHTTP1(false)
	cli := adminv1connect.NewAdminClient(&http.Client{Transport: tr}, "http://"+cs.Addr())
	return cli, func() {
		tr.CloseIdleConnections()
		cs.Close()
	}
}

// TestProcess_RegisterReconcileRun is the durable process-plane round-trip: a
// BPMN model registered through the Config RPC lands in shard 0's ModelTable,
// the host's TableResolver reconciles it into a runnable graph on the notifier
// wake, and a StartProcess against it runs to COMPLETED — all without injecting
// a resolver (Process.Enabled selects the table-backed path). This is the
// production wiring the run.go smoke and the component tests don't cover.
func TestProcess_RegisterReconcileRun(t *testing.T) {
	ingressAddr := freeAddr(t)
	cfg := reflw.Config{
		Node:    reflw.NodeConfig{ID: 1, RaftAddr: freeAddr(t)},
		Storage: reflw.StorageConfig{DataDir: t.TempDir()},
		Ingress: reflw.IngressConfig{Addr: ingressAddr},
		Process: reflw.ProcessConfig{Enabled: true},
		Metrics: reflw.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()

	host, err := reflw.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflw.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	awaitCtx, awaitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer awaitCancel()
	if err := host.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader(shard 1): %v", err)
	}

	// Register the model through the Config RPC → shard 0 ModelTable.
	eng := host.Engine()
	csrv, err := admin.NewServer(admin.Config{
		Host: eng, Runner: eng.MetadataRunner(), PlanModelSet: processengine.PlanModelSet,
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	ccli, closeC := newLoopbackAdminClient(t, ctx, csrv)
	defer closeC()
	modelRef := &enginev1.ModelRef{Kind: "bpmn", Name: "E2E", Version: "v1"}
	// RegisterModelSet targets shard 0 (metadata). On a freshly-started host it
	// can race metadata election and return "not the metadata leader"; retry
	// until shard 0 has a leader (sub-second on a single node). Mirrors the
	// StartProcess retry-until-deadline loop below.
	regDeadline := time.Now().Add(15 * time.Second)
	for {
		_, err := ccli.RegisterModelSet(ctx, connect.NewRequest(&adminv1.RegisterModelSetRequest{
			Entries: []*adminv1.ModelSetEntry{{
				Kind: modelRef.GetKind(), Name: modelRef.GetName(), Version: modelRef.GetVersion(),
				Xml: []byte(e2eStartEndBPMN),
			}},
		}))
		if err == nil {
			break
		}
		if time.Now().After(regDeadline) {
			t.Fatalf("RegisterModelSet: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	icli, err := ingressclient.Dial(ingressclient.Options{BaseURL: "http://" + ingressAddr})
	if err != nil {
		t.Fatalf("ingressclient.Dial: %v", err)
	}
	t.Cleanup(func() { _ = icli.Close() })

	// StartProcess only proposes; model resolution happens leader-side, so an
	// instance started before the reconcile lands fails model-not-found. Retry
	// with a fresh key until one reaches COMPLETED (reconcile is sub-second on a
	// single node, but the loop removes the race deterministically).
	deadline := time.Now().Add(20 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		key := fmt.Sprintf("e2e-%d", attempt)
		if _, err := icli.StartProcess(ctx, connect.NewRequest(&ingressv1.StartProcessRequest{
			Kind: modelRef.GetKind(), Name: modelRef.GetName(), Version: modelRef.GetVersion(), InstanceKey: key,
		})); err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		switch pollProcessTerminal(t, ctx, icli, modelRef, key, 5*time.Second) {
		case apiv1.ProcessStatus_PROCESS_STATUS_COMPLETED:
			return // success
		case apiv1.ProcessStatus_PROCESS_STATUS_FAILED:
			time.Sleep(200 * time.Millisecond) // model not reconciled yet; retry
		}
	}
	t.Fatal("model registered via the Config RPC never ran to COMPLETED")
}

// e2eUserTaskBPMN is start → userTask → end, carrying historyTimeToLive so the
// COMPLETED terminal is retained and observable after the task is completed.
const e2eUserTaskBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:camunda="http://camunda.org/schema/1.0/bpmn" targetNamespace="test">
  <process id="p" isExecutable="true" camunda:historyTimeToLive="P1D">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="u"/>
    <userTask id="u"><incoming>f1</incoming><outgoing>f2</outgoing></userTask>
    <sequenceFlow id="f2" sourceRef="u" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
  </process>
</definitions>`

// e2eProcessHost starts a single-node Host with the process plane enabled, awaits
// shard-1 leadership, registers model (kind/name/version) through the Config RPC,
// and returns an ingress client for driving it. All cleanup is registered on t.
// Shared by the process e2e tests; mirrors TestProcess_RegisterReconcileRun's
// inline setup.
func e2eProcessHost(t *testing.T, ctx context.Context, ref *enginev1.ModelRef, xml string) (*ingressclient.Client, string) {
	t.Helper()
	ingressAddr := freeAddr(t)
	cfg := reflw.Config{
		Node:    reflw.NodeConfig{ID: 1, RaftAddr: freeAddr(t)},
		Storage: reflw.StorageConfig{DataDir: t.TempDir()},
		Ingress: reflw.IngressConfig{Addr: ingressAddr},
		Process: reflw.ProcessConfig{Enabled: true},
		Metrics: reflw.MetricsConfig{Disabled: true},
	}
	host, err := reflw.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflw.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	awaitCtx, awaitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer awaitCancel()
	if err := host.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader(shard 1): %v", err)
	}

	eng := host.Engine()
	csrv, err := admin.NewServer(admin.Config{
		Host: eng, Runner: eng.MetadataRunner(), PlanModelSet: processengine.PlanModelSet,
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	ccli, closeC := newLoopbackAdminClient(t, ctx, csrv)
	t.Cleanup(closeC)
	regDeadline := time.Now().Add(15 * time.Second)
	for {
		_, err := ccli.RegisterModelSet(ctx, connect.NewRequest(&adminv1.RegisterModelSetRequest{
			Entries: []*adminv1.ModelSetEntry{{Kind: ref.GetKind(), Name: ref.GetName(), Version: ref.GetVersion(), Xml: []byte(xml)}},
		}))
		if err == nil {
			break
		}
		if time.Now().After(regDeadline) {
			t.Fatalf("RegisterModelSet: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	icli, err := ingressclient.Dial(ingressclient.Options{BaseURL: "http://" + ingressAddr})
	if err != nil {
		t.Fatalf("ingressclient.Dial: %v", err)
	}
	t.Cleanup(func() { _ = icli.Close() })
	return icli, ingressAddr
}

// TestProcess_UserTaskParkThenComplete closes the BPMN human-task gap end to end:
// a <userTask> parks the instance (RUNNING with no turn in flight and no
// dispatched work), and a DeliverProcessEvent carrying UserTaskCompleted completes
// it. Before Gaps 1+3 a userTask hard-errored the instance into an incident and
// there was no ingress path to complete a parked task. The model-reconcile race is
// handled the same way as TestProcess_RegisterReconcileRun (retry with fresh keys).
func TestProcess_UserTaskParkThenComplete(t *testing.T) {
	ctx := t.Context()
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "UT", Version: "v1"}
	icli, _ := e2eProcessHost(t, ctx, ref, e2eUserTaskBPMN)

	deadline := time.Now().Add(20 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		key := fmt.Sprintf("ut-%d", attempt)
		if _, err := icli.StartProcess(ctx, connect.NewRequest(&ingressv1.StartProcessRequest{
			Kind: ref.GetKind(), Name: ref.GetName(), Version: ref.GetVersion(), InstanceKey: key,
		})); err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		switch pollProcessParkedOrTerminal(t, ctx, icli, ref, key, 5*time.Second) {
		case apiv1.ProcessStatus_PROCESS_STATUS_RUNNING:
			// Parked at the user task — the resume-token surface lists it (BPMN keys
			// by flow-node id "u").
			assertAwaitingResumeToken(t, ctx, icli, ref, key, "u")
			// Complete it with an external event.
			if _, err := icli.DeliverProcessEvent(ctx, connect.NewRequest(&ingressv1.DeliverProcessEventRequest{
				Name:        ref.GetName(),
				InstanceKey: key,
				EventKind:   "UserTaskCompleted",
				Payload:     []byte(`{"NodeID":"u","Outputs":{"approved":true}}`),
			})); err != nil {
				t.Fatalf("DeliverProcessEvent: %v", err)
			}
			if got := pollProcessTerminal(t, ctx, icli, ref, key, 5*time.Second); got != apiv1.ProcessStatus_PROCESS_STATUS_COMPLETED {
				t.Fatalf("user task instance did not complete after delivery (got %v)", got)
			}
			return // success
		case apiv1.ProcessStatus_PROCESS_STATUS_FAILED:
			time.Sleep(200 * time.Millisecond) // model not reconciled yet; retry
		}
	}
	t.Fatal("user task instance never parked to complete")
}

// e2eHumanCaseCMMN is an autoComplete case whose single plan item is a blocking
// humanTask, carrying historyTimeToLive so the completed case is observable.
const e2eHumanCaseCMMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/CMMN/20151109/MODEL" xmlns:camunda="http://camunda.org/schema/1.0/cmmn">
  <case id="humancase" camunda:historyTimeToLive="P1D">
    <casePlanModel id="stage0" name="Root" autoComplete="true">
      <planItem id="pi1" definitionRef="h1"/>
      <humanTask id="h1" name="Approve"/>
    </casePlanModel>
  </case>
</definitions>`

// TestProcess_CMMNHumanTaskParkThenComplete is the CMMN counterpart to the BPMN
// user-task e2e: a humanTask parks the case, and a DeliverProcessEvent carrying
// the CMMN TaskCompleted event (keyed by PlanItemID) completes it; the
// autoComplete stage then finishes the case. Exercises the same ingress→apply
// wire path as the BPMN test, routed to the CMMN adapter.
func TestProcess_CMMNHumanTaskParkThenComplete(t *testing.T) {
	ctx := t.Context()
	ref := &enginev1.ModelRef{Kind: "cmmn", Name: "HC", Version: "v1"}
	icli, _ := e2eProcessHost(t, ctx, ref, e2eHumanCaseCMMN)

	deadline := time.Now().Add(20 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		key := fmt.Sprintf("hc-%d", attempt)
		if _, err := icli.StartProcess(ctx, connect.NewRequest(&ingressv1.StartProcessRequest{
			Kind: ref.GetKind(), Name: ref.GetName(), Version: ref.GetVersion(), InstanceKey: key,
		})); err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		switch pollProcessParkedOrTerminal(t, ctx, icli, ref, key, 5*time.Second) {
		case apiv1.ProcessStatus_PROCESS_STATUS_RUNNING:
			// Parked at the human task — the resume-token surface lists it keyed by
			// the planItem id (pi1), not the humanTask definition id (h1). Complete it
			// by token alone: the caller names no planItem id and sends only outputs;
			// the token carries (name, key, pi1) and the consume path maps it to the
			// typed cmmn.TaskCompleted. The headline CMMN UX win — and the full
			// resume-token round-trip (mint on GetProcessInstance → decode + validate
			// + typed propose on consume).
			tok := assertAwaitingResumeToken(t, ctx, icli, ref, key, "pi1")
			if _, err := icli.CompleteTask(ctx, connect.NewRequest(&ingressv1.CompleteTaskRequest{
				ResumeToken: tok,
				Output:      []byte(`{}`),
			})); err != nil {
				t.Fatalf("CompleteTask (resume token): %v", err)
			}
			if got := pollProcessTerminal(t, ctx, icli, ref, key, 5*time.Second); got != apiv1.ProcessStatus_PROCESS_STATUS_COMPLETED {
				t.Fatalf("human task case did not complete after delivery (got %v)", got)
			}
			return // success
		case apiv1.ProcessStatus_PROCESS_STATUS_FAILED:
			time.Sleep(200 * time.Millisecond) // model not reconciled yet; retry
		}
	}
	t.Fatal("human task case never parked to complete")
}

// e2eUserTaskSchemaBPMN is start → userTask(with a data output) → end. The data
// output gives the task a typed completion contract, so the resume-point read can
// surface a submission schema.
const e2eUserTaskSchemaBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:camunda="http://camunda.org/schema/1.0/bpmn" targetNamespace="test">
  <itemDefinition id="Item_Decision" structureRef="xsd:string"/>
  <process id="p" isExecutable="true" camunda:historyTimeToLive="P1D">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="u"/>
    <userTask id="u" name="Approve"><incoming>f1</incoming><outgoing>f2</outgoing>
      <ioSpecification><dataOutput id="o0" name="decision" itemSubjectRef="Item_Decision"/></ioSpecification>
    </userTask>
    <sequenceFlow id="f2" sourceRef="u" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
  </process>
</definitions>`

// TestProcess_GetTaskSchema_E2E proves the full GET /v1/tasks/{token} surface end
// to end: register an output-bearing user task, park it, mint a resume token off
// GetProcessInstance, then GET the token over the REST facade and get back the task
// descriptor PLUS its submission schema. This is the one seam the unit tests can't
// cover — the run.go wiring that hands the active model resolver to the ingress
// Server as a TaskSchemaResolver — exercised against a real reconciled model.
func TestProcess_GetTaskSchema_E2E(t *testing.T) {
	ctx := t.Context()
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "UTS", Version: "v1"}
	icli, addr := e2eProcessHost(t, ctx, ref, e2eUserTaskSchemaBPMN)

	deadline := time.Now().Add(20 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		key := fmt.Sprintf("uts-%d", attempt)
		if _, err := icli.StartProcess(ctx, connect.NewRequest(&ingressv1.StartProcessRequest{
			Kind: ref.GetKind(), Name: ref.GetName(), Version: ref.GetVersion(), InstanceKey: key,
		})); err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		switch pollProcessParkedOrTerminal(t, ctx, icli, ref, key, 5*time.Second) {
		case apiv1.ProcessStatus_PROCESS_STATUS_RUNNING:
			tok := assertAwaitingResumeToken(t, ctx, icli, ref, key, "u")
			body := httpGetTask(t, ctx, addr, tok)
			if body["service"] != ref.Name || body["instanceKey"] != key || body["nodeId"] != "u" {
				t.Fatalf("descriptor = %v", body)
			}
			schema, ok := body["schema"].(map[string]any)
			if !ok {
				t.Fatalf("schema absent/wrong type (run.go resolver wiring?): %v", body["schema"])
			}
			props, _ := schema["properties"].(map[string]any)
			if _, ok := props["decision"]; !ok {
				t.Fatalf("schema.properties.decision missing: %v", schema)
			}
			return // success
		case apiv1.ProcessStatus_PROCESS_STATUS_FAILED:
			time.Sleep(200 * time.Millisecond) // model not reconciled yet; retry
		}
	}
	t.Fatal("user task instance never parked")
}

// httpGetTask issues GET /v1/tasks/{token} over the REST facade and returns the
// decoded JSON body, failing the test on a non-200. Plain HTTP (the REST routes
// serve over HTTP/1.1), anonymous (the e2e ingress permits the read action).
func httpGetTask(t *testing.T, ctx context.Context, addr, token string) map[string]any {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/v1/tasks/"+token, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/tasks: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/tasks status = %d (%q)", resp.StatusCode, string(b))
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode %q: %v", b, err)
	}
	return m
}

// assertAwaitingResumeToken reads the parked instance and asserts GetProcessInstance
// surfaces exactly one awaiting task with the expected node_id and a resume_token
// that decodes back to (name, instance_key, node_id) — the Phase-3 discovery
// surface, proving the RUNNING-gated mint against a real linearizable read. Returns
// the token so a caller can round-trip it through the consume path.
func assertAwaitingResumeToken(t *testing.T, ctx context.Context, icli *ingressclient.Client, ref *enginev1.ModelRef, key, wantNode string) string {
	t.Helper()
	resp, err := icli.GetProcessInstance(ctx, connect.NewRequest(&ingressv1.GetProcessInstanceRequest{
		Name: ref.GetName(), InstanceKey: key,
	}))
	if err != nil {
		t.Fatalf("GetProcessInstance: %v", err)
	}
	tasks := resp.Msg.GetInstance().GetAwaitingTasks()
	if len(tasks) != 1 {
		t.Fatalf("awaiting_tasks = %d, want 1: %+v", len(tasks), tasks)
	}
	if tasks[0].GetNodeId() != wantNode {
		t.Fatalf("awaiting node_id = %q, want %q", tasks[0].GetNodeId(), wantNode)
	}
	tgt, err := keys.DecodeResumeToken(tasks[0].GetResumeToken())
	if err != nil {
		t.Fatalf("decode resume token %q: %v", tasks[0].GetResumeToken(), err)
	}
	if tgt.Service != ref.GetName() || tgt.InstanceKey != key || tgt.NodeID != wantNode {
		t.Fatalf("decoded token = %+v, want service=%q key=%q node=%q", tgt, ref.GetName(), key, wantNode)
	}
	return tasks[0].GetResumeToken()
}

// pollProcessParkedOrTerminal polls until the instance is parked at a passive wait
// (RUNNING, active_seq==0, outstanding==0, with the start turn already consumed) or
// reaches a terminal, returning the observed status; UNSPECIFIED on timeout.
func pollProcessParkedOrTerminal(t *testing.T, ctx context.Context, icli *ingressclient.Client, ref *enginev1.ModelRef, key string, timeout time.Duration) apiv1.ProcessStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := icli.GetProcessInstance(ctx, connect.NewRequest(&ingressv1.GetProcessInstanceRequest{
			Name: ref.GetName(), InstanceKey: key,
		}))
		if err == nil && resp.Msg.GetPresent() {
			switch st := resp.Msg.GetInstance().GetStatus(); st {
			case apiv1.ProcessStatus_PROCESS_STATUS_COMPLETED, apiv1.ProcessStatus_PROCESS_STATUS_FAILED:
				return st
			case apiv1.ProcessStatus_PROCESS_STATUS_RUNNING:
				// active_seq==0 + outstanding==0 + next_seq>1 means the start turn ran
				// and the instance is now parked with no dispatched work — a user/human
				// task wait, not a turn still in flight.
				if resp.Msg.GetInstance().GetActiveSeq() == 0 && resp.Msg.GetInstance().GetOutstanding() == 0 && resp.Msg.GetInstance().GetNextSeq() > 1 {
					return st
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return apiv1.ProcessStatus_PROCESS_STATUS_UNSPECIFIED
}

// pollProcessTerminal polls GetProcessInstance until the instance reaches a
// terminal status (COMPLETED/FAILED) or the timeout elapses; returns
// UNSPECIFIED on timeout.
func pollProcessTerminal(t *testing.T, ctx context.Context, icli *ingressclient.Client, ref *enginev1.ModelRef, key string, timeout time.Duration) apiv1.ProcessStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := icli.GetProcessInstance(ctx, connect.NewRequest(&ingressv1.GetProcessInstanceRequest{
			Name: ref.GetName(), InstanceKey: key,
		}))
		if err == nil && resp.Msg.GetPresent() {
			switch st := resp.Msg.GetInstance().GetStatus(); st {
			case apiv1.ProcessStatus_PROCESS_STATUS_COMPLETED,
				apiv1.ProcessStatus_PROCESS_STATUS_FAILED:
				return st
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return apiv1.ProcessStatus_PROCESS_STATUS_UNSPECIFIED
}
