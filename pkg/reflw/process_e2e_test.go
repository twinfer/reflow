package reflw_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/config"
	"github.com/twinfer/reflw/internal/connectserver"
	"github.com/twinfer/reflw/pkg/ingressclient"
	"github.com/twinfer/reflw/pkg/reflw"
	"github.com/twinfer/reflw/pkg/reflw/processengine"
	configv1 "github.com/twinfer/reflw/proto/configv1"
	"github.com/twinfer/reflw/proto/configv1/configv1connect"
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

// newLoopbackConfigClient mounts srv's Config handler on a loopback
// connectserver and returns a client + cleanup. No authz interceptor — the
// admin listener's mTLS/Cedar gating is exercised by the auth tests; this drives
// the handler logic directly, as the engine config integration tests do.
func newLoopbackConfigClient(t *testing.T, ctx context.Context, srv *config.Server) (configv1connect.ConfigClient, func()) {
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
	cli := configv1connect.NewConfigClient(&http.Client{Transport: tr}, "http://"+cs.Addr())
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
	csrv, err := config.NewServer(config.Config{
		Host: eng, Runner: eng.MetadataRunner(), PlanModelSet: processengine.PlanModelSet,
	})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}
	ccli, closeC := newLoopbackConfigClient(t, ctx, csrv)
	defer closeC()
	modelRef := &enginev1.ModelRef{Kind: "bpmn", Name: "E2E", Version: "v1"}
	// RegisterModelSet targets shard 0 (metadata). On a freshly-started host it
	// can race metadata election and return "not the metadata leader"; retry
	// until shard 0 has a leader (sub-second on a single node). Mirrors the
	// StartProcess retry-until-deadline loop below.
	regDeadline := time.Now().Add(15 * time.Second)
	for {
		_, err := ccli.RegisterModelSet(ctx, connect.NewRequest(&configv1.RegisterModelSetRequest{
			Entries: []*configv1.ModelSetEntry{{
				ModelRef: modelRef,
				Xml:      []byte(e2eStartEndBPMN),
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
			ModelRef: modelRef, InstanceKey: key,
		})); err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		switch pollProcessTerminal(t, ctx, icli, modelRef, key, 5*time.Second) {
		case enginev1.ProcessStatus_PROCESS_STATUS_COMPLETED:
			return // success
		case enginev1.ProcessStatus_PROCESS_STATUS_FAILED:
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
func e2eProcessHost(t *testing.T, ctx context.Context, ref *enginev1.ModelRef, xml string) *ingressclient.Client {
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
	csrv, err := config.NewServer(config.Config{
		Host: eng, Runner: eng.MetadataRunner(), PlanModelSet: processengine.PlanModelSet,
	})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}
	ccli, closeC := newLoopbackConfigClient(t, ctx, csrv)
	t.Cleanup(closeC)
	regDeadline := time.Now().Add(15 * time.Second)
	for {
		_, err := ccli.RegisterModelSet(ctx, connect.NewRequest(&configv1.RegisterModelSetRequest{
			Entries: []*configv1.ModelSetEntry{{ModelRef: ref, Xml: []byte(xml)}},
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
	return icli
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
	icli := e2eProcessHost(t, ctx, ref, e2eUserTaskBPMN)

	deadline := time.Now().Add(20 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		key := fmt.Sprintf("ut-%d", attempt)
		if _, err := icli.StartProcess(ctx, connect.NewRequest(&ingressv1.StartProcessRequest{
			ModelRef: ref, InstanceKey: key,
		})); err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		switch pollProcessParkedOrTerminal(t, ctx, icli, ref, key, 5*time.Second) {
		case enginev1.ProcessStatus_PROCESS_STATUS_RUNNING:
			// Parked at the user task — complete it with an external event.
			if _, err := icli.DeliverProcessEvent(ctx, connect.NewRequest(&ingressv1.DeliverProcessEventRequest{
				ModelRef:    ref,
				InstanceKey: key,
				EventKind:   "UserTaskCompleted",
				Payload:     []byte(`{"NodeID":"u","Outputs":{"approved":true}}`),
			})); err != nil {
				t.Fatalf("DeliverProcessEvent: %v", err)
			}
			if got := pollProcessTerminal(t, ctx, icli, ref, key, 5*time.Second); got != enginev1.ProcessStatus_PROCESS_STATUS_COMPLETED {
				t.Fatalf("user task instance did not complete after delivery (got %v)", got)
			}
			return // success
		case enginev1.ProcessStatus_PROCESS_STATUS_FAILED:
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
	icli := e2eProcessHost(t, ctx, ref, e2eHumanCaseCMMN)

	deadline := time.Now().Add(20 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		key := fmt.Sprintf("hc-%d", attempt)
		if _, err := icli.StartProcess(ctx, connect.NewRequest(&ingressv1.StartProcessRequest{
			ModelRef: ref, InstanceKey: key,
		})); err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		switch pollProcessParkedOrTerminal(t, ctx, icli, ref, key, 5*time.Second) {
		case enginev1.ProcessStatus_PROCESS_STATUS_RUNNING:
			if _, err := icli.DeliverProcessEvent(ctx, connect.NewRequest(&ingressv1.DeliverProcessEventRequest{
				ModelRef:    ref,
				InstanceKey: key,
				// CMMN keys plan items by the planItem id (pi1), not the task
				// definition id (h1) — firing on an unknown id fails the instance.
				EventKind: "TaskCompleted",
				Payload:   []byte(`{"PlanItemID":"pi1","Outputs":{}}`),
			})); err != nil {
				t.Fatalf("DeliverProcessEvent: %v", err)
			}
			if got := pollProcessTerminal(t, ctx, icli, ref, key, 5*time.Second); got != enginev1.ProcessStatus_PROCESS_STATUS_COMPLETED {
				t.Fatalf("human task case did not complete after delivery (got %v)", got)
			}
			return // success
		case enginev1.ProcessStatus_PROCESS_STATUS_FAILED:
			time.Sleep(200 * time.Millisecond) // model not reconciled yet; retry
		}
	}
	t.Fatal("human task case never parked to complete")
}

// pollProcessParkedOrTerminal polls until the instance is parked at a passive wait
// (RUNNING, active_seq==0, outstanding==0, with the start turn already consumed) or
// reaches a terminal, returning the observed status; UNSPECIFIED on timeout.
func pollProcessParkedOrTerminal(t *testing.T, ctx context.Context, icli *ingressclient.Client, ref *enginev1.ModelRef, key string, timeout time.Duration) enginev1.ProcessStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := icli.GetProcessInstance(ctx, connect.NewRequest(&ingressv1.GetProcessInstanceRequest{
			ModelRef: ref, InstanceKey: key,
		}))
		if err == nil && resp.Msg.GetPresent() {
			switch st := resp.Msg.GetStatus(); st {
			case enginev1.ProcessStatus_PROCESS_STATUS_COMPLETED, enginev1.ProcessStatus_PROCESS_STATUS_FAILED:
				return st
			case enginev1.ProcessStatus_PROCESS_STATUS_RUNNING:
				// active_seq==0 + outstanding==0 + next_seq>1 means the start turn ran
				// and the instance is now parked with no dispatched work — a user/human
				// task wait, not a turn still in flight.
				if resp.Msg.GetActiveSeq() == 0 && resp.Msg.GetOutstanding() == 0 && resp.Msg.GetNextSeq() > 1 {
					return st
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return enginev1.ProcessStatus_PROCESS_STATUS_UNSPECIFIED
}

// pollProcessTerminal polls GetProcessInstance until the instance reaches a
// terminal status (COMPLETED/FAILED) or the timeout elapses; returns
// UNSPECIFIED on timeout.
func pollProcessTerminal(t *testing.T, ctx context.Context, icli *ingressclient.Client, ref *enginev1.ModelRef, key string, timeout time.Duration) enginev1.ProcessStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := icli.GetProcessInstance(ctx, connect.NewRequest(&ingressv1.GetProcessInstanceRequest{
			ModelRef: ref, InstanceKey: key,
		}))
		if err == nil && resp.Msg.GetPresent() {
			switch st := resp.Msg.GetStatus(); st {
			case enginev1.ProcessStatus_PROCESS_STATUS_COMPLETED,
				enginev1.ProcessStatus_PROCESS_STATUS_FAILED:
				return st
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return enginev1.ProcessStatus_PROCESS_STATUS_UNSPECIFIED
}
