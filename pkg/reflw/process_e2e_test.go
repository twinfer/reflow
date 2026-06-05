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
		Host: eng, Runner: eng.MetadataRunner(), ValidateModel: processengine.ValidateModel,
	})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}
	ccli, closeC := newLoopbackConfigClient(t, ctx, csrv)
	defer closeC()
	modelRef := &enginev1.ModelRef{Kind: "bpmn", Name: "E2E", Version: "v1"}
	if _, err := ccli.UpsertModel(ctx, connect.NewRequest(&configv1.UpsertModelRequest{
		ModelRef: modelRef,
		Xml:      []byte(e2eStartEndBPMN),
	})); err != nil {
		t.Fatalf("UpsertModel: %v", err)
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
