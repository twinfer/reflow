package engine_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
	"github.com/twinfer/reflow/pkg/sdk/server"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestSDKServer_E2E_GRPC drives the real handler-side stack
// (pkg/sdk/server.NewGRPC) instead of the in-test fake used by
// TestWireDispatch_GRPC_RoundTrip. End-to-end path: engine →
// handlerclient/grpcclient → SessionService.Invoke → server.runSession →
// user handler → OutputCommandMessage → engine InvokerEffect.Completed.
//
// Asserts both the input round-trip (handler sees the engine's input
// bytes) and the output round-trip (engine's Completed.output matches
// the handler's return).
func TestSDKServer_E2E_GRPC(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(ctx sdk.Context, in []byte) ([]byte, error) {
		// Confirm we have a real wireContext (Input() / InvocationID()).
		if ctx.InvocationID() == nil {
			t.Errorf("handler: InvocationID() returned nil")
		}
		return append([]byte("sdk-srv:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	srv, err := server.NewGRPC(server.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewGRPC: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()
	handlerAddr := ln.Addr().String()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:        3,
		Handlers: sdk.NewRegistry(),
	})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host

	asrv, err := admin.NewServer(admin.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	regResp, err := callRegisterDeployment(regCtx, asrv, "grpc://"+handlerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}
	if regResp.GetDeploymentId() == "" {
		t.Fatal("RegisterDeployment returned empty deployment_id")
	}

	id := buildID(1, "sdkserver-grpc")
	target := &enginev1.InvocationTarget{ServiceName: "Echo", HandlerName: "echo"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/sdkserver-grpc", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, 1, id, 10*time.Second)
	if got, want := string(completed.GetOutput()), "sdk-srv:hello"; got != want {
		t.Errorf("output = %q; want %q", got, want)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}

// TestSDKServer_E2E_HTTP2 is the same end-to-end shape over raw HTTP/2.
func TestSDKServer_E2E_HTTP2(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ sdk.Context, in []byte) ([]byte, error) {
		return append([]byte("sdk-srv-h2:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	srv, err := server.NewHTTP2(server.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewHTTP2: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()
	handlerAddr := ln.Addr().String()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:        3,
		Handlers: sdk.NewRegistry(),
	})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host

	asrv, err := admin.NewServer(admin.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	regResp, err := callRegisterDeployment(regCtx, asrv, "http://"+handlerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	id := buildID(1, "sdkserver-http2")
	target := &enginev1.InvocationTarget{ServiceName: "Echo", HandlerName: "echo"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/sdkserver-http2", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("world"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, 1, id, 10*time.Second)
	if got, want := string(completed.GetOutput()), "sdk-srv-h2:world"; got != want {
		t.Errorf("output = %q; want %q", got, want)
	}
}
