package engine_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/admin"
	"github.com/twinfer/reflw/internal/loadgen"
	"github.com/twinfer/reflw/pkg/handler"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// TestSDKServer_E2E_HTTP2 drives the real handler-side stack
// (pkg/handler.NewServer) end-to-end through the engine wire path:
// admin.RegisterDeployment → connectclient → InvokeStream → server.runSession
// → user handler → OutputCommandMessage → engine InvokerEffect.Completed.
//
// Asserts both the input round-trip (handler sees the engine's input
// bytes) and the output round-trip (engine's Completed.output matches
// the handler's return).
func TestSDKServer_E2E_HTTP2(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(ctx handler.Context, in []byte) ([]byte, error) {
		// Confirm we have a real wireContext (Input() / InvocationID()).
		if ctx.InvocationID() == nil {
			t.Errorf("handler: InvocationID() returned nil")
		}
		return append([]byte("sdk-srv-h2:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	srv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()
	handlerAddr := ln.Addr().String()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N: 3,
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
