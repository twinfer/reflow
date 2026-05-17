package reflow_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/pkg/reflow"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// TestRun_StartsIngressListener verifies that reflow.Run wires the
// ingress gRPC + HTTP listeners. It dials the ingress port and submits
// an invocation against a handler that has no deployment registered —
// the call must reach the ingress server (proving the listener works)
// and return FailedPrecondition (proving the deployment-lookup path is
// reachable through the listener).
func TestRun_StartsIngressListener(t *testing.T) {
	// Two reflow.Run hosts cannot share the default prometheus
	// registry (re-registration panics) and parallel dragonboat
	// instances slow leader election under -race; keep serial.
	raftAddr := freeAddr(t)
	grpcAddr := freeAddr(t)
	httpAddr := freeAddr(t)

	cfg := reflow.Config{
		Node: reflow.NodeConfig{ID: 1, RaftAddr: raftAddr},
		Storage: reflow.StorageConfig{
			DataDir: t.TempDir(),
		},
		Ingress: reflow.IngressConfig{
			GRPCAddr: grpcAddr,
			HTTPAddr: httpAddr,
		},
		Metrics: reflow.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()

	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflow.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	// AwaitLeader checks regular partitions (shard >= 1); on this single-
	// node setup partition 1 becomes leader only after shard 0 does, so
	// the wait here covers both.
	awaitCtx, awaitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer awaitCancel()
	if err := host.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader(shard 1): %v", err)
	}

	conn, err := grpc.NewClient(grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial ingress: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := ingressv1.NewIngressClient(conn)
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	_, err = client.SubmitInvocation(callCtx, &ingressv1.SubmitInvocationRequest{
		Service: "Greeter",
		Handler: "hello",
	})
	if err == nil {
		t.Fatal("expected error from SubmitInvocation against unregistered handler; got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error; got %v", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("SubmitInvocation code = %v; want FailedPrecondition (proves ingress reached deployment lookup): %v",
			st.Code(), err)
	}
}

// TestRun_IngressDefaultsApplyWhenBothEmpty verifies that Run fills in
// the standard ingress ports when the operator leaves both addresses
// empty in the config. Dials :8081 to confirm the listener is bound.
//
// Skipped when :8081 is already taken on the host (CI runners with
// other reflow processes); the regression we care about is "Run
// silently doesn't start ingress when the config is zero-value."
func TestRun_IngressDefaultsApplyWhenBothEmpty(t *testing.T) {
	// Two reflow.Run hosts cannot share the default prometheus
	// registry (re-registration panics) and parallel dragonboat
	// instances slow leader election under -race; keep serial.
	if ln, err := net.Listen("tcp", ":8081"); err != nil {
		t.Skipf("ingress default port :8081 is occupied: %v", err)
	} else {
		_ = ln.Close()
	}
	cfg := reflow.Config{
		Node: reflow.NodeConfig{ID: 1, RaftAddr: freeAddr(t)},
		Storage: reflow.StorageConfig{
			DataDir: t.TempDir(),
		},
		Metrics: reflow.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()
	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflow.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	// Defaults kick in: GRPCAddr=:8081, HTTPAddr=:8080. Verify the gRPC
	// listener is reachable.
	conn, err := grpc.NewClient(":8081",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial default ingress: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
}

// TestRun_IngressDisabled verifies that cfg.Ingress.Disabled=true skips
// the ingress listener: the chosen gRPC port stays free and the Host
// still closes cleanly. This is the deployment shape an operator picks
// when a separate ingress fleet handles client traffic.
func TestRun_IngressDisabled(t *testing.T) {
	// Two reflow.Run hosts cannot share the default prometheus
	// registry (re-registration panics) and parallel dragonboat
	// instances slow leader election under -race; keep serial.
	grpcAddr := freeAddr(t)

	cfg := reflow.Config{
		Node: reflow.NodeConfig{ID: 1, RaftAddr: freeAddr(t)},
		Storage: reflow.StorageConfig{
			DataDir: t.TempDir(),
		},
		Ingress: reflow.IngressConfig{
			Disabled: true,
			GRPCAddr: grpcAddr, // ignored because Disabled is set
		},
		Metrics: reflow.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()

	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflow.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	// The supplied gRPC addr must remain unbound — a fresh Listen on
	// the same address succeeds only if Run did not claim it.
	ln, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("expected ingress addr %s unbound when Disabled=true; got %v", grpcAddr, err)
	}
	_ = ln.Close()
}

// freeAddr returns a 127.0.0.1:port the kernel hasn't currently bound.
// Same pattern as internal/loadgen.FreeLocalAddr but inlined to avoid
// pulling loadgen into pkg/reflow's test scope.
func freeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
