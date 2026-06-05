package reflw_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/twinfer/reflw/pkg/ingressclient"
	"github.com/twinfer/reflw/pkg/reflw"
)

// freeAddr returns a free 127.0.0.1 port. Bind-and-release; cheap, fine
// for tests.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// TestRun_StartsIngressListener verifies that reflw.Run wires the
// ingress Connect listener. It dials the ingress port and submits an
// invocation against a handler that has no deployment registered — the
// call must reach the ingress server (proving the listener works) and
// return FailedPrecondition (proving the deployment-lookup path is
// reachable through the listener).
func TestRun_StartsIngressListener(t *testing.T) {
	// Two reflw.Run hosts cannot share the default prometheus
	// registry (re-registration panics) and parallel dragonboat
	// instances slow leader election under -race; keep serial.
	raftAddr := freeAddr(t)
	ingressAddr := freeAddr(t)

	cfg := reflw.Config{
		Node: reflw.NodeConfig{ID: 1, RaftAddr: raftAddr},
		Storage: reflw.StorageConfig{
			DataDir: t.TempDir(),
		},
		Ingress: reflw.IngressConfig{
			Addr: ingressAddr,
		},
		Metrics: reflw.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()

	host, err := reflw.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflw.Run: %v", err)
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

	cli, err := ingressclient.Dial(ingressclient.Options{BaseURL: "http://" + ingressAddr})
	if err != nil {
		t.Fatalf("ingressclient.Dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	_, err = cli.Submit(callCtx, ingressclient.SubmitArgs{
		Service: "Greeter",
		Handler: "hello",
	})
	if err == nil {
		t.Fatal("expected error from Submit against unregistered handler; got nil")
	}
	var statusErr *ingressclient.HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected *ingressclient.HTTPStatusError; got %v", err)
	}
	if statusErr.Status != http.StatusPreconditionFailed {
		t.Fatalf("Submit status = %d; want 412 (FailedPrecondition): %v", statusErr.Status, err)
	}
}

// TestRun_ProcessEnabledStartsTableResolver verifies the durable process-plane
// wiring: Process.Enabled with no injected resolver builds the table-backed
// ModelResolver, installs it as the ProcessEngine, and spawns its reconciler
// over the shard-0 ModelTable — the host comes up and closes cleanly. (The full
// register-model -> reconcile -> run round-trip rides the auth-gated admin RPC;
// this is the run.go wiring smoke the component tests don't cover.)
func TestRun_ProcessEnabledStartsTableResolver(t *testing.T) {
	cfg := reflw.Config{
		Node:    reflw.NodeConfig{ID: 1, RaftAddr: freeAddr(t)},
		Storage: reflw.StorageConfig{DataDir: t.TempDir()},
		Ingress: reflw.IngressConfig{Addr: freeAddr(t)},
		Process: reflw.ProcessConfig{Enabled: true},
		Metrics: reflw.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()

	host, err := reflw.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflw.Run with Process.Enabled: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := host.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader(shard 1): %v", err)
	}
	// Host is up with the durable process plane wired; the ModelTable is empty
	// until an operator registers a model via `reflwd config register-model`.
}

// TestRun_IngressDefaultsApplyWhenEmpty verifies that Run fills in the
// standard ingress port when the operator leaves Addr empty in the
// config. Dials :8080 to confirm the listener is bound.
//
// Skipped when :8080 is already taken on the host (CI runners with
// other reflw processes); the regression we care about is "Run
// silently doesn't start ingress when the config is zero-value."
func TestRun_IngressDefaultsApplyWhenEmpty(t *testing.T) {
	if ln, err := net.Listen("tcp", ":8080"); err != nil {
		t.Skipf("ingress default port :8080 is occupied: %v", err)
	} else {
		_ = ln.Close()
	}
	cfg := reflw.Config{
		Node: reflw.NodeConfig{ID: 1, RaftAddr: freeAddr(t)},
		Storage: reflw.StorageConfig{
			DataDir: t.TempDir(),
		},
		Metrics: reflw.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()
	host, err := reflw.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflw.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	cli, err := ingressclient.Dial(ingressclient.Options{BaseURL: "http://127.0.0.1:8080"})
	if err != nil {
		t.Fatalf("dial default ingress: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
}

// TestRun_IngressDisabled verifies that cfg.Ingress.Disabled=true skips
// the ingress listener: the chosen port stays free and the Host still
// closes cleanly. This is the deployment shape an operator picks when a
// separate ingress fleet handles client traffic.
func TestRun_IngressDisabled(t *testing.T) {
	addr := freeAddr(t)

	cfg := reflw.Config{
		Node: reflw.NodeConfig{ID: 1, RaftAddr: freeAddr(t)},
		Storage: reflw.StorageConfig{
			DataDir: t.TempDir(),
		},
		Ingress: reflw.IngressConfig{
			Disabled: true,
			Addr:     addr, // ignored because Disabled is set
		},
		Metrics: reflw.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()

	host, err := reflw.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflw.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	// The supplied addr must remain unbound — a fresh Listen on the same
	// address succeeds only if Run did not claim it.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("expected addr %s to be free; got listen err: %v", addr, err)
	}
	_ = ln.Close()
}
