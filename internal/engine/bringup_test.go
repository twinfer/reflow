package engine_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/pkg/sdk"
	"github.com/twinfer/reflow/pkg/sdk/server"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
)

// singleNodeWithHandlers brings up a single-node Host on a temp dir
// with shard 0 (metadata) and shard 1 (partition) live, and starts a
// pkg/sdk/server hosting reg on a free local port, registering the URL
// as a deployment with the local metadata leader. Teardown is t.Cleanup.
//
// reg with zero handlers skips the SDK server / deployment registration.
func singleNodeWithHandlers(t *testing.T, reg *sdk.Registry) *engine.Host {
	t.Helper()
	dir := t.TempDir()
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeLocalAddr(t),
		DataDir:            filepath.Join(dir, "node1"),
		RTTMillisecond:     50,
		NumPartitionShards: 1,
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
	if err := h.AwaitLeader(ctx, 1); err != nil {
		t.Fatalf("AwaitLeader(1): %v", err)
	}
	if err := h.AwaitMetadataLeader(ctx); err != nil {
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}

	if reg != nil && reg.Len() > 0 {
		registerEmbeddedHandlers(t, h, reg)
	}
	return h
}

// bringUpHostWithIngress is singleNodeWithHandlers + an ingress runtime
// on ephemeral HTTP+gRPC ports. Convenience wrapper for tests that
// exercise the full ingress → engine → handler path.
func bringUpHostWithIngress(t *testing.T, reg *sdk.Registry) (*engine.Host, *ingress.Runtime) {
	t.Helper()
	h := singleNodeWithHandlers(t, reg)
	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		HTTPAddr: "127.0.0.1:0",
		GRPCAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return h, rt
}

// registerEmbeddedHandlers starts a pkg/sdk/server.NewHTTP2 endpoint
// hosting reg on a free local port and registers the URL as a
// deployment with h's admin server. Teardown is registered on t.
// Assumes h.MetadataRunner() is the metadata leader.
func registerEmbeddedHandlers(t *testing.T, h *engine.Host, reg *sdk.Registry) {
	t.Helper()
	srv, err := server.NewHTTP2(server.Config{Registry: reg})
	if err != nil {
		t.Fatalf("server.NewHTTP2: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Shutdown()
		_ = ln.Close()
	})

	asrv, err := admin.NewServer(admin.Config{Host: h, Runner: h.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	if _, err := asrv.RegisterDeployment(regCtx, &adminv1.RegisterDeploymentRequest{
		Url: "http://" + ln.Addr().String(),
	}); err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}
}
