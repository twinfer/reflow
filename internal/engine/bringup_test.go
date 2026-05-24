package engine_test

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/authz"
	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/pkg/handler"
)

// testIngressMiddleware returns the production authn middleware (no OIDC),
// which stamps the principal; authorization is the separate interceptor
// below. Exercising the middleware path keeps a regression that skips it in
// the same integration coverage.
func testIngressMiddleware(t *testing.T) func(http.Handler) http.Handler {
	t.Helper()
	mw, _, _, err := auth.HTTPMiddleware(auth.Config{}, nil)
	if err != nil {
		t.Fatalf("auth.HTTPMiddleware: %v", err)
	}
	return mw
}

// testAuthzInterceptor returns a Cedar authz interceptor over the in-binary
// foundational policies (ingress is open to all principals), authorizing the
// ingress data plane in integration tests the same way production wires it.
func testAuthzInterceptor(t *testing.T) *authz.Interceptor {
	t.Helper()
	ic, err := authz.NewFoundationalInterceptor(nil, false)
	if err != nil {
		t.Fatalf("authz.NewFoundationalInterceptor: %v", err)
	}
	return ic
}

// singleNodeWithHandlers brings up a single-node Host on a temp dir
// with shard 0 (metadata) and shard 1 (partition) live, and starts a
// pkg/handler hosting reg on a free local port, registering the URL
// as a deployment with the local metadata leader. Teardown is t.Cleanup.
//
// reg with zero handlers skips the SDK server / deployment registration.
func singleNodeWithHandlers(t *testing.T, reg *handler.Registry) *engine.Host {
	t.Helper()
	dir := t.TempDir()
	h, err := engine.NewHost(t.Context(), engine.HostConfig{
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

// singleNodeWithoutHandlers brings up a single-node Host without
// registering any deployments. Tests that need to control the
// deployment registration themselves (e.g. to stamp a custom step
// budget) construct + register manually after this returns.
func singleNodeWithoutHandlers(t *testing.T) *engine.Host {
	t.Helper()
	return singleNodeWithHandlers(t, handler.NewRegistry())
}

// bringUpHostWithIngress is singleNodeWithHandlers + an ingress runtime
// on ephemeral HTTP+gRPC ports. Convenience wrapper for tests that
// exercise the full ingress → engine → handler path.
func bringUpHostWithIngress(t *testing.T, reg *handler.Registry) (*engine.Host, *ingress.Runtime) {
	t.Helper()
	h := singleNodeWithHandlers(t, reg)
	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		Addr:             "127.0.0.1:0",
		Middleware:       testIngressMiddleware(t),
		AuthzInterceptor: testAuthzInterceptor(t),
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return h, rt
}

// registerEmbeddedHandlers starts a pkg/handler.NewServer endpoint
// hosting reg on a free local port and registers the URL as a
// deployment with h's admin server. Teardown is registered on t.
// Assumes h.MetadataRunner() is the metadata leader.
func registerEmbeddedHandlers(t *testing.T, h *engine.Host, reg *handler.Registry) {
	t.Helper()
	url := startSDKServer(t, reg)
	registerDeploymentURL(t, h, url)
}

// startSDKServer starts a pkg/handler.NewServer endpoint hosting reg
// on a free local port and returns the "http://addr" URL. The server's
// lifetime is bound to t — restart tests can reuse the URL across
// Host close/reopen cycles because the deployment registration is
// durable in shard 0.
func startSDKServer(t *testing.T, reg *handler.Registry) string {
	t.Helper()
	srv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		t.Fatalf("handler.NewServer: %v", err)
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
	return "http://" + ln.Addr().String()
}

// registerDeploymentURL proposes a RegisterDeployment for url against
// h's metadata shard. Assumes h.MetadataRunner() is the metadata leader.
func registerDeploymentURL(t *testing.T, h *engine.Host, url string) {
	t.Helper()
	registerDeploymentURLWithBudget(t, h, url, 0)
}

// registerDeploymentURLWithBudget mirrors registerDeploymentURL but
// stamps a per-invocation step-budget override onto the deployment.
// budget=0 → engine default.
func registerDeploymentURLWithBudget(t *testing.T, h *engine.Host, url string, budget uint32) {
	t.Helper()
	asrv, err := config.NewServer(config.Config{Host: h, Runner: h.MetadataRunner()})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	if _, err := asrv.AutoSeedWithBudget(regCtx, url, budget); err != nil {
		t.Fatalf("AutoSeedWithBudget: %v", err)
	}
}

// resolveDeploymentID returns the deployment_id stamped onto
// (service, handler) in shard 0's deployment index. Tests that propose
// InvokeCommand directly (bypassing ingress) use it to populate
// InvokeCommand.deployment_id so the invocation dispatches.
func resolveDeploymentID(t *testing.T, h *engine.Host, service, handler string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	depID, err := h.LookupDeploymentIDByHandler(ctx, service, handler)
	if err != nil {
		t.Fatalf("LookupDeploymentIDByHandler(%s, %s): %v", service, handler, err)
	}
	if depID == "" {
		t.Fatalf("no deployment_id stamped for (%s, %s)", service, handler)
	}
	return depID
}
