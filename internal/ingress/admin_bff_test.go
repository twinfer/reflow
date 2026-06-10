package ingress_test

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/admin"
	"github.com/twinfer/reflw/internal/auth"
	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/ingress"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	"github.com/twinfer/reflw/proto/adminv1/adminv1connect"
)

// principalHeaderMiddleware is a test stand-in for the real authn middleware: it
// stamps a Principal into the request context based on an X-Test-Principal header
// so one bring-up can exercise browser-admin / plain-user / anonymous against the
// admin service without a live OIDC provider or mTLS. The real middleware does
// the same stamping from a verified token/leaf.
func principalHeaderMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var p auth.Principal
			switch r.Header.Get("X-Test-Principal") {
			case "admin":
				p = auth.Principal{Kind: "user", Subject: "alice", Raw: "user/alice", Groups: []string{"reflw-admins"}}
			case "plain":
				p = auth.Principal{Kind: "user", Subject: "bob", Raw: "user/bob"}
			}
			next.ServeHTTP(w, r.WithContext(auth.ContextWithPrincipal(r.Context(), p)))
		})
	}
}

// bringUpIngressBFF starts a single-node host and an ingress listener with the
// admin service mounted on it (ServeAdmin), plus the header-driven principal
// middleware. Returns the ingress address. The admin handler carries only the
// Cedar authz interceptor — a read needs no proposal-principal stamping.
func bringUpIngressBFF(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeAddr(t),
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
	if err := h.AwaitMetadataLeader(ctx); err != nil {
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}

	asrv, err := admin.NewServer(admin.Config{Host: h, Runner: h.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	ic := testAuthzInterceptor(t)
	adminPath, adminH := asrv.NewHandler(connect.WithInterceptors(ic))

	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		Addr:             "127.0.0.1:0",
		Middleware:       principalHeaderMiddleware(),
		AuthzInterceptor: ic,
		ServeAdmin:       true,
		AdminPath:        adminPath,
		AdminHandler:     adminH,
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt.Addr()
}

// TestIngressBFF_AdminOnIngress proves the ingress-as-BFF wiring: the admin
// service is reachable on the ingress listener (a second Vanguard service), and
// the same Cedar policy gates it — a browser admin (OIDC User in reflw-admins)
// reaches an admin read, while an anonymous caller gets 401 and a plain user
// gets 403. This is the Connect-protocol path a connect-query browser client
// uses (no mTLS, bearer-derived principal).
func TestIngressBFF_AdminOnIngress(t *testing.T) {
	addr := bringUpIngressBFF(t)
	cli := adminv1connect.NewAdminClient(http.DefaultClient, "http://"+addr)

	call := func(principal string) error {
		req := connect.NewRequest(&adminv1.ListDeploymentsRequest{})
		if principal != "" {
			req.Header().Set("X-Test-Principal", principal)
		}
		_, err := cli.ListDeployments(context.Background(), req)
		return err
	}

	t.Run("browser admin reaches admin read", func(t *testing.T) {
		if err := call("admin"); err != nil {
			t.Fatalf("admin ListDeployments = %v, want success", err)
		}
	})
	t.Run("anonymous denied", func(t *testing.T) {
		if got := connect.CodeOf(call("")); got != connect.CodeUnauthenticated {
			t.Fatalf("anonymous code = %v, want Unauthenticated", got)
		}
	})
	t.Run("plain user denied", func(t *testing.T) {
		if got := connect.CodeOf(call("plain")); got != connect.CodePermissionDenied {
			t.Fatalf("plain-user code = %v, want PermissionDenied", got)
		}
	})
}
