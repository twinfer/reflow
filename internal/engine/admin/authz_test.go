package admin

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/twinfer/reflow/proto/adminv1"
)

func TestBuildMethodPolicy_InheritsServiceDefault(t *testing.T) {
	svc := adminv1.File_adminv1_admin_proto.Services().ByName("Admin")
	if svc == nil {
		t.Fatal("Admin service descriptor missing")
	}
	policy, err := BuildMethodPolicy(svc)
	if err != nil {
		t.Fatalf("BuildMethodPolicy: %v", err)
	}
	if got := len(policy); got != 6 {
		t.Errorf("policy entries = %d; want 6", got)
	}
	wantPrefix := "/reflow.admin.v1.Admin/"
	for fullMethod, role := range policy {
		if !strings.HasPrefix(fullMethod, wantPrefix) {
			t.Errorf("method %q does not match service prefix", fullMethod)
		}
		if role != "operator" {
			t.Errorf("method %q got role %q; want operator", fullMethod, role)
		}
	}
}

func TestBuildMethodPolicy_NilDescriptor(t *testing.T) {
	if _, err := BuildMethodPolicy(nil); err == nil {
		t.Fatal("expected error for nil descriptor")
	}
}

func TestAdminMethodPolicy_PopulatesEveryMethod(t *testing.T) {
	policy, err := AdminMethodPolicy()
	if err != nil {
		t.Fatalf("AdminMethodPolicy: %v", err)
	}
	wantMethods := []string{
		"/reflow.admin.v1.Admin/AddNode",
		"/reflow.admin.v1.Admin/RemoveNode",
		"/reflow.admin.v1.Admin/ListNodes",
		"/reflow.admin.v1.Admin/ListPartitions",
		"/reflow.admin.v1.Admin/CreateSnapshot",
		"/reflow.admin.v1.Admin/ListSnapshots",
	}
	for _, m := range wantMethods {
		if role, ok := policy[m]; !ok {
			t.Errorf("policy missing entry for %s", m)
		} else if role != "operator" {
			t.Errorf("policy[%s] = %q; want operator", m, role)
		}
	}
}

// ctxWithIdentity stashes a PeerIdentity on a fresh context using the
// same key AuditInterceptor would.
func ctxWithIdentity(kind, name string) context.Context {
	u := &url.URL{Scheme: "spiffe", Host: "reflow.local", Path: "/" + kind + "/" + name}
	id := PeerIdentity{Kind: kind, Name: name, URI: u}
	return context.WithValue(context.Background(), peerIdentityCtxKey{}, id)
}

func callerInvoked(invoked *bool) grpc.UnaryHandler {
	return func(_ context.Context, _ any) (any, error) {
		*invoked = true
		return "ok", nil
	}
}

func TestAuthzInterceptor_AllowsMatch(t *testing.T) {
	policy := map[string]string{"/svc/Method": "operator"}
	ic := AuthzInterceptor(policy, nil)
	ctx := ctxWithIdentity("operator", "alice")
	var called bool
	resp, err := ic(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/svc/Method"}, callerInvoked(&called))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !called {
		t.Fatal("handler was not invoked")
	}
	if resp != "ok" {
		t.Errorf("resp = %v; want ok", resp)
	}
}

func TestAuthzInterceptor_RejectsMismatch(t *testing.T) {
	policy := map[string]string{"/svc/Method": "operator"}
	ic := AuthzInterceptor(policy, nil)
	ctx := ctxWithIdentity("node", "3")
	var called bool
	_, err := ic(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/svc/Method"}, callerInvoked(&called))
	if err == nil {
		t.Fatal("expected PermissionDenied")
	}
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Errorf("status code = %v; want PermissionDenied", got)
	}
	if called {
		t.Error("handler should not have been invoked on mismatch")
	}
}

func TestAuthzInterceptor_RejectsMissingIdentity(t *testing.T) {
	policy := map[string]string{"/svc/Method": "operator"}
	ic := AuthzInterceptor(policy, nil)
	var called bool
	_, err := ic(context.Background(), "req", &grpc.UnaryServerInfo{FullMethod: "/svc/Method"}, callerInvoked(&called))
	if err == nil {
		t.Fatal("expected Unauthenticated")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Errorf("status code = %v; want Unauthenticated", got)
	}
	if called {
		t.Error("handler should not have been invoked")
	}
}

func TestAuthzInterceptor_RejectsUnknownMethod(t *testing.T) {
	policy := map[string]string{"/svc/Known": "operator"}
	ic := AuthzInterceptor(policy, nil)
	ctx := ctxWithIdentity("operator", "alice")
	var called bool
	_, err := ic(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/svc/Unknown"}, callerInvoked(&called))
	if err == nil {
		t.Fatal("expected Internal error for unknown method")
	}
	if got := status.Code(err); got != codes.Internal {
		t.Errorf("status code = %v; want Internal", got)
	}
	if called {
		t.Error("handler should not have been invoked")
	}
}

func TestAuthzInterceptor_HonoursCustomMatcher(t *testing.T) {
	policy := map[string]string{"/svc/Method": "operator"}
	// Custom matcher that only accepts node identities — inverts the
	// usual semantics to prove the hook is consulted.
	match := func(actual PeerIdentity, _ string) bool { return actual.Kind == "node" }
	ic := AuthzInterceptor(policy, match)

	var called bool
	_, err := ic(ctxWithIdentity("node", "3"), "req",
		&grpc.UnaryServerInfo{FullMethod: "/svc/Method"}, callerInvoked(&called))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !called {
		t.Fatal("handler should have been invoked under custom matcher")
	}

	called = false
	_, err = ic(ctxWithIdentity("operator", "alice"), "req",
		&grpc.UnaryServerInfo{FullMethod: "/svc/Method"}, callerInvoked(&called))
	if err == nil {
		t.Fatal("expected custom matcher to reject operator")
	}
	if called {
		t.Error("handler should not have been invoked")
	}
}

// guard against accidental loss of the errors-package wrapping when
// the interceptor's status errors get refactored.
var _ = errors.New
