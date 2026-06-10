package authz

import (
	"context"
	"testing"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/auth"
)

// Admin procedures spanning the planes a browser admin must / must not reach.
// actRegisterDeployment (AppConfig write) and actAddNode (ClusterAdmin) are
// declared in engine_test.go.
const (
	procListDeployments   = "/reflw.admin.v1.Admin/ListDeployments"          // ConfigRead
	procUpsertAuthzPolicy = "/reflw.admin.v1.Admin/UpsertClusterAuthzPolicy" // Platform
)

// TestFoundationalPolicy_BrowserAdminGroups proves the groups-gated grant in
// the foundational policy: an OIDC User in "reflw-admins" may read and write app
// config but never reaches the cluster-admin or platform planes; a User without
// the group is confined to the open ingress plane. Exercising ic.authorize runs
// the real path — PrincipalEntity (which stamps the groups set), actionEntity,
// resource selection, and Cedar evaluation together.
func TestFoundationalPolicy_BrowserAdminGroups(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	ic := NewInterceptor(e, nil, true)

	ctx := func(p auth.Principal) context.Context {
		return auth.ContextWithPrincipal(context.Background(), p)
	}
	admin := auth.Principal{Kind: "user", Subject: "alice", Raw: "user/alice", Groups: []string{"reflw-admins"}}
	plain := auth.Principal{Kind: "user", Subject: "bob", Raw: "user/bob"}
	operator := auth.Principal{Kind: "operator", Subject: "root", Raw: "operator/root"}

	cases := []struct {
		name      string
		ctx       context.Context
		procedure string
		wantCode  connect.Code // 0 => allowed (nil error)
	}{
		// Browser admin (in reflw-admins): app config yes, cluster/platform no.
		{"admin-configread-allow", ctx(admin), procListDeployments, 0},
		{"admin-appconfig-allow", ctx(admin), actRegisterDeployment, 0},
		{"admin-ingress-allow", ctx(admin), actAwaitInvocation, 0},
		{"admin-clusteradmin-denied", ctx(admin), actAddNode, connect.CodePermissionDenied},
		{"admin-platform-denied", ctx(admin), procUpsertAuthzPolicy, connect.CodePermissionDenied},

		// Plain user (no groups): only the open ingress plane.
		{"plain-ingress-allow", ctx(plain), actAwaitInvocation, 0},
		{"plain-configread-denied", ctx(plain), procListDeployments, connect.CodePermissionDenied},
		{"plain-appconfig-denied", ctx(plain), actRegisterDeployment, connect.CodePermissionDenied},

		// Operator keeps full access regardless of groups.
		{"operator-clusteradmin-allow", ctx(operator), actAddNode, 0},
		{"operator-platform-allow", ctx(operator), procUpsertAuthzPolicy, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ic.authorize(tc.ctx, tc.procedure)
			if tc.wantCode == 0 {
				if err != nil {
					t.Fatalf("authorize(%s) = %v, want allow", tc.procedure, err)
				}
				return
			}
			if got := connect.CodeOf(err); got != tc.wantCode {
				t.Fatalf("authorize(%s) code = %v, want %v (err %v)", tc.procedure, got, tc.wantCode, err)
			}
		})
	}
}
