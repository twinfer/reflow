package auth

import (
	"context"
	"strings"
	"testing"

	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
)

func TestBuildMethodPolicy_InheritsAdminServiceDefault(t *testing.T) {
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
	for fullMethod, role := range policy {
		if !strings.HasPrefix(fullMethod, "/reflow.admin.v1.Admin/") {
			t.Errorf("method %q does not match Admin prefix", fullMethod)
		}
		if role != "operator" {
			t.Errorf("method %q got role %q; want operator", fullMethod, role)
		}
	}
}

func TestBuildMethodPolicy_InheritsDeliveryServiceDefault(t *testing.T) {
	svc := deliveryv1.File_deliveryv1_delivery_proto.Services().ByName("Delivery")
	if svc == nil {
		t.Fatal("Delivery service descriptor missing")
	}
	policy, err := BuildMethodPolicy(svc)
	if err != nil {
		t.Fatalf("BuildMethodPolicy: %v", err)
	}
	want := "/reflow.delivery.v1.Delivery/Deliver"
	role, ok := policy[want]
	if !ok {
		t.Fatalf("policy missing entry for %s; have %+v", want, policy)
	}
	if role != "node" {
		t.Errorf("policy[%s] = %q; want node", want, role)
	}
}

func TestBuildMethodPolicy_NilDescriptor(t *testing.T) {
	if _, err := BuildMethodPolicy(nil); err == nil {
		t.Fatal("expected error for nil descriptor")
	}
}

func TestProtoPolicyAuthorizer_AllowsMatchingKind(t *testing.T) {
	a := NewProtoPolicyAuthorizer(map[string]string{"/svc/M": "operator"})
	claims := &Claims{Kind: "operator", Subject: "alice"}
	res, err := a.Authorize(context.Background(), claims, &CallTarget{APIName: "/svc/M"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != DecisionAllow {
		t.Errorf("decision = %v; want Allow", res.Decision)
	}
}

func TestProtoPolicyAuthorizer_DeniesMismatch(t *testing.T) {
	a := NewProtoPolicyAuthorizer(map[string]string{"/svc/M": "operator"})
	claims := &Claims{Kind: "node", Subject: "3"}
	res, err := a.Authorize(context.Background(), claims, &CallTarget{APIName: "/svc/M"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != DecisionDeny {
		t.Errorf("decision = %v; want Deny", res.Decision)
	}
	if !strings.Contains(res.Reason, "operator") || !strings.Contains(res.Reason, "node") {
		t.Errorf("reason %q should mention required + caller roles", res.Reason)
	}
}

func TestProtoPolicyAuthorizer_DeniesUnknownMethod(t *testing.T) {
	a := NewProtoPolicyAuthorizer(map[string]string{"/svc/Known": "operator"})
	claims := &Claims{Kind: "operator", Subject: "alice"}
	res, err := a.Authorize(context.Background(), claims, &CallTarget{APIName: "/svc/Unknown"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != DecisionDeny {
		t.Errorf("decision = %v; want Deny", res.Decision)
	}
	if !strings.Contains(res.Reason, "no policy") {
		t.Errorf("reason %q should mention missing policy", res.Reason)
	}
}

func TestProtoPolicyAuthorizer_DeniesNilClaims(t *testing.T) {
	a := NewProtoPolicyAuthorizer(map[string]string{"/svc/M": "operator"})
	res, err := a.Authorize(context.Background(), nil, &CallTarget{APIName: "/svc/M"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != DecisionDeny {
		t.Errorf("decision = %v; want Deny", res.Decision)
	}
}

func TestProtoPolicyAuthorizer_HonoursCustomMatcher(t *testing.T) {
	// Invert the semantics: only "node" callers are allowed.
	a := NewProtoPolicyAuthorizer(map[string]string{"/svc/M": "operator"}).
		WithMatcher(func(c *Claims, _ string) bool { return c != nil && c.Kind == "node" })

	res, err := a.Authorize(context.Background(), &Claims{Kind: "node"},
		&CallTarget{APIName: "/svc/M"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != DecisionAllow {
		t.Errorf("custom matcher should have allowed node; got %v", res.Decision)
	}

	res, err = a.Authorize(context.Background(), &Claims{Kind: "operator"},
		&CallTarget{APIName: "/svc/M"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != DecisionDeny {
		t.Errorf("custom matcher should have denied operator; got %v", res.Decision)
	}
}
