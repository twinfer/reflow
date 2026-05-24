package authz

import (
	"testing"

	"github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"

	"github.com/twinfer/reflow/internal/auth"
)

func mustEngine(t *testing.T, policies string) *Engine {
	t.Helper()
	e, err := NewEngine([]byte(policies))
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

// evalReq builds a single-principal, single-resource request and authorizes
// it. Anonymous principals (PrincipalEntity ok=false) get a sentinel UID of a
// type absent from the schema, so they match no `principal is X` head.
func evalReq(e *Engine, action string, p auth.Principal, resType cedar.EntityType, resID string, resAttrs types.RecordMap) cedar.Decision {
	em := types.EntityMap{}
	pUID, pEnt, ok := PrincipalEntity(p)
	if ok {
		em[pUID] = pEnt
	} else {
		pUID = cedar.NewEntityUID("Anonymous", "anonymous")
	}
	if resAttrs == nil {
		resAttrs = types.RecordMap{}
	}
	rUID := cedar.NewEntityUID(resType, cedar.String(resID))
	em[rUID] = types.Entity{UID: rUID, Attributes: types.NewRecord(resAttrs)}
	dec, _ := e.Authorize(cedar.Request{
		Principal: pUID,
		Action:    cedar.NewEntityUID("Action", cedar.String(action)),
		Resource:  rUID,
	}, em)
	return dec
}

// TestNewEngine_FoundationalPoliciesValidate proves the embedded schema
// resolves and the foundational policies pass layer-1 schema validation.
func TestNewEngine_FoundationalPoliciesValidate(t *testing.T) {
	mustEngine(t, FoundationalClusterPolicies)
}

// TestAuthorize_PlaneSeparation is the golden fixture anchoring the current
// (pre-Cedar) plane separation: operators full access, nodes restricted to
// inter-node mesh RPCs, everyone else denied. Survives the PR2 cutover.
func TestAuthorize_PlaneSeparation(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	operator := auth.Principal{Kind: "operator", Subject: "alice", Raw: "operator/alice"}
	node := auth.Principal{Kind: "node", Subject: "3", Raw: "node/3"}
	anon := auth.Principal{}

	cases := []struct {
		name    string
		p       auth.Principal
		action  string
		resType cedar.EntityType
		resID   string
		want    cedar.Decision
	}{
		{"operator-config", operator, "UpsertEventSource", TypeEventSourceRecord, "kafka", cedar.Allow},
		{"operator-addnode", operator, "AddNode", TypePlatformConfig, "cluster", cedar.Allow},
		{"operator-submit", operator, "SubmitInvocation", TypeInvocation, "svc", cedar.Allow},
		{"node-delivery", node, "DeliveryDeliver", TypePlatformConfig, "cluster", cedar.Allow},
		{"node-list-undelivered", node, "DeliveryListUndelivered", TypePlatformConfig, "cluster", cedar.Allow},
		{"node-selfjoin", node, "SelfJoin", TypePlatformConfig, "cluster", cedar.Allow},
		{"node-config-denied", node, "UpsertEventSource", TypeEventSourceRecord, "kafka", cedar.Deny},
		{"node-addnode-denied", node, "AddNode", TypePlatformConfig, "cluster", cedar.Deny},
		{"anon-config-denied", anon, "UpsertEventSource", TypeEventSourceRecord, "kafka", cedar.Deny},
		{"anon-delivery-denied", anon, "DeliveryDeliver", TypePlatformConfig, "cluster", cedar.Deny},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := evalReq(e, c.action, c.p, c.resType, c.resID, nil); got != c.want {
				t.Errorf("action=%s principal=%q: got %v want %v", c.action, c.p.Raw, got, c.want)
			}
		})
	}
}

// TestPrincipalEntity_Mapping checks the auth.Principal -> Cedar UID + attrs
// mapping for each represented kind, and that anonymous/user map to ok=false.
func TestPrincipalEntity_Mapping(t *testing.T) {
	op, _, ok := PrincipalEntity(auth.Principal{Kind: "operator", Subject: "alice"})
	if !ok || op.Type != TypeClusterOperator || op.ID != "alice" {
		t.Errorf("operator: uid=%v ok=%v", op, ok)
	}

	nodeUID, nodeEnt, ok := PrincipalEntity(auth.Principal{Kind: "node", Subject: "7"})
	if !ok || nodeUID.Type != TypeNode {
		t.Fatalf("node: uid=%v ok=%v", nodeUID, ok)
	}
	if v, _ := nodeEnt.Attributes.Get("node_id"); v != types.Long(7) {
		t.Errorf("node_id = %v want 7", v)
	}

	tUID, tEnt, ok := PrincipalEntity(auth.Principal{Kind: "tenant", Subject: "12/bob"})
	if !ok || tUID.Type != TypeTenantAdmin {
		t.Fatalf("tenant: uid=%v ok=%v", tUID, ok)
	}
	if v, _ := tEnt.Attributes.Get("tenant_id"); v != types.Long(12) {
		t.Errorf("tenant_id = %v want 12", v)
	}

	if _, _, ok := PrincipalEntity(auth.Principal{}); ok {
		t.Error("anonymous should map to ok=false")
	}
	if _, _, ok := PrincipalEntity(auth.Principal{Kind: "user", Subject: "x"}); ok {
		t.Error("user should map to ok=false (no User entity in schema)")
	}
}

// TestCompileAndValidate_RejectsAppliesToViolation proves layer-1 validation
// rejects a policy that violates the schema's appliesTo: TenantAdmin cannot
// be a principal for AddNode (operator-only). Caught at compile, not eval.
func TestCompileAndValidate_RejectsAppliesToViolation(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	bad := `permit (principal is TenantAdmin, action == Action::"AddNode", resource);`
	if _, err := e.CompileAndValidate([]byte(bad)); err == nil {
		t.Fatal("expected schema validation to reject TenantAdmin on AddNode")
	}
}

// TestTenantIsolation_ValidatesAndEnforces proves the headline mechanism
// (the PR5 guarantee) works: a tenant-isolation policy validates against the
// schema and denies cross-tenant access while allowing same-tenant access.
// Uses an explicit action match (not the TenantConfigActions group) so the
// request needs no action-hierarchy entities.
func TestTenantIsolation_ValidatesAndEnforces(t *testing.T) {
	const tenantPolicy = `
permit (
    principal is TenantAdmin,
    action == Action::"UpsertEventSource",
    resource
) when { resource.tenant_id == principal.tenant_id && principal.tenant_id > 0 };
`
	e := mustEngine(t, FoundationalClusterPolicies+tenantPolicy)
	tenant12 := auth.Principal{Kind: "tenant", Subject: "12/alice"}

	if got := evalReq(e, "UpsertEventSource", tenant12, TypeEventSourceRecord, "kafka",
		types.RecordMap{"tenant_id": types.Long(12), "name": types.String("kafka")}); got != cedar.Allow {
		t.Errorf("same-tenant: got %v want Allow", got)
	}
	if got := evalReq(e, "UpsertEventSource", tenant12, TypeEventSourceRecord, "kafka",
		types.RecordMap{"tenant_id": types.Long(99), "name": types.String("kafka")}); got != cedar.Deny {
		t.Errorf("cross-tenant: got %v want Deny", got)
	}
}
