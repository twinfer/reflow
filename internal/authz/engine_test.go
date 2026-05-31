package authz

import (
	"testing"

	"github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"

	"github.com/twinfer/reflow/internal/auth"
)

// Procedure-path keys into procMap (what the interceptor receives). The Cedar
// action id is the bare method name (procmap.go); evalReq and ic.authorize both
// resolve these through actionEntity.
const (
	actRegisterDeployment = "/reflow.config.v1.Config/RegisterDeployment"
	actAddNode            = "/reflow.clusterctl.v1.ClusterCtl/AddNode"
	actSelfJoin           = "/reflow.clusterctl.v1.ClusterCtl/SelfJoin"
	actDeliver            = "/reflow.delivery.v1.Delivery/Deliver"
	actUploadSST          = "/reflow.delivery.v1.Delivery/UploadLPTransferSST"
	actSubmitInvocation   = "/reflow.ingress.v1.Ingress/SubmitInvocation"
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
// it. The action (and its plane-group parents) is resolved from the procedure
// through actionEntity, exactly as the interceptor does. Every principal maps
// to a typed entity (anonymous -> Anonymous).
func evalReq(e *Engine, procedure string, p auth.Principal, resType cedar.EntityType, resID string, resAttrs types.RecordMap) cedar.Decision {
	pUID, pEnt := PrincipalEntity(p)
	aUID, aEnt, ok := actionEntity(procedure)
	if !ok {
		return cedar.Deny // unmapped procedure: interceptor default-denies
	}
	if resAttrs == nil {
		resAttrs = types.RecordMap{}
	}
	rUID := cedar.NewEntityUID(resType, cedar.String(resID))
	em := types.EntityMap{
		pUID: pEnt,
		aUID: aEnt,
		rUID: types.Entity{UID: rUID, Attributes: types.NewRecord(resAttrs)},
	}
	dec, _ := e.Authorize(cedar.Request{
		Principal: pUID,
		Action:    aUID,
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
// plane separation through the Cedar engine: operators full access, nodes
// restricted to mesh RPCs, ingress open to all, config/clusterctl denied to
// non-operators. Survives the PR2 cutover unchanged.
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
		{"operator-config", operator, actRegisterDeployment, TypeDeploymentRecord, "kafka", cedar.Allow},
		{"operator-addnode", operator, actAddNode, TypePlatformConfig, "cluster", cedar.Allow},
		{"operator-submit", operator, actSubmitInvocation, TypeInvocation, "svc", cedar.Allow},
		{"node-deliver", node, actDeliver, TypePlatformConfig, "cluster", cedar.Allow},
		{"node-upload-sst", node, actUploadSST, TypePlatformConfig, "cluster", cedar.Allow},
		{"node-selfjoin", node, actSelfJoin, TypePlatformConfig, "cluster", cedar.Allow},
		{"node-config-denied", node, actRegisterDeployment, TypeDeploymentRecord, "kafka", cedar.Deny},
		{"node-addnode-denied", node, actAddNode, TypePlatformConfig, "cluster", cedar.Deny},
		{"anon-submit-open", anon, actSubmitInvocation, TypeInvocation, "svc", cedar.Allow},
		{"anon-config-denied", anon, actRegisterDeployment, TypeDeploymentRecord, "kafka", cedar.Deny},
		{"anon-addnode-denied", anon, actAddNode, TypePlatformConfig, "cluster", cedar.Deny},
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
// mapping for each kind, including the always-typed anonymous and user cases.
func TestPrincipalEntity_Mapping(t *testing.T) {
	op, _ := PrincipalEntity(auth.Principal{Kind: "operator", Subject: "alice"})
	if op.Type != TypeClusterOperator || op.ID != "alice" {
		t.Errorf("operator: uid=%v", op)
	}

	nodeUID, nodeEnt := PrincipalEntity(auth.Principal{Kind: "node", Subject: "7"})
	if nodeUID.Type != TypeNode {
		t.Fatalf("node: uid=%v", nodeUID)
	}
	if v, _ := nodeEnt.Attributes.Get("node_id"); v != types.Long(7) {
		t.Errorf("node_id = %v want 7", v)
	}

	tUID, tEnt := PrincipalEntity(auth.Principal{Kind: "tenant", Subject: "12/bob"})
	if tUID.Type != TypeTenantAdmin {
		t.Fatalf("tenant: uid=%v", tUID)
	}
	if v, _ := tEnt.Attributes.Get("tenant_id"); v != types.Long(12) {
		t.Errorf("tenant_id = %v want 12", v)
	}

	if uUID, _ := PrincipalEntity(auth.Principal{Kind: "user", Subject: "x"}); uUID.Type != TypeUser {
		t.Errorf("user: uid=%v want type User", uUID)
	}
	if aUID, _ := PrincipalEntity(auth.Principal{}); aUID.Type != TypeAnonymous {
		t.Errorf("anonymous: uid=%v want type Anonymous", aUID)
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
func TestTenantIsolation_ValidatesAndEnforces(t *testing.T) {
	tenantPolicy := `
permit (
    principal is TenantAdmin,
    action == Action::"RegisterDeployment",
    resource
) when { resource.tenant_id == principal.tenant_id && principal.tenant_id > 0 };
`
	e := mustEngine(t, FoundationalClusterPolicies+tenantPolicy)
	tenant12 := auth.Principal{Kind: "tenant", Subject: "12/alice"}

	if got := evalReq(e, actRegisterDeployment, tenant12, TypeDeploymentRecord, "kafka",
		types.RecordMap{"tenant_id": types.Long(12), "name": types.String("kafka")}); got != cedar.Allow {
		t.Errorf("same-tenant: got %v want Allow", got)
	}
	if got := evalReq(e, actRegisterDeployment, tenant12, TypeDeploymentRecord, "kafka",
		types.RecordMap{"tenant_id": types.Long(99), "name": types.String("kafka")}); got != cedar.Deny {
		t.Errorf("cross-tenant: got %v want Deny", got)
	}
}
