package authz

import (
	"slices"

	"github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"

	clusterctlv1connect "github.com/twinfer/reflow/proto/clusterctlv1/clusterctlv1connect"
	configv1connect "github.com/twinfer/reflow/proto/configv1/configv1connect"
	deliveryv1connect "github.com/twinfer/reflow/proto/deliveryv1/deliveryv1connect"
	ingressv1connect "github.com/twinfer/reflow/proto/ingressv1/ingressv1connect"
)

// Cedar action-group ids — the administrative planes of SAD §6.15.1. A policy
// matches a whole plane with `action in [Action::"<group>"]`. They are
// declared in schema.cedar for the upload-time validator; at evaluation
// cedar.Authorize ignores the schema, so actionEntity stamps them as the
// action's parent edges.
const (
	groupIngress        = "IngressActions"
	groupTenantConfig   = "TenantConfigActions"
	groupConfigRead     = "ConfigReadActions"
	groupPlatformConfig = "PlatformConfigActions"
	groupClusterAdmin   = "ClusterAdminActions"
	groupMesh           = "MeshActions"
)

// actionType is Cedar's reserved entity type for actions (`Action::"…"`).
const actionType cedar.EntityType = "Action"

// procEntry is one authorized procedure's Cedar action: the short, writable
// action id (the bare RPC method name) and the plane group(s) it belongs to.
type procEntry struct {
	action string
	groups []string
}

// procMap maps every Connect procedure behind the authz interceptor to its
// Cedar action. The in-scope services are ingress, config, clusterctl, and
// delivery; bootstrap.MeshSign, discovery, and handler.HandlerService sit on
// separate listeners and never reach this interceptor. An unmapped procedure
// is default-denied (interceptor.authorize). procmap_test enforces full
// coverage of those four services + action-id uniqueness, so a newly-added
// RPC fails tests until it is classified into a plane — no silent authz gap.
var procMap = map[string]procEntry{
	// ----- Ingress: data plane -----
	ingressv1connect.IngressSubmitInvocationProcedure:       {"SubmitInvocation", []string{groupIngress}},
	ingressv1connect.IngressAwaitInvocationProcedure:        {"AwaitInvocation", []string{groupIngress}},
	ingressv1connect.IngressAttachInvocationProcedure:       {"AttachInvocation", []string{groupIngress}},
	ingressv1connect.IngressGetInvocationOutputProcedure:    {"GetInvocationOutput", []string{groupIngress}},
	ingressv1connect.IngressDescribeInvocationProcedure:     {"DescribeInvocation", []string{groupIngress}},
	ingressv1connect.IngressCancelInvocationProcedure:       {"CancelInvocation", []string{groupIngress}},
	ingressv1connect.IngressResolveAwakeableProcedure:       {"ResolveAwakeable", []string{groupIngress}},
	ingressv1connect.IngressResolveWorkflowPromiseProcedure: {"ResolveWorkflowPromise", []string{groupIngress}},
	ingressv1connect.IngressGetObjectStateProcedure:         {"GetObjectState", []string{groupIngress}},
	ingressv1connect.IngressStartProcessProcedure:           {"StartProcess", []string{groupIngress}},
	ingressv1connect.IngressDeliverMessageProcedure:         {"DeliverMessage", []string{groupIngress}},
	ingressv1connect.IngressGetProcessInstanceProcedure:     {"GetProcessInstance", []string{groupIngress}},
	ingressv1connect.IngressListProcessInstancesProcedure:   {"ListProcessInstances", []string{groupIngress}},

	// ----- Ingress: operator-only maintenance (no open plane) -----
	// PurgeInvocation permanently deletes a Completed invocation's durable
	// rows. It rides the ingress listener (it needs that server's partition
	// routing) but is deliberately NOT in IngressActions — the foundational
	// policy opens that plane to anonymous, and a destructive purge must
	// not be. With no group it's reachable only by the operator god-mode
	// rule; node/anonymous principals are default-denied.
	ingressv1connect.IngressPurgeInvocationProcedure: {"PurgeInvocation", nil},

	// ----- Delivery + SelfJoin: node mesh -----
	deliveryv1connect.DeliveryDeliverProcedure:             {"Deliver", []string{groupMesh}},
	deliveryv1connect.DeliveryUploadLPTransferSSTProcedure: {"UploadLPTransferSST", []string{groupMesh}},
	clusterctlv1connect.ClusterCtlSelfJoinProcedure:        {"SelfJoin", []string{groupMesh}},

	// ----- Config: tenant-config writes -----
	configv1connect.ConfigUpsertSecretProcedure:       {"UpsertSecret", []string{groupTenantConfig}},
	configv1connect.ConfigDeleteSecretProcedure:       {"DeleteSecret", []string{groupTenantConfig}},
	configv1connect.ConfigRegisterDeploymentProcedure: {"RegisterDeployment", []string{groupTenantConfig}},
	configv1connect.ConfigDeleteDeploymentProcedure:   {"DeleteDeployment", []string{groupTenantConfig}},

	// ----- Config: tenant-config reads -----
	configv1connect.ConfigListDeploymentsProcedure:    {"ListDeployments", []string{groupConfigRead}},
	configv1connect.ConfigDescribeDeploymentProcedure: {"DescribeDeployment", []string{groupConfigRead}},
	configv1connect.ConfigListSecretsProcedure:        {"ListSecrets", []string{groupConfigRead}},

	// ----- Config: platform plane (operator only) -----
	configv1connect.ConfigUpsertCARootProcedure:             {"UpsertCARoot", []string{groupPlatformConfig}},
	configv1connect.ConfigDeleteCARootProcedure:             {"DeleteCARoot", []string{groupPlatformConfig}},
	configv1connect.ConfigListCARootsProcedure:              {"ListCARoots", []string{groupPlatformConfig}},
	configv1connect.ConfigCreateJoinTokenProcedure:          {"CreateJoinToken", []string{groupPlatformConfig}},
	configv1connect.ConfigDeleteJoinTokenProcedure:          {"DeleteJoinToken", []string{groupPlatformConfig}},
	configv1connect.ConfigListJoinTokensProcedure:           {"ListJoinTokens", []string{groupPlatformConfig}},
	configv1connect.ConfigIssueOperatorProcedure:            {"IssueOperator", []string{groupPlatformConfig}},
	configv1connect.ConfigIssueTenantProcedure:              {"IssueTenant", []string{groupPlatformConfig}},
	configv1connect.ConfigUpsertClusterAuthzPolicyProcedure: {"UpsertClusterAuthzPolicy", []string{groupPlatformConfig}},
	configv1connect.ConfigGetClusterAuthzPolicyProcedure:    {"GetClusterAuthzPolicy", []string{groupPlatformConfig}},

	// ----- ClusterCtl: cluster admin (operator only) -----
	clusterctlv1connect.ClusterCtlAddNodeProcedure:         {"AddNode", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlRemoveNodeProcedure:      {"RemoveNode", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlListNodesProcedure:       {"ListNodes", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlListPartitionsProcedure:  {"ListPartitions", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlNodeLeadershipProcedure:  {"NodeLeadership", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlCreateSnapshotProcedure:  {"CreateSnapshot", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlListSnapshotsProcedure:   {"ListSnapshots", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlDeleteSnapshotProcedure:  {"DeleteSnapshot", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlTransferLPProcedure:      {"TransferLP", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlListLPTransfersProcedure: {"ListLPTransfers", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlRebalanceAdviseProcedure: {"RebalanceAdvise", []string{groupClusterAdmin}},
	clusterctlv1connect.ClusterCtlRebalanceDrainProcedure:  {"RebalanceDrain", []string{groupClusterAdmin}},
}

// isIngressProcedure reports whether procedure is in the ingress data plane —
// the procedures whose resource is a tenant-scoped Invocation rather than the
// PlatformConfig sentinel. Drives the interceptor's per-request resource build.
func isIngressProcedure(procedure string) bool {
	e, ok := procMap[procedure]
	if !ok {
		return false
	}
	return slices.Contains(e.groups, groupIngress)
}

// actionEntity returns the Cedar action UID for a procedure plus its entity
// carrying the plane-group parent edges, so `action in [Action::"<group>"]`
// resolves against the entity map (cedar.Authorize never consults the schema).
// ok is false for an unmapped procedure — the interceptor default-denies those.
func actionEntity(procedure string) (cedar.EntityUID, types.Entity, bool) {
	e, ok := procMap[procedure]
	if !ok {
		return cedar.EntityUID{}, types.Entity{}, false
	}
	uid := cedar.NewEntityUID(actionType, cedar.String(e.action))
	parents := make([]cedar.EntityUID, len(e.groups))
	for i, g := range e.groups {
		parents[i] = cedar.NewEntityUID(actionType, cedar.String(g))
	}
	return uid, types.Entity{UID: uid, Parents: types.NewEntityUIDSet(parents...)}, true
}
