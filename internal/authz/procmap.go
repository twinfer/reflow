package authz

import (
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
	groupIngress         = "IngressActions"
	groupTenantConfig    = "TenantConfigActions"
	groupConfigRead      = "ConfigReadActions"
	groupPlatformConfig  = "PlatformConfigActions"
	groupTenantLifecycle = "TenantLifecycleActions"
	groupClusterAdmin    = "ClusterAdminActions"
	groupMesh            = "MeshActions"
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

	// ----- Delivery + SelfJoin: node mesh -----
	deliveryv1connect.DeliveryDeliverProcedure:             {"Deliver", []string{groupMesh}},
	deliveryv1connect.DeliveryUploadLPTransferSSTProcedure: {"UploadLPTransferSST", []string{groupMesh}},
	clusterctlv1connect.ClusterCtlSelfJoinProcedure:        {"SelfJoin", []string{groupMesh}},

	// ----- Config: tenant-config writes -----
	configv1connect.ConfigUpsertWebhookSourceProcedure: {"UpsertWebhookSource", []string{groupTenantConfig}},
	configv1connect.ConfigDeleteWebhookSourceProcedure: {"DeleteWebhookSource", []string{groupTenantConfig}},
	configv1connect.ConfigUpsertSecretProcedure:        {"UpsertSecret", []string{groupTenantConfig}},
	configv1connect.ConfigDeleteSecretProcedure:        {"DeleteSecret", []string{groupTenantConfig}},
	configv1connect.ConfigRegisterDeploymentProcedure:  {"RegisterDeployment", []string{groupTenantConfig}},
	configv1connect.ConfigDeleteDeploymentProcedure:    {"DeleteDeployment", []string{groupTenantConfig}},

	// ----- Config: tenant-config reads -----
	configv1connect.ConfigListDeploymentsProcedure:    {"ListDeployments", []string{groupConfigRead}},
	configv1connect.ConfigDescribeDeploymentProcedure: {"DescribeDeployment", []string{groupConfigRead}},
	configv1connect.ConfigListWebhookSourcesProcedure: {"ListWebhookSources", []string{groupConfigRead}},
	configv1connect.ConfigListSecretsProcedure:        {"ListSecrets", []string{groupConfigRead}},

	// ----- Config: platform plane (operator only) -----
	configv1connect.ConfigListAuditLogProcedure:             {"ListAuditLog", []string{groupPlatformConfig}},
	configv1connect.ConfigUpsertCARootProcedure:             {"UpsertCARoot", []string{groupPlatformConfig}},
	configv1connect.ConfigDeleteCARootProcedure:             {"DeleteCARoot", []string{groupPlatformConfig}},
	configv1connect.ConfigListCARootsProcedure:              {"ListCARoots", []string{groupPlatformConfig}},
	configv1connect.ConfigCreateJoinTokenProcedure:          {"CreateJoinToken", []string{groupPlatformConfig}},
	configv1connect.ConfigDeleteJoinTokenProcedure:          {"DeleteJoinToken", []string{groupPlatformConfig}},
	configv1connect.ConfigListJoinTokensProcedure:           {"ListJoinTokens", []string{groupPlatformConfig}},
	configv1connect.ConfigIssueOperatorProcedure:            {"IssueOperator", []string{groupPlatformConfig}},
	configv1connect.ConfigUpsertClusterAuthzPolicyProcedure: {"UpsertClusterAuthzPolicy", []string{groupPlatformConfig}},
	configv1connect.ConfigGetClusterAuthzPolicyProcedure:    {"GetClusterAuthzPolicy", []string{groupPlatformConfig}},

	// ----- ClusterCtl: tenant-lifecycle (operator only) -----
	clusterctlv1connect.ClusterCtlUpsertTenantProcedure:    {"UpsertTenant", []string{groupTenantLifecycle}},
	clusterctlv1connect.ClusterCtlDeleteTenantProcedure:    {"DeleteTenant", []string{groupTenantLifecycle}},
	clusterctlv1connect.ClusterCtlListTenantsProcedure:     {"ListTenants", []string{groupTenantLifecycle}},
	clusterctlv1connect.ClusterCtlDescribeTenantProcedure:  {"DescribeTenant", []string{groupTenantLifecycle}},
	clusterctlv1connect.ClusterCtlUpsertTenantDEKProcedure: {"UpsertTenantDEK", []string{groupTenantLifecycle}},
	clusterctlv1connect.ClusterCtlDeleteTenantDEKProcedure: {"DeleteTenantDEK", []string{groupTenantLifecycle}},
	clusterctlv1connect.ClusterCtlListTenantDEKsProcedure:  {"ListTenantDEKs", []string{groupTenantLifecycle}},

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
