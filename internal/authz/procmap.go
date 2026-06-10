package authz

import (
	"slices"

	"github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"

	adminv1connect "github.com/twinfer/reflw/proto/adminv1/adminv1connect"
	deliveryv1connect "github.com/twinfer/reflw/proto/deliveryv1/deliveryv1connect"
	ingressv1connect "github.com/twinfer/reflw/proto/ingressv1/ingressv1connect"
)

// Cedar action-group ids — the administrative planes of SAD §6.15.1. A policy
// matches a whole plane with `action in [Action::"<group>"]`. They are
// declared in schema.cedar for the upload-time validator; at evaluation
// cedar.Authorize ignores the schema, so actionEntity stamps them as the
// action's parent edges.
const (
	groupIngress        = "IngressActions"
	groupAppConfig      = "AppConfigActions"
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
	ingressv1connect.IngressSubmitInvocationProcedure:          {"SubmitInvocation", []string{groupIngress}},
	ingressv1connect.IngressAwaitInvocationProcedure:           {"AwaitInvocation", []string{groupIngress}},
	ingressv1connect.IngressAttachInvocationProcedure:          {"AttachInvocation", []string{groupIngress}},
	ingressv1connect.IngressGetInvocationOutputProcedure:       {"GetInvocationOutput", []string{groupIngress}},
	ingressv1connect.IngressDescribeInvocationProcedure:        {"DescribeInvocation", []string{groupIngress}},
	ingressv1connect.IngressListInvocationsProcedure:           {"ListInvocations", []string{groupIngress}},
	ingressv1connect.IngressCancelInvocationProcedure:          {"CancelInvocation", []string{groupIngress}},
	ingressv1connect.IngressResolveAwakeableProcedure:          {"ResolveAwakeable", []string{groupIngress}},
	ingressv1connect.IngressResolveWorkflowPromiseProcedure:    {"ResolveWorkflowPromise", []string{groupIngress}},
	ingressv1connect.IngressGetObjectStateProcedure:            {"GetObjectState", []string{groupIngress}},
	ingressv1connect.IngressStartProcessProcedure:              {"StartProcess", []string{groupIngress}},
	ingressv1connect.IngressDeliverMessageProcedure:            {"DeliverMessage", []string{groupIngress}},
	ingressv1connect.IngressDeliverProcessEventProcedure:       {"DeliverProcessEvent", []string{groupIngress}},
	ingressv1connect.IngressGetProcessInstanceProcedure:        {"GetProcessInstance", []string{groupIngress}},
	ingressv1connect.IngressListProcessInstancesProcedure:      {"ListProcessInstances", []string{groupIngress}},
	ingressv1connect.IngressGetProcessInstanceHistoryProcedure: {"GetProcessInstanceHistory", []string{groupIngress}},
	ingressv1connect.IngressResolveProcessIncidentProcedure:    {"ResolveProcessIncident", []string{groupIngress}},
	ingressv1connect.IngressGetTaskProcedure:                   {"GetTask", []string{groupIngress}},
	ingressv1connect.IngressCompleteTaskProcedure:              {"CompleteTask", []string{groupIngress}},

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
	adminv1connect.AdminSelfJoinProcedure:                  {"SelfJoin", []string{groupMesh}},

	// ----- Admin: app-config writes -----
	adminv1connect.AdminUpsertSecretProcedure:       {"UpsertSecret", []string{groupAppConfig}},
	adminv1connect.AdminDeleteSecretProcedure:       {"DeleteSecret", []string{groupAppConfig}},
	adminv1connect.AdminRegisterDeploymentProcedure: {"RegisterDeployment", []string{groupAppConfig}},
	adminv1connect.AdminDeleteDeploymentProcedure:   {"DeleteDeployment", []string{groupAppConfig}},
	adminv1connect.AdminRegisterModelSetProcedure:   {"RegisterModelSet", []string{groupAppConfig}},
	adminv1connect.AdminDeleteModelProcedure:        {"DeleteModel", []string{groupAppConfig}},

	// ----- Admin: app-config reads -----
	adminv1connect.AdminListDeploymentsProcedure:    {"ListDeployments", []string{groupConfigRead}},
	adminv1connect.AdminDescribeDeploymentProcedure: {"DescribeDeployment", []string{groupConfigRead}},
	adminv1connect.AdminListSecretsProcedure:        {"ListSecrets", []string{groupConfigRead}},
	adminv1connect.AdminListModelsProcedure:         {"ListModels", []string{groupConfigRead}},
	adminv1connect.AdminDescribeModelProcedure:      {"DescribeModel", []string{groupConfigRead}},

	// ----- Admin: platform plane (operator only) -----
	adminv1connect.AdminUpsertClusterAuthzPolicyProcedure: {"UpsertClusterAuthzPolicy", []string{groupPlatformConfig}},
	adminv1connect.AdminGetClusterAuthzPolicyProcedure:    {"GetClusterAuthzPolicy", []string{groupPlatformConfig}},

	// ----- Admin: cluster admin (operator only) -----
	adminv1connect.AdminAddNodeProcedure:         {"AddNode", []string{groupClusterAdmin}},
	adminv1connect.AdminRemoveNodeProcedure:      {"RemoveNode", []string{groupClusterAdmin}},
	adminv1connect.AdminListNodesProcedure:       {"ListNodes", []string{groupClusterAdmin}},
	adminv1connect.AdminListPartitionsProcedure:  {"ListPartitions", []string{groupClusterAdmin}},
	adminv1connect.AdminNodeLeadershipProcedure:  {"NodeLeadership", []string{groupClusterAdmin}},
	adminv1connect.AdminCreateSnapshotProcedure:  {"CreateSnapshot", []string{groupClusterAdmin}},
	adminv1connect.AdminListSnapshotsProcedure:   {"ListSnapshots", []string{groupClusterAdmin}},
	adminv1connect.AdminDeleteSnapshotProcedure:  {"DeleteSnapshot", []string{groupClusterAdmin}},
	adminv1connect.AdminTransferLPProcedure:      {"TransferLP", []string{groupClusterAdmin}},
	adminv1connect.AdminListLPTransfersProcedure: {"ListLPTransfers", []string{groupClusterAdmin}},
	adminv1connect.AdminRebalanceAdviseProcedure: {"RebalanceAdvise", []string{groupClusterAdmin}},
	adminv1connect.AdminRebalanceDrainProcedure:  {"RebalanceDrain", []string{groupClusterAdmin}},
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

// buildActionEntity returns the Cedar action UID for an action id plus its
// entity carrying the plane-group parent edges, so `action in [Action::"<group>"]`
// resolves against the entity map (cedar.Authorize never consults the schema).
// Shared by actionEntity (the RPC path, keyed on procedure) and the REST
// facade's Interceptor.AuthorizeIngressAction (keyed on a bare action id).
func buildActionEntity(action string, groups []string) (cedar.EntityUID, types.Entity) {
	uid := cedar.NewEntityUID(actionType, cedar.String(action))
	parents := make([]cedar.EntityUID, len(groups))
	for i, g := range groups {
		parents[i] = cedar.NewEntityUID(actionType, cedar.String(g))
	}
	return uid, types.Entity{UID: uid, Parents: types.NewEntityUIDSet(parents...)}
}

// actionEntity returns the action entity for a Connect procedure. ok is false
// for an unmapped procedure — the interceptor default-denies those.
func actionEntity(procedure string) (cedar.EntityUID, types.Entity, bool) {
	e, ok := procMap[procedure]
	if !ok {
		return cedar.EntityUID{}, types.Entity{}, false
	}
	uid, ent := buildActionEntity(e.action, e.groups)
	return uid, ent, true
}
