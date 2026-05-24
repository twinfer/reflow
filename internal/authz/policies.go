package authz

// FoundationalClusterPolicies is the in-binary default policy set, the
// temporary source of policy until PR3 moves policy text into shard-0's
// PlatformConfigTable. It reproduces the pre-Cedar plane separation:
// operators have full access; nodes are restricted to inter-node mesh RPCs.
//
// The tenant-isolation rule (`when { resource.tenant_id ==
// principal.tenant_id }`) is intentionally absent: it is inert until config
// records carry tenant_id (PR4) and is added in PR5. The node rule uses an
// explicit action list rather than the TenantConfigActions group so it needs
// no action-hierarchy entities at evaluation time.
const FoundationalClusterPolicies = `
// Cluster operators have full access.
permit (principal is ClusterOperator, action, resource);

// Nodes may only call inter-node mesh RPCs (Delivery + SelfJoin).
permit (
    principal is Node,
    action in [Action::"DeliveryDeliver", Action::"DeliveryListUndelivered", Action::"SelfJoin"],
    resource
);
`
