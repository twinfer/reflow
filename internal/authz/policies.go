package authz

// FoundationalClusterPolicies is the in-binary default policy set, the
// temporary source of policy until PR3 moves policy text into shard-0's
// PlatformConfigTable. It reproduces the pre-Cedar plane separation:
//   - operators have full access (the old operator/* rules);
//   - nodes may only call the inter-node mesh RPCs (Delivery) + SelfJoin
//     (the old delivery_node + clusterctl_node_selfjoin rules);
//   - ingress is open to any principal, anonymous included (the old
//     ingress_open / ingress_rest_open rules).
//
// Everything else is default-denied — config + clusterctl are operator-only
// because only the god-mode rule reaches them. The tenant-isolation rule
// (`when { resource.tenant_id == principal.tenant_id }`) is inert until config
// records carry tenant_id (PR4) and is added in PR5. Action ids are full
// Connect procedure paths; the node and ingress rules use explicit action
// lists (not the TenantConfigActions group) so they need no action-hierarchy
// entities at evaluation time.
const FoundationalClusterPolicies = `
// Cluster operators have full access.
permit (principal is ClusterOperator, action, resource);

// Nodes may only call the inter-node mesh RPCs and SelfJoin.
permit (
    principal is Node,
    action in [
        Action::"/reflow.delivery.v1.Delivery/Deliver",
        Action::"/reflow.delivery.v1.Delivery/UploadLPTransferSST",
        Action::"/reflow.clusterctl.v1.ClusterCtl/SelfJoin"
    ],
    resource
);

// Ingress is open by default: any principal (including Anonymous) may call
// the ingress data plane. Operators tighten this by replacing the cluster
// policy (PR3).
permit (
    principal,
    action in [
        Action::"/reflow.ingress.v1.Ingress/SubmitInvocation",
        Action::"/reflow.ingress.v1.Ingress/AwaitInvocation",
        Action::"/reflow.ingress.v1.Ingress/AttachInvocation",
        Action::"/reflow.ingress.v1.Ingress/GetInvocationOutput",
        Action::"/reflow.ingress.v1.Ingress/DescribeInvocation",
        Action::"/reflow.ingress.v1.Ingress/CancelInvocation",
        Action::"/reflow.ingress.v1.Ingress/ResolveAwakeable",
        Action::"/reflow.ingress.v1.Ingress/ResolveWorkflowPromise",
        Action::"/reflow.ingress.v1.Ingress/GetObjectState",
        Action::"/reflow.ingress.v1.Ingress/ListPartitions"
    ],
    resource
);
`
