package authz

// FoundationalClusterPolicies is the in-binary default policy set — the
// temporary source of policy until PR3 moves policy text into shard-0's
// PlatformConfigTable. It reproduces the pre-Cedar plane separation:
//   - operators have full access (the old operator/* rules);
//   - nodes may only call the inter-node mesh actions (Delivery + SelfJoin);
//   - ingress is open to any principal, anonymous included.
//
// Everything else is default-denied — config + clusterctl are operator-only
// because only the god-mode rule reaches them. The tenant-isolation rule
// (`when { resource.tenant_id == principal.tenant_id }`) is inert until config
// records carry tenant_id (PR4) and is added in PR5. Rules reference the plane
// action-groups from schema.cedar (IngressActions / MeshActions); the
// interceptor stamps each action's group parents at eval (procmap.actionEntity).
const FoundationalClusterPolicies = `
// Cluster operators have full access — platform, tenant-lifecycle,
// cluster-admin, and every tenant-config surface.
permit (principal is ClusterOperator, action, resource);

// Nodes may only call the inter-node mesh actions: Deliver,
// UploadLPTransferSST, SelfJoin.
permit (principal is Node, action in [Action::"MeshActions"], resource);

// Ingress is open by default: any principal (Anonymous included) may call the
// data plane. Operators tighten this by replacing the cluster policy (PR3).
permit (principal, action in [Action::"IngressActions"], resource);
`
