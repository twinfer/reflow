package authz

// FoundationalClusterPolicies is the in-binary default policy set — the
// temporary source of policy until cluster-managed policy moves into shard-0's
// PlatformConfigTable. It reproduces the plane separation:
//   - operators have full access (the old operator/* rules);
//   - nodes may only call the inter-node mesh actions (Delivery + SelfJoin);
//   - the ingress data plane is open (anonymous + User).
//
// The engine is single-tenant — multi-tenancy is a deployment concern (one
// instance per customer), so there is no in-policy tenant isolation. Everything
// else is default-denied — config + clusterctl are operator-only because only
// the god-mode rule reaches them. Rules reference the plane action-groups from
// schema.cedar; the interceptor stamps each action's group parents at eval
// (procmap.actionEntity). Operators tighten the open ingress plane by pushing a
// cluster policy.
const FoundationalClusterPolicies = `
// Cluster operators have full access — platform, cluster-admin, and every
// config surface.
permit (principal is ClusterOperator, action, resource);

// Nodes may only call the inter-node mesh actions: Deliver,
// UploadLPTransferSST, SelfJoin.
permit (principal is Node, action in [Action::"MeshActions"], resource);

// Anonymous and User callers may use the data plane. Operators tighten or
// replace this by pushing a cluster policy.
permit (principal is Anonymous, action in [Action::"IngressActions"], resource);
permit (principal is User, action in [Action::"IngressActions"], resource);
`
