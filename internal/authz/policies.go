package authz

// FoundationalClusterPolicies is the in-binary default policy set — the
// temporary source of policy until cluster-managed policy moves into shard-0's
// PlatformConfigTable. It reproduces the plane separation:
//   - operators have full access (the old operator/* rules);
//   - nodes may only call the inter-node mesh actions (Delivery + SelfJoin);
//   - the ingress data plane is open (anonymous + User);
//   - OIDC users in the "reflw-admins" group additionally reach the app-config
//     read + write planes (cluster-admin + platform stay operator-only).
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

// Browser operators (OIDC users) in the "reflw-admins" group may read app
// config and perform app-config writes (deployments / models / secrets).
// Cluster-admin and platform planes stay operator-mTLS-only — a browser admin
// never reaches them via the foundational policy; operators widen per-cluster
// via UpsertClusterAuthzPolicy.
permit (principal is User, action in [Action::"ConfigReadActions"], resource)
when { principal.groups.contains("reflw-admins") };
permit (principal is User, action in [Action::"AppConfigActions"], resource)
when { principal.groups.contains("reflw-admins") };
`
