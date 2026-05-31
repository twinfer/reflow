package authz

// FoundationalClusterPolicies is the in-binary default policy set — the
// temporary source of policy until cluster-managed policy moves into shard-0's
// PlatformConfigTable. It reproduces the plane separation:
//   - operators have full access (the old operator/* rules);
//   - nodes may only call the inter-node mesh actions (Delivery + SelfJoin);
//   - the ingress data plane is tenant-isolated: a verified tenant/<n>
//     principal may act only on its own band's resources, while anonymous and
//     pre-tenancy User principals may act only on the default band (tenant 0).
//
// Tenant isolation rests on the interceptor building a tenant-scoped
// Invocation resource for every ingress procedure (resource.tenant_id is the
// routed band for by-target RPCs, the band recovered from the request id for
// by-id RPCs). Everything else is default-denied — config + clusterctl are
// operator-only because only the god-mode rule reaches them. Rules reference
// the plane action-groups from schema.cedar; the interceptor stamps each
// action's group parents at eval (procmap.actionEntity).
//
// Offboarding a tenant is an additive forbid pushed via the cluster authz
// policy, e.g.:
//
//	forbid (principal is TenantAdmin, action, resource)
//	  when { principal.tenant_id == <n> };
const FoundationalClusterPolicies = `
// Cluster operators have full access — platform, tenant-lifecycle,
// cluster-admin, and every tenant-config surface. God-mode spans all tenants.
permit (principal is ClusterOperator, action, resource);

// Nodes may only call the inter-node mesh actions: Deliver,
// UploadLPTransferSST, SelfJoin.
permit (principal is Node, action in [Action::"MeshActions"], resource);

// A tenant principal may use the data plane only within its own band.
permit (principal is TenantAdmin, action in [Action::"IngressActions"], resource)
when { resource.tenant_id == principal.tenant_id };

// Anonymous and pre-tenancy User callers may use the data plane only on the
// default/untenanted band (tenant 0). Operators tighten or replace this by
// pushing a cluster policy.
permit (principal is Anonymous, action in [Action::"IngressActions"], resource)
when { resource.tenant_id == 0 };
permit (principal is User, action in [Action::"IngressActions"], resource)
when { resource.tenant_id == 0 };
`
