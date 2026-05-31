// Reflow authorization schema — the typed trust shape for Cedar authz.
// Embedded into the binary via //go:embed and versioned alongside the protos.
//
// Cedar has no entity-id wildcard: "match any principal of a type" is the
// `is` type-test (written `principal is ClusterOperator`), not
// `ClusterOperator::"*"`. The SAD's AnonymousJoiner entity is intentionally
// omitted — SelfJoin callers present a node/* leaf by the time they reach the
// ClusterCtl interceptor; the MeshSign token-bearer path is a separate
// no-mTLS bootstrap listener that does not flow through this schema.
//
// Action ids are the bare RPC method name (e.g. "SubmitInvocation"), unique
// across the four services behind the interceptor, so policies stay readable:
// `action == Action::"UpsertEventSource"`. The procedure->action mapping and
// the plane action-groups live in procmap.go; internal/authz/procmap_test
// enforces full coverage + id uniqueness. The `in [Action::"<group>"]`
// membership declared here is for the upload-time validator; at evaluation
// cedar.Authorize ignores the schema, so the interceptor stamps each action's
// group parents onto the entity map (see actionEntity).

// ---- Principal entity types — one per authn outcome ----
entity ClusterOperator;                  // mTLS CN=operator/<name> or OIDC kind=operator
entity Node { node_id: Long };           // mTLS CN=node/<id>
entity TenantAdmin {
    // tenant_id is a server-side binding, not a token claim: it is the tenant
    // whose operator-registered OIDC issuer (TenantRecord.oidc_issuers, set via
    // ClusterCtl/UpsertTenant) validated the bearer token. Resolution lives in
    // the per-tenant OIDC reconciler + auth.TenantIDFromPrincipal.
    tenant_id: Long,
    subject: String
};
entity User { subject: String };         // OIDC bearer without a tenant binding (pre-tenancy)
entity Anonymous;                        // no verified identity — open-ingress traffic

// ---- Resource entity types — one per persisted record + action target ----
// tenant_id is declared now; records gain the field in PR4 and the
// tenant-isolation when-clause activates in PR5. Today every request evaluates
// against a single PlatformConfig sentinel resource (foundational policies
// leave resource unconstrained), so resource attributes are not yet read.
entity SecretRecord        { tenant_id: Long, name: String };
entity DeploymentRecord    { tenant_id: Long, name: String };
entity Invocation          { tenant_id: Long, service: String };
entity PlatformConfig;                   // cluster-scoped sentinel resource

// ---- Action groups — the administrative planes of SAD §6.15.1 ----
action IngressActions;          // data plane (open by default)
action TenantConfigActions;     // tenant-config writes (tenant-scoped in PR5)
action ConfigReadActions;       // tenant-config reads (tenant-scoped in PR5)
action PlatformConfigActions;   // platform plane — operator only
action ClusterAdminActions;     // cluster topology / DR / rebalance — operator only
action MeshActions;             // inter-node mesh — node only

// ===================== Ingress (data plane) =====================
action "SubmitInvocation"       in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "AwaitInvocation"        in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "AttachInvocation"       in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "GetInvocationOutput"    in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "DescribeInvocation"     in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "CancelInvocation"       in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "ResolveAwakeable"       in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "ResolveWorkflowPromise" in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "GetObjectState"         in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };

// ===================== Delivery + SelfJoin (node mesh) =====================
action "Deliver"             in [Action::"MeshActions"] appliesTo { principal: [Node], resource: PlatformConfig };
action "UploadLPTransferSST" in [Action::"MeshActions"] appliesTo { principal: [Node], resource: PlatformConfig };
action "SelfJoin"            in [Action::"MeshActions"] appliesTo { principal: [Node], resource: PlatformConfig };

// ===================== Config: tenant-config writes =====================
action "UpsertSecret"        in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };
action "DeleteSecret"        in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };
action "RegisterDeployment"  in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };
action "DeleteDeployment"    in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };

// ===================== Config: tenant-config reads =====================
action "ListDeployments"    in [Action::"ConfigReadActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };
action "DescribeDeployment" in [Action::"ConfigReadActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };
action "ListSecrets"        in [Action::"ConfigReadActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };

// ===================== Config: platform plane (operator only) =====================
action "UpsertCARoot"    in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "DeleteCARoot"    in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "ListCARoots"     in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "CreateJoinToken" in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "DeleteJoinToken" in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "ListJoinTokens"  in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "IssueOperator"            in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "IssueTenant"              in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "UpsertClusterAuthzPolicy" in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "GetClusterAuthzPolicy"    in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };

// ===================== ClusterCtl: cluster admin (operator only) =====================
action "AddNode"         in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "RemoveNode"      in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "ListNodes"       in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "ListPartitions"  in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "NodeLeadership"  in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "CreateSnapshot"  in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "ListSnapshots"   in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "DeleteSnapshot"  in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "TransferLP"      in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "ListLPTransfers" in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "RebalanceAdvise" in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "RebalanceDrain"  in [Action::"ClusterAdminActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
