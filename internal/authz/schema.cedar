// Reflow authorization schema — the typed trust shape for Cedar authz.
// Embedded into the binary via //go:embed and versioned alongside the
// protos.
//
// Cedar has no entity-id wildcard: "match any principal of a type" is the
// `is` type-test (written `principal is ClusterOperator` in a policy), not
// `ClusterOperator::"*"`. The SAD's AnonymousJoiner entity is intentionally
// omitted — SelfJoin callers present a node/* leaf by the time they reach the
// ClusterCtl interceptor; the MeshSign token-bearer path is a separate
// no-mTLS bootstrap listener that does not flow through this schema.
//
// Action ids are the full Connect procedure path (e.g.
// "/reflow.config.v1.Config/UpsertEventSource"). The method name alone is
// ambiguous — ListPartitions exists on both Ingress and ClusterCtl — so the
// full path is the unique, collision-free id, derived directly from
// connect.Spec().Procedure with no lookup table.

// ---- Principal entity types — one per authn outcome ----
entity ClusterOperator;                  // mTLS CN=operator/<name> or OIDC kind=operator
entity Node { node_id: Long };           // mTLS CN=node/<id>
entity TenantAdmin {
    tenant_id: Long,                     // from the OIDC tenant claim (auth.TenantIDFromPrincipal)
    subject: String
};
entity User { subject: String };         // OIDC bearer without a tenant binding (pre-tenancy)
entity Anonymous;                        // no verified identity — open-ingress traffic

// ---- Resource entity types — one per persisted record + action target ----
// tenant_id is declared now; the records gain the field in PR4 and the
// tenant-isolation when-clause activates in PR5. PR2 evaluates every request
// against a single PlatformConfig sentinel resource (foundational policies
// leave resource unconstrained), so resource attributes are not yet read.
entity EventSourceRecord   { tenant_id: Long, name: String };
entity WebhookSourceRecord { tenant_id: Long, name: String };
entity SecretRecord        { tenant_id: Long, name: String };
entity DeploymentRecord    { tenant_id: Long, name: String };
entity TenantRecord        { tenant_id: Long };
entity Invocation          { tenant_id: Long, service: String };
entity PlatformConfig;                   // cluster-scoped sentinel resource

// ---- Action group: the tenant-scoped config mutations gated by tenant_id ----
// Policies referencing this group via `action in Action::"TenantConfigActions"`
// must inject the action entities + their group parents into the entity map at
// eval time (PR5). The foundational policies use explicit action lists and
// need no action-hierarchy entities.
action TenantConfigActions;

// ===================== Ingress (open by default) =====================
// principal is unconstrained at the policy level (open ingress); appliesTo
// enumerates who may legitimately appear.
action "/reflow.ingress.v1.Ingress/SubmitInvocation"       appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/AwaitInvocation"        appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/AttachInvocation"       appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/GetInvocationOutput"    appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/DescribeInvocation"     appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/CancelInvocation"       appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/ResolveAwakeable"       appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/ResolveWorkflowPromise" appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/GetObjectState"         appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: Invocation };
action "/reflow.ingress.v1.Ingress/ListPartitions"         appliesTo { principal: [Anonymous, User, TenantAdmin, ClusterOperator], resource: PlatformConfig };

// ===================== Delivery (node mesh only) =====================
action "/reflow.delivery.v1.Delivery/Deliver"             appliesTo { principal: [Node], resource: PlatformConfig };
action "/reflow.delivery.v1.Delivery/UploadLPTransferSST" appliesTo { principal: [Node], resource: PlatformConfig };

// ===================== Config: tenant-scopable mutations =====================
action "/reflow.config.v1.Config/UpsertEventSource"   in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: EventSourceRecord };
action "/reflow.config.v1.Config/DeleteEventSource"   in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: EventSourceRecord };
action "/reflow.config.v1.Config/UpsertWebhookSource" in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: WebhookSourceRecord };
action "/reflow.config.v1.Config/DeleteWebhookSource" in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: WebhookSourceRecord };
action "/reflow.config.v1.Config/UpsertSecret"        in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };
action "/reflow.config.v1.Config/DeleteSecret"        in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };
action "/reflow.config.v1.Config/RegisterDeployment"  in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };
action "/reflow.config.v1.Config/DeleteDeployment"    in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };

// ===================== Config: reads + cluster-scoped surfaces =====================
action "/reflow.config.v1.Config/ListDeployments"    appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };
action "/reflow.config.v1.Config/DescribeDeployment" appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };
action "/reflow.config.v1.Config/ListEventSources"   appliesTo { principal: [ClusterOperator, TenantAdmin], resource: EventSourceRecord };
action "/reflow.config.v1.Config/ListWebhookSources" appliesTo { principal: [ClusterOperator, TenantAdmin], resource: WebhookSourceRecord };
action "/reflow.config.v1.Config/ListSecrets"        appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };
action "/reflow.config.v1.Config/ListAuditLog"       appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.config.v1.Config/UpsertCARoot"       appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.config.v1.Config/DeleteCARoot"       appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.config.v1.Config/ListCARoots"        appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.config.v1.Config/CreateJoinToken"    appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.config.v1.Config/DeleteJoinToken"    appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.config.v1.Config/ListJoinTokens"     appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.config.v1.Config/IssueOperator"      appliesTo { principal: [ClusterOperator], resource: PlatformConfig };

// ===================== ClusterCtl =====================
action "/reflow.clusterctl.v1.ClusterCtl/SelfJoin"         appliesTo { principal: [Node],           resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/AddNode"          appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/RemoveNode"       appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/ListNodes"        appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/ListPartitions"   appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/CreateSnapshot"   appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/ListSnapshots"    appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/DeleteSnapshot"   appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/TransferLP"       appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/ListLPTransfers"  appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/RebalanceAdvise"  appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/RebalanceDrain"   appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "/reflow.clusterctl.v1.ClusterCtl/UpsertTenant"     appliesTo { principal: [ClusterOperator], resource: TenantRecord };
action "/reflow.clusterctl.v1.ClusterCtl/DeleteTenant"     appliesTo { principal: [ClusterOperator], resource: TenantRecord };
action "/reflow.clusterctl.v1.ClusterCtl/ListTenants"      appliesTo { principal: [ClusterOperator], resource: TenantRecord };
action "/reflow.clusterctl.v1.ClusterCtl/DescribeTenant"   appliesTo { principal: [ClusterOperator], resource: TenantRecord };
action "/reflow.clusterctl.v1.ClusterCtl/UpsertTenantDEK"  appliesTo { principal: [ClusterOperator], resource: TenantRecord };
action "/reflow.clusterctl.v1.ClusterCtl/DeleteTenantDEK"  appliesTo { principal: [ClusterOperator], resource: TenantRecord };
action "/reflow.clusterctl.v1.ClusterCtl/ListTenantDEKs"   appliesTo { principal: [ClusterOperator], resource: TenantRecord };
