// Reflow authorization schema — the typed trust shape for Cedar authz.
// Embedded into the binary via //go:embed and versioned alongside the
// protos. Mirrors doc/06-component-design.md §6.15.8.
//
// Cedar has no entity-id wildcard: "match any principal of a type" is the
// `is` type-test (written `principal is ClusterOperator` in a policy), not
// `ClusterOperator::"*"`. The SAD's AnonymousJoiner entity is intentionally
// omitted here — SelfJoin callers present a node/* leaf by the time they
// reach the ClusterCtl interceptor; the MeshSign token-bearer path is a
// separate no-mTLS bootstrap listener that does not flow through this schema.

// ---- Principal entity types — one per authn outcome ----
entity ClusterOperator;                  // mTLS CN=operator/<name> or OIDC kind=operator
entity Node { node_id: Long };           // mTLS CN=node/<id>
entity TenantAdmin {
    tenant_id: Long,                     // from the OIDC tenant claim (auth.TenantIDFromPrincipal)
    subject: String
};

// ---- Resource entity types — one per persisted record + action target ----
// tenant_id is declared now; the records gain the field in PR4 and the
// tenant-isolation when-clause activates in PR5.
entity EventSourceRecord   { tenant_id: Long, name: String };
entity WebhookSourceRecord { tenant_id: Long, name: String };
entity SecretRecord        { tenant_id: Long, name: String };
entity DeploymentRecord    { tenant_id: Long, name: String };
entity TenantRecord        { tenant_id: Long };
entity Invocation          { tenant_id: Long, service: String };
entity PlatformConfig;                   // cluster-scoped sentinel resource

// ---- Action group: the tenant-scoped actions gated by resource.tenant_id ----
// Policies referencing this group via `action in Action::"TenantConfigActions"`
// must inject the action entities + their group parents into the entity map at
// eval time (handled by the interceptor in PR5); the foundational policies in
// PR1 use explicit action lists and need no action-hierarchy entities.
action TenantConfigActions;

// ---- Tenant-scoped config-plane mutations (members of the group) ----
action UpsertEventSource  in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: EventSourceRecord };
action DeleteEventSource  in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: EventSourceRecord };
action UpsertWebhook      in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: WebhookSourceRecord };
action DeleteWebhook      in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: WebhookSourceRecord };
action UpsertSecret       in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };
action DeleteSecret       in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: SecretRecord };
action RegisterDeployment in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };
action DeleteDeployment   in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: DeploymentRecord };
action SubmitInvocation   in [Action::"TenantConfigActions"] appliesTo { principal: [ClusterOperator, TenantAdmin], resource: Invocation };

// ---- Cluster-admin actions (operator-only; not tenant-scoped) ----
action AddNode              appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action UpsertTenant         appliesTo { principal: [ClusterOperator], resource: TenantRecord };
action UpsertPlatformConfig appliesTo { principal: [ClusterOperator], resource: PlatformConfig };

// ---- Node mesh actions (node-only) ----
action SelfJoin                appliesTo { principal: [Node], resource: PlatformConfig };
action DeliveryDeliver         appliesTo { principal: [Node], resource: PlatformConfig };
action DeliveryListUndelivered appliesTo { principal: [Node], resource: PlatformConfig };

// PR2 expands the action set to cover every Connect RPC across
// ClusterCtl + Config + Ingress.
