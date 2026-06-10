// Reflw authorization schema — the typed trust shape for Cedar authz.
// Embedded into the binary via //go:embed and versioned alongside the protos.
//
// Cedar has no entity-id wildcard: "match any principal of a type" is the
// `is` type-test (written `principal is ClusterOperator`), not
// `ClusterOperator::"*"`. The MeshSign token-bearer path is gone; SelfJoin
// callers present a node/* leaf by the time they reach the Admin interceptor.
//
// Action ids are the bare RPC method name (e.g. "AwaitInvocation"), unique
// across the three services behind the interceptor (ingress, admin, delivery),
// so policies stay readable: `action == Action::"UpsertSecret"`. The
// procedure->action mapping and the plane action-groups live in procmap.go;
// internal/authz/procmap_test enforces full coverage, id uniqueness, AND
// schema<->procmap consistency. The `in [Action::"<group>"]` membership
// declared here is for the upload-time validator; at evaluation cedar.Authorize
// ignores the schema, so the interceptor stamps each action's group parents onto
// the entity map (see actionEntity).

// ---- Principal entity types — one per authn outcome ----
entity ClusterOperator;                  // mTLS CN=operator/<name>
entity Node { node_id: Long };           // mTLS CN=node/<id>
entity User { subject: String };         // OIDC bearer
entity Anonymous;                        // no verified identity — open-ingress traffic

// ---- Resource entity types — one per persisted record + action target ----
// The engine is single-tenant (multi-tenancy is a deployment concern), so
// resources carry no tenant attribute. At evaluation every non-ingress request
// resolves against the PlatformConfig sentinel; ingress resolves against the
// singleton Invocation resource (the `service` attribute is the seam for future
// per-service ingress rules). The record entities below are declared so operator
// policies can constrain by resource type at upload time.
entity SecretRecord     { name: String };
entity DeploymentRecord { name: String };
entity ModelRecord      { name: String };
entity Invocation       { service: String };
entity PlatformConfig;                   // cluster-scoped sentinel resource

// ---- Action groups — the administrative planes ----
action IngressActions;          // data plane (open by default)
action AppConfigActions;        // app-config writes (deployments / models / secrets)
action ConfigReadActions;       // app-config reads
action PlatformConfigActions;   // platform plane — operator only
action ClusterAdminActions;     // cluster topology / DR / rebalance — operator only
action MeshActions;             // inter-node mesh — node only

// ===================== Ingress (data plane) =====================
action "SubmitInvocation"          in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "AwaitInvocation"           in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "AttachInvocation"          in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "GetInvocationOutput"       in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "DescribeInvocation"        in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "ListInvocations"           in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "CancelInvocation"          in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "ResolveAwakeable"          in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "ResolveWorkflowPromise"    in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "GetObjectState"            in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "StartProcess"              in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "DeliverMessage"            in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "DeliverProcessEvent"       in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "GetProcessInstance"        in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "ListProcessInstances"      in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "GetProcessInstanceHistory" in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "ResolveProcessIncident"    in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "GetTask"                   in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };
action "CompleteTask"              in [Action::"IngressActions"] appliesTo { principal: [Anonymous, User, ClusterOperator], resource: Invocation };

// ===================== Ingress: operator-only maintenance (no open plane) =====================
action "PurgeInvocation" appliesTo { principal: [ClusterOperator], resource: Invocation };

// ===================== Delivery + SelfJoin (node mesh) =====================
action "Deliver"             in [Action::"MeshActions"] appliesTo { principal: [Node], resource: PlatformConfig };
action "UploadLPTransferSST" in [Action::"MeshActions"] appliesTo { principal: [Node], resource: PlatformConfig };
action "SelfJoin"            in [Action::"MeshActions"] appliesTo { principal: [Node], resource: PlatformConfig };

// ===================== Admin: app-config writes =====================
action "UpsertSecret"       in [Action::"AppConfigActions"] appliesTo { principal: [ClusterOperator, User], resource: SecretRecord };
action "DeleteSecret"       in [Action::"AppConfigActions"] appliesTo { principal: [ClusterOperator, User], resource: SecretRecord };
action "RegisterDeployment" in [Action::"AppConfigActions"] appliesTo { principal: [ClusterOperator, User], resource: DeploymentRecord };
action "DeleteDeployment"   in [Action::"AppConfigActions"] appliesTo { principal: [ClusterOperator, User], resource: DeploymentRecord };
action "RegisterModelSet"   in [Action::"AppConfigActions"] appliesTo { principal: [ClusterOperator, User], resource: ModelRecord };
action "DeleteModel"        in [Action::"AppConfigActions"] appliesTo { principal: [ClusterOperator, User], resource: ModelRecord };

// ===================== Admin: app-config reads =====================
action "ListDeployments"    in [Action::"ConfigReadActions"] appliesTo { principal: [ClusterOperator, User], resource: DeploymentRecord };
action "DescribeDeployment" in [Action::"ConfigReadActions"] appliesTo { principal: [ClusterOperator, User], resource: DeploymentRecord };
action "ListSecrets"        in [Action::"ConfigReadActions"] appliesTo { principal: [ClusterOperator, User], resource: SecretRecord };
action "ListModels"         in [Action::"ConfigReadActions"] appliesTo { principal: [ClusterOperator, User], resource: ModelRecord };
action "DescribeModel"      in [Action::"ConfigReadActions"] appliesTo { principal: [ClusterOperator, User], resource: ModelRecord };

// ===================== Admin: platform plane (operator only) =====================
action "UpsertClusterAuthzPolicy" in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };
action "GetClusterAuthzPolicy"    in [Action::"PlatformConfigActions"] appliesTo { principal: [ClusterOperator], resource: PlatformConfig };

// ===================== Admin: cluster admin (operator only) =====================
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
