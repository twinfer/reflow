package authz

import (
	"strconv"

	"github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"

	"github.com/twinfer/reflow/internal/auth"
)

// Cedar entity type names — must match schema.cedar verbatim.
const (
	TypeClusterOperator cedar.EntityType = "ClusterOperator"
	TypeNode            cedar.EntityType = "Node"
	TypeUser            cedar.EntityType = "User"
	TypeAnonymous       cedar.EntityType = "Anonymous"

	TypeSecretRecord     cedar.EntityType = "SecretRecord"
	TypeDeploymentRecord cedar.EntityType = "DeploymentRecord"
	TypeInvocation       cedar.EntityType = "Invocation"
	TypePlatformConfig   cedar.EntityType = "PlatformConfig"
)

// PlatformConfigUID is the singleton cluster-scoped resource used by
// operator/node actions that have no per-record target (AddNode, SelfJoin,
// Delivery*, UpsertPlatformConfig).
var PlatformConfigUID = cedar.NewEntityUID(TypePlatformConfig, "cluster")

// InvocationResourceUID is the singleton resource for ingress data-plane
// authorization. The engine is single-tenant, so the resource carries no
// tenant attribute; the resource type is the seam for future per-service
// ingress rules (the `service` attribute the interceptor stamps).
var InvocationResourceUID = cedar.NewEntityUID(TypeInvocation, "request")

// PrincipalEntity maps a server-verified auth.Principal to its Cedar
// principal UID and entity, carrying the attributes the schema declares for
// that type. Every principal — including the anonymous one — maps to a typed
// entity: Cedar has no null principal, and uniform ingress authorization
// needs a typed subject for open (unauthenticated) traffic. Anonymous maps to
// Anonymous::"anonymous"; a pre-tenancy OIDC caller (Kind "user") maps to a
// User entity so it stays distinguishable from anonymous in audit.
func PrincipalEntity(p auth.Principal) (cedar.EntityUID, types.Entity) {
	switch p.Kind {
	case "operator":
		uid := cedar.NewEntityUID(TypeClusterOperator, cedar.String(p.Subject))
		return uid, types.Entity{UID: uid}
	case "node":
		uid := cedar.NewEntityUID(TypeNode, cedar.String(p.Subject))
		return uid, types.Entity{UID: uid, Attributes: types.NewRecord(types.RecordMap{
			"node_id": types.Long(parseNodeID(p.Subject)),
		})}
	case "user":
		uid := cedar.NewEntityUID(TypeUser, cedar.String(p.Subject))
		return uid, types.Entity{UID: uid, Attributes: types.NewRecord(types.RecordMap{
			"subject": types.String(p.Subject),
		})}
	default:
		uid := cedar.NewEntityUID(TypeAnonymous, "anonymous")
		return uid, types.Entity{UID: uid}
	}
}

// parseNodeID extracts the numeric node id from a node principal's Subject
// (the CN is "node/<id>", so Subject is the bare "<id>"). Unparseable ids
// fall back to 0, which matches no real node.
func parseNodeID(subject string) int64 {
	id, err := strconv.ParseInt(subject, 10, 64)
	if err != nil {
		return 0
	}
	return id
}
