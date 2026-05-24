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
	TypeTenantAdmin     cedar.EntityType = "TenantAdmin"

	TypeEventSourceRecord   cedar.EntityType = "EventSourceRecord"
	TypeWebhookSourceRecord cedar.EntityType = "WebhookSourceRecord"
	TypeSecretRecord        cedar.EntityType = "SecretRecord"
	TypeDeploymentRecord    cedar.EntityType = "DeploymentRecord"
	TypeTenantRecord        cedar.EntityType = "TenantRecord"
	TypeInvocation          cedar.EntityType = "Invocation"
	TypePlatformConfig      cedar.EntityType = "PlatformConfig"
)

// PlatformConfigUID is the singleton cluster-scoped resource used by
// operator/node actions that have no per-record target (AddNode, SelfJoin,
// Delivery*, UpsertPlatformConfig).
var PlatformConfigUID = cedar.NewEntityUID(TypePlatformConfig, "cluster")

// PrincipalEntity maps a server-verified auth.Principal to its Cedar
// principal UID and entity (carrying the attributes the schema declares for
// that type). The bool is false for principals with no Cedar representation —
// the anonymous principal and, for now, pre-tenancy user/* principals; those
// are authorized only by policies with an unconstrained principal scope
// (PR2 decides the open-ingress rule). It never returns a User entity because
// the schema has none.
func PrincipalEntity(p auth.Principal) (cedar.EntityUID, types.Entity, bool) {
	switch p.Kind {
	case "operator":
		uid := cedar.NewEntityUID(TypeClusterOperator, cedar.String(p.Subject))
		return uid, types.Entity{UID: uid}, true
	case "node":
		uid := cedar.NewEntityUID(TypeNode, cedar.String(p.Subject))
		ent := types.Entity{UID: uid, Attributes: types.NewRecord(types.RecordMap{
			"node_id": types.Long(parseNodeID(p.Subject)),
		})}
		return uid, ent, true
	case "tenant":
		uid := cedar.NewEntityUID(TypeTenantAdmin, cedar.String(p.Subject))
		ent := types.Entity{UID: uid, Attributes: types.NewRecord(types.RecordMap{
			"tenant_id": types.Long(int64(auth.TenantIDFromPrincipal(p))),
			"subject":   types.String(p.Subject),
		})}
		return uid, ent, true
	default:
		return cedar.EntityUID{}, types.Entity{}, false
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
