package reflow

import (
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// ingressResourceTenant recovers the tenant band of the resource a by-id
// ingress request names, for the authz interceptor's Invocation resource. It
// is the security-critical half of tenant isolation: a tenant/<A> caller that
// presents tenant/<B>'s invocation id must be denied, and that only works if
// authz attributes the resource to band B exactly as the handler will route it.
//
// It therefore recovers the partition_key the same way the handlers do
// (ingress.resolveID's preference: the proto id when its uuid is 16 bytes,
// else the string form via ingress.ParseInvocationID) — no parser differential
// for an attacker to exploit. ok=false for by-target RPCs (Submit,
// GetObjectState, ResolveWorkflowPromise) and unparseable ids; the interceptor
// then uses the principal's own band, which is where by-target ingress routes.
func ingressResourceTenant(_ string, msg any) (uint32, bool) {
	pk, ok := func() (uint64, bool) {
		switch m := msg.(type) {
		case *ingressv1.AwaitInvocationRequest:
			return idPartitionKey(m.GetInvocationId(), m.GetInvocationIdProto())
		case *ingressv1.AttachInvocationRequest:
			return idPartitionKey(m.GetInvocationId(), m.GetInvocationIdProto())
		case *ingressv1.GetInvocationOutputRequest:
			return idPartitionKey(m.GetInvocationId(), m.GetInvocationIdProto())
		case *ingressv1.DescribeInvocationRequest:
			return idPartitionKey(m.GetInvocationId(), m.GetInvocationIdProto())
		case *ingressv1.CancelInvocationRequest:
			return idPartitionKey(m.GetInvocationId(), m.GetInvocationIdProto())
		case *ingressv1.ResolveAwakeableRequest:
			owner, err := keys.AwakeableOwnerPartitionKey(m.GetAwakeableId())
			if err != nil {
				return 0, false
			}
			return owner, true
		default:
			return 0, false
		}
	}()
	if !ok {
		return 0, false
	}
	return keys.TenantFromPartitionKey(pk), true
}

// idPartitionKey mirrors ingress.resolveID: prefer the proto id when it carries
// a full 16-byte uuid, else parse the string form. Returns ok=false when
// neither yields a usable id (the handler then rejects it as InvalidArgument).
func idPartitionKey(idStr string, idProto *enginev1.InvocationId) (uint64, bool) {
	if idProto != nil && len(idProto.GetUuid()) == 16 {
		return idProto.GetPartitionKey(), true
	}
	if idStr == "" {
		return 0, false
	}
	id, err := ingress.ParseInvocationID(idStr)
	if err != nil {
		return 0, false
	}
	return id.GetPartitionKey(), true
}
