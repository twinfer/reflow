package ingress

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// CancelInvocation forces a running invocation to terminate with
// FailureCode=CancelledCode. The flow:
//
//  1. Look up the InvocationStatus on the owning partition (routed by
//     the partition_key encoded in the invocation id).
//  2. Extract the invocation's Target (service + key).
//  3. Propose an InvokerEffect on the same partition:
//     - Keyed target → SignalDelivered{target, __cancel__}; the apply
//     arm resolves Target → active InvocationId via KeyLeaseTable and
//     synthesizes a terminal Completed (cancels the current lease
//     holder of the key).
//     - Unkeyed target → CancelById{id}; the apply arm force-terminates
//     that exact invocation directly (no lease indirection).
//
// Best-effort (keyed only): between the lookup and the apply, the lease
// could move to a different invocation (e.g. the original completed,
// queue promoted the next one), so the cancel hits the new lease holder.
// The by-id path has no such race — an already-completed id is a clean
// no-op. Callers concerned about the keyed race should AwaitInvocation /
// GetInvocationOutput after to verify.
//
// Returns accepted=true once the propose succeeds; the actual
// termination is observable via AwaitInvocation.
func (s *Server) CancelInvocation(ctx context.Context, req *connect.Request[ingressv1.CancelInvocationRequest]) (*connect.Response[ingressv1.CancelInvocationResponse], error) {
	msg := req.Msg
	id, err := resolveID(msg.GetInvocationId(), msg.GetInvocationIdProto())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	shardID := s.host.Partitioner().ShardForKey(id.GetPartitionKey())
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}

	status, err := s.host.LookupInvocationStatus(ctx, shardID, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup status: %w", err))
	}
	target := targetFromStatus(status)
	if target == nil {
		// Either the invocation doesn't exist or has no target on its
		// current status (Free / Completed without target). Treat as a
		// no-op cancellation.
		return connect.NewResponse(&ingressv1.CancelInvocationResponse{Accepted: false}), nil
	}
	var effect *enginev1.InvokerEffect
	if target.GetObjectKey() != "" {
		// Keyed target: route by Target via KeyLeaseTable so the cancel hits
		// whoever currently holds the key (the documented VO semantic — see
		// the best-effort note above).
		effect = &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_SignalDelivered{SignalDelivered: &enginev1.SignalDelivered{
				Target:     target,
				SignalName: wire.WellKnownCancelSignal,
			}},
		}
	} else {
		// Unkeyed target: no key lease to route through. Force-cancel the
		// invocation directly by id (applyCancelById on the owning shard).
		effect = &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_CancelById{CancelById: id},
		}
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: effect}}
	producerID := "cancel/" + FormatInvocationID(id)
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("propose cancel: %w", err))
	}
	return connect.NewResponse(&ingressv1.CancelInvocationResponse{Accepted: true}), nil
}

// targetFromStatus returns the InvocationTarget carried by the current
// status, or nil if the status has no target (Free, or a malformed
// Completed status with empty Target field).
func targetFromStatus(status *enginev1.InvocationStatus) *enginev1.InvocationTarget {
	switch s := status.GetStatus().(type) {
	case *enginev1.InvocationStatus_Scheduled:
		return s.Scheduled.GetTarget()
	case *enginev1.InvocationStatus_Invoked:
		return s.Invoked.GetTarget()
	case *enginev1.InvocationStatus_Suspended:
		return s.Suspended.GetTarget()
	case *enginev1.InvocationStatus_Completed:
		return s.Completed.GetTarget()
	default:
		return nil
	}
}
