package ingress

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// CancelInvocation forces a running invocation to terminate with
// FailureCode=CancelledCode. The flow:
//
//  1. Look up the InvocationStatus on the owning partition (routed by
//     the partition_key encoded in the invocation id).
//  2. Extract the invocation's Target (service + key).
//  3. Propose an InvokerEffect.SignalDelivered{target, __cancel__} on
//     the same partition. The apply arm resolves Target → active
//     InvocationId via KeyLeaseTable and synthesizes a terminal
//     Completed.
//
// Best-effort: between the lookup and the apply, the lease could move
// to a different invocation (e.g. the original completed, queue
// promoted the next one). The cancel then hits the new lease holder.
// Callers concerned about that race should AwaitInvocation /
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
	if target.GetObjectKey() == "" {
		// MVP: cancellation routes by Target via KeyLeaseTable, which
		// requires a keyed target. Cancelling an unkeyed Service
		// invocation isn't supported in this release.
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("CancelInvocation: unkeyed services not supported in v1"))
	}

	effect := &enginev1.InvokerEffect{
		Kind: &enginev1.InvokerEffect_SignalDelivered{SignalDelivered: &enginev1.SignalDelivered{
			Target:     target,
			SignalName: wire.WellKnownCancelSignal,
		}},
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
