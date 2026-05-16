package ingress

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// ResolveAwakeable looks up the awakeable directory to find the owning
// invocation, then proposes an InvokerEffect.AwakeableResolved on the
// owner's partition. The FSM appends a JEAwakeableResult to the owner's
// journal and (if Suspended) wakes the invocation.
//
// The awakeable id carries the owner's partition_key in its first 8
// decoded bytes (see invoker.newAwakeableID and keys.AwakeableOwnerPartitionKey),
// so a single SyncRead on the owner's shard suffices — no fan-out across
// partitions.
func (s *Server) ResolveAwakeable(ctx context.Context, req *ingressv1.ResolveAwakeableRequest) (*ingressv1.ResolveAwakeableResponse, error) {
	awkID := req.GetAwakeableId()
	if awkID == "" {
		return nil, status.Error(codes.InvalidArgument, "awakeable_id is required")
	}
	ownerPK, err := keys.AwakeableOwnerPartitionKey(awkID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "awakeable_id: %v", err)
	}

	shardID := s.host.Partitioner().ShardForKey(ownerPK)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no partition for shard %d", shardID)
	}
	// Deadline is guaranteed by withDefaultDeadline at the gRPC server level.
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupAwakeable{ID: awkID})
	if err != nil {
		// The handler hasn't journaled JEAwakeable yet — the
		// AwakeableTable.Get path returns storage.ErrNotFound through
		// SyncRead. Map to NotFound so callers can retry rather than
		// treating it as a server fault.
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "awakeable %q not yet registered", awkID)
		}
		return nil, status.Errorf(codes.Internal, "lookup awakeable: %v", err)
	}
	entry, ok := res.(*enginev1.AwakeableEntry)
	if !ok || entry == nil {
		return nil, status.Errorf(codes.NotFound, "awakeable %q not found", awkID)
	}

	effect := &enginev1.InvokerEffect{
		InvocationId: entry.GetOwner(),
		Kind: &enginev1.InvokerEffect_AwakeableResolved{AwakeableResolved: &enginev1.AwakeableResolved{
			AwakeableId:    awkID,
			Value:          req.GetValue(),
			FailureMessage: req.GetFailureMessage(),
		}},
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: effect}}
	producerID := "awk/" + awkID
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, status.Error(codes.Unavailable, "shard closed")
		}
		return nil, status.Errorf(codes.Internal, "propose awakeable_resolved: %v", err)
	}
	return &ingressv1.ResolveAwakeableResponse{Resolved: true}, nil
}
