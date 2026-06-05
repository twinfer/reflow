package ingress

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// ResolveAwakeable looks up the awakeable directory to find the owning
// invocation, then proposes an InvokerEffect.AwakeableResolved on the
// owner's partition. The FSM appends a JEAwakeableResult to the owner's
// journal and (if Suspended) wakes the invocation.
//
// The awakeable id carries the owner's partition_key in its first 8
// decoded bytes (see invoker.newAwakeableID and
// keys.AwakeableOwnerPartitionKey), so a single SyncRead on the owner's
// shard suffices — no fan-out across partitions.
func (s *Server) ResolveAwakeable(ctx context.Context, req *connect.Request[ingressv1.ResolveAwakeableRequest]) (*connect.Response[ingressv1.ResolveAwakeableResponse], error) {
	msg := req.Msg
	awkID := msg.GetAwakeableId()
	if awkID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("awakeable_id is required"))
	}
	ownerPK, err := keys.AwakeableOwnerPartitionKey(awkID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("awakeable_id: %w", err))
	}

	shardID := s.host.Partitioner().ShardForKey(ownerPK)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupAwakeable{ID: awkID})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("awakeable %q not yet registered", awkID))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup awakeable: %w", err))
	}
	entry, ok := res.(*enginev1.AwakeableEntry)
	if !ok || entry == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("awakeable %q not found", awkID))
	}

	effect := &enginev1.InvokerEffect{
		InvocationId: entry.GetOwner(),
		Kind: &enginev1.InvokerEffect_AwakeableResolved{AwakeableResolved: &enginev1.AwakeableResolved{
			AwakeableId:    awkID,
			Value:          msg.GetValue(),
			FailureMessage: msg.GetFailureMessage(),
		}},
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: effect}}
	producerID := "awk/" + awkID
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("propose awakeable_resolved: %w", err))
	}
	return connect.NewResponse(&ingressv1.ResolveAwakeableResponse{Resolved: true}), nil
}
