package ingress

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/engine"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// PurgeInvocation deletes a Completed invocation's durable rows (status,
// journal, signal inbox/awaiter) immediately, instead of waiting for the
// retention reaper. It proposes Command.Purge on the partition owning the
// invocation id (routed by the partition_key encoded in the id); the apply
// arm (onPurge) is a no-op when the invocation isn't in a purgeable
// (Completed/Free) state, so purging a running or absent invocation is
// safe. Virtual-object state and workflow promise rows are NOT touched —
// those are the timed reaper's (onReap) concern.
//
// Returns accepted=true once the propose succeeds; the rows are gone once
// the proposal commits. Callers needing confirmation can DescribeInvocation
// afterward (it reports UNKNOWN once the row is purged).
func (s *Server) PurgeInvocation(ctx context.Context, req *connect.Request[ingressv1.PurgeInvocationRequest]) (*connect.Response[ingressv1.PurgeInvocationResponse], error) {
	msg := req.Msg
	id, err := resolveID(msg.GetInvocationId())
	if err != nil {
		return nil, err
	}

	shardID := s.host.Partitioner().ShardForKey(id.GetPartitionKey())
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}

	cmd := &enginev1.Command{Kind: &enginev1.Command_Purge{Purge: &enginev1.PurgeInvocation{InvocationId: id}}}
	producerID := "purge/" + FormatInvocationID(id)
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("propose purge: %w", err))
	}
	return connect.NewResponse(&ingressv1.PurgeInvocationResponse{Accepted: true}), nil
}
