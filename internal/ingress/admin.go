package ingress

import (
	"context"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// ListPartitions returns the partitions hosted on this node and their
// per-partition leadership state. Sorted by shard_id ascending.
func (s *Server) ListPartitions(_ context.Context, _ *ingressv1.ListPartitionsRequest) (*ingressv1.ListPartitionsResponse, error) {
	parts := s.host.Partitions()
	out := make([]*ingressv1.PartitionInfo, 0, len(parts))
	for shardID, runner := range parts {
		out = append(out, &ingressv1.PartitionInfo{
			ShardId:     shardID,
			IsLeader:    runner.Leadership().IsLeader(),
			LeaderEpoch: runner.Leadership().LeaderEpoch(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ShardId < out[j].ShardId })
	return &ingressv1.ListPartitionsResponse{Partitions: out}, nil
}

// DescribeInvocation returns the current status of an invocation without
// blocking on completion.
func (s *Server) DescribeInvocation(ctx context.Context, req *ingressv1.DescribeInvocationRequest) (*ingressv1.DescribeInvocationResponse, error) {
	id, err := resolveID(req.GetInvocationId(), req.GetInvocationIdProto())
	if err != nil {
		return nil, err
	}
	shardID, err := s.shardForID(id)
	if err != nil {
		return nil, err
	}
	// Deadline is guaranteed by withDefaultDeadline at the gRPC server level.
	st, err := s.host.LookupInvocationStatus(ctx, shardID, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lookup invocation: %v", err)
	}
	if st == nil {
		// Treat as Free — invocation never seen.
		return &ingressv1.DescribeInvocationResponse{Status: &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Free{Free: &enginev1.Free{}},
		}}, nil
	}
	return &ingressv1.DescribeInvocationResponse{Status: st}, nil
}
