package ingress

import (
	"context"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/internal/engine"
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

// GetObjectState reads a single state row for a virtual object. Routes to
// the partition owning (service, object_key) via the Host's Partitioner.
// present=false (not an error) signals an absent key, distinct from a
// present-but-empty value.
func (s *Server) GetObjectState(ctx context.Context, req *ingressv1.GetObjectStateRequest) (*ingressv1.GetObjectStateResponse, error) {
	if req.GetService() == "" {
		return nil, status.Error(codes.InvalidArgument, "service is required")
	}
	if req.GetStateKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "state_key is required")
	}
	target := &enginev1.InvocationTarget{
		ServiceName: req.GetService(),
		ObjectKey:   req.GetObjectKey(),
	}
	shardID := s.host.Partitioner().ShardForTarget(target)
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupState{
		Target: target,
		Key:    req.GetStateKey(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lookup state: %v", err)
	}
	r, ok := res.(engine.StateLookupResult)
	if !ok {
		return nil, status.Errorf(codes.Internal, "lookup state: unexpected type %T", res)
	}
	return &ingressv1.GetObjectStateResponse{
		Value:   r.Value,
		Present: r.Present,
	}, nil
}
