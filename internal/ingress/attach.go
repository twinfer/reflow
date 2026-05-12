package ingress

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// AttachInvocation blocks until the named invocation reaches Completed
// (or the timeout fires) and returns its outcome. Same long-poll pattern
// as AwaitInvocation; the distinction is explicit intent: "I already
// submitted this id; give me its result" vs. "I just submitted, give
// me the result of that submission". Phase 3.
func (s *Server) AttachInvocation(ctx context.Context, req *ingressv1.AttachInvocationRequest) (*ingressv1.AttachInvocationResponse, error) {
	id, err := resolveID(req.GetInvocationId(), req.GetInvocationIdProto())
	if err != nil {
		return nil, err
	}

	timeout := time.Duration(req.GetTimeoutMs()) * time.Millisecond
	if timeout <= 0 || timeout > awaitMaxTimeout {
		timeout = awaitMaxTimeout
	}
	deadline := time.Now().Add(timeout)
	shardID := id.GetPartitionKey()
	if shardID == 0 {
		shardID = Phase2ShardID
	}

	for {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		st, lerr := s.host.LookupInvocationStatus(readCtx, shardID, id)
		cancel()
		if lerr == nil && st != nil {
			if c, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				return &ingressv1.AttachInvocationResponse{
					Output:         c.Completed.GetOutput(),
					FailureMessage: c.Completed.GetFailureMessage(),
					Completed:      true,
				}, nil
			}
		} else if lerr != nil && !isTransientLookupErr(lerr) {
			return nil, status.Errorf(codes.Internal, "lookup invocation: %v", lerr)
		}
		if time.Now().After(deadline) {
			return &ingressv1.AttachInvocationResponse{Completed: false}, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(awaitPollInterval):
		}
	}
}

// GetInvocationOutput is a non-blocking lookup. Returns PENDING for
// non-terminal invocations, COMPLETED_OK / COMPLETED_FAILED for terminal
// ones, and UNKNOWN if the invocation_id is not registered on this node.
// Phase 3.
func (s *Server) GetInvocationOutput(ctx context.Context, req *ingressv1.GetInvocationOutputRequest) (*ingressv1.GetInvocationOutputResponse, error) {
	id, err := resolveID(req.GetInvocationId(), req.GetInvocationIdProto())
	if err != nil {
		return nil, err
	}
	shardID := id.GetPartitionKey()
	if shardID == 0 {
		shardID = Phase2ShardID
	}

	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	st, err := s.host.LookupInvocationStatus(readCtx, shardID, id)
	if err != nil {
		if isTransientLookupErr(err) {
			return &ingressv1.GetInvocationOutputResponse{Status: ingressv1.GetInvocationOutputResponse_UNKNOWN}, nil
		}
		return nil, status.Errorf(codes.Internal, "lookup invocation: %v", err)
	}
	if st == nil {
		return &ingressv1.GetInvocationOutputResponse{Status: ingressv1.GetInvocationOutputResponse_UNKNOWN}, nil
	}
	switch s := st.GetStatus().(type) {
	case *enginev1.InvocationStatus_Completed:
		if msg := s.Completed.GetFailureMessage(); msg != "" {
			return &ingressv1.GetInvocationOutputResponse{
				Status:         ingressv1.GetInvocationOutputResponse_COMPLETED_FAILED,
				FailureMessage: msg,
			}, nil
		}
		return &ingressv1.GetInvocationOutputResponse{
			Status: ingressv1.GetInvocationOutputResponse_COMPLETED_OK,
			Output: s.Completed.GetOutput(),
		}, nil
	case nil, *enginev1.InvocationStatus_Free:
		return &ingressv1.GetInvocationOutputResponse{Status: ingressv1.GetInvocationOutputResponse_UNKNOWN}, nil
	default:
		return &ingressv1.GetInvocationOutputResponse{Status: ingressv1.GetInvocationOutputResponse_PENDING}, nil
	}
}
