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
// me the result of that submission".
func (s *Server) AttachInvocation(ctx context.Context, req *ingressv1.AttachInvocationRequest) (*ingressv1.AttachInvocationResponse, error) {
	id, err := resolveID(req.GetInvocationId(), req.GetInvocationIdProto())
	if err != nil {
		return nil, err
	}
	shardID, err := s.shardForID(id)
	if err != nil {
		return nil, err
	}
	c, err := s.pollUntilCompleted(ctx, id, shardID, req.GetTimeoutMs())
	if err != nil {
		return nil, err
	}
	if c == nil {
		return &ingressv1.AttachInvocationResponse{Completed: false}, nil
	}
	return &ingressv1.AttachInvocationResponse{
		Output:         c.GetOutput(),
		FailureMessage: c.GetFailureMessage(),
		Completed:      true,
	}, nil
}

// GetInvocationOutput is a non-blocking lookup. Returns PENDING for
// non-terminal invocations, COMPLETED_OK / COMPLETED_FAILED for terminal
// ones, and UNKNOWN if the invocation_id is not registered on this node.
func (s *Server) GetInvocationOutput(ctx context.Context, req *ingressv1.GetInvocationOutputRequest) (*ingressv1.GetInvocationOutputResponse, error) {
	id, err := resolveID(req.GetInvocationId(), req.GetInvocationIdProto())
	if err != nil {
		return nil, err
	}
	shardID, err := s.shardForID(id)
	if err != nil {
		return nil, err
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
