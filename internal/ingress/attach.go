package ingress

import (
	"context"
	"fmt"
	"time"

	connect "connectrpc.com/connect"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// AttachInvocation blocks until the named invocation reaches Completed
// (or the timeout fires) and returns its outcome. Same long-poll shape
// as AwaitInvocation; the distinction is explicit intent: "I already
// submitted this id; give me its result".
func (s *Server) AttachInvocation(ctx context.Context, req *connect.Request[ingressv1.AttachInvocationRequest]) (*connect.Response[ingressv1.AttachInvocationResponse], error) {
	msg := req.Msg
	id, err := resolveID(msg.GetInvocationId(), msg.GetInvocationIdProto())
	if err != nil {
		return nil, err
	}
	shardID, err := s.shardForID(id)
	if err != nil {
		return nil, err
	}
	c, err := s.pollUntilCompleted(ctx, id, shardID, msg.GetTimeoutMs())
	if err != nil {
		return nil, err
	}
	if c == nil {
		return connect.NewResponse(&ingressv1.AttachInvocationResponse{Completed: false}), nil
	}
	return connect.NewResponse(&ingressv1.AttachInvocationResponse{
		Output:         c.GetOutput(),
		FailureMessage: c.GetFailureMessage(),
		FailureCode:    c.GetFailureCode(),
		Completed:      true,
	}), nil
}

// GetInvocationOutput is a non-blocking lookup. Returns PENDING for
// non-terminal invocations, COMPLETED_OK / COMPLETED_FAILED for
// terminal ones, and UNKNOWN if the invocation_id is not registered on
// this node.
func (s *Server) GetInvocationOutput(ctx context.Context, req *connect.Request[ingressv1.GetInvocationOutputRequest]) (*connect.Response[ingressv1.GetInvocationOutputResponse], error) {
	msg := req.Msg
	id, err := resolveID(msg.GetInvocationId(), msg.GetInvocationIdProto())
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
			return connect.NewResponse(&ingressv1.GetInvocationOutputResponse{Status: ingressv1.GetInvocationOutputResponse_UNKNOWN}), nil
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup invocation: %w", err))
	}
	if st == nil {
		return connect.NewResponse(&ingressv1.GetInvocationOutputResponse{Status: ingressv1.GetInvocationOutputResponse_UNKNOWN}), nil
	}
	switch s := st.GetStatus().(type) {
	case *enginev1.InvocationStatus_Completed:
		if fmsg := s.Completed.GetFailureMessage(); fmsg != "" {
			return connect.NewResponse(&ingressv1.GetInvocationOutputResponse{
				Status:         ingressv1.GetInvocationOutputResponse_COMPLETED_FAILED,
				FailureMessage: fmsg,
				FailureCode:    s.Completed.GetFailureCode(),
			}), nil
		}
		return connect.NewResponse(&ingressv1.GetInvocationOutputResponse{
			Status: ingressv1.GetInvocationOutputResponse_COMPLETED_OK,
			Output: s.Completed.GetOutput(),
		}), nil
	case nil, *enginev1.InvocationStatus_Free:
		return connect.NewResponse(&ingressv1.GetInvocationOutputResponse{Status: ingressv1.GetInvocationOutputResponse_UNKNOWN}), nil
	default:
		return connect.NewResponse(&ingressv1.GetInvocationOutputResponse{Status: ingressv1.GetInvocationOutputResponse_PENDING}), nil
	}
}
