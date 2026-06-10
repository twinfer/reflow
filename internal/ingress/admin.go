package ingress

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/apimap"
	"github.com/twinfer/reflw/internal/engine"
	apiv1 "github.com/twinfer/reflw/proto/apiv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// DescribeInvocation returns the current status of an invocation
// without blocking on completion.
func (s *Server) DescribeInvocation(ctx context.Context, req *connect.Request[ingressv1.DescribeInvocationRequest]) (*connect.Response[ingressv1.DescribeInvocationResponse], error) {
	msg := req.Msg
	id, err := resolveID(msg.GetInvocationId())
	if err != nil {
		return nil, err
	}
	shardID, err := s.shardForID(id)
	if err != nil {
		return nil, err
	}
	st, err := s.host.LookupInvocationStatus(ctx, shardID, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup invocation: %w", err))
	}
	// st == nil → never seen; apimap maps nil to nil, so surface an explicit
	// UNSPECIFIED-state view (the view's "Free" equivalent) so the field is set.
	view := apimap.InvocationStatusView(st)
	if view == nil {
		view = &apiv1.InvocationStatusView{State: apiv1.InvocationState_INVOCATION_STATE_UNSPECIFIED}
	}
	return connect.NewResponse(&ingressv1.DescribeInvocationResponse{Status: view}), nil
}

// GetObjectState reads a single state row for a virtual object. Routes
// to the partition owning (service, object_key) via the Host's
// Partitioner. present=false (not an error) signals an absent key,
// distinct from a present-but-empty value.
func (s *Server) GetObjectState(ctx context.Context, req *connect.Request[ingressv1.GetObjectStateRequest]) (*connect.Response[ingressv1.GetObjectStateResponse], error) {
	msg := req.Msg
	if msg.GetService() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("service is required"))
	}
	if msg.GetStateKey() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("state_key is required"))
	}
	target := &enginev1.InvocationTarget{
		ServiceName: msg.GetService(),
		ObjectKey:   msg.GetObjectKey(),
	}
	shardID := s.host.Partitioner().ShardForTarget(target)
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupState{
		Target: target,
		Key:    msg.GetStateKey(),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup state: %w", err))
	}
	r, ok := res.(engine.StateLookupResult)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup state: unexpected type %T", res))
	}
	return connect.NewResponse(&ingressv1.GetObjectStateResponse{
		Value:   r.Value,
		Present: r.Present,
	}), nil
}
