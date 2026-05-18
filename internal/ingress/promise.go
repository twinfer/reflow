package ingress

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// ResolveWorkflowPromise resolves (value) or rejects (failure_message)
// a workflow-scoped named promise. Routes by (service, workflow_key)
// via Partitioner.ShardForTarget; the receiving partition writes
// PromiseValue and wakes any in-flight Promise(name).Result() awaiter.
//
// Idempotent: a second Resolve/Reject for an already-completed
// promise is silently absorbed by the apply path. Callers that need
// to observe loser-state can re-read via the workflow's output.
func (s *Server) ResolveWorkflowPromise(ctx context.Context, req *connect.Request[ingressv1.ResolveWorkflowPromiseRequest]) (*connect.Response[ingressv1.ResolveWorkflowPromiseResponse], error) {
	msg := req.Msg
	if msg.GetService() == "" || msg.GetWorkflowKey() == "" || msg.GetPromiseName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("service, workflow_key, and promise_name are all required"))
	}

	target := &enginev1.InvocationTarget{
		ServiceName: msg.GetService(),
		ObjectKey:   msg.GetWorkflowKey(),
	}
	shardID := s.host.Partitioner().ShardForTarget(target)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("no partition for shard %d", shardID))
	}

	effect := &enginev1.InvokerEffect{
		Kind: &enginev1.InvokerEffect_PromiseCompleted{PromiseCompleted: &enginev1.PromiseCompleted{
			Service:        msg.GetService(),
			WorkflowKey:    msg.GetWorkflowKey(),
			PromiseName:    msg.GetPromiseName(),
			Value:          msg.GetValue(),
			FailureMessage: msg.GetFailureMessage(),
		}},
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: effect}}
	// producerID scopes dedup to one (service, key, name) tuple — retries
	// of the same Resolve land on the same row and are absorbed.
	producerID := "promise/" + msg.GetService() + "/" + msg.GetWorkflowKey() + "/" + msg.GetPromiseName()
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("propose resolve_workflow_promise: %w", err))
	}
	return connect.NewResponse(&ingressv1.ResolveWorkflowPromiseResponse{Accepted: true}), nil
}
