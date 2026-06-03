package ingress

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/routing"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// StartProcess launches a new iflow BPMN/CMMN instance. It routes by
// (tenant, model name, instance_key) — the same scheme the worker's
// procSession and ChildStart use — and proposes a start ProcessEvent (model_ref
// + kind set, which makes the apply path create the instance record). When the
// caller leaves instance_key empty the server mints a random one; a caller-
// supplied key makes the start idempotent (the apply path drops a start for an
// already-existing instance, and the deterministic producerID dedups retries).
func (s *Server) StartProcess(ctx context.Context, req *connect.Request[ingressv1.StartProcessRequest]) (*connect.Response[ingressv1.StartProcessResponse], error) {
	msg := req.Msg
	mr := msg.GetModelRef()
	if mr.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("model_ref.name is required"))
	}
	kind, err := processKindFromString(mr.GetKind())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	tenant, terr := principalTenant(ctx)
	if terr != nil {
		return nil, terr
	}

	instanceKey := msg.GetInstanceKey()
	if instanceKey == "" {
		k, kerr := mintProcessInstanceKey()
		if kerr != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("mint instance key: %w", kerr))
		}
		instanceKey = k
	}

	pk := routing.PartitionKey(tenant, mr.GetName(), instanceKey)
	shardID := s.host.Partitioner().ShardForKey(pk)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}

	cmd := &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk:          pk,
		Service:     mr.GetName(),
		InstanceKey: instanceKey,
		Payload:     &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_External{External: msg.GetVars()}},
		ModelRef:    mr,
		Kind:        kind,
	}}}
	// Deterministic per (instance pk, key): a retried StartProcess dedups at the
	// Raft session layer; the apply-path existing-instance guard is the
	// authoritative backstop for cross-node races.
	producerID := "startproc/" + strconv.FormatUint(pk, 16) + "/" + instanceKey
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("propose start process: %w", err))
	}
	return connect.NewResponse(&ingressv1.StartProcessResponse{Pk: pk, InstanceKey: instanceKey}), nil
}

// DeliverMessage correlates an inbound message/signal to parked process
// instances. It routes by (tenant, message_name, correlation_key) — the same
// key actuateProcessInstructions writes the subscription under — and proposes a
// DeliverProcessMessage, whose apply fans the message out to every subscribed
// instance and one-shot-consumes the matched subscriptions. Delivery is
// at-least-once: each call is a distinct delivery (unique producerID), but
// one-shot consumption makes re-delivery to an already-consumed subscription a
// no-op. An empty correlation_key broadcasts to every instance waiting on the
// name (BPMN signal semantics).
func (s *Server) DeliverMessage(ctx context.Context, req *connect.Request[ingressv1.DeliverMessageRequest]) (*connect.Response[ingressv1.DeliverMessageResponse], error) {
	msg := req.Msg
	if msg.GetMessageName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("message_name is required"))
	}

	tenant, terr := principalTenant(ctx)
	if terr != nil {
		return nil, terr
	}

	pk := routing.PartitionKey(tenant, msg.GetMessageName(), msg.GetCorrelationKey())
	shardID := s.host.Partitioner().ShardForKey(pk)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}

	cmd := &enginev1.Command{Kind: &enginev1.Command_DeliverProcessMessage{DeliverProcessMessage: &enginev1.DeliverProcessMessage{
		Pk:             pk,
		MessageName:    msg.GetMessageName(),
		CorrelationKey: msg.GetCorrelationKey(),
		Payload:        msg.GetPayload(),
	}}}
	nonce, err := mintNonce()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("mint nonce: %w", err))
	}
	producerID := "msg/" + nonce
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("propose deliver message: %w", err))
	}
	return connect.NewResponse(&ingressv1.DeliverMessageResponse{Pk: pk, Accepted: true}), nil
}

// GetProcessInstance performs a linearizable read of one instance's record from
// the partition owning (tenant, model name, instance_key) — the same routing
// StartProcess uses. It proposes nothing; it exists so a caller without an await
// RPC can observe whether an instance is running, parked, or terminal-and-reaped.
func (s *Server) GetProcessInstance(ctx context.Context, req *connect.Request[ingressv1.GetProcessInstanceRequest]) (*connect.Response[ingressv1.GetProcessInstanceResponse], error) {
	msg := req.Msg
	name := msg.GetModelRef().GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("model_ref.name is required"))
	}
	if msg.GetInstanceKey() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("instance_key is required"))
	}

	tenant, terr := principalTenant(ctx)
	if terr != nil {
		return nil, terr
	}

	pk := routing.PartitionKey(tenant, name, msg.GetInstanceKey())
	shardID := s.host.Partitioner().ShardForKey(pk)
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupProcessInstance{
		Service:     name,
		InstanceKey: msg.GetInstanceKey(),
		Tenant:      tenant,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup process instance: %w", err))
	}
	r, ok := res.(engine.ProcessInstanceLookupResult)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup process instance: unexpected type %T", res))
	}
	resp := &ingressv1.GetProcessInstanceResponse{Present: r.Present}
	if r.Present {
		resp.Status = r.Record.GetStatus()
		resp.Kind = r.Record.GetKind()
		resp.ActiveSeq = r.Record.GetActiveSeq()
		resp.NextSeq = r.Record.GetNextSeq()
		resp.Outstanding = r.Record.GetOutstanding()
	}
	return connect.NewResponse(resp), nil
}

// processKindFromString maps the wire model_ref.kind onto the ProcessKind enum.
func processKindFromString(kind string) (enginev1.ProcessKind, error) {
	switch kind {
	case "bpmn":
		return enginev1.ProcessKind_PROCESS_KIND_BPMN, nil
	case "cmmn":
		return enginev1.ProcessKind_PROCESS_KIND_CMMN, nil
	default:
		return enginev1.ProcessKind_PROCESS_KIND_UNSPECIFIED,
			fmt.Errorf("model_ref.kind must be \"bpmn\" or \"cmmn\", got %q", kind)
	}
}

// mintProcessInstanceKey generates a random instance key for callers that don't
// supply their own. Such a start is not idempotent (each call mints a fresh key).
func mintProcessInstanceKey() (string, error) {
	n, err := mintNonce()
	if err != nil {
		return "", err
	}
	return "p-" + n, nil
}

// mintNonce returns 16 random bytes hex-encoded — used for non-idempotent
// producer IDs and minted instance keys.
func mintNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
