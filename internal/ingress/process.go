package ingress

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// StartProcessArgs is the transport-agnostic input to StartProcessCore — the
// fields the StartProcess RPC carries, shared by the RPC shell and the REST
// process facade (invoke_http.go). Version may be empty (the REST facade
// dispatches by name only).
type StartProcessArgs struct {
	Name        string
	Kind        string // "bpmn" | "cmmn"
	Version     string
	InstanceKey string
	Vars        []byte
}

// StartProcessCore launches a new reflwos BPMN/CMMN instance. It routes by
// (model name, instance_key) — the same scheme the worker's procSession and
// ChildStart use — and proposes a start ProcessEvent (model_ref + kind set,
// which makes the apply path create the instance record). When the caller
// leaves instance_key empty the server mints a random one; a caller-supplied
// key makes the start idempotent (the apply path drops a start for an
// already-existing instance, and the deterministic producerID dedups retries).
// Returns (partition_key, instance_key). The non-RPC core extracted from the
// StartProcess RPC; errors are connect.Errors.
func (s *Server) StartProcessCore(ctx context.Context, a StartProcessArgs) (uint64, string, error) {
	if a.Name == "" {
		return 0, "", connect.NewError(connect.CodeInvalidArgument, errors.New("model_ref.name is required"))
	}
	kind, err := processKindFromString(a.Kind)
	if err != nil {
		return 0, "", connect.NewError(connect.CodeInvalidArgument, err)
	}

	instanceKey := a.InstanceKey
	if instanceKey == "" {
		k, kerr := mintProcessInstanceKey()
		if kerr != nil {
			return 0, "", connect.NewError(connect.CodeInternal, fmt.Errorf("mint instance key: %w", kerr))
		}
		instanceKey = k
	}

	pk := routing.PartitionKey(a.Name, instanceKey)
	shardID := s.host.Partitioner().ShardForKey(pk)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return 0, "", connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}

	cmd := &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk:          pk,
		Service:     a.Name,
		InstanceKey: instanceKey,
		Payload:     &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_External{External: a.Vars}},
		ModelRef:    &enginev1.ModelRef{Kind: a.Kind, Name: a.Name, Version: a.Version},
		Kind:        kind,
	}}}
	// Deterministic per (instance pk, key): a retried StartProcess dedups at the
	// Raft session layer; the apply-path existing-instance guard is the
	// authoritative backstop for cross-node races.
	producerID := "startproc/" + strconv.FormatUint(pk, 16) + "/" + instanceKey
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return 0, "", connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return 0, "", connect.NewError(connect.CodeInternal, fmt.Errorf("propose start process: %w", err))
	}
	return pk, instanceKey, nil
}

// StartProcess is the Connect RPC shell over StartProcessCore.
func (s *Server) StartProcess(ctx context.Context, req *connect.Request[ingressv1.StartProcessRequest]) (*connect.Response[ingressv1.StartProcessResponse], error) {
	msg := req.Msg
	mr := msg.GetModelRef()
	pk, instanceKey, err := s.StartProcessCore(ctx, StartProcessArgs{
		Name:        mr.GetName(),
		Kind:        mr.GetKind(),
		Version:     mr.GetVersion(),
		InstanceKey: msg.GetInstanceKey(),
		Vars:        msg.GetVars(),
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&ingressv1.StartProcessResponse{Pk: pk, InstanceKey: instanceKey}), nil
}

// DeliverMessage correlates an inbound message/signal to parked process
// instances. It routes by (message_name, correlation_key) — the same
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

	pk := routing.PartitionKey(msg.GetMessageName(), msg.GetCorrelationKey())
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

// ResolveProcessIncident resolves an incident-parked instance, routed by
// (model name, instance_key) like StartProcess. TERMINATE fails the instance
// terminally; RETRY re-drives the failed element (Phase 2b — rejected as
// unimplemented until the reflwos resume entry lands). Delivered at-least-once
// (unique producerID); the apply path no-ops a resolve for a non-incident
// instance, so re-delivery is safe.
func (s *Server) ResolveProcessIncident(ctx context.Context, req *connect.Request[ingressv1.ResolveProcessIncidentRequest]) (*connect.Response[ingressv1.ResolveProcessIncidentResponse], error) {
	msg := req.Msg
	name := msg.GetModelRef().GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("model_ref.name is required"))
	}
	if msg.GetInstanceKey() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("instance_key is required"))
	}
	switch msg.GetResolution() {
	case enginev1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_TERMINATE:
		// supported
	case enginev1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_RETRY:
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("RETRY resolution is not yet supported"))
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("resolution must be RETRY or TERMINATE"))
	}

	pk := routing.PartitionKey(name, msg.GetInstanceKey())
	shardID := s.host.Partitioner().ShardForKey(pk)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_ResolveProcessIncident{ResolveProcessIncident: &enginev1.ResolveProcessIncident{
		Pk:          pk,
		Service:     name,
		InstanceKey: msg.GetInstanceKey(),
		Resolution:  msg.GetResolution(),
		VarPatch:    msg.GetVarPatch(),
	}}}
	nonce, err := mintNonce()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("mint nonce: %w", err))
	}
	producerID := "incident/" + nonce
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("propose resolve process incident: %w", err))
	}
	return connect.NewResponse(&ingressv1.ResolveProcessIncidentResponse{Accepted: true}), nil
}

// GetProcessInstance performs a linearizable read of one instance's record from
// the partition owning (model name, instance_key) — the same routing
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

	pk := routing.PartitionKey(name, msg.GetInstanceKey())
	shardID := s.host.Partitioner().ShardForKey(pk)
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupProcessInstance{
		Service:     name,
		InstanceKey: msg.GetInstanceKey(),
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
		resp.Output = r.Record.GetOutput()
		resp.CreatedAtMs = r.Record.GetCreatedAtMs()
		resp.EndedAtMs = r.Record.GetEndedAtMs()
		resp.Incident = r.Record.GetIncident() // set iff status == INCIDENT
	}
	return connect.NewResponse(resp), nil
}

// ListProcessInstances lists the deployment's process instances: one
// whole-namespace scan per partition shard via the shared fanOut substrate,
// merging up to limit rows.
func (s *Server) ListProcessInstances(ctx context.Context, req *connect.Request[ingressv1.ListProcessInstancesRequest]) (*connect.Response[ingressv1.ListProcessInstancesResponse], error) {
	msg := req.Msg
	cur, err := decodePageToken(msg.GetPageToken())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	limit := clampListLimit(int(msg.GetLimit()))
	var out []*ingressv1.ProcessInstanceSummary
	var nextToken string
	ferr := s.fanOut(ctx, cur,
		func(shard uint64, after []byte) any {
			return engine.LookupProcessInstances{
				Service:         msg.GetModelRef().GetName(),
				StatusFilter:    msg.GetStatusFilter(),
				CreatedAfterMs:  msg.GetCreatedAfterMs(),
				CreatedBeforeMs: msg.GetCreatedBeforeMs(),
				After:           after,
				Limit:           limit,
			}
		},
		func(shard uint64, res any) (bool, error) {
			r, ok := res.(engine.ProcessInstancesLookupResult)
			if !ok {
				return false, fmt.Errorf("unexpected result type %T", res)
			}
			for _, si := range r.Instances {
				out = append(out, &ingressv1.ProcessInstanceSummary{
					Service:     si.Service,
					InstanceKey: si.InstanceKey,
					Status:      si.Record.GetStatus(),
					Kind:        si.Record.GetKind(),
					ActiveSeq:   si.Record.GetActiveSeq(),
					NextSeq:     si.Record.GetNextSeq(),
					Outstanding: si.Record.GetOutstanding(),
					CreatedAtMs: si.Record.GetCreatedAtMs(),
					EndedAtMs:   si.Record.GetEndedAtMs(),
				})
				if len(out) >= limit {
					lp := keys.LPFromPartitionKey(routing.PartitionKey(si.Service, si.InstanceKey))
					nextToken = encodePageToken(shard, keys.ProcessInstanceKey(lp, si.Service, si.InstanceKey))
					return true, nil
				}
			}
			return false, nil
		},
	)
	if ferr != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list process instances: %w", ferr))
	}
	return connect.NewResponse(&ingressv1.ListProcessInstancesResponse{Instances: out, NextPageToken: nextToken}), nil
}

// GetProcessInstanceHistoryCore reads one instance's activity timeline, routed by
// (model name, instance_key) — the same routing GetProcessInstance uses. present
// is false when no record exists (never started or reaped). nextAfterSeq is the
// cursor to pass back to page forward, 0 when the page did not fill to limit (end
// of timeline). limit is clamped to the server list cap. The non-RPC core shared
// by the RPC shell and the REST history facade; errors are connect.Errors.
func (s *Server) GetProcessInstanceHistoryCore(ctx context.Context, name, instanceKey string, afterSeq uint64, limit int) (bool, []*enginev1.ProcessHistoryEvent, uint64, error) {
	if name == "" {
		return false, nil, 0, connect.NewError(connect.CodeInvalidArgument, errors.New("model_ref.name is required"))
	}
	if instanceKey == "" {
		return false, nil, 0, connect.NewError(connect.CodeInvalidArgument, errors.New("instance_key is required"))
	}
	limit = clampListLimit(limit)
	pk := routing.PartitionKey(name, instanceKey)
	shardID := s.host.Partitioner().ShardForKey(pk)
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupProcessInstanceHistory{
		Service:     name,
		InstanceKey: instanceKey,
		AfterSeq:    afterSeq,
		Limit:       limit,
	})
	if err != nil {
		return false, nil, 0, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup process instance history: %w", err))
	}
	r, ok := res.(engine.ProcessInstanceHistoryLookupResult)
	if !ok {
		return false, nil, 0, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup process instance history: unexpected type %T", res))
	}
	var nextAfterSeq uint64
	if len(r.Events) >= limit {
		nextAfterSeq = r.Events[len(r.Events)-1].GetSeq()
	}
	return r.Present, r.Events, nextAfterSeq, nil
}

// GetProcessInstanceHistory is the Connect RPC shell over
// GetProcessInstanceHistoryCore — a linearizable, paged read of one instance's
// activity timeline. It proposes nothing.
func (s *Server) GetProcessInstanceHistory(ctx context.Context, req *connect.Request[ingressv1.GetProcessInstanceHistoryRequest]) (*connect.Response[ingressv1.GetProcessInstanceHistoryResponse], error) {
	msg := req.Msg
	present, events, nextAfterSeq, err := s.GetProcessInstanceHistoryCore(
		ctx, msg.GetModelRef().GetName(), msg.GetInstanceKey(), msg.GetAfterSeq(), int(msg.GetLimit()))
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&ingressv1.GetProcessInstanceHistoryResponse{
		Present:      present,
		Events:       events,
		NextAfterSeq: nextAfterSeq,
	}), nil
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
