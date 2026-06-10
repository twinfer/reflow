package ingress

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

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

// DeliverProcessEventArgs is the transport-agnostic input to
// DeliverProcessEventCore — shared by the RPC shell and the REST process facade.
type DeliverProcessEventArgs struct {
	Name        string
	InstanceKey string
	EventKind   string
	Payload     []byte
}

// DeliverProcessEventCore injects an external typed engine event into a running
// instance, routed by (name, instance_key) like StartProcess. It wraps
// (event_kind, payload) in the external-event envelope the worker's adapter
// decodes (processengine.decodeBPMNExternalEvent / decodeCMMNExternalEvent) and
// proposes a continuation ProcessEvent — NO model_ref, so the apply path appends
// to the running instance's inbox rather than creating one; an absent or terminal
// instance is a benign drop (enqueueInstanceEvent). It is the human-task /
// user-event / variable-update completion path. Returns the routed partition_key;
// errors are connect.Errors. The non-RPC core extracted from the RPC shell.
func (s *Server) DeliverProcessEventCore(ctx context.Context, a DeliverProcessEventArgs) (uint64, error) {
	if a.Name == "" {
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("model_ref.name is required"))
	}
	if a.InstanceKey == "" {
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("instance_key is required"))
	}
	if a.EventKind == "" {
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("event_kind is required"))
	}
	envelope, err := encodeExternalEventEnvelope(a.EventKind, a.Payload)
	if err != nil {
		return 0, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("encode external event: %w", err))
	}

	pk := routing.PartitionKey(a.Name, a.InstanceKey)
	shardID := s.host.Partitioner().ShardForKey(pk)
	runner := s.host.Partition(shardID)
	if runner == nil {
		return 0, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}

	cmd := &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk:          pk,
		Service:     a.Name,
		InstanceKey: a.InstanceKey,
		Payload:     &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_External{External: envelope}},
		// No ModelRef/Kind: this is a continuation event for an existing
		// instance, not a start — the apply path appends it to the inbox.
	}}}
	nonce, err := mintNonce()
	if err != nil {
		return 0, connect.NewError(connect.CodeInternal, fmt.Errorf("mint nonce: %w", err))
	}
	producerID := "procevent/" + nonce
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return 0, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return 0, connect.NewError(connect.CodeInternal, fmt.Errorf("propose deliver process event: %w", err))
	}
	return pk, nil
}

// DeliverProcessEvent is the Connect RPC shell over DeliverProcessEventCore. When
// resume_token is set it instead routes through CompleteTaskCore (the typed
// resume-token consume path): event_kind is the action ("fail" → fail, else
// complete) and payload is the output vars (or, when failing, the failure message).
func (s *Server) DeliverProcessEvent(ctx context.Context, req *connect.Request[ingressv1.DeliverProcessEventRequest]) (*connect.Response[ingressv1.DeliverProcessEventResponse], error) {
	msg := req.Msg
	if tok := msg.GetResumeToken(); tok != "" {
		args := CompleteTaskArgs{ResumeToken: tok, Fail: msg.GetEventKind() == "fail"}
		if args.Fail {
			args.FailureMessage = string(msg.GetPayload())
		} else {
			args.Output = msg.GetPayload()
		}
		pk, err := s.CompleteTaskCore(ctx, args)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&ingressv1.DeliverProcessEventResponse{Pk: pk, Accepted: true}), nil
	}
	pk, err := s.DeliverProcessEventCore(ctx, DeliverProcessEventArgs{
		Name:        msg.GetModelRef().GetName(),
		InstanceKey: msg.GetInstanceKey(),
		EventKind:   msg.GetEventKind(),
		Payload:     msg.GetPayload(),
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&ingressv1.DeliverProcessEventResponse{Pk: pk, Accepted: true}), nil
}

// CompleteTaskArgs is the transport-agnostic input to CompleteTaskCore — shared by
// the resume-token RPC path (DeliverProcessEvent with resume_token set) and the
// REST facade (POST /v1/tasks/{token}).
type CompleteTaskArgs struct {
	ResumeToken    string
	Output         []byte // JSON output vars merged on completion (ignored when Fail)
	Fail           bool   // complete-as-failed — drives an error boundary / incident
	FailureMessage string
}

// CompleteTaskCore completes (or fails) a parked BPMN user task / CMMN human task
// addressed by an opaque resume token — the consume half of the resume-token flow.
// It decodes the token to (partition_key, name, instance_key, node_id), validates
// against live state with one linearizable read (the instance must be present,
// RUNNING, and still list node_id in its awaiting set — exactly what
// GetProcessInstance would have surfaced — else the token is stale, already
// completed, or forged: FailedPrecondition), then proposes the typed
// ProcessEvent{task_completed} both adapters map plane-agnostically (BPMN
// ServiceTaskCompleted / CMMN TaskCompleted, both keyed by node_id). The caller
// supplies only outputs; node_id rides the token, so there is no
// BPMN-NodeID-vs-CMMN-PlanItemID field to get wrong. task_invocation_id is left
// unset, so the apply path treats this as external input (no proc_invoke_idx drop,
// no outstanding decrement). Returns the routed partition_key; errors are
// connect.Errors. The non-RPC core shared by the RPC shell and the REST facade.
func (s *Server) CompleteTaskCore(ctx context.Context, a CompleteTaskArgs) (uint64, error) {
	tgt, err := keys.DecodeResumeToken(a.ResumeToken)
	if err != nil {
		return 0, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("resume token: %w", err))
	}
	// The token embeds partition_key for self-routing; recompute the canonical pk
	// from (service, instance_key) and reject a mismatch so a corrupt/forged token
	// can't address a different shard than the instance actually lives on.
	pk := routing.PartitionKey(tgt.Service, tgt.InstanceKey)
	if pk != tgt.PartitionKey {
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("resume token routing mismatch"))
	}
	shardID := s.host.Partitioner().ShardForKey(pk)

	// Validate against live state: the task must still be parked and awaiting input.
	// Mirrors ResolveProcessIncident's RETRY precondition read.
	ctx, cancel := ensureReadDeadline(ctx)
	defer cancel()
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupProcessInstance{
		Service:     tgt.Service,
		InstanceKey: tgt.InstanceKey,
	})
	if err != nil {
		return 0, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup process instance: %w", err))
	}
	r, ok := res.(engine.ProcessInstanceLookupResult)
	if !ok {
		return 0, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup process instance: unexpected type %T", res))
	}
	if !r.Present || r.Record.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING ||
		!awaitingContains(r.Record.GetAwaiting(), tgt.NodeID) {
		return 0, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("task %q is not awaiting input (already completed, or the instance is not running)", tgt.NodeID))
	}

	runner := s.host.Partition(shardID)
	if runner == nil {
		return 0, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no partition for shard %d", shardID))
	}
	tc := &enginev1.ProcessTaskCompleted{NodeId: tgt.NodeID}
	if a.Fail {
		tc.Failed = true
		tc.FailureMessage = a.FailureMessage
	} else {
		tc.Output = a.Output
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk:          pk,
		Service:     tgt.Service,
		InstanceKey: tgt.InstanceKey,
		Payload:     &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{TaskCompleted: tc}},
		// No ModelRef/Kind: a continuation event for the existing instance.
	}}}
	nonce, err := mintNonce()
	if err != nil {
		return 0, connect.NewError(connect.CodeInternal, fmt.Errorf("mint nonce: %w", err))
	}
	// Fresh nonce per call (not idempotent by id): the SyncRead validation above is
	// the dedup guard — a re-submitted token whose task already completed no longer
	// appears in the awaiting set and is rejected before this propose.
	producerID := "resumetask/" + nonce
	if err := runner.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return 0, connect.NewError(connect.CodeUnavailable, errors.New("shard closed"))
		}
		return 0, connect.NewError(connect.CodeInternal, fmt.Errorf("propose complete task: %w", err))
	}
	return pk, nil
}

// GetTaskCore resolves a parked task by opaque resume token — the discovery (read)
// half of the resume-token flow, the GET counterpart to CompleteTaskCore. It
// decodes the token to (partition_key, name, instance_key, node_id) and validates
// against live state with one linearizable read exactly as CompleteTaskCore does
// (present, RUNNING, node_id still in the awaiting set — else the token is stale,
// already completed, or forged: FailedPrecondition), then returns the task
// descriptor. When a schema resolver is wired it best-effort enriches the response
// with the task's submission JSON Schema, resolved against the instance's pinned
// model_ref; a schema-resolution failure is logged and omitted (the descriptor is
// authoritative). Read-only — proposes nothing. The non-RPC core behind GET
// /v1/tasks/{token}; errors are connect.Errors.
func (s *Server) GetTaskCore(ctx context.Context, resumeToken string) (*ingressv1.GetTaskResponse, error) {
	tgt, err := keys.DecodeResumeToken(resumeToken)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("resume token: %w", err))
	}
	// Recompute the canonical pk and reject a mismatch — same forgery guard as
	// CompleteTaskCore so the read can't address a shard the instance never lived on.
	pk := routing.PartitionKey(tgt.Service, tgt.InstanceKey)
	if pk != tgt.PartitionKey {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("resume token routing mismatch"))
	}
	shardID := s.host.Partitioner().ShardForKey(pk)

	ctx, cancel := ensureReadDeadline(ctx)
	defer cancel()
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupProcessInstance{
		Service:     tgt.Service,
		InstanceKey: tgt.InstanceKey,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup process instance: %w", err))
	}
	r, ok := res.(engine.ProcessInstanceLookupResult)
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("lookup process instance: unexpected type %T", res))
	}
	aw := awaitingTask(r.Record.GetAwaiting(), tgt.NodeID)
	if !r.Present || r.Record.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING || aw == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("task %q is not awaiting input (already completed, or the instance is not running)", tgt.NodeID))
	}
	resp := &ingressv1.GetTaskResponse{
		Service:     tgt.Service,
		InstanceKey: tgt.InstanceKey,
		NodeId:      tgt.NodeID,
		Name:        aw.GetName(),
	}
	if s.schemaResolver != nil {
		resp.Schema = s.taskSchema(ctx, r.Record.GetModelRef(), tgt.NodeID)
	}
	return resp, nil
}

// taskSchema best-effort resolves a parked task's submission JSON Schema via the
// injected resolver and decodes it into a structpb.Struct for the response. Any
// failure (model unresolvable, no typed contract, decode error) yields nil — the
// descriptor stands on its own. Schema generation is a reflwos (gateway) concern;
// the bytes are forwarded opaquely.
func (s *Server) taskSchema(ctx context.Context, modelRef *enginev1.ModelRef, nodeID string) *structpb.Struct {
	b, err := s.schemaResolver.TaskSchema(ctx, modelRef, nodeID)
	if err != nil {
		s.log.Debug("ingress: task schema unavailable", "node", nodeID, "err", err)
		return nil
	}
	if len(b) == 0 {
		return nil
	}
	var st structpb.Struct
	if err := protojson.Unmarshal(b, &st); err != nil {
		s.log.Warn("ingress: task schema decode failed", "node", nodeID, "err", err)
		return nil
	}
	return &st
}

// awaitingContains reports whether node is in the record's persisted awaiting set.
func awaitingContains(awaiting []*enginev1.AwaitingTask, node string) bool {
	return awaitingTask(awaiting, node) != nil
}

// awaitingTask returns the record's awaiting entry for node, or nil when absent.
func awaitingTask(awaiting []*enginev1.AwaitingTask, node string) *enginev1.AwaitingTask {
	for _, aw := range awaiting {
		if aw.GetNodeId() == node {
			return aw
		}
	}
	return nil
}

// encodeExternalEventEnvelope wraps a reflwos engine-event (kind discriminator +
// JSON body) in the external-event envelope the worker's process adapter decodes
// on a continuation turn. The shape — {"kind","payload"} — is a stable wire
// contract: the decode counterpart is processengine.externalEvent (and it mirrors
// dboshost/wire's BPMNEventEnvelope). This layer cannot import the pkg/ adapter
// (internal must not depend on pkg/*), so the two-field shape is replicated here;
// keep the JSON tags in lockstep with processengine.externalEvent. An empty
// payload normalizes to "{}" so json.RawMessage stays valid.
func encodeExternalEventEnvelope(kind string, payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	return json.Marshal(struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Kind: kind, Payload: json.RawMessage(payload)})
}

// ResolveProcessIncident resolves an incident-parked instance, routed by
// (model name, instance_key) like StartProcess. TERMINATE fails the instance
// terminally; RETRY re-drives the failed element (a linearizable lookup gates it
// so a non-incident instance gets a precise error instead of a silent no-op).
// Delivered at-least-once (unique producerID); the
// apply path no-ops a resolve for a non-incident instance, so re-delivery is
// safe (a duplicate RETRY whose first delivery already un-parked finds the
// instance RUNNING and is dropped).
func (s *Server) ResolveProcessIncident(ctx context.Context, req *connect.Request[ingressv1.ResolveProcessIncidentRequest]) (*connect.Response[ingressv1.ResolveProcessIncidentResponse], error) {
	msg := req.Msg
	name := msg.GetModelRef().GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("model_ref.name is required"))
	}
	if msg.GetInstanceKey() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("instance_key is required"))
	}
	retry := false
	switch msg.GetResolution() {
	case enginev1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_TERMINATE:
		// supported
	case enginev1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_RETRY:
		retry = true
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("resolution must be RETRY or TERMINATE"))
	}

	pk := routing.PartitionKey(name, msg.GetInstanceKey())
	shardID := s.host.Partitioner().ShardForKey(pk)

	if retry {
		// RETRY needs an incident-parked instance (BPMN or CMMN). Look it up so the
		// operator gets a precise error instead of a silently no-op'd resolve (the
		// apply path drops a retry for a non-incident instance).
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
		if !r.Present || r.Record.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_INCIDENT {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("instance is not incident-parked"))
		}
	}

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

// GetProcessInstanceCore performs a linearizable read of one instance's record
// from the partition owning (model name, instance_key) — the same routing
// StartProcess uses. It proposes nothing; it exists so a caller without an await
// RPC can observe whether an instance is running, parked, or terminal-and-reaped.
// When the instance is present and RUNNING it mints one resume token per parked
// task in rec.Awaiting (the discovery half of the resume-token flow). The apply
// path mirrors the awaiting set onto the record on a normal turn but does not
// clear it on incident-park / terminal, so gating token minting on RUNNING keeps a
// stale set from leaking as live resume points. The non-RPC core shared by the
// RPC shell and the REST instance facade; errors are connect.Errors.
func (s *Server) GetProcessInstanceCore(ctx context.Context, name, instanceKey string) (*ingressv1.GetProcessInstanceResponse, error) {
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("model_ref.name is required"))
	}
	if instanceKey == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("instance_key is required"))
	}

	pk := routing.PartitionKey(name, instanceKey)
	shardID := s.host.Partitioner().ShardForKey(pk)
	ctx, cancel := ensureReadDeadline(ctx)
	defer cancel()
	res, err := s.host.NodeHost().SyncRead(ctx, shardID, engine.LookupProcessInstance{
		Service:     name,
		InstanceKey: instanceKey,
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
		if r.Record.GetStatus() == enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
			resp.AwaitingTasks = awaitingTaskInfos(pk, name, instanceKey, r.Record.GetAwaiting())
		}
	}
	return resp, nil
}

// GetProcessInstance is the Connect RPC shell over GetProcessInstanceCore.
func (s *Server) GetProcessInstance(ctx context.Context, req *connect.Request[ingressv1.GetProcessInstanceRequest]) (*connect.Response[ingressv1.GetProcessInstanceResponse], error) {
	resp, err := s.GetProcessInstanceCore(ctx, req.Msg.GetModelRef().GetName(), req.Msg.GetInstanceKey())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

// awaitingTaskInfos maps a record's parked-task set to the ingress surface,
// minting a self-routing resume token per task so an external caller completes it
// by token alone (POST /v1/tasks/{token}) without naming the inner element. A task
// whose fields overflow the token codec's u16 ceiling is skipped (unreachable for
// real model names / instance keys / node ids) rather than failing the read.
func awaitingTaskInfos(pk uint64, name, instanceKey string, awaiting []*enginev1.AwaitingTask) []*ingressv1.AwaitingTaskInfo {
	if len(awaiting) == 0 {
		return nil
	}
	out := make([]*ingressv1.AwaitingTaskInfo, 0, len(awaiting))
	for _, aw := range awaiting {
		tok, err := keys.MintResumeToken(pk, name, instanceKey, aw.GetNodeId())
		if err != nil {
			continue
		}
		out = append(out, &ingressv1.AwaitingTaskInfo{
			NodeId:      aw.GetNodeId(),
			Name:        aw.GetName(),
			ResumeToken: tok,
		})
	}
	return out
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
	ctx, cancel := ensureReadDeadline(ctx)
	defer cancel()
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
