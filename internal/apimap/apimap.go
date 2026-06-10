// Package apimap maps internal enginev1 (Raft/on-disk) messages to apiv1
// (public front-end view) DTOs. The functions are pure and depend only on the
// two proto packages — no engine/runtime deps — so they are trivially
// unit-testable and importable by both internal/ingress (data plane) and the
// merged admin server (control plane).
//
// Enum casts rely on apiv1 enum values being numerically equal to their
// enginev1 counterparts (apimap_test.go asserts this for every value); a plain
// conversion therefore preserves meaning. Message mappers return nil for nil
// input.
package apimap

import (
	apiv1 "github.com/twinfer/reflw/proto/apiv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// --- enum casts (values kept equal; see apimap_test.go) ---

// InvocationState maps the engine invocation state to the view enum.
func InvocationState(s enginev1.InvocationState) apiv1.InvocationState {
	return apiv1.InvocationState(s)
}

// ProcessStatus maps the engine process status to the view enum.
func ProcessStatus(s enginev1.ProcessStatus) apiv1.ProcessStatus {
	return apiv1.ProcessStatus(s)
}

// ProcessKind maps the engine process kind to the view enum.
func ProcessKind(k enginev1.ProcessKind) apiv1.ProcessKind {
	return apiv1.ProcessKind(k)
}

// ProcessHistoryKind maps the engine history-event kind to the view enum.
func ProcessHistoryKind(k enginev1.ProcessHistoryKind) apiv1.ProcessHistoryKind {
	return apiv1.ProcessHistoryKind(k)
}

// LPTransferPhase maps the engine transfer phase to the view enum.
func LPTransferPhase(p enginev1.LPTransferPhase) apiv1.LPTransferPhase {
	return apiv1.LPTransferPhase(p)
}

// IncidentResolutionToEngine maps the view resolution enum (a request input)
// back to the engine enum.
func IncidentResolutionToEngine(r apiv1.ProcessIncidentResolution) enginev1.ProcessIncidentResolution {
	return enginev1.ProcessIncidentResolution(r)
}

// --- invocation views ---

// InvocationView builds a list/summary view. The caller passes the canonical
// id string (FormatInvocationID) so apimap stays free of the format logic.
func InvocationView(idStr string, t *enginev1.InvocationTarget, state enginev1.InvocationState, deploymentID string, createdMs, completedMs uint64) *apiv1.InvocationView {
	v := &apiv1.InvocationView{
		InvocationId:  idStr,
		State:         InvocationState(state),
		DeploymentId:  deploymentID,
		CreatedAtMs:   createdMs,
		CompletedAtMs: completedMs,
	}
	if t != nil {
		v.Service = t.GetServiceName()
		v.Handler = t.GetHandlerName()
		v.ObjectKey = t.GetObjectKey()
	}
	return v
}

// InvocationStatusView flattens the enginev1.InvocationStatus oneof into the
// discriminated view (DescribeInvocation).
func InvocationStatusView(st *enginev1.InvocationStatus) *apiv1.InvocationStatusView {
	if st == nil {
		return nil
	}
	v := &apiv1.InvocationStatusView{
		DeploymentId: st.GetDeploymentId(),
		Kind:         st.GetKind(),
	}
	switch {
	case st.GetScheduled() != nil:
		s := st.GetScheduled()
		v.State = apiv1.InvocationState_INVOCATION_STATE_SCHEDULED
		setTarget(v, s.GetTarget())
		v.CreatedAtMs = s.GetCreatedAtMs()
	case st.GetInvoked() != nil:
		in := st.GetInvoked()
		v.State = apiv1.InvocationState_INVOCATION_STATE_INVOKED
		setTarget(v, in.GetTarget())
		v.CreatedAtMs = in.GetCreatedAtMs()
	case st.GetSuspended() != nil:
		su := st.GetSuspended()
		v.State = apiv1.InvocationState_INVOCATION_STATE_SUSPENDED
		setTarget(v, su.GetTarget())
		v.AwaitingOn = su.GetAwaitingOn()
	case st.GetCompleted() != nil:
		c := st.GetCompleted()
		v.State = apiv1.InvocationState_INVOCATION_STATE_COMPLETED
		setTarget(v, c.GetTarget())
		v.Completed = true
		v.Output = c.GetOutput()
		v.FailureMessage = c.GetFailureMessage()
		v.FailureCode = c.GetFailureCode()
		v.CompletedAtMs = c.GetCompletedAtMs()
	default: // Free / unset
		v.State = apiv1.InvocationState_INVOCATION_STATE_UNSPECIFIED
	}
	return v
}

func setTarget(v *apiv1.InvocationStatusView, t *enginev1.InvocationTarget) {
	if t == nil {
		return
	}
	v.Service = t.GetServiceName()
	v.Handler = t.GetHandlerName()
	v.ObjectKey = t.GetObjectKey()
}

// --- process views ---

// ProcessInstanceView maps a process instance record. AwaitingTasks is left
// unset — the caller (ingress) fills it, since minting resume tokens needs the
// storage-keys codec apimap deliberately doesn't import.
func ProcessInstanceView(rec *enginev1.ProcessInstanceRecord, service, instanceKey string) *apiv1.ProcessInstanceView {
	if rec == nil {
		return nil
	}
	return &apiv1.ProcessInstanceView{
		Service:     service,
		InstanceKey: instanceKey,
		Status:      ProcessStatus(rec.GetStatus()),
		Kind:        ProcessKind(rec.GetKind()),
		ActiveSeq:   rec.GetActiveSeq(),
		NextSeq:     rec.GetNextSeq(),
		Outstanding: rec.GetOutstanding(),
		Output:      rec.GetOutput(),
		CreatedAtMs: rec.GetCreatedAtMs(),
		EndedAtMs:   rec.GetEndedAtMs(),
		Incident:    ProcessIncidentView(rec.GetIncident()),
	}
}

// ProcessIncidentView maps an incident; nil in, nil out.
func ProcessIncidentView(in *enginev1.ProcessIncident) *apiv1.ProcessIncidentView {
	if in == nil {
		return nil
	}
	return &apiv1.ProcessIncidentView{
		NodeId:     in.GetNodeId(),
		Cause:      in.GetCause(),
		RaisedAtMs: in.GetRaisedAtMs(),
	}
}

// ProcessHistoryEventView maps one timeline event.
func ProcessHistoryEventView(e *enginev1.ProcessHistoryEvent) *apiv1.ProcessHistoryEventView {
	if e == nil {
		return nil
	}
	return &apiv1.ProcessHistoryEventView{
		Seq:            e.GetSeq(),
		Kind:           ProcessHistoryKind(e.GetKind()),
		NodeId:         e.GetNodeId(),
		TsMs:           e.GetTsMs(),
		Failed:         e.GetFailed(),
		FailureMessage: e.GetFailureMessage(),
		InstanceIdx:    e.GetInstanceIdx(),
		Detail:         e.GetDetail(),
	}
}

// ProcessHistoryEventViews maps a slice of timeline events.
func ProcessHistoryEventViews(in []*enginev1.ProcessHistoryEvent) []*apiv1.ProcessHistoryEventView {
	if len(in) == 0 {
		return nil
	}
	out := make([]*apiv1.ProcessHistoryEventView, 0, len(in))
	for _, e := range in {
		out = append(out, ProcessHistoryEventView(e))
	}
	return out
}

// --- admin views ---

// DeploymentView maps a deployment record.
func DeploymentView(d *enginev1.DeploymentRecord) *apiv1.DeploymentView {
	if d == nil {
		return nil
	}
	v := &apiv1.DeploymentView{
		Id:                    d.GetId(),
		Url:                   d.GetUrl(),
		RegisteredAtMs:        d.GetRegisteredAtMs(),
		MaxJournalEntries:     d.GetMaxJournalEntries(),
		InvocationRetentionMs: d.GetInvocationRetentionMs(),
		WorkflowRetentionMs:   d.GetWorkflowRetentionMs(),
	}
	if h := d.GetHandlers(); len(h) > 0 {
		v.Handlers = make([]*apiv1.DeploymentHandlerView, 0, len(h))
		for _, dh := range h {
			v.Handlers = append(v.Handlers, &apiv1.DeploymentHandlerView{
				Service: dh.GetService(),
				Handler: dh.GetHandler(),
				Kind:    dh.GetKind(),
			})
		}
	}
	return v
}

// DeploymentViews maps a slice of deployment records.
func DeploymentViews(in []*enginev1.DeploymentRecord) []*apiv1.DeploymentView {
	if len(in) == 0 {
		return nil
	}
	out := make([]*apiv1.DeploymentView, 0, len(in))
	for _, d := range in {
		out = append(out, DeploymentView(d))
	}
	return out
}

// SecretView maps a secret record — name + ciphertext/KEK pointers, never
// plaintext (the engine record carries none either).
func SecretView(s *enginev1.SecretRecord) *apiv1.SecretView {
	if s == nil {
		return nil
	}
	v := &apiv1.SecretView{Name: s.GetName()}
	if re := s.GetRemoteEncrypted(); re != nil {
		v.BlobUri = re.GetBlobUri()
		v.KekUri = re.GetKekUri()
	}
	return v
}

// SecretViews maps a slice of secret records.
func SecretViews(in []*enginev1.SecretRecord) []*apiv1.SecretView {
	if len(in) == 0 {
		return nil
	}
	out := make([]*apiv1.SecretView, 0, len(in))
	for _, s := range in {
		out = append(out, SecretView(s))
	}
	return out
}

// ModelView maps a model record (XML source not surfaced).
func ModelView(m *enginev1.ModelRecord) *apiv1.ModelView {
	if m == nil {
		return nil
	}
	v := &apiv1.ModelView{RegisteredAtMs: m.GetRegisteredAtMs()}
	if r := m.GetModelRef(); r != nil {
		v.Kind = r.GetKind()
		v.Name = r.GetName()
		v.Version = r.GetVersion()
	}
	return v
}

// ModelViews maps a slice of model records.
func ModelViews(in []*enginev1.ModelRecord) []*apiv1.ModelView {
	if len(in) == 0 {
		return nil
	}
	out := make([]*apiv1.ModelView, 0, len(in))
	for _, m := range in {
		out = append(out, ModelView(m))
	}
	return out
}

// NodeView maps a node membership record.
func NodeView(n *enginev1.NodeMembership) *apiv1.NodeView {
	if n == nil {
		return nil
	}
	return &apiv1.NodeView{
		NodeId:     n.GetNodeId(),
		RaftAddr:   n.GetRaftAddr(),
		NodeHostId: n.GetNodeHostId(),
		LastSeenMs: n.GetLastSeenMs(),
	}
}

// NodeViews maps a slice of node membership records.
func NodeViews(in []*enginev1.NodeMembership) []*apiv1.NodeView {
	if len(in) == 0 {
		return nil
	}
	out := make([]*apiv1.NodeView, 0, len(in))
	for _, n := range in {
		out = append(out, NodeView(n))
	}
	return out
}

// PartitionTableView flattens the engine partition table (a shard->ReplicaSet
// map) into a stable view. Shard order is not guaranteed (the caller sorts if
// it needs determinism).
func PartitionTableView(t *enginev1.PartitionTable) *apiv1.PartitionTableView {
	if t == nil {
		return nil
	}
	v := &apiv1.PartitionTableView{AssignmentEpoch: t.GetAssignmentEpoch()}
	for shardID, rs := range t.GetShards() {
		v.Shards = append(v.Shards, &apiv1.PartitionView{
			ShardId: shardID,
			NodeIds: rs.GetNodeIds(),
		})
	}
	if mr := t.GetMetaReplicas(); mr != nil {
		v.MetaReplicas = mr.GetNodeIds()
	}
	return v
}

// LPTransferView maps an LP transfer record.
func LPTransferView(r *enginev1.LPTransferRecord) *apiv1.LPTransferView {
	if r == nil {
		return nil
	}
	return &apiv1.LPTransferView{
		TransferId:  r.GetTransferId(),
		Lp:          r.GetLp(),
		SourceShard: r.GetSourceShard(),
		DestShard:   r.GetDestShard(),
		Phase:       LPTransferPhase(r.GetPhase()),
		StartedAtMs: r.GetStartedAtMs(),
		LastEventMs: r.GetLastEventMs(),
	}
}

// LPTransferViews maps a slice of LP transfer records.
func LPTransferViews(in []*enginev1.LPTransferRecord) []*apiv1.LPTransferView {
	if len(in) == 0 {
		return nil
	}
	out := make([]*apiv1.LPTransferView, 0, len(in))
	for _, r := range in {
		out = append(out, LPTransferView(r))
	}
	return out
}
