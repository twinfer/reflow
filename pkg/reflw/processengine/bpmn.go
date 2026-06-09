package processengine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflwos/bpmn"
)

// escalationPrefix tags a child ProcessFailed cause that originated from an
// uncaught escalation throw / escalation end event (reflwos bpmn/messages.go's
// escalationErrPrefix; reflwos bpmn/events_intermediate.go emits
// ProcessFailed{Cause:"escalation:CODE"}). reflwos delegates the cross-process
// escalation surface to "the workflow driver" (bpmn/throws.go, bpmn/messages.go)
// — for reflw that driver is this adapter: it promotes such a cause into the
// bridgeFault code channel so a calling process's CallActivity escalation
// boundary catches it via TaskFailed.ErrorCode (bpmn/advance.go advanceTaskFailed).
const escalationPrefix = "escalation:"

// advanceBPMN runs one turn of the BPMN reducer: load the (cached) graph, build
// an engine pinned to the turn's logical clock, Start (first turn) or Advance
// (continuation), reserialize state, and translate the emitted commands.
func (a *Adapter) advanceBPMN(in invoker.ProcessAdvanceInput) (*enginev1.ProcessAdvanced, error) {
	rec := in.Record
	graph, err := a.models.BPMN(rec.GetModelRef())
	if err != nil {
		return nil, err
	}

	logical := in.Entry.GetLogicalTimeMs()
	eng, err := bpmn.NewTokenEngine(graph,
		bpmn.WithClock(func() time.Time { return time.UnixMilli(int64(logical)) }),
		bpmn.WithDecisionResolver(a.models.BPMNDecisions(rec.GetModelRef())),
	)
	if err != nil {
		return nil, fmt.Errorf("processengine: new token engine: %w", err)
	}

	var cmds []bpmn.Command
	var state *bpmn.ExecutionState
	if len(rec.GetStateBlob()) == 0 {
		// First turn: the External payload carries the start variables.
		vars, derr := decodeVars(in.Entry.GetPayload().GetExternal())
		if derr != nil {
			return nil, fmt.Errorf("processengine: decode start vars: %w", derr)
		}
		cmds, state = eng.Start(vars)
	} else {
		state = new(bpmn.ExecutionState)
		if uerr := json.Unmarshal(rec.GetStateBlob(), state); uerr != nil {
			return nil, fmt.Errorf("processengine: unmarshal state: %w", uerr)
		}
		ev, eerr := eventForBPMN(in.Entry.GetPayload())
		if eerr != nil {
			return nil, eerr
		}
		var aerr error
		cmds, state, aerr = eng.Advance(state, ev)
		if aerr != nil {
			return nil, fmt.Errorf("processengine: advance: %w", aerr)
		}
	}

	newState, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("processengine: marshal state: %w", err)
	}
	adv, err := a.translateBPMN(in, graph, cmds, newState)
	if err != nil {
		return nil, err
	}
	adv.Awaiting = awaitingBPMN(graph, state)
	return adv, nil
}

// awaitingBPMN converts the engine's parked user-task list into the proto carrier
// the apply path mirrors onto ProcessInstanceRecord.awaiting. Rides every turn so
// the persisted set always reflects the current parked tasks.
func awaitingBPMN(graph *bpmn.ProcessGraph, state *bpmn.ExecutionState) []*enginev1.AwaitingTask {
	src := bpmn.AwaitingExternalTasks(graph, state)
	if len(src) == 0 {
		return nil
	}
	out := make([]*enginev1.AwaitingTask, len(src))
	for i, w := range src {
		out[i] = &enginev1.AwaitingTask{NodeId: w.NodeID, Name: w.Name}
	}
	return out
}

// childInstanceKey mints a deterministic key for a CallActivity child instance
// from (parent instance key, node, MI instance). reflw's apply path uses this
// key verbatim and dedups on it, so it must reproduce identically on replay.
// Uniqueness holds because nodeID and instanceID are BPMN/CMMN ids (XML
// NCNames, never containing '/'): the tuple is recoverable by splitting on the
// last two separators, so a '/'-laden parentKey (a nested child's own key) can't
// alias a different (parent, node, instance) triple.
func childInstanceKey(parentKey, nodeID, instanceID string) string {
	return fmt.Sprintf("%s/%s/%s", parentKey, nodeID, instanceID)
}

// isTimerWaitNode reports whether nodeID names a timer catch (an intermediate
// catch event or a boundary event carrying a TimerEventDefinition). A CancelWait
// carries no timer/signal discriminator, so the graph is the only way to tell
// whether the wait being torn down was a timer (→ TimerCancel) or a signal /
// message wait (→ nothing, since reflw doesn't actuate Subscribe yet).
func isTimerWaitNode(graph *bpmn.ProcessGraph, nodeID string) bool {
	node := graph.Nodes[nodeID]
	if node == nil {
		return false
	}
	switch n := node.Node.(type) {
	case *bpmn.IntermediateCatchEvent:
		return len(n.TimerEventDefinitions) > 0
	case *bpmn.BoundaryEvent:
		return len(n.TimerEventDefinitions) > 0
	}
	return false
}

// slotForInstance encodes a reflwos MI instance id into the reflw timer slot. A
// reflwos instance id is a stringified non-negative int ("" for a non-MI scalar
// wait); the +1 offset keeps the non-MI case ("" → slot 0) distinct from MI
// instance 0 (→ slot 1) so the fire side can recover the exact instance. The slot
// rides the durable timer key (processTimerID) and the fired ProcessTimerFired, so
// N instances parked at one timer node get N distinct durable rows and fire
// independently — the engine treats slot as an opaque disambiguator.
func slotForInstance(instanceID string) uint32 {
	if instanceID == "" {
		return 0
	}
	n, err := strconv.Atoi(instanceID)
	if err != nil || n < 0 {
		return 0
	}
	return uint32(n) + 1
}

// instanceForSlot inverts slotForInstance: slot 0 → "" (non-MI), slot k>0 → the
// MI instance id itoa(k-1). Used by eventForBPMN to recover the firing instance
// from ProcessTimerFired.slot so advanceWaitFired advances the right instance token.
func instanceForSlot(slot uint32) string {
	if slot == 0 {
		return ""
	}
	return strconv.Itoa(int(slot) - 1)
}

// eventForBPMN decodes a ProcessEventPayload into the reflwos EngineEvent that
// drives the continuation Advance. (The first-turn External-as-start-vars case is
// handled in advanceBPMN before this is reached.)
func eventForBPMN(p *enginev1.ProcessEventPayload) (bpmn.EngineEvent, error) {
	switch of := p.GetOf().(type) {
	case *enginev1.ProcessEventPayload_External:
		return decodeBPMNExternalEvent(of.External)
	case *enginev1.ProcessEventPayload_TaskCompleted:
		tc := of.TaskCompleted
		if tc.GetFailed() {
			// A coded capability fault rides the message as a bridgeFault envelope;
			// split it so an error boundary keyed on that code can catch it (empty
			// code → catch-all).
			code, cause := decodeBridgeFault(tc.GetFailureMessage())
			return bpmn.TaskFailed{NodeID: tc.GetNodeId(), InstanceID: tc.GetInstanceIdx(), ErrorCode: code, Cause: cause}, nil
		}
		outputs, err := decodeVars(tc.GetOutput())
		if err != nil {
			return nil, fmt.Errorf("processengine: decode task output: %w", err)
		}
		return bpmn.ServiceTaskCompleted{NodeID: tc.GetNodeId(), InstanceID: tc.GetInstanceIdx(), Outputs: outputs}, nil
	case *enginev1.ProcessEventPayload_TimerFired:
		// Recover the MI instance from the slot the arm encoded, so a per-instance
		// timer inside an MI subprocess body advances the right instance's token
		// (advanceWaitFired is instance-aware). Non-MI → slot 0 → InstanceID "".
		tf := of.TimerFired
		return bpmn.TimerFired{NodeID: tf.GetNodeId(), InstanceID: instanceForSlot(tf.GetSlot())}, nil
	case *enginev1.ProcessEventPayload_ChildCompleted:
		cc := of.ChildCompleted
		if cc.GetFailed() {
			// A failed child = a failed CallActivity. Split the coded fault the
			// same way the service-task path does, so an escalation the child
			// threw (ProcessFailed{Cause:"escalation:CODE"}, promoted into the
			// envelope by encodeProcessFailure) reaches the call-activity's
			// escalation boundary via TaskFailed.ErrorCode; a plain failure
			// decodes to ErrorCode "" (the catch-all error boundary).
			code, cause := decodeBridgeFault(cc.GetFailureMessage())
			return bpmn.TaskFailed{NodeID: cc.GetNodeId(), InstanceID: cc.GetInstanceIdx(), ErrorCode: code, Cause: cause}, nil
		}
		outputs, err := decodeVars(cc.GetOutput())
		if err != nil {
			return nil, fmt.Errorf("processengine: decode child output: %w", err)
		}
		return bpmn.SubProcessCompleted{NodeID: cc.GetNodeId(), InstanceID: cc.GetInstanceIdx(), Outputs: outputs}, nil
	case *enginev1.ProcessEventPayload_MessageReceived:
		// A correlated inbound message resuming a parked WaitForSignal. reflw's
		// read path already did the (message_name, correlation_key) matching and
		// stamped the node_id; the reflwos engine matches purely by NodeID and
		// merges Payload into the instance variables.
		mr := of.MessageReceived
		payload, err := decodeVars(mr.GetPayload())
		if err != nil {
			return nil, fmt.Errorf("processengine: decode message payload: %w", err)
		}
		return bpmn.SignalReceived{NodeID: mr.GetNodeId(), Payload: payload}, nil
	case *enginev1.ProcessEventPayload_Retry:
		// An operator incident RETRY: re-drive the parked node, merging the
		// optional variable patch first (so a corrected gateway condition / task
		// input takes effect). The apply path only enqueues this for BPMN.
		r := of.Retry
		vars, err := decodeVars(r.GetVarPatch())
		if err != nil {
			return nil, fmt.Errorf("processengine: decode retry var patch: %w", err)
		}
		return bpmn.RetryIncident{NodeID: r.GetNodeId(), Vars: vars}, nil
	default:
		return nil, fmt.Errorf("processengine: unset process event payload (no event in inbox entry)")
	}
}

// translateBPMN maps the engine's emitted commands onto a ProcessAdvanced.
func (a *Adapter) translateBPMN(in invoker.ProcessAdvanceInput, graph *bpmn.ProcessGraph, cmds []bpmn.Command, newState []byte) (*enginev1.ProcessAdvanced, error) {
	adv := &enginev1.ProcessAdvanced{
		Pk:          in.Pk,
		Service:     in.Service,
		InstanceKey: in.InstanceKey,
		NewState:    newState,
	}

	// Terminal wins: a completed/failed process emits only its terminal. reflw
	// ignores non-terminal fields when Terminal is set, so any cleanup the engine
	// emitted alongside the terminal (e.g. a CancelWait for a boundary timer
	// cleared on completion) is dropped. That is safe: the reaped instance's
	// record is deleted, and a later stale TimerFired finds no record and is
	// dropped by the partition (enqueueInstanceEvent drops events for absent
	// instances). reflw GC'ing instance timers on terminal would avoid the
	// wasted fire, but it is not a correctness issue.
	for _, c := range cmds {
		switch t := c.(type) {
		case bpmn.ProcessCompleted:
			out, err := encodeVars(t.Outputs)
			if err != nil {
				return nil, fmt.Errorf("processengine: encode process outputs: %w", err)
			}
			// retention_ms carries the model's Camunda historyTimeToLive,
			// resolved at model-materialize time (0 when undeclared → the engine
			// deletes the terminal record immediately).
			adv.Terminal = &enginev1.ProcessTerminal{
				Output:      out,
				RetentionMs: a.retentionMs(in.Record.GetModelRef()),
			}
			return adv, nil
		case bpmn.ProcessFailed:
			// A genuine (non-escalation) uncaught failure parks the instance as an
			// incident — child or top-level alike — retaining new_state so a
			// ResolveProcessIncident RETRY can re-drive the failed node. A child
			// parks on its own failing element (siblings preserved) rather than
			// terminating; the parent stays blocked awaiting the child's completion,
			// and a TERMINATE on the child's incident is the operator's
			// deliver-to-parent escape hatch.
			//
			// An escalation is cross-process by definition: it terminates so it
			// delivers to the parent's CallActivity boundary (encodeProcessFailure
			// promotes it to a bridgeFault code), or — at the top level, with
			// nowhere left to propagate — fails the instance terminally.
			if !strings.HasPrefix(t.Cause, escalationPrefix) {
				adv.Incident = &enginev1.ProcessIncident{NodeId: t.NodeID, Cause: t.Cause}
				return adv, nil
			}
			adv.Terminal = &enginev1.ProcessTerminal{
				Failed:         true,
				FailureMessage: encodeProcessFailure(t.NodeID, t.Cause),
				RetentionMs:    a.retentionMs(in.Record.GetModelRef()),
			}
			return adv, nil
		}
	}

	for _, c := range cmds {
		switch t := c.(type) {
		case bpmn.RunServiceTask:
			input, err := encodeBridgeInput(t.ServiceRef, t.Inputs, t.ExtensionsXML)
			if err != nil {
				return nil, fmt.Errorf("processengine: encode bridge input: %w", err)
			}
			adv.Invoke = append(adv.Invoke, &enginev1.TaskInvoke{
				NodeId:      t.NodeID,
				InstanceIdx: t.InstanceID,
				Target:      bridgeTarget(),
				Input:       input,
			})
		case bpmn.WaitForSignal:
			// A message/signal catch. reflw actuates Subscribe: the apply path
			// writes a MessageSubscription keyed by (message_name, correlation_key)
			// on the partition owning that routing key, and a later DeliverMessage
			// resumes this node via ProcessMessageReceived. A message catch sets
			// MessageName + CorrelationKey; a signal catch sets SignalRef and no
			// correlation (an empty key → name-only broadcast).
			name := t.MessageName
			if name == "" {
				name = t.SignalRef
			}
			adv.Subscribe = append(adv.Subscribe, &enginev1.SignalSubscribe{
				NodeId:         t.NodeID,
				MessageName:    name,
				CorrelationKey: t.CorrelationKey,
			})
		case bpmn.WaitForCondition:
			// Engine-internal placeholder; re-evaluated intra-engine on the next
			// VariablesUpdated. Nothing for the driver to actuate.
		case bpmn.CancelActivity:
			// An interrupting boundary event fired on this activity's host: tear
			// down whatever it had in flight — a service-task invocation or a
			// child process/case — by node id, the same silent by-id cancel
			// CMMN's CancelTask uses (the engine already moved the token off the
			// activity, so no completion is wanted). InvokeCancel has no instance
			// dimension, so an MI activity cancels all instances' work by node;
			// per-instance precision rides the same instance_idx plumbing as the
			// MI-timer path. A CancelActivity for a node with nothing in flight
			// (e.g. a user task) naturally no-ops in the apply scan.
			adv.CancelInvoke = append(adv.CancelInvoke, &enginev1.InvokeCancel{NodeId: t.NodeID})
		case bpmn.WaitForTimer:
			// FireAtMs is absolute logical time. Slot disambiguates multiple timers
			// on one node; reflwos correlates by (NodeID, InstanceID), so the slot
			// carries the MI instance (slotForInstance) — a per-instance timer inside
			// an MI subprocess body thus gets its own durable timer row and fires
			// independently. A non-MI wait is slot 0.
			//
			// A TimeCycle (Repeat != 0) re-arms itself: reflwos re-emits a fresh
			// WaitForTimer on every TimerFired — with the remaining count carried
			// in its durable ExecutionState — until the count is exhausted, so each
			// repetition arrives here as an independent one-shot arm. The adapter
			// owns no cycle bookkeeping; it just arms the next fire at FireAtMs. The
			// prior fire already deleted its row and the re-arm's FireAtMs is later,
			// so re-arming the same (node, slot) never collides on the timer key.
			adv.ArmTimer = append(adv.ArmTimer, &enginev1.TimerArm{
				NodeId:   t.NodeID,
				FireAtMs: in.Entry.GetLogicalTimeMs() + uint64(t.Duration.Milliseconds()),
				Slot:     slotForInstance(t.InstanceID),
			})
		case bpmn.CancelWait:
			// CancelWait tears down a timer OR a signal/message wait. A timer-wait
			// node maps to TimerCancel; any other catch maps to SignalUnsubscribe,
			// and the engine resolves the node id to the forward MessageSubscription
			// via its per-instance reverse index and deletes it (same-shard or via
			// the outbox). The catch's correlation key is not re-derivable here,
			// which is why the instruction carries only the node id.
			if isTimerWaitNode(graph, t.NodeID) {
				// Same slot encoding as the arm so the cancel targets this instance's
				// durable timer row (processTimerID folds in the slot).
				adv.CancelTimer = append(adv.CancelTimer, &enginev1.TimerCancel{NodeId: t.NodeID, Slot: slotForInstance(t.InstanceID)})
			} else {
				adv.Unsubscribe = append(adv.Unsubscribe, &enginev1.SignalUnsubscribe{NodeId: t.NodeID})
			}
		case bpmn.SpawnSubProcess:
			// Only CallActivity spawns a child instance (CalledElement set);
			// embedded subprocesses run inline in the engine and emit no command.
			if t.CalledElement == "" {
				return nil, fmt.Errorf("processengine: subprocess spawn with no calledElement (node %q)", t.NodeID)
			}
			childRef, err := a.models.ChildRef(in.Record.GetModelRef(), "bpmn", t.CalledElement)
			if err != nil {
				return nil, fmt.Errorf("processengine: resolve child %q: %w", t.CalledElement, err)
			}
			vars, err := encodeVars(t.Vars)
			if err != nil {
				return nil, fmt.Errorf("processengine: encode child vars: %w", err)
			}
			adv.StartChild = append(adv.StartChild, &enginev1.ChildStart{
				NodeId:      t.NodeID,
				ModelRef:    childRef,
				Kind:        enginev1.ProcessKind_PROCESS_KIND_BPMN,
				InstanceKey: childInstanceKey(in.InstanceKey, t.NodeID, t.InstanceID),
				Vars:        vars,
			})
		case bpmn.RunUserTask:
			// Passive: the engine parks the user task; a person completes it later,
			// arriving as an external ProcessEvent (UserTaskCompleted, delivered via
			// the DeliverProcessEvent ingress RPC) that eventForBPMN turns back into
			// a completion the reflwos engine handles identically to a service task
			// (engine.go: UserTaskCompleted and ServiceTaskCompleted share one apply
			// path). Nothing to actuate now — same shape as cmmn.RunHumanTask.
		default:
			return nil, fmt.Errorf("processengine: unsupported BPMN command %T", c)
		}
	}
	return adv, nil
}
