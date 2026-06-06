package processengine

import (
	"encoding/json"
	"fmt"
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
	return a.translateBPMN(in, graph, cmds, newState)
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
		tf := of.TimerFired
		return bpmn.TimerFired{NodeID: tf.GetNodeId()}, nil
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
			// A child's failure (and any escalation, which is cross-process by
			// definition) terminates so it delivers to its parent — the parent's
			// CallActivity boundary may catch it (encodeProcessFailure promotes an
			// escalation into a bridgeFault code) or the parent fails in turn. Only a
			// top-level, non-escalation uncaught failure — one with nowhere left to
			// propagate — parks as an incident (non-terminal), retaining new_state so
			// a ResolveProcessIncident RETRY can re-drive the failed element.
			if incidentEligible(in.Record) && !strings.HasPrefix(t.Cause, escalationPrefix) {
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
			// No in-flight-invocation cancel on the process plane; a late task
			// completion for a torn-down token is ignored by the engine.
		case bpmn.WaitForTimer:
			// FireAtMs is absolute logical time. Slot disambiguates multiple
			// timers on one node; reflwos correlates by (NodeID, InstanceID) and
			// has no slot, so the single-timer-per-node case is slot 0.
			if t.InstanceID != "" {
				return nil, fmt.Errorf("processengine: multi-instance timer not yet supported (node %q instance %q)", t.NodeID, t.InstanceID)
			}
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
				Slot:     0,
			})
		case bpmn.CancelWait:
			// CancelWait tears down a timer OR a signal/message wait. A timer-wait
			// node maps to TimerCancel; any other catch maps to SignalUnsubscribe,
			// and the engine resolves the node id to the forward MessageSubscription
			// via its per-instance reverse index and deletes it (same-shard or via
			// the outbox). The catch's correlation key is not re-derivable here,
			// which is why the instruction carries only the node id.
			if isTimerWaitNode(graph, t.NodeID) {
				adv.CancelTimer = append(adv.CancelTimer, &enginev1.TimerCancel{NodeId: t.NodeID, Slot: 0})
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
			return nil, fmt.Errorf("processengine: RunUserTask (human task) not supported (node %q)", t.NodeID)
		default:
			return nil, fmt.Errorf("processengine: unsupported BPMN command %T", c)
		}
	}
	return adv, nil
}
