package iflowengine

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/twinfer/iflow/bpmn"
	"github.com/twinfer/reflow/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

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
		return nil, fmt.Errorf("iflowengine: new token engine: %w", err)
	}

	var cmds []bpmn.Command
	var state *bpmn.ExecutionState
	if len(rec.GetStateBlob()) == 0 {
		// First turn: the External payload carries the start variables.
		vars, derr := decodeVars(in.Entry.GetPayload().GetExternal())
		if derr != nil {
			return nil, fmt.Errorf("iflowengine: decode start vars: %w", derr)
		}
		cmds, state = eng.Start(vars)
	} else {
		state = new(bpmn.ExecutionState)
		if uerr := json.Unmarshal(rec.GetStateBlob(), state); uerr != nil {
			return nil, fmt.Errorf("iflowengine: unmarshal state: %w", uerr)
		}
		ev, eerr := eventForBPMN(in.Entry.GetPayload())
		if eerr != nil {
			return nil, eerr
		}
		var aerr error
		cmds, state, aerr = eng.Advance(state, ev)
		if aerr != nil {
			return nil, fmt.Errorf("iflowengine: advance: %w", aerr)
		}
	}

	newState, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("iflowengine: marshal state: %w", err)
	}
	return a.translateBPMN(in, graph, cmds, newState)
}

// childInstanceKey mints a deterministic key for a CallActivity child instance
// from (parent instance key, node, MI instance). reflow's apply path uses this
// key verbatim and dedups on it, so it must reproduce identically on replay.
func childInstanceKey(parentKey, nodeID, instanceID string) string {
	return fmt.Sprintf("%s/%s/%s", parentKey, nodeID, instanceID)
}

// isTimerWaitNode reports whether nodeID names a timer catch (an intermediate
// catch event or a boundary event carrying a TimerEventDefinition). A CancelWait
// carries no timer/signal discriminator, so the graph is the only way to tell
// whether the wait being torn down was a timer (→ TimerCancel) or a signal /
// message wait (→ nothing, since reflow doesn't actuate Subscribe yet).
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

// eventForBPMN decodes a ProcessEventPayload into the iflow EngineEvent that
// drives the continuation Advance. (The first-turn External-as-start-vars case is
// handled in advanceBPMN before this is reached.)
func eventForBPMN(p *enginev1.ProcessEventPayload) (bpmn.EngineEvent, error) {
	switch of := p.GetOf().(type) {
	case *enginev1.ProcessEventPayload_External:
		return decodeBPMNExternalEvent(of.External)
	case *enginev1.ProcessEventPayload_TaskCompleted:
		tc := of.TaskCompleted
		if tc.GetFailed() {
			return bpmn.TaskFailed{NodeID: tc.GetNodeId(), InstanceID: tc.GetInstanceIdx(), Cause: tc.GetFailureMessage()}, nil
		}
		outputs, err := decodeVars(tc.GetOutput())
		if err != nil {
			return nil, fmt.Errorf("iflowengine: decode task output: %w", err)
		}
		return bpmn.ServiceTaskCompleted{NodeID: tc.GetNodeId(), InstanceID: tc.GetInstanceIdx(), Outputs: outputs}, nil
	case *enginev1.ProcessEventPayload_TimerFired:
		tf := of.TimerFired
		return bpmn.TimerFired{NodeID: tf.GetNodeId()}, nil
	case *enginev1.ProcessEventPayload_ChildCompleted:
		cc := of.ChildCompleted
		if cc.GetFailed() {
			// A failed child = a failed CallActivity; TaskFailed lets an error
			// boundary on the call-activity node catch it.
			return bpmn.TaskFailed{NodeID: cc.GetNodeId(), InstanceID: cc.GetInstanceIdx(), Cause: cc.GetFailureMessage()}, nil
		}
		outputs, err := decodeVars(cc.GetOutput())
		if err != nil {
			return nil, fmt.Errorf("iflowengine: decode child output: %w", err)
		}
		return bpmn.SubProcessCompleted{NodeID: cc.GetNodeId(), InstanceID: cc.GetInstanceIdx(), Outputs: outputs}, nil
	default:
		return nil, fmt.Errorf("iflowengine: empty process event payload")
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

	// Terminal wins: a completed/failed process emits only its terminal. reflow
	// ignores non-terminal fields when Terminal is set, so any cleanup the engine
	// emitted alongside the terminal (e.g. a CancelWait for a boundary timer
	// cleared on completion) is dropped. That is safe: the reaped instance's
	// record is deleted, and a later stale TimerFired finds no record and is
	// dropped by the partition (enqueueInstanceEvent drops events for absent
	// instances). reflow GC'ing instance timers on terminal would avoid the
	// wasted fire, but it is not a correctness issue.
	for _, c := range cmds {
		switch t := c.(type) {
		case bpmn.ProcessCompleted:
			out, err := encodeVars(t.Outputs)
			if err != nil {
				return nil, fmt.Errorf("iflowengine: encode process outputs: %w", err)
			}
			adv.Terminal = &enginev1.ProcessTerminal{Output: out}
			return adv, nil
		case bpmn.ProcessFailed:
			adv.Terminal = &enginev1.ProcessTerminal{
				Failed:         true,
				FailureMessage: fmt.Sprintf("process failed at %q: %s", t.NodeID, t.Cause),
			}
			return adv, nil
		}
	}

	tenant := tenantOf(in.Pk)
	for _, c := range cmds {
		switch t := c.(type) {
		case bpmn.RunServiceTask:
			input, err := encodeBridgeInput(t.ServiceRef, t.Inputs, t.ExtensionsXML, tenant)
			if err != nil {
				return nil, fmt.Errorf("iflowengine: encode bridge input: %w", err)
			}
			adv.Invoke = append(adv.Invoke, &enginev1.TaskInvoke{
				NodeId:      t.NodeID,
				InstanceIdx: t.InstanceID,
				Target:      bridgeTarget(),
				Input:       input,
			})
		case bpmn.WaitForSignal:
			// reflow does not actuate Subscribe yet (the deferred inbound
			// correlation read path); emit it forward-looking and harmless.
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
			// timers on one node; iflow correlates by (NodeID, InstanceID) and
			// has no slot, so the single-timer-per-node case is slot 0.
			if t.InstanceID != "" {
				return nil, fmt.Errorf("iflowengine: multi-instance timer not yet supported (node %q instance %q)", t.NodeID, t.InstanceID)
			}
			if t.Repeat != 0 {
				return nil, fmt.Errorf("iflowengine: timer cycle (repeat=%d) not yet supported (node %q)", t.Repeat, t.NodeID)
			}
			adv.ArmTimer = append(adv.ArmTimer, &enginev1.TimerArm{
				NodeId:   t.NodeID,
				FireAtMs: in.Entry.GetLogicalTimeMs() + uint64(t.Duration.Milliseconds()),
				Slot:     0,
			})
		case bpmn.CancelWait:
			// CancelWait tears down a timer OR a signal/message wait. Only timer
			// waits have a reflow actuation (TimerCancel); signal subscriptions
			// aren't actuated yet, so there is nothing to cancel for those.
			if isTimerWaitNode(graph, t.NodeID) {
				adv.CancelTimer = append(adv.CancelTimer, &enginev1.TimerCancel{NodeId: t.NodeID, Slot: 0})
			}
		case bpmn.SpawnSubProcess:
			// Only CallActivity spawns a child instance (CalledElement set);
			// embedded subprocesses run inline in the engine and emit no command.
			if t.CalledElement == "" {
				return nil, fmt.Errorf("iflowengine: subprocess spawn with no calledElement (node %q)", t.NodeID)
			}
			childRef, err := a.models.ChildRef(in.Record.GetModelRef(), "bpmn", t.CalledElement)
			if err != nil {
				return nil, fmt.Errorf("iflowengine: resolve child %q: %w", t.CalledElement, err)
			}
			vars, err := encodeVars(t.Vars)
			if err != nil {
				return nil, fmt.Errorf("iflowengine: encode child vars: %w", err)
			}
			adv.StartChild = append(adv.StartChild, &enginev1.ChildStart{
				NodeId:      t.NodeID,
				ModelRef:    childRef,
				Kind:        enginev1.ProcessKind_PROCESS_KIND_BPMN,
				InstanceKey: childInstanceKey(in.InstanceKey, t.NodeID, t.InstanceID),
				Vars:        vars,
			})
		case bpmn.RunUserTask:
			return nil, fmt.Errorf("iflowengine: RunUserTask (human task) not supported (node %q)", t.NodeID)
		default:
			return nil, fmt.Errorf("iflowengine: unsupported BPMN command %T", c)
		}
	}
	return adv, nil
}
