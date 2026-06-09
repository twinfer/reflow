package processengine

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/twinfer/reflw/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflwos/capability"
	"github.com/twinfer/reflwos/cmmn"
)

// advanceCMMN runs one turn of the CMMN reducer. It mirrors advanceBPMN against
// SentryEngine/CaseState; note Start returns a 3-tuple with an error, and the
// engine correlates by PlanItemID rather than NodeID.
func (a *Adapter) advanceCMMN(in invoker.ProcessAdvanceInput) (*enginev1.ProcessAdvanced, error) {
	rec := in.Record
	def, err := a.models.CMMN(rec.GetModelRef())
	if err != nil {
		return nil, err
	}

	logical := in.Entry.GetLogicalTimeMs()
	eng, err := cmmn.NewSentryEngine(def,
		cmmn.WithDecisionResolver(a.models.CMMNDecisions(rec.GetModelRef())),
		cmmn.WithClock(func() time.Time {
			return time.UnixMilli(int64(logical))
		}))
	if err != nil {
		return nil, fmt.Errorf("processengine: new sentry engine: %w", err)
	}

	var cmds []cmmn.Command
	var state *cmmn.CaseState
	if len(rec.GetStateBlob()) == 0 {
		vars, derr := decodeVars(in.Entry.GetPayload().GetExternal())
		if derr != nil {
			return nil, fmt.Errorf("processengine: decode start vars: %w", derr)
		}
		var serr error
		cmds, state, serr = eng.Start(vars)
		if serr != nil {
			return nil, fmt.Errorf("processengine: cmmn start: %w", serr)
		}
	} else {
		state = new(cmmn.CaseState)
		if uerr := json.Unmarshal(rec.GetStateBlob(), state); uerr != nil {
			return nil, fmt.Errorf("processengine: unmarshal case state: %w", uerr)
		}
		ev, eerr := eventForCMMN(in.Entry.GetPayload())
		if eerr != nil {
			return nil, eerr
		}
		if node, ok := heldNodeForSuspended(state, ev); ok {
			// The targeted item is Suspended; CMMN §7.6.1 requires the host to defer
			// its completion until ManualResume. Decline to advance (state unchanged)
			// and emit a hold so the apply path buffers this event in proc_held and
			// replays it on resume — feeding it now fires triggerComplete/triggerFault,
			// which PISuspended rejects, failing the whole case.
			return &enginev1.ProcessAdvanced{
				Pk:            in.Pk,
				Service:       in.Service,
				InstanceKey:   in.InstanceKey,
				NewState:      rec.GetStateBlob(),
				HoldEventNode: node,
				Awaiting:      awaitingCMMN(def, state),
			}, nil
		}
		var aerr error
		cmds, state, aerr = eng.Advance(state, ev)
		if aerr != nil {
			return nil, fmt.Errorf("processengine: cmmn advance: %w", aerr)
		}
	}

	newState, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("processengine: marshal case state: %w", err)
	}
	adv, err := a.translateCMMN(in, cmds, state, newState)
	if err != nil {
		return nil, err
	}
	adv.Awaiting = awaitingCMMN(def, state)
	return adv, nil
}

// awaitingCMMN converts the engine's parked human-task list into the proto carrier
// the apply path mirrors onto ProcessInstanceRecord.awaiting. Rides every turn
// (including the suspend-hold early return) so the persisted set stays current.
func awaitingCMMN(def *cmmn.CaseDefinition, state *cmmn.CaseState) []*enginev1.AwaitingTask {
	src := cmmn.AwaitingExternalTasks(def, state)
	if len(src) == 0 {
		return nil
	}
	out := make([]*enginev1.AwaitingTask, len(src))
	for i, w := range src {
		out[i] = &enginev1.AwaitingTask{NodeId: w.NodeID, Name: w.Name}
	}
	return out
}

// eventForCMMN decodes a ProcessEventPayload into the reflwos CMMN EngineEvent that
// drives the continuation Advance.
func eventForCMMN(p *enginev1.ProcessEventPayload) (cmmn.EngineEvent, error) {
	switch of := p.GetOf().(type) {
	case *enginev1.ProcessEventPayload_External:
		return decodeCMMNExternalEvent(of.External)
	case *enginev1.ProcessEventPayload_TaskCompleted:
		tc := of.TaskCompleted
		if tc.GetFailed() {
			// The shared bridge envelopes a coded fault into the message; cmmn.
			// TaskFailed has no error-code slot, so keep only the human cause.
			_, cause := decodeBridgeFault(tc.GetFailureMessage())
			return cmmn.TaskFailed{PlanItemID: tc.GetNodeId(), Cause: cause}, nil
		}
		outputs, err := decodeVars(tc.GetOutput())
		if err != nil {
			return nil, fmt.Errorf("processengine: decode task output: %w", err)
		}
		return cmmn.TaskCompleted{PlanItemID: tc.GetNodeId(), Outputs: outputs}, nil
	case *enginev1.ProcessEventPayload_TimerFired:
		return cmmn.TimerExpired{PlanItemID: of.TimerFired.GetNodeId()}, nil
	case *enginev1.ProcessEventPayload_ChildCompleted:
		cc := of.ChildCompleted
		if cc.GetFailed() {
			return cmmn.TaskFailed{PlanItemID: cc.GetNodeId(), Cause: cc.GetFailureMessage()}, nil
		}
		outputs, err := decodeVars(cc.GetOutput())
		if err != nil {
			return nil, fmt.Errorf("processengine: decode child output: %w", err)
		}
		return cmmn.TaskCompleted{PlanItemID: cc.GetNodeId(), Outputs: outputs}, nil
	case *enginev1.ProcessEventPayload_MessageReceived:
		// Unreachable in practice: the CMMN translation emits no SignalSubscribe,
		// so the read path never routes a ProcessMessageReceived to a case. Kept
		// explicit so the failure is legible if that ever changes.
		return nil, fmt.Errorf("processengine: CMMN message correlation not supported (plan item %q)", of.MessageReceived.GetNodeId())
	case *enginev1.ProcessEventPayload_Retry:
		// An operator incident RETRY: reactivate the faulted plan item
		// (CMMN reactivate: Failed → Active), merging the optional variable
		// patch first so the re-run reads the corrected inputs.
		r := of.Retry
		vars, err := decodeVars(r.GetVarPatch())
		if err != nil {
			return nil, fmt.Errorf("processengine: decode retry var patch: %w", err)
		}
		return cmmn.ManualReactivate{PlanItemID: r.GetNodeId(), Vars: vars}, nil
	default:
		return nil, fmt.Errorf("processengine: unset process event payload (no event in inbox entry)")
	}
}

// translateCMMN maps the engine's emitted commands onto a ProcessAdvanced.
func (a *Adapter) translateCMMN(in invoker.ProcessAdvanceInput, cmds []cmmn.Command, state *cmmn.CaseState, newState []byte) (*enginev1.ProcessAdvanced, error) {
	adv := &enginev1.ProcessAdvanced{
		Pk:          in.Pk,
		Service:     in.Service,
		InstanceKey: in.InstanceKey,
		NewState:    newState,
	}

	// Terminal wins (see translateBPMN for the cleanup-drop reasoning).
	for _, c := range cmds {
		switch t := c.(type) {
		case cmmn.CaseCompleted:
			out, err := encodeVars(t.Outputs)
			if err != nil {
				return nil, fmt.Errorf("processengine: encode case outputs: %w", err)
			}
			adv.Terminal = &enginev1.ProcessTerminal{
				Output:      out,
				RetentionMs: a.retentionMs(in.Record.GetModelRef()),
			}
			return adv, nil
		case cmmn.CaseFailed:
			// CaseFailed is a case-level hard error — a bad CaseInputs binding, a
			// missing decision resolver, a decode failure — terminal, not a
			// retry-able incident (re-running re-hits the broken model). A child
			// case's failure also terminates so it delivers to its parent. A
			// runtime plan-item fault is non-terminal and surfaces via
			// PlanItemFaulted / cmmnOpenIncident below, never here.
			adv.Terminal = &enginev1.ProcessTerminal{
				Failed:         true,
				FailureMessage: fmt.Sprintf("case failed at %q: %s", t.PlanItemID, t.Cause),
				RetentionMs:    a.retentionMs(in.Record.GetModelRef()),
			}
			return adv, nil
		case cmmn.CaseTerminated:
			// Exit-criterion termination is a clean end (matches dboshost).
			adv.Terminal = &enginev1.ProcessTerminal{
				RetentionMs: a.retentionMs(in.Record.GetModelRef()),
			}
			return adv, nil
		}
	}

	// A non-propagating plan-item fault (CMMN §8.4) leaves the case running its
	// other items and parks the instance as a non-terminal incident pinned to the
	// faulted item — child or top-level alike. Derived from the case state (any
	// PIFailed item), not this turn's PlanItemFaulted command, so the quiescence
	// turn that finally parks the instance still carries the open incident even
	// when the fault happened on an earlier turn (a parallel sibling finishing
	// later) or a prior item is still failed after a retry. Fall through to
	// actuate any sibling instructions; the engine parks only once the case is
	// quiescent, so siblings are not abandoned.
	//
	// CMMN has no escalation channel, so a child case never auto-delivers its
	// fault to the parent: it parks on its own deep element (siblings preserved)
	// and the parent's case-task stays blocked awaiting completion. A TERMINATE on
	// the child's incident is the operator's deliver-to-parent escape hatch (the
	// parent's case-task then faults in turn).
	if node, cause, ok := cmmnOpenIncident(state); ok {
		adv.Incident = &enginev1.ProcessIncident{NodeId: node, Cause: cause}
	}

	for _, c := range cmds {
		switch t := c.(type) {
		case cmmn.RunTask:
			if err := a.translateCMMNRunTask(in, adv, t); err != nil {
				return nil, err
			}
		case cmmn.RunProcessTask:
			childRef, err := a.models.ChildRef(in.Record.GetModelRef(), "bpmn", t.ProcessRef)
			if err != nil {
				return nil, fmt.Errorf("processengine: resolve process child %q: %w", t.ProcessRef, err)
			}
			vars, err := encodeVars(t.Vars)
			if err != nil {
				return nil, fmt.Errorf("processengine: encode child vars: %w", err)
			}
			adv.StartChild = append(adv.StartChild, &enginev1.ChildStart{
				NodeId:      t.PlanItemID,
				ModelRef:    childRef,
				Kind:        enginev1.ProcessKind_PROCESS_KIND_BPMN,
				InstanceKey: childInstanceKey(in.InstanceKey, t.PlanItemID, ""),
				Vars:        vars,
			})
		case cmmn.RunCaseTask:
			childRef, err := a.models.ChildRef(in.Record.GetModelRef(), "cmmn", t.CaseRef)
			if err != nil {
				return nil, fmt.Errorf("processengine: resolve case child %q: %w", t.CaseRef, err)
			}
			vars, err := encodeVars(t.Vars)
			if err != nil {
				return nil, fmt.Errorf("processengine: encode child vars: %w", err)
			}
			adv.StartChild = append(adv.StartChild, &enginev1.ChildStart{
				NodeId:      t.PlanItemID,
				ModelRef:    childRef,
				Kind:        enginev1.ProcessKind_PROCESS_KIND_CMMN,
				InstanceKey: childInstanceKey(in.InstanceKey, t.PlanItemID, ""),
				Vars:        vars,
			})
		case cmmn.RunHumanTask:
			// Passive: the case parks the human task; a person completes it later,
			// arriving as an external ProcessEvent{task_completed} that eventForCMMN
			// turns back into cmmn.TaskCompleted. Nothing to actuate now (same shape
			// as the user/event-listener kinds in translateCMMNRunTask).
		case cmmn.SuspendTask:
			// The engine moved an active item to PISuspended. Nothing to dispatch:
			// the host's CMMN §7.6.1 obligation is to DEFER this node's completion
			// until resume, which advanceCMMN does by buffering the inbox event —
			// keyed off the engine's own PISuspended state, not a separate flag, so
			// SuspendTask itself needs no instruction here.
		case cmmn.ResumeTask:
			// The item is Active again; release any completion buffered while it was
			// suspended so the apply path replays it as a fresh turn. A no-op in the
			// apply path when nothing was buffered (resumed before the task returned).
			adv.ReleaseHeldNode = append(adv.ReleaseHeldNode, t.PlanItemID)
		case cmmn.CancelTask:
			// An exit criterion / stage termination exited an active plan item this
			// turn. Tear down whatever it had in flight — a service-task invocation
			// or a child process/case instance — by node id; the engine cancels it
			// silently (the item is already terminal, so no feedback is wanted). A
			// CancelTask on a passive human task naturally no-ops (nothing in flight).
			adv.CancelInvoke = append(adv.CancelInvoke, &enginev1.InvokeCancel{NodeId: t.PlanItemID})
		case cmmn.CaseFileItemEventRejected:
			// Observational: a receiving instance rejected a broadcast CFI event
			// for an out-of-§A.5-state CFI. Nothing to actuate.
		case cmmn.PlanItemFaulted:
			// A plan item faulted. The incident is derived from the case state
			// (cmmnOpenIncident above), not from this command — nothing to
			// actuate here; the case keeps running its other items.
		default:
			return nil, fmt.Errorf("processengine: unsupported CMMN command %T", c)
		}
	}
	return adv, nil
}

// cmmnOpenIncident reports the case's open incident, if any: the first
// (sorted) plan item in the Failed state plus its own cause. A Failed item is
// a non-terminal, retry-able fault (CMMN §8.4) the instance parks on once
// quiescent. Both the pinned node and its cause are accurate even under
// multiple concurrent faults — CaseState.FailureCauses keys the cause per
// failed item, so the reported cause always belongs to the pinned node.
func cmmnOpenIncident(state *cmmn.CaseState) (node, cause string, ok bool) {
	if state == nil {
		return "", "", false
	}
	first := ""
	for id, st := range state.Items {
		if st != cmmn.PIFailed {
			continue
		}
		if first == "" || id < first {
			first = id
		}
	}
	if first == "" {
		return "", "", false
	}
	return first, state.FailureCauses[first], true
}

// translateCMMNRunTask handles the polymorphic RunTask: a leaf task → Invoke, a
// timer listener → ArmTimer, user/plain event listeners → park (no-op). These are
// the only kinds the engine emits as a RunTask (see emitRunCommand): a human task
// is its own RunHumanTask command; a decision task / milestone / stage are
// engine-internal (inline DMN eval / auto-complete / child cascade) and emit no
// command at all. The default below is a defensive guard, not a known gap.
func (a *Adapter) translateCMMNRunTask(in invoker.ProcessAdvanceInput, adv *enginev1.ProcessAdvanced, t cmmn.RunTask) error {
	switch t.Kind {
	case cmmn.KindTask:
		ref, err := cmmnTaskRef(t)
		if err != nil {
			return err
		}
		input, err := encodeBridgeInput(ref, t.Vars, t.ExtensionsXML)
		if err != nil {
			return fmt.Errorf("processengine: encode bridge input: %w", err)
		}
		adv.Invoke = append(adv.Invoke, &enginev1.TaskInvoke{
			NodeId: t.PlanItemID,
			Target: bridgeTarget(),
			Input:  input,
		})
	case cmmn.KindTimerEventListener:
		fireAt, err := cmmnTimerFireAt(in.Entry.GetLogicalTimeMs(), t)
		if err != nil {
			return err
		}
		adv.ArmTimer = append(adv.ArmTimer, &enginev1.TimerArm{NodeId: t.PlanItemID, FireAtMs: fireAt, Slot: 0})
	case cmmn.KindUserEventListener, cmmn.KindEventListener:
		// Passive: the case parks the listener; its occurrence arrives later as
		// an external event (the future delivery path). Nothing to actuate now.
	default:
		return fmt.Errorf("processengine: unsupported CMMN task kind %q (plan item %q)", t.Kind, t.PlanItemID)
	}
	return nil
}

// cmmnTimerFireAt resolves a timer listener's absolute fire instant: a relative
// duration is added to the turn's logical time; an absolute TimerAt is used
// verbatim.
func cmmnTimerFireAt(logicalMs uint64, t cmmn.RunTask) (uint64, error) {
	if t.TimerDuration > 0 {
		return logicalMs + uint64(t.TimerDuration.Milliseconds()), nil
	}
	if !t.TimerAt.IsZero() {
		return uint64(t.TimerAt.UnixMilli()), nil
	}
	return 0, fmt.Errorf("processengine: timer listener %q has neither duration nor absolute time", t.PlanItemID)
}

// cmmnTaskRef mirrors cmmnhost.taskRef: read <capability ref="…"/> from the
// task's extension elements.
func cmmnTaskRef(t cmmn.RunTask) (string, error) {
	var cfg struct {
		Capability struct {
			Ref string `xml:"ref,attr"`
		} `xml:"capability"`
	}
	if err := capability.DecodeExtension(t.ExtensionsXML, &cfg); err != nil {
		return "", fmt.Errorf("processengine: decode <capability> for %q: %w", t.PlanItemID, err)
	}
	if cfg.Capability.Ref == "" {
		return "", fmt.Errorf("processengine: task %q names no capability (set <capability ref=\"ns:op\"/>)", t.PlanItemID)
	}
	return cfg.Capability.Ref, nil
}

// heldNodeForSuspended reports the plan item a completion-class event targets
// when that item is currently Suspended — the events CMMN §7.6.1 requires the
// host to defer until ManualResume. Feeding them to the engine fires
// triggerComplete / triggerFault, which PISuspended rejects (cmmn/fsm.go
// OnUnhandledTrigger), failing the case. Control events (resume / suspend /
// terminate / reactivate) are never held — they are how suspension is lifted or
// changed. Returns ("", false) when the event is not a completion or its target
// is not suspended.
func heldNodeForSuspended(state *cmmn.CaseState, ev cmmn.EngineEvent) (string, bool) {
	if state == nil {
		return "", false
	}
	var node string
	switch e := ev.(type) {
	case cmmn.TaskCompleted:
		node = e.PlanItemID
	case cmmn.TaskFailed:
		node = e.PlanItemID
	default:
		return "", false
	}
	if node != "" && state.Items[node] == cmmn.PISuspended {
		return node, true
	}
	return "", false
}

// decodeCMMNExternalEvent reconstructs a typed cmmn.EngineEvent from an external
// event envelope via the engine's own codec (cmmn.UnmarshalEvent).
func decodeCMMNExternalEvent(b []byte) (cmmn.EngineEvent, error) {
	var env externalEvent
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("processengine: decode external event envelope: %w", err)
	}
	ev, err := cmmn.UnmarshalEvent(env.Kind, env.Payload)
	if err != nil {
		return nil, fmt.Errorf("processengine: external event: %w", err)
	}
	return ev, nil
}
