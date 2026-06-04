package iflowengine

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/twinfer/iflow/capability"
	"github.com/twinfer/iflow/cmmn"
	"github.com/twinfer/reflow/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
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
	eng, err := cmmn.NewSentryEngine(def, cmmn.WithClock(func() time.Time {
		return time.UnixMilli(int64(logical))
	}))
	if err != nil {
		return nil, fmt.Errorf("iflowengine: new sentry engine: %w", err)
	}

	var cmds []cmmn.Command
	var state *cmmn.CaseState
	if len(rec.GetStateBlob()) == 0 {
		vars, derr := decodeVars(in.Entry.GetPayload().GetExternal())
		if derr != nil {
			return nil, fmt.Errorf("iflowengine: decode start vars: %w", derr)
		}
		var serr error
		cmds, state, serr = eng.Start(vars)
		if serr != nil {
			return nil, fmt.Errorf("iflowengine: cmmn start: %w", serr)
		}
	} else {
		state = new(cmmn.CaseState)
		if uerr := json.Unmarshal(rec.GetStateBlob(), state); uerr != nil {
			return nil, fmt.Errorf("iflowengine: unmarshal case state: %w", uerr)
		}
		ev, eerr := eventForCMMN(in.Entry.GetPayload())
		if eerr != nil {
			return nil, eerr
		}
		var aerr error
		cmds, state, aerr = eng.Advance(state, ev)
		if aerr != nil {
			return nil, fmt.Errorf("iflowengine: cmmn advance: %w", aerr)
		}
	}

	newState, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("iflowengine: marshal case state: %w", err)
	}
	return a.translateCMMN(in, cmds, newState)
}

// eventForCMMN decodes a ProcessEventPayload into the iflow CMMN EngineEvent that
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
			return nil, fmt.Errorf("iflowengine: decode task output: %w", err)
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
			return nil, fmt.Errorf("iflowengine: decode child output: %w", err)
		}
		return cmmn.TaskCompleted{PlanItemID: cc.GetNodeId(), Outputs: outputs}, nil
	case *enginev1.ProcessEventPayload_MessageReceived:
		// Unreachable in practice: the CMMN translation emits no SignalSubscribe,
		// so the read path never routes a ProcessMessageReceived to a case. Kept
		// explicit so the failure is legible if that ever changes.
		return nil, fmt.Errorf("iflowengine: CMMN message correlation not supported (plan item %q)", of.MessageReceived.GetNodeId())
	default:
		return nil, fmt.Errorf("iflowengine: unset process event payload (no event in inbox entry)")
	}
}

// translateCMMN maps the engine's emitted commands onto a ProcessAdvanced.
func (a *Adapter) translateCMMN(in invoker.ProcessAdvanceInput, cmds []cmmn.Command, newState []byte) (*enginev1.ProcessAdvanced, error) {
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
				return nil, fmt.Errorf("iflowengine: encode case outputs: %w", err)
			}
			adv.Terminal = &enginev1.ProcessTerminal{
				Output:      out,
				RetentionMs: a.retentionMs(in.Record.GetModelRef()),
			}
			return adv, nil
		case cmmn.CaseFailed:
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

	tenant := tenantOf(in.Pk)
	for _, c := range cmds {
		switch t := c.(type) {
		case cmmn.RunTask:
			if err := a.translateCMMNRunTask(in, adv, t, tenant); err != nil {
				return nil, err
			}
		case cmmn.RunProcessTask:
			childRef, err := a.models.ChildRef(in.Record.GetModelRef(), "bpmn", t.ProcessRef)
			if err != nil {
				return nil, fmt.Errorf("iflowengine: resolve process child %q: %w", t.ProcessRef, err)
			}
			vars, err := encodeVars(t.Vars)
			if err != nil {
				return nil, fmt.Errorf("iflowengine: encode child vars: %w", err)
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
				return nil, fmt.Errorf("iflowengine: resolve case child %q: %w", t.CaseRef, err)
			}
			vars, err := encodeVars(t.Vars)
			if err != nil {
				return nil, fmt.Errorf("iflowengine: encode child vars: %w", err)
			}
			adv.StartChild = append(adv.StartChild, &enginev1.ChildStart{
				NodeId:      t.PlanItemID,
				ModelRef:    childRef,
				Kind:        enginev1.ProcessKind_PROCESS_KIND_CMMN,
				InstanceKey: childInstanceKey(in.InstanceKey, t.PlanItemID, ""),
				Vars:        vars,
			})
		case cmmn.CaseFileItemEventRejected:
			// Observational: a receiving instance rejected a broadcast CFI event
			// for an out-of-§A.5-state CFI. Nothing to actuate.
		default:
			return nil, fmt.Errorf("iflowengine: unsupported CMMN command %T", c)
		}
	}
	return adv, nil
}

// translateCMMNRunTask handles the polymorphic RunTask: a leaf task → Invoke, a
// timer listener → ArmTimer, user/plain event listeners → park (no-op). Other
// kinds (human task, decision, milestone, stage) are unsupported this iteration.
func (a *Adapter) translateCMMNRunTask(in invoker.ProcessAdvanceInput, adv *enginev1.ProcessAdvanced, t cmmn.RunTask, tenant string) error {
	switch t.Kind {
	case cmmn.KindTask:
		ref, err := cmmnTaskRef(t)
		if err != nil {
			return err
		}
		input, err := encodeBridgeInput(ref, t.Vars, t.ExtensionsXML, tenant)
		if err != nil {
			return fmt.Errorf("iflowengine: encode bridge input: %w", err)
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
		return fmt.Errorf("iflowengine: unsupported CMMN task kind %q (plan item %q)", t.Kind, t.PlanItemID)
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
	return 0, fmt.Errorf("iflowengine: timer listener %q has neither duration nor absolute time", t.PlanItemID)
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
		return "", fmt.Errorf("iflowengine: decode <capability> for %q: %w", t.PlanItemID, err)
	}
	if cfg.Capability.Ref == "" {
		return "", fmt.Errorf("iflowengine: task %q names no capability (set <capability ref=\"ns:op\"/>)", t.PlanItemID)
	}
	return cfg.Capability.Ref, nil
}

// decodeCMMNExternalEvent reconstructs a typed cmmn.EngineEvent from an external
// event envelope via the engine's own codec (cmmn.UnmarshalEvent).
func decodeCMMNExternalEvent(b []byte) (cmmn.EngineEvent, error) {
	var env externalEvent
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("iflowengine: decode external event envelope: %w", err)
	}
	ev, err := cmmn.UnmarshalEvent(env.Kind, env.Payload)
	if err != nil {
		return nil, fmt.Errorf("iflowengine: external event: %w", err)
	}
	return ev, nil
}
