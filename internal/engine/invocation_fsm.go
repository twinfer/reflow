package engine

import (
	"errors"
	"fmt"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// ErrInvalidTransition is returned by FSM transitions when the event is not
// legal in the current state. The partition's apply path logs and continues —
// returning this to dragonboat from Update would halt the shard (see
// dragonboat v4 statemachine/disk.go:113).
var ErrInvalidTransition = errors.New("invocation fsm: invalid transition")

// transitionOnInvoke handles a new InvokeCommand. Phase 1 transitions:
//
//	Free       → Scheduled (+ ActInvoke)
//	Scheduled  → Scheduled (no-op; idempotent duplicate)
//	Invoked    → Invoked   (no-op; idempotent — the invoker is already running)
//	*          → ErrInvalidTransition
func transitionOnInvoke(
	id *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	cmd *enginev1.InvokeCommand,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	switch cur.GetStatus().(type) {
	case nil, *enginev1.InvocationStatus_Free:
		next := &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Scheduled{
				Scheduled: &enginev1.Scheduled{
					Target:      cmd.GetTarget(),
					Input:       cmd.GetInput(),
					CreatedAtMs: nowMs,
				},
			},
		}
		return next, []Action{ActInvoke{ID: id, Target: cmd.GetTarget()}}, nil
	case *enginev1.InvocationStatus_Scheduled, *enginev1.InvocationStatus_Invoked:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: Invoke from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnJournalAppend handles an InvokerEffect.JournalAppended.
// Transitions:
//
//	Scheduled  --Input    → Invoked
//	Scheduled  --other    → Scheduled (no-op; should not happen but tolerated)
//	Invoked    --*        → Invoked   (no-op; just a journal write)
//	Suspended  --*Result  → Invoked   (+ ActInvoke; resumes execution)
//	*          → ErrInvalidTransition
func transitionOnJournalAppend(
	id *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	appended *enginev1.JournalEntryAppended,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	entry := appended.GetEntry()
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Scheduled:
		if _, isInput := entry.GetEntry().(*enginev1.JournalEntry_Input); isInput {
			return &enginev1.InvocationStatus{
				Status: &enginev1.InvocationStatus_Invoked{
					Invoked: &enginev1.Invoked{
						Target:      s.Scheduled.GetTarget(),
						CreatedAtMs: s.Scheduled.GetCreatedAtMs(),
						InvokedAtMs: nowMs,
					},
				},
			}, nil, nil
		}
		return cur, nil, nil
	case *enginev1.InvocationStatus_Invoked:
		return cur, nil, nil
	case *enginev1.InvocationStatus_Suspended:
		return &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Invoked{
				Invoked: &enginev1.Invoked{
					Target:      s.Suspended.GetTarget(),
					InvokedAtMs: nowMs,
				},
			},
		}, []Action{ActInvoke{ID: id, Target: s.Suspended.GetTarget()}}, nil
	default:
		return cur, nil, fmt.Errorf("%w: JournalAppend from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnComplete handles an InvokerEffect.Completed.
// Transitions:
//
//	Invoked    → Completed
//	Completed  → Completed (idempotent)
//	*          → ErrInvalidTransition
func transitionOnComplete(
	_ *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	eff *enginev1.InvocationCompleted,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Invoked:
		return &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Completed{
				Completed: &enginev1.Completed{
					Target:         s.Invoked.GetTarget(),
					Output:         eff.GetOutput(),
					FailureMessage: eff.GetFailureMessage(),
					CompletedAtMs:  nowMs,
				},
			},
		}, nil, nil
	case *enginev1.InvocationStatus_Completed:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: Complete from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnSuspend handles an InvokerEffect.Suspended.
// Transitions:
//
//	Invoked    → Suspended
//	Suspended  → Suspended (idempotent)
//	*          → ErrInvalidTransition
func transitionOnSuspend(
	_ *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	eff *enginev1.InvocationSuspended,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Invoked:
		return &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Suspended{
				Suspended: &enginev1.Suspended{
					Target:        s.Invoked.GetTarget(),
					SuspendedAtMs: nowMs,
					AwaitingOn:    append([]string(nil), eff.GetAwaitingOn()...),
				},
			},
		}, nil, nil
	case *enginev1.InvocationStatus_Suspended:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: Suspend from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnTimerFired handles a TimerFired command.
// Transitions:
//
//	Suspended  → Invoked   (+ ActInvoke)
//	Invoked    → Invoked   (late-arriving timer; no-op)
//	Completed  → Completed (no-op)
//	*          → ErrInvalidTransition
func transitionOnTimerFired(
	id *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	_ *enginev1.TimerFired,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Suspended:
		return &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Invoked{
				Invoked: &enginev1.Invoked{
					Target:      s.Suspended.GetTarget(),
					InvokedAtMs: nowMs,
				},
			},
		}, []Action{ActInvoke{ID: id, Target: s.Suspended.GetTarget()}}, nil
	case *enginev1.InvocationStatus_Invoked, *enginev1.InvocationStatus_Completed:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: TimerFired from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnPurge moves a Completed (or Free / nil) row to Free, which the
// caller treats as "delete the row".
func transitionOnPurge(
	_ *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	_ uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	switch cur.GetStatus().(type) {
	case nil, *enginev1.InvocationStatus_Completed, *enginev1.InvocationStatus_Free:
		return &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Free{Free: &enginev1.Free{}},
		}, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: Purge from %T", ErrInvalidTransition, cur.GetStatus())
	}
}
