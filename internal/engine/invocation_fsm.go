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
					ParentLink:  cmd.GetParentLink(),
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
//	Scheduled  --Input          → Invoked
//	Scheduled  --other          → Scheduled (no-op; should not happen but tolerated)
//	Invoked    --*              → Invoked   (no-op; just a journal write)
//	Suspended  --*              → Invoked   (+ ActInvoke; resumes execution)
//	*          → ErrInvalidTransition
//
// The Suspended wake-up is lenient: any journal append resumes a suspended
// invocation. In practice only *Result entries (SleepResult, CallResult,
// AwakeableResult) and external completions ever arrive in Suspended state;
// the SDK does not propose fresh command entries while its session is
// suspended. The lenient default protects against replay races and keeps
// the FSM agnostic to the exact entry-type taxonomy.
//
// Outbox queueing for Call / OneWayCall / outbound JESignal is layered on
// in partition.go before the transition runs — Step 7 wires that arm.
// Phase 2 entry types (JERun, JEAwakeable, JEAwakeableResult, JESignal,
// JEClearState, JEGetEagerState) are accepted by the Invoked / Suspended
// arms without per-type cases.
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
						ParentLink:  s.Scheduled.GetParentLink(),
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
					ParentLink:  s.Suspended.GetParentLink(),
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
//	Suspended  → Completed   (race-safe; see below)
//	Completed  → Completed   (idempotent)
//	*          → ErrInvalidTransition
//
// Suspended → Completed is legitimate under this race: TimerFired (or any
// wake-event) commits before the session's in-flight Suspended propose;
// the wake's ActInvoke spawns a fresh session via pendingRespawn while
// the prior Suspended propose is still in flight; the new session reads
// the journal including the just-written result entry, runs the handler
// to completion, and proposes Completed. By the time Completed applies,
// the in-flight Suspended has committed and status is Suspended again.
// Rejecting this would strand the invocation forever.
func transitionOnComplete(
	_ *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	eff *enginev1.InvocationCompleted,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	var target *enginev1.InvocationTarget
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Invoked:
		target = s.Invoked.GetTarget()
	case *enginev1.InvocationStatus_Suspended:
		target = s.Suspended.GetTarget()
	case *enginev1.InvocationStatus_Completed:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: Complete from %T", ErrInvalidTransition, cur.GetStatus())
	}
	return &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Completed{
			Completed: &enginev1.Completed{
				Target:         target,
				Output:         eff.GetOutput(),
				FailureMessage: eff.GetFailureMessage(),
				CompletedAtMs:  nowMs,
			},
		},
	}, nil, nil
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
					ParentLink:    s.Invoked.GetParentLink(),
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
//	Invoked    → Invoked   (+ ActInvoke; ensures the wake doesn't get lost
//	                        when this fire races with a session's in-flight
//	                        InvokerEffect.Suspended propose — the FSM has
//	                        committed the SleepResult already, but the
//	                        session that needs to consume it has a stale
//	                        journal snapshot and is about to exit. The
//	                        Invoker is idempotent against an already-
//	                        running session: a true late-arriving fire
//	                        sees the running session and no-ops, while
//	                        the race-with-Suspend case queues a respawn
//	                        for after the session exits.)
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
					ParentLink:  s.Suspended.GetParentLink(),
				},
			},
		}, []Action{ActInvoke{ID: id, Target: s.Suspended.GetTarget()}}, nil
	case *enginev1.InvocationStatus_Invoked:
		return cur, []Action{ActInvoke{ID: id, Target: s.Invoked.GetTarget()}}, nil
	case *enginev1.InvocationStatus_Completed:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: TimerFired from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnAwakeableResolved handles an InvokerEffect.AwakeableResolved.
// The caller (partition.go) has already journaled the JEAwakeableResult at
// completionIdx and deleted the awakeable directory row.
//
// Transitions:
//
//	Suspended  → Invoked   (+ ActInvoke + ActDeliverNotification)
//	Invoked    → Invoked   (+ ActDeliverNotification; live session)
//	Completed  → Completed (late arrival; no-op)
//	*          → ErrInvalidTransition
//
// On the Suspended path both actions fire: ActInvoke makes the Invoker
// spawn a fresh session goroutine; ActDeliverNotification is the redundant
// side-band carrying the completion in case the session's known_entries
// does not yet include this index. Phase 2.
func transitionOnAwakeableResolved(
	id *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	completionIdx uint32,
	value []byte,
	failure string,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	notify := ActDeliverNotification{
		ID:           id,
		CompletionID: completionIdx,
		Value:        value,
		Failure:      failure,
	}
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Suspended:
		next := &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Invoked{
				Invoked: &enginev1.Invoked{
					Target:      s.Suspended.GetTarget(),
					InvokedAtMs: nowMs,
					ParentLink:  s.Suspended.GetParentLink(),
				},
			},
		}
		return next, []Action{ActInvoke{ID: id, Target: s.Suspended.GetTarget()}, notify}, nil
	case *enginev1.InvocationStatus_Invoked:
		return cur, []Action{notify}, nil
	case *enginev1.InvocationStatus_Completed:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: AwakeableResolved from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnSignalDelivered handles an InvokerEffect.SignalDelivered.
// Same wake-up shape as transitionOnAwakeableResolved — Phase 2 does not
// filter Suspended.awaiting_on by signal name; the session goroutine
// inspects its waker queue on resume.
//
// Transitions:
//
//	Suspended  → Invoked   (+ ActInvoke + ActDeliverNotification void)
//	Invoked    → Invoked   (+ ActDeliverNotification void)
//	Completed  → Completed (no-op)
//	*          → ErrInvalidTransition
//
// Phase 2.
func transitionOnSignalDelivered(
	id *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	completionIdx uint32,
	_ string, // signalName — surfaced via the journal entry, not the action
	payload []byte,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	notify := ActDeliverNotification{
		ID:           id,
		CompletionID: completionIdx,
		Value:        payload,
	}
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Suspended:
		next := &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Invoked{
				Invoked: &enginev1.Invoked{
					Target:      s.Suspended.GetTarget(),
					InvokedAtMs: nowMs,
					ParentLink:  s.Suspended.GetParentLink(),
				},
			},
		}
		return next, []Action{ActInvoke{ID: id, Target: s.Suspended.GetTarget()}, notify}, nil
	case *enginev1.InvocationStatus_Invoked:
		return cur, []Action{notify}, nil
	case *enginev1.InvocationStatus_Completed:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: SignalDelivered from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnCallResultDelivered handles the apply-side delivery of a callee's
// result to its parent invocation. The JECallResult journal entry has already
// been appended at completionIdx by the caller of this function (partition.go's
// onInvokerEffect_Completed arm, when the callee's prior status carried a
// ParentLink). Wake-up shape mirrors transitionOnAwakeableResolved.
//
// Transitions:
//
//	Suspended  → Invoked   (+ ActInvoke + ActDeliverNotification)
//	Invoked    → Invoked   (+ ActDeliverNotification; live session)
//	Completed  → Completed (late arrival; no-op)
//	*          → ErrInvalidTransition
//
// Phase 2.5.
func transitionOnCallResultDelivered(
	id *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	completionIdx uint32,
	value []byte,
	failure string,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	notify := ActDeliverNotification{
		ID:           id,
		CompletionID: completionIdx,
		Value:        value,
		Failure:      failure,
	}
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Suspended:
		next := &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Invoked{
				Invoked: &enginev1.Invoked{
					Target:      s.Suspended.GetTarget(),
					InvokedAtMs: nowMs,
					ParentLink:  s.Suspended.GetParentLink(),
				},
			},
		}
		return next, []Action{ActInvoke{ID: id, Target: s.Suspended.GetTarget()}, notify}, nil
	case *enginev1.InvocationStatus_Invoked:
		return cur, []Action{notify}, nil
	case *enginev1.InvocationStatus_Completed:
		return cur, nil, nil
	default:
		return cur, nil, fmt.Errorf("%w: CallResultDelivered from %T", ErrInvalidTransition, cur.GetStatus())
	}
}

// transitionOnPurge moves a Completed (or Free / nil) row to Free, which the
// caller treats as "delete the row". Takes no wall-clock argument: the
// resulting Free status carries no timestamps, so the caller has no
// reason to sample NowFn just to call this.
func transitionOnPurge(
	_ *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
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
