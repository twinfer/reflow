package handler

import (
	"errors"
	"fmt"

	"github.com/twinfer/reflow/pkg/handler/wire"
)

// ErrSuspended is returned by Context methods when the engine has decided
// to suspend the running invocation — loss of leadership, host shutdown,
// or an extended wait on an external completion. Handlers should
// propagate it upward without further work; the next leader will resume
// from the journal.
var ErrSuspended = errors.New("reflow: invocation suspended")

// Failure is a typed terminal handler error. Returning a *Failure from a
// handler or from a fn passed to Context.Run causes the invocation to
// complete with the failure recorded in its journal — future awaiters
// receive the same Failure on every read.
//
// Any non-*Failure error returned from a handler is treated as transient
// and triggers a retry with the configured backoff policy.
type Failure struct {
	// Code is an application-defined error code. Reflow itself never
	// interprets the value; it is round-tripped through the wire as-is.
	Code uint32
	// Message is the human-readable error description.
	Message string
}

// Error implements the error interface.
func (f *Failure) Error() string {
	if f == nil {
		return "<nil failure>"
	}
	if f.Code == 0 {
		return f.Message
	}
	return fmt.Sprintf("reflow: failure code=%d: %s", f.Code, f.Message)
}

// NewFailure constructs a *Failure with the given code and message.
func NewFailure(code uint32, message string) *Failure {
	return &Failure{Code: code, Message: message}
}

// StepBudgetExhaustedCode is the reserved Failure.Code reflow assigns
// when a handler exceeds its per-invocation journal-entry cap. Callers
// can errors.As / *Failure-type-assert and compare the code to
// distinguish step-budget exhaustion from user-defined failures.
const StepBudgetExhaustedCode uint32 = 9001

// CancelledCode is the reserved Failure.Code reflow stamps onto an
// invocation that was terminated by a CancelInvocation request (or a
// well-known __cancel__ signal). The canonical value lives in
// pkg/handler/wire so the engine can reference it without importing
// pkg/handler; re-exported here for handler-side convenience.
const CancelledCode = wire.CancelledCode

// WellKnownCancelSignal is the reserved signal name interpreted by the
// engine as "force this invocation to terminate with CancelledCode".
// SendSignal delivers it like any other signal; the receiver's apply
// arm short-circuits the inbox/awaiter path and synthesizes a terminal
// Completed{FailureCode=CancelledCode} instead of routing into the
// handler's signal-await machinery. Re-exported from
// pkg/handler/wire — both sides of the wire share the literal string.
const WellKnownCancelSignal = wire.WellKnownCancelSignal

// SendSignalUnkeyedCode is the reserved Failure.Code returned when the
// SDK rejects a SendSignal/CancelInvocation call whose Target carries
// an empty Key. Signals are only valid for keyed services (Virtual
// Objects and Workflows) — addressing an unkeyed Service has no
// well-defined receiver since multiple concurrent invocations may exist
// for the same (service, handler).
const SendSignalUnkeyedCode uint32 = 9003

// NewSendSignalUnkeyedFailure builds the terminal Failure returned when
// SendSignal/CancelInvocation is called with a Target that has an empty
// Key.
func NewSendSignalUnkeyedFailure(service, handler string) *Failure {
	return &Failure{
		Code:    SendSignalUnkeyedCode,
		Message: fmt.Sprintf("reflow: SendSignal target %s/%s has empty key (signals require a keyed target)", service, handler),
	}
}

// PromiseNotWorkflowCode is the reserved Failure.Code returned when a
// Promise method is called from a handler whose Kind is not Workflow or
// WorkflowShared. Promises are scoped to workflow runs; calling them
// from a stateless service or virtual object has no well-defined scope.
const PromiseNotWorkflowCode uint32 = 9004

// NewPromiseNotWorkflowFailure builds the terminal Failure for a Promise
// method called outside a workflow context.
func NewPromiseNotWorkflowFailure(service, handler string) *Failure {
	return &Failure{
		Code:    PromiseNotWorkflowCode,
		Message: fmt.Sprintf("reflow: Context.Promise requires KIND_WORKFLOW or KIND_WORKFLOW_SHARED (handler %s/%s)", service, handler),
	}
}

// PromiseAlreadyCompletedCode is the reserved Failure.Code returned when
// Promise.Resolve or Promise.Reject is called on a promise that already
// has a terminal state. The handler can use this to detect the loser of
// a complete-race without inspecting Message strings.
const PromiseAlreadyCompletedCode uint32 = 9005

// NewPromiseAlreadyCompletedFailure builds the terminal Failure surfaced
// when a second Resolve/Reject hits an already-completed promise.
func NewPromiseAlreadyCompletedFailure(name string) *Failure {
	return &Failure{
		Code:    PromiseAlreadyCompletedCode,
		Message: fmt.Sprintf("reflow: promise %q already completed", name),
	}
}

// NewStepBudgetExhaustedFailure builds the terminal Failure surfaced
// when a ctx primitive would push the invocation past its journal
// budget. used is the slot the SDK was about to allocate; max is the
// configured cap.
func NewStepBudgetExhaustedFailure(used, max uint32) *Failure {
	return &Failure{
		Code:    StepBudgetExhaustedCode,
		Message: fmt.Sprintf("reflow: step budget exhausted (%d/%d entries)", used, max),
	}
}

// AsFailure reports whether err (or anything errors.As-unwrappable from
// it) is a *Failure, returning the failure if so.
func AsFailure(err error) (*Failure, bool) {
	if f, ok := errors.AsType[*Failure](err); ok {
		return f, true
	}
	return nil, false
}
