package sdk

import (
	"errors"
	"fmt"
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

// AsFailure reports whether err (or anything errors.As-unwrappable from
// it) is a *Failure, returning the failure if so.
func AsFailure(err error) (*Failure, bool) {
	if f, ok := errors.AsType[*Failure](err); ok {
		return f, true
	}
	return nil, false
}
