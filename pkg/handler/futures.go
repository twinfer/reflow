package handler

import (
	"errors"
	"fmt"

	"github.com/twinfer/reflow/pkg/handler/wire"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// futures.go holds Future implementations bound to the wire path.
// The pattern mirrors internal/engine/invoker/futures.go: Poll consults
// the session's replay buffer for the result slot; Result either
// returns the resolved value or suspends with a waker token. Because
// suspension is deferred to Result, primitives can be composed via
// All/Any without each call short-circuiting the next.

// readyFuture wraps a resolved value (replay-hit on result slot).
type readyFuture struct {
	value []byte
}

func (f readyFuture) Result() ([]byte, error) { return f.value, nil }
func (f readyFuture) Poll() (bool, []string)  { return true, nil }

// errFuture surfaces a setup-time error (marshal, send, divergence)
// from a primitive that couldn't even allocate its slot. Result
// returns the captured error; Poll reports resolved so combinators
// don't trap on an unrecoverable child.
type errFuture struct{ err error }

func (f errFuture) Result() ([]byte, error) { return nil, f.err }
func (f errFuture) Poll() (bool, []string)  { return true, nil }

// suspendedFuture is returned by Sleep / Call / Awakeable when the
// context was already suspended at the time of the call (allocSlot
// returned ErrSuspended). Every operation short-circuits to ErrSuspended.
type suspendedFuture struct{}

func (suspendedFuture) Result() ([]byte, error) { return nil, ErrSuspended }
func (suspendedFuture) Poll() (bool, []string)  { return false, nil }

// futureFromAllocErr packages an allocSlot error into a Future.
// ErrSuspended → suspendedFuture (preserves suspend semantics); any
// other error (a terminal *Failure such as StepBudgetExhausted) →
// errFuture so Future.Result surfaces it immediately.
func futureFromAllocErr(err error) Future {
	if errors.Is(err, ErrSuspended) {
		return suspendedFuture{}
	}
	return errFuture{err: err}
}

// --- Sleep ---

// sleepFuture is the deferred handle returned by wireContext.Sleep.
// resultSlot is the slot the JESleepResult (translated as
// TypeNoteSleepDone) will arrive at on the next respawn.
type sleepFuture struct {
	ctx        *wireContext
	resultSlot uint32
}

func (f sleepFuture) Poll() (bool, []string) {
	if f.ctx.lookupReplay(f.resultSlot) != nil {
		return true, nil
	}
	return false, []string{fmt.Sprintf("completion:%d", f.resultSlot)}
}

func (f sleepFuture) Result() ([]byte, error) {
	if f.ctx.lookupReplay(f.resultSlot) != nil {
		return nil, nil
	}
	f.ctx.suspend(fmt.Sprintf("completion:%d", f.resultSlot))
	return nil, ErrSuspended
}

// --- Call ---

// callFuture is the deferred handle returned by wireContext.Call.
// resultSlot is the slot the JECallResult (translated as
// TypeNoteCallDone) will arrive at on the next respawn.
type callFuture struct {
	ctx        *wireContext
	resultSlot uint32
}

func (f callFuture) Poll() (bool, []string) {
	if f.ctx.lookupReplay(f.resultSlot) != nil {
		return true, nil
	}
	return false, []string{fmt.Sprintf("completion:%d", f.resultSlot)}
}

func (f callFuture) Result() ([]byte, error) {
	entry := f.ctx.lookupReplay(f.resultSlot)
	if entry == nil {
		f.ctx.suspend(fmt.Sprintf("completion:%d", f.resultSlot))
		return nil, ErrSuspended
	}
	var note protocolv1.CallCompletionNotificationMessage
	if err := f.ctx.codec.Unmarshal(entry.payload, &note); err != nil {
		return nil, fmt.Errorf("decode replayed CallCompletionNotificationMessage: %w", err)
	}
	switch r := note.GetResult().(type) {
	case *protocolv1.CallCompletionNotificationMessage_Value:
		return r.Value.GetContent(), nil
	case *protocolv1.CallCompletionNotificationMessage_Failure:
		return nil, NewFailure(r.Failure.GetCode(), r.Failure.GetMessage())
	default:
		return nil, fmt.Errorf("CallCompletionNotificationMessage at slot %d carries no result", f.resultSlot)
	}
}

// --- Awakeable ---

// awakeableFuture is the deferred handle returned by wireContext.Awakeable.
// id is the externally-resolvable identifier the SDK minted; resultSlot
// is the slot the SignalNotificationMessage will arrive at.
type awakeableFuture struct {
	ctx        *wireContext
	resultSlot uint32
	id         string
}

func (f awakeableFuture) Poll() (bool, []string) {
	if f.ctx.lookupReplay(f.resultSlot) != nil {
		return true, nil
	}
	return false, []string{"awakeable:" + f.id}
}

func (f awakeableFuture) Result() ([]byte, error) {
	entry := f.ctx.lookupReplay(f.resultSlot)
	if entry == nil {
		f.ctx.suspend("awakeable:" + f.id)
		return nil, ErrSuspended
	}
	if entry.typeCode != wire.TypeNoteSignal {
		return nil, fmt.Errorf("awakeable slot %d carries unexpected frame type 0x%04x", f.resultSlot, entry.typeCode)
	}
	var note protocolv1.SignalNotificationMessage
	if err := f.ctx.codec.Unmarshal(entry.payload, &note); err != nil {
		return nil, fmt.Errorf("decode replayed SignalNotificationMessage: %w", err)
	}
	switch r := note.GetResult().(type) {
	case *protocolv1.SignalNotificationMessage_Value:
		return r.Value.GetContent(), nil
	case *protocolv1.SignalNotificationMessage_Failure:
		return nil, NewFailure(r.Failure.GetCode(), r.Failure.GetMessage())
	default:
		return nil, nil
	}
}

// --- WaitSignal ---

// signalFuture is the deferred handle returned by wireContext.WaitSignal.
// name is the signal name the SDK is awaiting; resultSlot is the slot
// the JESignalResult notification will arrive at.
type signalFuture struct {
	ctx        *wireContext
	resultSlot uint32
	name       string
}

func (f signalFuture) Poll() (bool, []string) {
	if f.ctx.lookupReplay(f.resultSlot) != nil {
		return true, nil
	}
	return false, []string{f.token()}
}

func (f signalFuture) Result() ([]byte, error) {
	entry := f.ctx.lookupReplay(f.resultSlot)
	if entry == nil {
		f.ctx.suspend(f.token())
		return nil, ErrSuspended
	}
	if entry.typeCode != wire.TypeNoteSignal {
		return nil, fmt.Errorf("signal slot %d carries unexpected frame type 0x%04x", f.resultSlot, entry.typeCode)
	}
	var note protocolv1.SignalNotificationMessage
	if err := f.ctx.codec.Unmarshal(entry.payload, &note); err != nil {
		return nil, fmt.Errorf("decode replayed SignalNotificationMessage: %w", err)
	}
	switch r := note.GetResult().(type) {
	case *protocolv1.SignalNotificationMessage_Value:
		return r.Value.GetContent(), nil
	case *protocolv1.SignalNotificationMessage_Failure:
		return nil, NewFailure(r.Failure.GetCode(), r.Failure.GetMessage())
	default:
		return nil, nil
	}
}

// token returns the suspension token format the engine recognises in
// awaiting_on: signal:<name>:<slot>. Slot is included so two concurrent
// waits with the same name produce distinct tokens.
func (f signalFuture) token() string {
	return fmt.Sprintf("signal:%s:%d", f.name, f.resultSlot)
}

// --- Promise.Result ---

// promiseResultFuture is the deferred handle returned by
// DurablePromise.Result. name is the promise name; resultSlot is the
// slot where the apply arm appends JEPromiseResult once the promise
// completes (synchronously on inline-hit or asynchronously on a later
// JECompletePromise / InvokerEffect.PromiseCompleted).
type promiseResultFuture struct {
	ctx        *wireContext
	resultSlot uint32
	name       string
}

func (f promiseResultFuture) Poll() (bool, []string) {
	if f.ctx.lookupReplay(f.resultSlot) != nil {
		return true, nil
	}
	return false, []string{f.token()}
}

func (f promiseResultFuture) Result() ([]byte, error) {
	entry := f.ctx.lookupReplay(f.resultSlot)
	if entry == nil {
		f.ctx.suspend(f.token())
		return nil, ErrSuspended
	}
	if entry.typeCode != wire.TypeNoteGetPromise {
		return nil, fmt.Errorf("promise slot %d carries unexpected frame type 0x%04x", f.resultSlot, entry.typeCode)
	}
	var note protocolv1.GetPromiseCompletionNotificationMessage
	if err := f.ctx.codec.Unmarshal(entry.payload, &note); err != nil {
		return nil, fmt.Errorf("decode replayed GetPromiseCompletionNotificationMessage: %w", err)
	}
	switch r := note.GetResult().(type) {
	case *protocolv1.GetPromiseCompletionNotificationMessage_Value:
		return r.Value.GetContent(), nil
	case *protocolv1.GetPromiseCompletionNotificationMessage_Failure:
		return nil, NewFailure(r.Failure.GetCode(), r.Failure.GetMessage())
	default:
		return nil, nil
	}
}

func (f promiseResultFuture) token() string {
	return fmt.Sprintf("promise:%s:%d", f.name, f.resultSlot)
}

// --- All ---

// allResult is the composite returned by wireContext.All. Pure SDK
// composition, no journal slot.
type allResult struct {
	ctx      *wireContext
	children []Future
}

func (a *allResult) Poll() (bool, []string) {
	var pending []string
	for _, c := range a.children {
		p, ok := c.(Poller)
		if !ok {
			continue
		}
		if done, tokens := p.Poll(); !done {
			pending = append(pending, tokens...)
		}
	}
	return len(pending) == 0, pending
}

func (a *allResult) Results() ([][]byte, error) {
	if done, tokens := a.Poll(); !done {
		a.ctx.suspend(tokens...)
		return nil, ErrSuspended
	}
	out := make([][]byte, len(a.children))
	for i, c := range a.children {
		v, err := c.Result()
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// --- Any ---

// anyFuture resolves to the lowest-indexed resolved child. Deterministic
// across replays — argument order is the tiebreaker, not wall clock.
type anyFuture struct {
	ctx      *wireContext
	children []Future
}

func (f *anyFuture) Poll() (bool, []string) {
	var pending []string
	anyResolved := false
	for _, c := range f.children {
		p, ok := c.(Poller)
		if !ok {
			continue
		}
		done, tokens := p.Poll()
		if done {
			anyResolved = true
			continue
		}
		pending = append(pending, tokens...)
	}
	if anyResolved {
		return true, nil
	}
	return false, pending
}

func (f *anyFuture) Result() ([]byte, error) {
	if done, tokens := f.Poll(); !done {
		f.ctx.suspend(tokens...)
		return nil, ErrSuspended
	}
	for _, c := range f.children {
		p, ok := c.(Poller)
		if !ok {
			continue
		}
		if resolved, _ := p.Poll(); resolved {
			return c.Result()
		}
	}
	return nil, ErrSuspended
}
