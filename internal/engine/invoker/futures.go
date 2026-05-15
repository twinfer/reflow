package invoker

import (
	"fmt"

	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// futures.go holds every concrete sdk.Future implementation. Each is
// stateless past construction: Poll consults the invocation context's
// journal snapshot, Result either returns the resolved value or
// suspends with the appropriate waker token. The combinator types
// (allResult, anyFuture) layer pure composition over child Pollers.
//
// All types here implement both sdk.Future / sdk.AllResult and the
// sealed sdk.Poller — combinators down-cast via the Poller interface;
// the sealed markers keep external implementations out.

// --- Call ---

// callFuture is the deferred handle returned by Context.Call. start is
// the JECall journal index; the matching JECallResult lands at start+1.
type callFuture struct {
	ctx   *invocationContext
	start uint32
}

// Poll returns (true, nil) when JECallResult has landed at start+1;
// otherwise the waker token "call:<start>" so a wake from the engine
// puts the future back into a resolvable state.
func (f *callFuture) Poll() (bool, []string) {
	if f.ctx.lookupEntry(f.start+1) != nil {
		return true, nil
	}
	return false, []string{fmt.Sprintf("call:%d", f.start)}
}

func (f *callFuture) Result() ([]byte, error) {
	if r := f.ctx.lookupEntry(f.start + 1); r != nil {
		cr, ok := r.GetEntry().(*enginev1.JournalEntry_CallResult)
		if !ok {
			return nil, divergenceErr(f.start+1, "JECallResult", r)
		}
		if msg := cr.CallResult.GetFailureMessage(); msg != "" {
			return nil, sdk.NewFailure(0, msg)
		}
		return cloneBytes(cr.CallResult.GetResult()), nil
	}
	return nil, f.ctx.suspend(fmt.Sprintf("call:%d", f.start))
}

// --- Sleep ---

// sleepFuture is the deferred handle returned by Context.Sleep. start is
// the JESleep journal index; the matching JESleepResult lands at start+1.
// The byte payload is always nil — the resolution itself is the signal.
type sleepFuture struct {
	ctx   *invocationContext
	start uint32
}

func (f *sleepFuture) Poll() (bool, []string) {
	if f.ctx.lookupEntry(f.start+1) != nil {
		return true, nil
	}
	return false, []string{fmt.Sprintf("sleep:%d", f.start)}
}

func (f *sleepFuture) Result() ([]byte, error) {
	if r := f.ctx.lookupEntry(f.start + 1); r != nil {
		if _, ok := r.GetEntry().(*enginev1.JournalEntry_SleepResult); !ok {
			return nil, divergenceErr(f.start+1, "JESleepResult", r)
		}
		return nil, nil
	}
	return nil, f.ctx.suspend(fmt.Sprintf("sleep:%d", f.start))
}

// --- Awakeable ---

// awakeableFuture is the deferred handle returned by Context.Awakeable.
// originIdx is the JEAwakeable journal index; the matching
// JEAwakeableResult lands at originIdx+1. id is the externally-resolvable
// awakeable identifier the engine wakes on.
type awakeableFuture struct {
	ctx       *invocationContext
	originIdx uint32
	id        string
}

func (f *awakeableFuture) Poll() (bool, []string) {
	if f.ctx.lookupEntry(f.originIdx+1) != nil {
		return true, nil
	}
	return false, []string{"awakeable:" + f.id}
}

func (f *awakeableFuture) Result() ([]byte, error) {
	if r := f.ctx.lookupEntry(f.originIdx + 1); r != nil {
		ar, ok := r.GetEntry().(*enginev1.JournalEntry_AwakeableResult)
		if !ok {
			return nil, divergenceErr(f.originIdx+1, "JEAwakeableResult", r)
		}
		if msg := ar.AwakeableResult.GetFailureMessage(); msg != "" {
			return nil, sdk.NewFailure(0, msg)
		}
		return cloneBytes(ar.AwakeableResult.GetValue()), nil
	}
	return nil, f.ctx.suspend("awakeable:" + f.id)
}

// erroredFuture surfaces a setup-time error from a primitive that could
// not even allocate its journal slot (e.g. awakeable id rng failure).
// Result returns the captured error; Poll reports resolved to avoid
// trapping combinators in a suspend loop on something unrecoverable.
type erroredFuture struct{ err error }

func (e *erroredFuture) Poll() (bool, []string)  { return true, nil }
func (e *erroredFuture) Result() ([]byte, error) { return nil, e.err }

// suspendedFuture is returned when a primitive is called after the
// context has already entered the suspended state. Every operation
// short-circuits to ErrSuspended so the handler unwinds promptly.
type suspendedFuture struct{}

func (s *suspendedFuture) Poll() (bool, []string)  { return false, nil }
func (s *suspendedFuture) Result() ([]byte, error) { return nil, sdk.ErrSuspended }

// --- All ---

// allResult is the composite returned by Context.All. children holds
// the supplied futures in argument order; the public AllResult.Results
// blocks until every child has resolved, then returns their values in
// the same order.
type allResult struct {
	ctx      *invocationContext
	children []sdk.Future
}

// Poll aggregates child Pollers: resolved when every child is resolved;
// otherwise returns the union of pending tokens (deduped within a single
// child is not necessary — duplicate tokens in suspendedOn are
// equivalent to a single token, the engine wakes on any match).
func (a *allResult) Poll() (bool, []string) {
	var pending []string
	for _, c := range a.children {
		p, ok := c.(sdk.Poller)
		if !ok {
			// External Future impls are blocked by the sealed marker, so
			// this branch is unreachable from user code. Defend against
			// future SDK refactors by surfacing a divergence-style error
			// via the union of tokens (empty), which keeps Poll honest.
			continue
		}
		if done, tokens := p.Poll(); !done {
			pending = append(pending, tokens...)
		}
	}
	return len(pending) == 0, pending
}

// Results blocks until every child resolves, then returns their values
// in argument order. A *Failure from any child is propagated immediately
// (lowest-indexed child wins on ties), discarding sibling values to
// match Restate semantics where partial successes do not surface
// alongside a failure.
func (a *allResult) Results() ([][]byte, error) {
	if done, tokens := a.Poll(); !done {
		return nil, a.ctx.suspend(tokens...)
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

// anyFuture is the composite returned by Context.Any. Result resolves to
// the lowest-indexed child whose Poll reports resolved. "First" is by
// argument order, not wall-clock — the journal records resolutions
// independently, so replay must pick the same winner deterministically.
type anyFuture struct {
	ctx      *invocationContext
	children []sdk.Future
}

// Poll reports resolved if any child is resolved; otherwise the union of
// pending tokens so a wake from any child returns control to the
// handler.
func (f *anyFuture) Poll() (bool, []string) {
	var pending []string
	anyResolved := false
	for _, c := range f.children {
		p, ok := c.(sdk.Poller)
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
		return nil, f.ctx.suspend(tokens...)
	}
	for _, c := range f.children {
		p, ok := c.(sdk.Poller)
		if !ok {
			continue
		}
		if resolved, _ := p.Poll(); resolved {
			return c.Result()
		}
	}
	// Defensive: Poll said resolved but no child reports so. Treat as a
	// transient suspend; the next wake will re-evaluate cleanly.
	return nil, sdk.ErrSuspended
}
