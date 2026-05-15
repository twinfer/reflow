package sdk

import (
	"context"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Context is the durable-execution handle every reflow handler receives.
// Every method on Context is journaled: on replay, the same call sequence
// returns the same values, so the handler body can be re-run safely after
// a crash without re-executing its side effects.
//
// Determinism rules:
//
//   - All non-deterministic operations (timers, RNG, external I/O) MUST go
//     through ctx — never through time.Now, rand, net/http, etc. directly
//     in the handler. Direct calls bypass the journal and break replay.
//   - Branch decisions can read non-journaled values (CPU caches,
//     environment variables) only when they are stable across replays.
//
// All blocking methods respect the engine's lifecycle: if the partition
// loses leadership or the host shuts down, in-flight calls return
// ErrSuspended (or context.Canceled when ctx.Context is cancelled).
// Handlers should propagate that error upward without further work; the
// next leader resumes from the journal.
type Context interface {
	// Context returns a Go context.Context cancelled when the invocation
	// is being torn down by the engine. Pass it into Run-body I/O so a
	// timely shutdown propagates.
	Context() context.Context

	// Input returns the input payload as passed to SubmitInvocation.
	// Same value on every replay; nil when no input was provided.
	Input() []byte

	// InvocationID returns this invocation's durable identifier
	// (partition_key + 16-byte uuid). Stable across replays.
	InvocationID() *enginev1.InvocationId

	// Sleep schedules a durable wake-up d into the future. The returned
	// Future's Result blocks until the wake-up fires (the byte payload
	// is always nil — the resolution itself is the signal). The wake-up
	// instant is durable: a restart preserves the original wake time so
	// the handler resumes exactly when it would have, even if the
	// process was down for some of the interval.
	//
	// Returning a Future lets Sleep race against other awaitables,
	// e.g. Any(callFuture, sleepFuture) for a timeout.
	Sleep(d time.Duration) Future

	// Run executes fn at most once and journals the outcome. On every
	// replay the journaled bytes are returned without re-invoking fn.
	//
	// name is a debug label; durability uses the journal entry index,
	// not the name. The same handler may call Run with the same name
	// multiple times — each call is a fresh entry.
	//
	// If fn returns a *Failure, the failure is recorded as terminal and
	// future replays return the same Failure. Any other error is
	// treated as transient and retried according to the configured
	// backoff policy.
	Run(name string, fn func() ([]byte, error)) ([]byte, error)

	// Call invokes target with input. The returned Future resolves to
	// the callee's response (use Result to block until it lands). The
	// callee runs in its own invocation; this Call appears as a single
	// journal entry on the caller. Optional CallOptions tune the
	// invocation — currently WithIdempotencyKey for the dedup tuple
	// stamped on the outgoing InvokeCommand.
	//
	// Returning a Future instead of blocking inline lets the handler
	// compose Call with other awaitables via All / Any.
	Call(target Target, input []byte, opts ...CallOption) Future

	// OneWayCall invokes target with input and returns immediately. No
	// response is delivered back to this invocation.
	OneWayCall(target Target, input []byte) error

	// GetState reads durable state for key, scoped to the invocation's
	// owning virtual object. Present is false when key is unset; value
	// is nil in that case.
	GetState(key string) (value []byte, present bool, err error)

	// SetState writes durable state for key.
	SetState(key string, value []byte) error

	// ClearState removes durable state for key.
	ClearState(key string) error

	// ClearAllState wipes every state key scoped to the invocation's
	// virtual object. Equivalent to calling ClearState for each key,
	// but performed as a single journal entry + Pebble range delete.
	ClearAllState() error

	// Awakeable creates an awakeable scoped to this invocation. The
	// returned id is a 26-byte "awk_<22 base64url>" string the caller
	// can hand to external systems via SDK output, signals, or any
	// other channel. future.Result blocks until ResolveAwakeable
	// resolves the id (or the invocation is torn down).
	Awakeable() (id string, future Future)

	// All returns a composite handle that resolves when every supplied
	// future has resolved. Results returns the children's values in
	// argument order. If any child resolved with a terminal *Failure,
	// the All composite surfaces that failure on Results (lowest-indexed
	// child wins on ties) — sibling values are discarded.
	//
	// All is pure SDK composition over the children: it does not allocate
	// a journal slot. On replay it reconstructs from the same children
	// (each holding stable journal indices) and re-derives the same
	// outcome deterministically.
	All(futures ...Future) AllResult

	// Any returns a Future that resolves to the first child future to
	// resolve. "First" is deterministic: when poll-time finds multiple
	// children already resolved, the lowest-indexed argument wins —
	// resolution wall-clock order on the original run does not matter.
	// On terminal *Failure of the winning child, Any's Result surfaces
	// that failure.
	Any(futures ...Future) Future

	// SendSignal delivers signalName + payload to target. The signal is
	// journaled on the sender side; the receiver's engine apply path
	// delivers it to the running invocation.
	SendSignal(target Target, signalName string, payload []byte) error
}

// CallOption tunes a Call. Pass via Context.Call's variadic argument.
type CallOption func(*CallOptions)

// CallOptions is the resolved bag of per-Call settings. SDK internals
// build one from the CallOption variadics and forward into the JECall
// journal entry.
type CallOptions struct {
	// IdempotencyKey, when non-empty, is stamped onto the outgoing
	// InvokeCommand.idempotency_key so the engine's onInvoke dedups
	// against (service, handler, object_key, idempotency_key).
	IdempotencyKey string
}

// WithIdempotencyKey requests idempotency dedup for this Call. The
// (target.Service, target.Handler, target.ObjectKey, key) tuple keys
// the dedup; a second Call with the same tuple from any caller reuses
// the first invocation rather than creating a new one.
func WithIdempotencyKey(key string) CallOption {
	return func(o *CallOptions) { o.IdempotencyKey = key }
}

// ApplyCallOptions resolves a slice of options into a single CallOptions.
// Exposed so SDK shim code can collapse the variadic list once and pass
// the resolved struct down.
func ApplyCallOptions(opts []CallOption) CallOptions {
	var resolved CallOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	return resolved
}

// Future is the deferred-result handle returned by Call, Awakeable,
// Sleep, and the Any combinator. Result blocks until the value lands in
// the journal (or the invocation is torn down). On terminal failure
// Result returns a *Failure; on transient abort, ErrSuspended.
//
// Future is the uniform shape combinators (Context.All, Context.Any)
// operate on. Sleep's Future carries a nil byte payload — the resolution
// itself is the signal.
//
// User code MUST NOT implement Future. Combinators down-cast their
// children to Poller (see below) and will panic on any Future obtained
// outside Context methods. This keeps replay determinism predictable:
// only futures backed by journal indices may participate in
// suspend/wake.
type Future interface {
	Result() (value []byte, err error)
}

// Poller is the companion contract every concrete Future returned by
// Context methods also satisfies. The combinators (Context.All,
// Context.Any) use Poller to inspect resolution state and aggregate
// pending suspend tokens without driving suspension through the
// public Future.Result path.
//
// Poller's Poll reports whether the future is resolved. When not yet
// resolved, the second return is the set of suspend tokens the engine
// must observe before progress is possible — the combinators union
// these across children and pass them to invocationContext.suspend.
//
// Poller is exported so the engine invoker package can implement it,
// but it is part of the implementation contract — user code should not
// rely on it.
type Poller interface {
	Poll() (resolved bool, pendingTokens []string)
}

// AllResult is the composite handle returned by Context.All. Results
// blocks until every child future has resolved and then returns the
// children's values in argument order. On terminal failure of any
// child, Results returns the lowest-indexed child's *Failure.
//
// AllResult is intentionally distinct from Future because its payload
// type differs ([][]byte vs []byte). Nest combinators by feeding Any
// outputs (which are Futures) into All; conversely, an AllResult is not
// directly composable as a child — call Results and rewrap if needed.
type AllResult interface {
	Results() (values [][]byte, err error)
}

// AwakeableFuture is retained as an alias of Future so existing callers
// continue to compile; Awakeable returns a Future-shaped handle.
type AwakeableFuture = Future
