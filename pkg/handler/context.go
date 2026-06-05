package handler

import (
	"context"
	"time"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
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

	// Metadata returns the caller-supplied map stamped onto the
	// SubmitInvocationRequest. Read-only at the handler — mutations to
	// the returned map are local. Useful for webhook adapters: an HTTP
	// middleware verifies a vendor signature and stashes the verified
	// facts (event id, event type, tenant) here so the durable handler
	// routes without re-verifying. Returns an empty (non-nil) map when
	// no metadata was supplied. Stable across replays.
	Metadata() map[string]string

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
	// fn receives a *RunContext carrying the 1-based attempt counter
	// and a stable idempotency key derived from (invocation_id, slot,
	// attempt). Forward the key to any downstream API that supports
	// dedup; the engine guarantees the same key on the same attempt
	// across replays.
	//
	// name is a debug label; durability uses the journal entry index,
	// not the name. The same handler may call Run with the same name
	// multiple times — each call is a fresh entry.
	//
	// If fn returns a *Failure, the failure is recorded as terminal
	// and future replays return the same Failure. Any other error is
	// classified as transient; opts controls how many attempts are
	// allowed before the transient error is promoted to terminal
	// (default 1 — no retry). Retries are scheduled by the engine via
	// a durable backoff timer; the SDK does not loop in-process.
	Run(name string, fn RunFunc, opts ...RunOption) ([]byte, error)

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
	//
	// The engine eager-preloads the (service, object_key) snapshot onto
	// StartMessage.state_map up to a 64 KiB cap; reads hit the local cache
	// without a round-trip. When the snapshot overflowed the cap
	// (StartMessage.partial_state=true) and the key isn't cached, GetState
	// emits a journaled lazy fetch — 2 slots (JEGetState +
	// JEGetStateResult) and suspends pending the engine's reply.
	GetState(key string) (value []byte, present bool, err error)

	// GetStateKeys returns the set of state keys present for the
	// invocation's owning virtual object, in lexicographic order.
	// Equivalent to enumerating the state snapshot eagerly preloaded onto
	// StartMessage.state_map, but always journals a fetch (2 slots:
	// JEGetStateKeys + JEGetStateKeysResult) so the answer is durable —
	// concurrent writes between two GetStateKeys calls are observable.
	//
	// Slot cost: 2 slots.
	GetStateKeys() ([]string, error)

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

	// SendSignal delivers signalName + payload to the active invocation
	// for target's (service, key). The signal is journaled as a single-
	// slot JESignal on the sender side; the engine routes the outbox to
	// the receiver shard via Partitioner.ShardForTarget, where the apply
	// arm resolves target → active InvocationId via KeyLeaseTable and
	// dispatches the signal into the receiver's inbox.
	//
	// target.Key must be non-empty — signals are only valid for keyed
	// services (Virtual Objects and Workflows). SendSignal returns a
	// *Failure with SendSignalUnkeyedCode if Key is empty. Signals to a
	// (service, key) with no active invocation are dropped with a
	// logged warning on the receiver shard.
	//
	// SendSignal is fire-and-forget: the returned error reports local
	// failures (suspended, unkeyed target, step-budget exhausted) only.
	// It does not block on delivery confirmation.
	//
	// Slot cost: 1 slot (JESignal).
	SendSignal(target Target, signalName string, payload []byte) error

	// CancelInvocation forces the active invocation for target's
	// (service, key) to terminate with FailureCode=CancelledCode (9002).
	// Sugar for SendSignal(target, WellKnownCancelSignal, nil) — the
	// receiver shard special-cases the __cancel__ signal name and
	// synthesizes a terminal Completed rather than buffering the signal
	// in the inbox.
	//
	// target.Key must be non-empty (same reason as SendSignal).
	// Cancelling an already-completed invocation is a no-op.
	//
	// Slot cost: 1 slot (the underlying JESignal).
	CancelInvocation(target Target) error

	// WaitSignal returns a Future that resolves when a named signal
	// arrives for this invocation. The result's value is the signal's
	// payload (possibly empty). Two-slot — one for the JEAwaitSignal
	// command, one for the JESignalResult notification.
	//
	// Semantics:
	//   - If a matching signal is already buffered in the inbox (sent
	//     before this WaitSignal call), it is consumed synchronously
	//     and the Future resolves without suspension.
	//   - Otherwise the SDK suspends with a signal:<name>:<slot> token
	//     until a future SendSignal lands; the receiver's apply arm
	//     stitches the result into this invocation's journal at the
	//     awaiter's recorded slot, then wakes the session.
	//   - Each call is single-shot — a second WaitSignal(name) consumes
	//     the next arrival.
	//
	// Slot cost: 2 slots (JEAwaitSignal + JESignalResult).
	WaitSignal(signalName string) Future

	// Promise returns a handle to a workflow-scoped named durable promise.
	// The handle's methods (Result, Peek, Resolve, Reject) operate on the
	// (workflow_service, workflow_key, name) tuple; the workflow's own
	// service + key come from this invocation's Target. Each method
	// allocates journal slots and validates that the invocation is a
	// workflow (KindWorkflow or KindWorkflowShared) — non-workflow
	// callers receive a *Failure on first method use.
	//
	// Promise lifetime is tied to the owning workflow run's retention
	// window; rows persist past the run's Completed status until the
	// retention reaper sweeps them.
	//
	// Constraint: invocations on the workflow's own (service, key) reach
	// the promise table directly; cross-partition resolvers (a different
	// (service, key) explicitly addressing the workflow) use
	// WorkflowPromise(target, name) below — the apply path routes
	// completion via OutboxEnvelope.PromiseCompletion + an ack
	// round-trip.
	//
	// Slot cost per DurablePromise method (counts against
	// DeploymentRecord.max_journal_entries; default
	// limits.DefaultMaxJournalEntries = 10_000):
	//   Result():                 2 slots (JEGetPromise + JEPromiseResult)
	//   Peek():                   1 slot  (JEPeekPromise; snapshot inlined)
	//   Resolve(v) / Reject(err): 2 slots (JECompletePromise +
	//                                      JEPromiseCompleteResult)
	// Promise-heavy workflows hitting the cap will surface as
	// reflow_invocations_completed_total{outcome="step_budget_exhausted"};
	// raise max_journal_entries on the DeploymentRecord rather than
	// silently truncating handler logic.
	Promise(name string) DurablePromise

	// WorkflowPromise returns a handle to a workflow-scoped named
	// promise on a *foreign* workflow — target identifies the workflow
	// to address (target.Service + target.Key); name selects the
	// promise. Use when a child invocation (or any caller off the
	// workflow's own (service, key)) needs to Resolve/Reject/Peek/Result
	// on a workflow it doesn't co-locate with. Apply-path routes the
	// JECompletePromise cross-partition via outbox when target hashes
	// off this caller's shard.
	//
	// target.Handler is ignored — promises are workflow-run-scoped, not
	// handler-scoped. The caller itself must still be a workflow kind
	// (KindWorkflow or KindWorkflowShared); non-workflow callers receive
	// a *Failure on first method use, same as Promise.
	//
	// Slot cost: identical to Promise — see the slot-cost block on
	// Promise above. Cross-partition Resolve/Reject incur ~1 outbox
	// round-trip of additional latency before the ack lands.
	WorkflowPromise(target Target, name string) DurablePromise
}

// DurablePromise is a workflow-scoped named promise. The handle is bound
// to (workflow_service, workflow_key, name) — service+key come from the
// surrounding workflow invocation; the caller supplies name. The four
// methods each allocate their own journal slots; calling Promise multiple
// times with the same name in the same handler is fine — each call
// records an independent operation in the journal.
type DurablePromise interface {
	// Result returns a Future that resolves when the promise is
	// resolved/rejected. Two slots (JEGetPromise + JEPromiseResult). On
	// replay, an already-completed promise resolves synchronously
	// without suspension.
	Result() Future

	// Peek returns the current state without blocking. completed=false
	// means "still pending or absent". A rejected promise surfaces via
	// failure (non-nil). Single slot (JEPeekPromise); store-only on replay.
	Peek() (value []byte, completed bool, failure *Failure, err error)

	// Resolve writes a Resolved PromiseValue and wakes any awaiter.
	// Returns a *Failure with code 0 + message "promise already
	// completed" if a prior Resolve / Reject already terminated the
	// promise. Two slots (JECompletePromise + JEPromiseCompleteResult).
	Resolve(value []byte) error

	// Reject writes a Rejected PromiseValue and wakes any awaiter with
	// the failure. Same conflict semantics as Resolve. Two slots.
	Reject(failure *Failure) error
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
// these across children so the runtime can wait on the merged set.
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
