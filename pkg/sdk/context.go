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

	// Sleep blocks until d has elapsed in wall-clock time. The wake-up
	// instant is durable: a restart preserves the original wake time so
	// the handler resumes exactly when it would have, even if the
	// process was down for some of the interval.
	Sleep(d time.Duration) error

	// Run executes fn at most once and journals the outcome. On every
	// replay the journaled bytes are returned without re-invoking fn.
	//
	// name is a debug label; durability uses the journal entry index,
	// not the name. The same handler may call Run with the same name
	// multiple times — each call is a fresh entry.
	//
	// If fn returns a *Failure, the failure is recorded as terminal and
	// future replays return the same Failure. Any other error is
	// treated as transient (Phase 2: retried indefinitely; Phase 3 adds
	// policies).
	Run(name string, fn func() ([]byte, error)) ([]byte, error)

	// Call invokes target with input and blocks for the response. The
	// callee runs in its own invocation; this Call appears as a single
	// journal entry on the caller.
	Call(target Target, input []byte) ([]byte, error)

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

	// Awakeable creates an awakeable scoped to this invocation. The
	// returned id is a 26-byte "awk_<22 base64url>" string the caller
	// can hand to external systems via SDK output, signals, or any
	// other channel. future.Result blocks until ResolveAwakeable
	// resolves the id (or the invocation is torn down).
	Awakeable() (id string, future AwakeableFuture)

	// SendSignal delivers signalName + payload to target. The signal is
	// journaled on the sender side; the receiver's engine apply path
	// delivers it to the running invocation.
	SendSignal(target Target, signalName string, payload []byte) error
}

// AwakeableFuture is the per-awakeable handle returned by Context.Awakeable.
// Result blocks until ResolveAwakeable resolves the awakeable, the
// resolution journal entry is observed on replay, or the invocation is
// torn down. On terminal failure, Result returns a *Failure; on transient
// abort, ErrSuspended.
type AwakeableFuture interface {
	Result() (value []byte, err error)
}
