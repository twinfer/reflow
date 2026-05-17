package sdk

import (
	"context"
	"time"
)

// RunContext is the parameter passed to fn in ctx.Run. It exposes the
// per-attempt metadata the journal records about this invocation of
// the side-effect body — attempt count, a stable idempotency key the
// user should forward to any downstream API that supports dedup, and
// a cancellation channel.
//
// The same RunContext is rebuilt on retries with a fresh attempt
// number + idempotency key; downstream systems can tell a retry from
// a duplicate by comparing keys.
type RunContext struct {
	attempt        uint32
	idempotencyKey string
	ctx            context.Context
}

// Attempt is the 1-based count of fn invocations for this ctx.Run
// site. attempt=1 on the first call, attempt=2 on the first retry,
// and so on.
func (r *RunContext) Attempt() uint32 { return r.attempt }

// IdempotencyKey is a stable token derived from (invocation_id, slot,
// attempt). The engine recomputes the same value when replaying so
// the SDK and engine agree on the per-attempt key without a round
// trip. Forward this on outbound requests whose backend supports
// idempotency (Stripe Idempotency-Key, AWS request tokens, etc.) so
// retries dedup downstream.
func (r *RunContext) IdempotencyKey() string { return r.idempotencyKey }

// Done is cancelled when the engine is tearing down this attempt
// (invocation shutdown or per-attempt deadline). Wire it into outbound
// I/O so a timely shutdown propagates instead of fn blocking past the
// engine's tolerance.
func (r *RunContext) Done() <-chan struct{} {
	if r.ctx == nil {
		return nil
	}
	return r.ctx.Done()
}

// NewRunContext constructs a RunContext for tests or for SDK-internal
// fixtures. Production code receives a RunContext synthesized by the
// SDK runtime; this constructor is here so callers writing handler
// unit tests can stand one up without reaching into unexported fields.
func NewRunContext(ctx context.Context, attempt uint32, idempotencyKey string) *RunContext {
	return &RunContext{ctx: ctx, attempt: attempt, idempotencyKey: idempotencyKey}
}

// RunFunc is the user-supplied side-effect body passed to ctx.Run.
type RunFunc = func(*RunContext) ([]byte, error)

// RunOption tunes a ctx.Run call. Construct via MaxAttempts, Backoff,
// or RetryPolicy and pass through the variadic argument.
type RunOption func(*RunOptions)

// RunOptions is the resolved bag of per-Run settings. Field
// visibility mirrors CallOptions: SDK shim code reads these directly
// to fill the wire policy and route per-attempt options.
type RunOptions struct {
	// MaxAttempts is the maximum total fn invocations (initial run +
	// retries combined). Zero defers to the engine default (1 — no
	// retry). To request effectively-unbounded retries set a large
	// explicit value such as math.MaxUint32.
	MaxAttempts uint32

	// InitialInterval is the first retry delay. Zero defers to the
	// engine default (50ms).
	InitialInterval time.Duration

	// Factor is the exponential growth factor. Zero defers to the
	// engine default (2.0).
	Factor float64

	// MaxInterval caps the per-retry delay. Zero defers to the
	// engine default (10s).
	MaxInterval time.Duration
}

// MaxAttempts sets the retry budget. MaxAttempts(1) (the default)
// means no retry — fn runs once and any transient error becomes
// terminal. MaxAttempts(N) permits up to (N-1) retries.
func MaxAttempts(n uint32) RunOption {
	return func(o *RunOptions) { o.MaxAttempts = n }
}

// Backoff overrides the exponential backoff schedule. Pass zeros for
// fields you want to leave at engine defaults.
func Backoff(initial time.Duration, factor float64, maxInterval time.Duration) RunOption {
	return func(o *RunOptions) {
		o.InitialInterval = initial
		o.Factor = factor
		o.MaxInterval = maxInterval
	}
}

// ApplyRunOptions resolves a slice of options into a single RunOptions.
// Exposed so SDK shim code can collapse the variadic list once and pass
// the resolved struct down.
func ApplyRunOptions(opts []RunOption) RunOptions {
	var resolved RunOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	return resolved
}
