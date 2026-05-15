package server

import (
	"context"
	"time"

	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// wireContext implements sdk.Context for handlers running on the
// protocolv1 wire. The minimal-viable surface served today is:
//
//   - Context() — the server's request-scoped context
//   - Input()   — bytes received via InputCommandMessage
//   - InvocationID() — reconstructed from StartMessage.id + partition_key
//
// Every durable-execution primitive currently returns ErrWireNotImplemented.
// Wire-protocol expansion for Sleep / Run / Call / State / Awakeable
// lands as the engine-side wire_session grows to emit and consume the
// matching command / notification frames.
type wireContext struct {
	ctx          context.Context
	input        []byte
	invocationID *enginev1.InvocationId
}

// Compile-time check that wireContext satisfies sdk.Context.
var _ sdk.Context = (*wireContext)(nil)

// newWireContext constructs a wireContext for one session. partitionKey
// is derived engine-side from the InvokeCommand's routing; the wire does
// not currently carry it on StartMessage, so we expose 0 here and the
// engine will fill it on the response side. Callers that need a stable
// id round-trip use InvocationID().GetUuid() — the 16-byte uuid that
// the StartMessage frame did carry.
func newWireContext(ctx context.Context, id *enginev1.InvocationId, input []byte) *wireContext {
	return &wireContext{
		ctx:          ctx,
		input:        input,
		invocationID: id,
	}
}

// Context returns the wire session's context.Context. Cancelled when the
// server tears down the session (peer disconnect, Shutdown, or the
// engine cancelling the stream).
func (c *wireContext) Context() context.Context { return c.ctx }

// Input returns the bytes the engine delivered on InputCommandMessage.
// Stable across the lifetime of the session.
func (c *wireContext) Input() []byte { return c.input }

// InvocationID returns the invocation handle reconstructed from the
// StartMessage frame.
func (c *wireContext) InvocationID() *enginev1.InvocationId { return c.invocationID }

// --- Durable primitives: wire path not yet supported ------------------
//
// The wire-protocol expansion that adds command / notification frames
// for these primitives is the next maturation step. Until then every
// method returns ErrWireNotImplemented so handlers running on the wire
// can detect the gap and degrade explicitly instead of silently looking
// like an in-proc context.

func (c *wireContext) Sleep(time.Duration) sdk.Future {
	return notImplementedFuture{}
}

func (c *wireContext) Run(string, func() ([]byte, error)) ([]byte, error) {
	return nil, ErrWireNotImplemented
}

func (c *wireContext) Call(sdk.Target, []byte, ...sdk.CallOption) sdk.Future {
	return notImplementedFuture{}
}

func (c *wireContext) OneWayCall(sdk.Target, []byte) error {
	return ErrWireNotImplemented
}

func (c *wireContext) GetState(string) ([]byte, bool, error) {
	return nil, false, ErrWireNotImplemented
}

func (c *wireContext) SetState(string, []byte) error   { return ErrWireNotImplemented }
func (c *wireContext) ClearState(string) error         { return ErrWireNotImplemented }
func (c *wireContext) ClearAllState() error            { return ErrWireNotImplemented }
func (c *wireContext) Awakeable() (string, sdk.Future) { return "", notImplementedFuture{} }

func (c *wireContext) All(futures ...sdk.Future) sdk.AllResult {
	return notImplementedAllResult{n: len(futures)}
}

func (c *wireContext) Any(...sdk.Future) sdk.Future {
	return notImplementedFuture{}
}

func (c *wireContext) SendSignal(sdk.Target, string, []byte) error {
	return ErrWireNotImplemented
}

// notImplementedFuture is the placeholder Future every wire-only
// durable primitive returns. Result short-circuits with
// ErrWireNotImplemented so handlers see a clean error instead of
// blocking on a never-resolving channel.
type notImplementedFuture struct{}

func (notImplementedFuture) Result() ([]byte, error) { return nil, ErrWireNotImplemented }

// Poll satisfies the sdk.Poller contract that the combinators rely on.
// Reporting resolved=true keeps All / Any from spinning forever waiting
// for the never-resolving future.
func (notImplementedFuture) Poll() (bool, []string) { return true, nil }

type notImplementedAllResult struct{ n int }

func (r notImplementedAllResult) Results() ([][]byte, error) {
	return make([][]byte, r.n), ErrWireNotImplemented
}
