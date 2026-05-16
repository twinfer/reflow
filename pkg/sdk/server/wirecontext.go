package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// wireContext implements sdk.Context for handlers running on the
// protocolv1 wire. Serves Context() / Input() / InvocationID() and the
// state-write primitives (SetState / ClearState / ClearAllState), which
// are pure command frames with no completion notification.
//
// Sleep / Run / Call / OneWayCall / GetState / Awakeable / SendSignal
// still return ErrWireNotImplemented; the replay-and-suspend
// infrastructure that backs them lands in 5f.2-5f.6.
type wireContext struct {
	ctx          context.Context
	input        []byte
	invocationID *enginev1.InvocationId

	stream frameStream
	codec  handlerclient.Codec

	mu       sync.Mutex
	nextSlot uint32
}

var _ sdk.Context = (*wireContext)(nil)

// newWireContext constructs a wireContext for one session. stream is the
// transport-neutral frame view (HTTP/2 server adapter on the handler
// side); codec must match the engine's. nextSlot starts at 1 because
// slot 0 is reserved for JEInput (the engine writes it before the
// handler runs).
func newWireContext(
	ctx context.Context,
	id *enginev1.InvocationId,
	input []byte,
	stream frameStream,
	codec handlerclient.Codec,
) *wireContext {
	return &wireContext{
		ctx:          ctx,
		input:        input,
		invocationID: id,
		stream:       stream,
		codec:        codec,
		nextSlot:     1,
	}
}

func (c *wireContext) Context() context.Context             { return c.ctx }
func (c *wireContext) Input() []byte                        { return c.input }
func (c *wireContext) InvocationID() *enginev1.InvocationId { return c.invocationID }

// allocSlot reserves span consecutive journal indices and returns the
// first. Mirrors inproc.go's allocSlot contract so replay-by-slot lines
// up across the two impls.
func (c *wireContext) allocSlot(span uint32) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	start := c.nextSlot
	c.nextSlot += span
	return start
}

// SetState journals a state write by emitting SetStateCommandMessage.
// The engine decodes the frame, proposes JESetState, and the apply path
// commits the row to StateTable.
func (c *wireContext) SetState(key string, value []byte) error {
	c.allocSlot(1)
	msg := &protocolv1.SetStateCommandMessage{
		Key:   []byte(key),
		Value: &protocolv1.Value{Content: value},
	}
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal SetStateCommandMessage: %w", err)
	}
	return c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdSetState, payload))
}

// ClearState removes durable state for key. Write-only — no completion.
func (c *wireContext) ClearState(key string) error {
	c.allocSlot(1)
	msg := &protocolv1.ClearStateCommandMessage{Key: []byte(key)}
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal ClearStateCommandMessage: %w", err)
	}
	return c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdClearState, payload))
}

// ClearAllState wipes every state row scoped to the invocation's
// (service, object_key). Journaled as a single JEClearAllState entry.
func (c *wireContext) ClearAllState() error {
	c.allocSlot(1)
	msg := &protocolv1.ClearAllStateCommandMessage{}
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal ClearAllStateCommandMessage: %w", err)
	}
	return c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdClearAllState, payload))
}

// --- Durable primitives still gated on wire-protocol expansion --------

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
func (notImplementedFuture) Poll() (bool, []string)  { return true, nil }

type notImplementedAllResult struct{ n int }

func (r notImplementedAllResult) Results() ([][]byte, error) {
	return make([][]byte, r.n), ErrWireNotImplemented
}
