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
// protocolv1 wire. Serves Context() / Input() / InvocationID(), the
// state-write primitives (SetState / ClearState / ClearAllState), and
// GetState backed by the eager-preloaded state_map shipped in
// StartMessage.
//
// Sleep / Run / Call / OneWayCall / Awakeable / SendSignal still return
// ErrWireNotImplemented; the replay-and-suspend infrastructure that
// backs them lands in 5f.3-5f.6.
type wireContext struct {
	ctx          context.Context
	input        []byte
	invocationID *enginev1.InvocationId

	stream frameStream
	codec  handlerclient.Codec

	// stateCache is the eager-preloaded K/V snapshot for this
	// invocation's (service, object_key), populated from
	// StartMessage.state_map. GetState reads from this directly; writes
	// (SetState, ClearState, ClearAllState) update it inline to keep
	// reads in the same session coherent with their preceding writes.
	stateCache map[string][]byte

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
	stateCache map[string][]byte,
) *wireContext {
	return &wireContext{
		ctx:          ctx,
		input:        input,
		invocationID: id,
		stream:       stream,
		codec:        codec,
		stateCache:   stateCache,
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

// GetState serves the eager-preloaded value for key. Returns
// (nil, false, nil) when the key isn't present in the snapshot.
// Reads after SetState / ClearState within the same session see the
// updated cache so handlers don't double-bounce through the wire to
// observe their own writes.
func (c *wireContext) GetState(key string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stateCache == nil {
		return nil, false, nil
	}
	v, ok := c.stateCache[key]
	if !ok {
		return nil, false, nil
	}
	// Copy out so handler mutations don't poison the cache.
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

// SetState journals a state write by emitting SetStateCommandMessage.
// The engine decodes the frame, proposes JESetState, and the apply path
// commits the row to StateTable. The eager cache is updated inline so
// subsequent GetState calls in this session observe the write.
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
	if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdSetState, payload)); err != nil {
		return err
	}
	c.mu.Lock()
	if c.stateCache == nil {
		c.stateCache = make(map[string][]byte)
	}
	c.stateCache[key] = append([]byte(nil), value...)
	c.mu.Unlock()
	return nil
}

// ClearState removes durable state for key. Write-only — no completion.
// Eager cache is updated inline so subsequent GetState in this session
// returns (nil, false, nil).
func (c *wireContext) ClearState(key string) error {
	c.allocSlot(1)
	msg := &protocolv1.ClearStateCommandMessage{Key: []byte(key)}
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal ClearStateCommandMessage: %w", err)
	}
	if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdClearState, payload)); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.stateCache, key)
	c.mu.Unlock()
	return nil
}

// ClearAllState wipes every state row scoped to the invocation's
// (service, object_key). Journaled as a single JEClearAllState entry;
// the eager cache is reset inline.
func (c *wireContext) ClearAllState() error {
	c.allocSlot(1)
	msg := &protocolv1.ClearAllStateCommandMessage{}
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal ClearAllStateCommandMessage: %w", err)
	}
	if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdClearAllState, payload)); err != nil {
		return err
	}
	c.mu.Lock()
	for k := range c.stateCache {
		delete(c.stateCache, k)
	}
	c.mu.Unlock()
	return nil
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
