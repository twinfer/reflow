package invoker

import (
	"errors"
	"sync"

	sdkv1 "github.com/twinfer/reflow/proto/sdkv1"
)

// ErrTransportClosed is returned by Send and Recv after Close has been
// called on either side of a transport.
var ErrTransportClosed = errors.New("invoker: transport closed")

// SessionTransport carries SDKMessages between the engine-side session
// goroutine and the SDK-side handler runtime. The in-process Go SDK uses
// a channel-pair (NewChanTransport); the out-of-process wire shim
// (Step 15) implements the same interface over HTTP/2 framing.
//
// Send and Recv are message-oriented. Implementations MUST NOT buffer
// silently — failure to deliver returns an error so the session can
// react (typically by tearing down and proposing Suspended).
type SessionTransport interface {
	Send(msg *sdkv1.SDKMessage) error
	Recv() (*sdkv1.SDKMessage, error)
	Close() error
}

// chanTransport is the in-process implementation backing the Go SDK.
// Two endpoints share the same underlying buffered channels in opposite
// directions; closing either side closes the pair.
type chanTransport struct {
	outbound chan *sdkv1.SDKMessage
	inbound  chan *sdkv1.SDKMessage
	closed   chan struct{}
	once     *sync.Once
}

// NewChanTransport returns a paired engine-side / SDK-side transport.
// Each side's Send reaches the other's Recv. Buffer size is chosen
// generously enough that bidi handshake doesn't deadlock during replay.
func NewChanTransport() (engineSide, sdkSide SessionTransport) {
	const bufSize = 32
	a := make(chan *sdkv1.SDKMessage, bufSize)
	b := make(chan *sdkv1.SDKMessage, bufSize)
	closed := make(chan struct{})
	once := &sync.Once{}
	engineSide = &chanTransport{outbound: a, inbound: b, closed: closed, once: once}
	sdkSide = &chanTransport{outbound: b, inbound: a, closed: closed, once: once}
	return engineSide, sdkSide
}

func (c *chanTransport) Send(msg *sdkv1.SDKMessage) error {
	select {
	case <-c.closed:
		return ErrTransportClosed
	case c.outbound <- msg:
		return nil
	}
}

func (c *chanTransport) Recv() (*sdkv1.SDKMessage, error) {
	select {
	case <-c.closed:
		return nil, ErrTransportClosed
	case msg := <-c.inbound:
		return msg, nil
	}
}

// Close shuts down BOTH sides of the pair. Subsequent Send / Recv
// returns ErrTransportClosed. Safe to call from either endpoint and
// safe to call multiple times.
func (c *chanTransport) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
