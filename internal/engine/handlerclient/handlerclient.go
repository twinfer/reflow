// Package handlerclient is the engine-side wire client for remote
// (out-of-process) handler deployments. It owns the connection lifecycle
// to one deployment URL and exposes a transport-neutral Stream that the
// invoker drives to run a single session.
//
// The only transport is connectclient: a Connect RPC client opening
// HandlerService.InvokeStream against /reflow.handler.v1.HandlerService/
// for both h2c (http://) and TLS (https://) deployments.
//
// The engine selects the dialer by URL scheme via Registry.For; once a
// Client is in hand, the invoker calls Invoke to open a stream and drives
// protocolv1 frames through it. Translation between protocolv1 commands /
// notifications and enginev1.JournalEntry lives in the invoker's
// wire-session path, not here — the handlerclient layer is the byte pump.
//
// Shared wire vocabulary (Codec, Type* constants, Frame helpers, Route)
// lives in pkg/handler/wire — both engine and handler SDK speak it.
package handlerclient

import (
	"context"
	"errors"

	"github.com/twinfer/reflow/pkg/handler/wire"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// ErrClientClosed is returned by Invoke and Stream methods after Close
// has been called on the Client.
var ErrClientClosed = errors.New("handlerclient: client closed")

// Client is the connection-pool view of one deployment. A Client is bound
// to a single deployment URL + transport + creds tuple at construction
// and is safe for concurrent Invoke calls; the engine reuses one Client
// across every invocation that pins to the deployment.
type Client interface {
	// Invoke opens a single session stream addressed to route. The engine
	// sends a StartMessage as the first frame and reads command /
	// notification frames until the stream returns io.EOF or an error.
	//
	// ctx scopes the lifetime of the stream; cancelling ctx tears it
	// down. The returned Stream is owned by the caller and must be
	// drained or closed.
	Invoke(ctx context.Context, route wire.Route) (Stream, error)

	// Close releases the underlying HTTP/2 transport pool. Idempotent.
	// After Close, Invoke returns ErrClientClosed.
	Close() error
}

// Stream is the bidirectional frame pipe for one session. Send is called
// by the engine after preparing a frame (StartMessage, notifications,
// acks); Recv blocks until the handler sends a frame, returning io.EOF
// when the handler closes the stream.
type Stream interface {
	Send(*protocolv1.Frame) error
	Recv() (*protocolv1.Frame, error)
	// CloseSend signals the handler that the engine will send no more
	// frames. The handler may continue to send until it sends EndMessage.
	CloseSend() error
}
