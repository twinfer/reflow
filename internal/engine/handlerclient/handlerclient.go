// Package handlerclient is the engine-side client for handler deployments.
// It owns the connection lifecycle to one deployment URL and runs a single
// invocation session per Invoke call: the engine hands the handler a batch
// of frames (StartMessage + replay) and receives the handler's emitted
// frames (command frames + the terminal frame) back. The exchange is unary
// — the session is half-duplex (the engine never sends mid-session; every
// await resolves via suspend→respawn), so no streaming is needed.
//
// Transports register by URL scheme on the Registry. connectclient is the
// remote transport: a Connect RPC client calling
// /reflw.handler.v1.HandlerService/Invoke for both h2c (http://) and TLS
// (https://) deployments. Translation between protocolv1 commands /
// notifications and enginev1.JournalEntry lives in the invoker's
// wire-session path, not here.
//
// Shared wire vocabulary (Codec, Type* constants, Frame helpers, Route)
// lives in pkg/handler/wire — both engine and handler SDK speak it.
package handlerclient

import (
	"context"
	"errors"

	"github.com/twinfer/reflw/pkg/handler/wire"
	handlerv1 "github.com/twinfer/reflw/proto/handlerv1"
)

// ErrClientClosed is returned by Invoke after Close has been called.
var ErrClientClosed = errors.New("handlerclient: client closed")

// Client is the connection view of one deployment. A Client is bound to a
// single deployment URL + transport + creds tuple at construction and is
// safe for concurrent Invoke calls; the engine reuses one Client across
// every invocation that pins to the deployment.
type Client interface {
	// Invoke runs one full invocation session addressed to route. req
	// carries the StartMessage frame followed by the replay frames; the
	// response carries the handler's emitted command frames followed by
	// the terminal Output/Suspension/Error frame. ctx scopes the call;
	// cancelling it aborts the session.
	Invoke(ctx context.Context, route wire.Route, req *handlerv1.InvokeRequest) (*handlerv1.InvokeResponse, error)

	// Close releases the underlying transport pool. Idempotent. After
	// Close, Invoke returns ErrClientClosed.
	Close() error
}
