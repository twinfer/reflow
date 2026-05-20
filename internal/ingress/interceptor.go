package ingress

import (
	"context"
	"time"

	connect "connectrpc.com/connect"
)

// defaultLookupTimeout is the upper bound applied by deadlineInterceptor
// when an inbound RPC arrives without a deadline. dragonboat SyncRead
// requires a deadline; the HTTP-JSON path through Connect forwards the
// request context unchanged and so often has none.
const defaultLookupTimeout = 2 * time.Second

// withDefaultDeadline returns a connect.Interceptor that ensures every
// inbound unary or streaming RPC carries a deadline. If the context
// already has one the handler runs untouched; otherwise a fresh ctx
// with timeout d is installed for the duration of the call.
//
// Implements the full connect.Interceptor interface so a future
// streaming ingress RPC inherits the same deadline guarantee — using
// connect.UnaryInterceptorFunc would silently skip streaming handlers
// (see https://connectrpc.com/docs/go/streaming/).
func withDefaultDeadline(d time.Duration) connect.Interceptor {
	return &deadlineInterceptor{d: d}
}

type deadlineInterceptor struct {
	d time.Duration
}

func (i *deadlineInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if _, ok := ctx.Deadline(); ok {
			return next(ctx, req)
		}
		ctx, cancel := context.WithTimeout(ctx, i.d)
		defer cancel()
		return next(ctx, req)
	}
}

func (i *deadlineInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		if _, ok := ctx.Deadline(); ok {
			return next(ctx, spec)
		}
		ctx, cancel := context.WithTimeout(ctx, i.d)
		conn := next(ctx, spec)
		return &cancelOnCloseConn{StreamingClientConn: conn, cancel: cancel}
	}
}

func (i *deadlineInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if _, ok := ctx.Deadline(); ok {
			return next(ctx, conn)
		}
		ctx, cancel := context.WithTimeout(ctx, i.d)
		defer cancel()
		return next(ctx, conn)
	}
}

// cancelOnCloseConn wires the WithTimeout cancel into the streaming
// client connection's lifecycle: cancel fires when the caller closes
// either side of the stream, so the deadline context is released
// promptly instead of leaking until the timeout expires.
type cancelOnCloseConn struct {
	connect.StreamingClientConn
	cancel context.CancelFunc
}

func (c *cancelOnCloseConn) CloseRequest() error {
	err := c.StreamingClientConn.CloseRequest()
	c.cancel()
	return err
}

func (c *cancelOnCloseConn) CloseResponse() error {
	err := c.StreamingClientConn.CloseResponse()
	c.cancel()
	return err
}
