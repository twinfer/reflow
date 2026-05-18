package ingress

import (
	"context"
	"time"

	connect "connectrpc.com/connect"
)

// defaultLookupTimeout is the upper bound applied by withDefaultDeadline
// when an inbound RPC arrives without a deadline. dragonboat SyncRead
// requires a deadline; the HTTP-JSON path through Connect forwards the
// request context unchanged and so often has none.
const defaultLookupTimeout = 2 * time.Second

// withDefaultDeadline ensures every inbound RPC carries a deadline. If
// the context already has one the handler runs untouched; otherwise a
// fresh ctx with timeout d is installed for the duration of the call.
func withDefaultDeadline(d time.Duration) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if _, ok := ctx.Deadline(); ok {
				return next(ctx, req)
			}
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, req)
		}
	}
}
