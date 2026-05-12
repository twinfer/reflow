package ingress

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

// defaultLookupTimeout is the upper bound applied by withDefaultDeadline
// when an inbound RPC arrives without a deadline. Chosen to match the
// budget previously held by the ad-hoc DescribeInvocation /
// ResolveAwakeable guards. dragonboat SyncRead requires a deadline; the
// HTTP/JSON path through grpc-gateway forwards the request context
// unchanged and so often has none.
const defaultLookupTimeout = 2 * time.Second

// withDefaultDeadline ensures every inbound unary RPC carries a deadline.
// If the context already has one, the handler runs untouched; otherwise
// a fresh ctx with timeout d is installed for the duration of the call.
// The cancel func is released via defer to release runtime timer state.
func withDefaultDeadline(d time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := ctx.Deadline(); ok {
			return handler(ctx, req)
		}
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return handler(ctx, req)
	}
}

// withHTTPDefaultDeadline applies the same "ensure a deadline" guarantee to
// requests served via grpc-gateway. Required because the gateway invokes
// the Server's methods directly (via RegisterIngressHandlerServer) and
// therefore bypasses the gRPC server's UnaryInterceptor chain. The HTTP
// request context carries no deadline by default, which would cause
// dragonboat SyncRead to fail with "deadline not set".
func withHTTPDefaultDeadline(next http.Handler, d time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Deadline(); ok {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
