package auth

import (
	"context"
	"log/slog"
	"net/http"

	"connectrpc.com/authn"
)

// PrincipalHeader is the canonical HTTP header reflw stamps with the
// server-verified principal (e.g. "node/3", "operator/alice"). Inbound
// values are stripped by the stamp handler to prevent forgery — only
// the server-stamped header survives into downstream handlers.
const PrincipalHeader = "X-Reflow-Principal"

// HTTPMiddleware builds the inbound authentication chain for the Connect-
// mounted HTTP handlers. It does authn only — authorization is enforced
// downstream by the Cedar Connect interceptor (internal/authz), which sees
// the decoded request body. The chain has two steps:
//
//  1. authn.Middleware runs an AuthFunc that resolves the Principal from the
//     verified mTLS leaf CN. Verification failures emit
//     connect.CodeUnauthenticated before any handler or interceptor runs.
//  2. principalStampHandler reads the verified Principal, strips any forged
//     inbound X-Reflow-Principal header, stamps the server-controlled value,
//     and attaches the Principal to the request context for the authz
//     interceptor and the in-process handlers.
//
// closer is a lifecycle hook for the auth middleware; it is currently a
// no-op but is retained so callers' teardown wiring stays stable as authn
// grows resources.
func HTTPMiddleware(log *slog.Logger) (mw func(http.Handler) http.Handler, closer func() error, err error) {
	if log == nil {
		log = slog.Default()
	}
	authnMW := authn.NewMiddleware(composeAuthFunc())
	stamp := principalStampHandler(log)
	mw = func(next http.Handler) http.Handler {
		return authnMW.Wrap(stamp(next))
	}
	return mw, func() error { return nil }, nil
}

// composeAuthFunc resolves the Principal from the verified mTLS leaf CN.
func composeAuthFunc() authn.AuthFunc {
	mesh := meshAuthFunc()
	return func(ctx context.Context, r *http.Request) (any, error) {
		return mesh(ctx, r)
	}
}
