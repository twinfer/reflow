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
//  1. authn.Middleware runs an AuthFunc that resolves the Principal mesh-mTLS
//     first, then (when oidc is non-nil and enabled) an OIDC bearer token.
//     Verification failures emit connect.CodeUnauthenticated before any handler
//     or interceptor runs.
//  2. principalStampHandler reads the verified Principal, strips any forged
//     inbound X-Reflow-Principal header, stamps the server-controlled value,
//     and attaches the Principal to the request context for the authz
//     interceptor and the in-process handlers.
//
// ctx is used only to discover the OIDC provider at build time (network I/O);
// a discovery failure surfaces here as an error. oidc == nil (or disabled)
// yields the mesh-only chain — every mesh-only listener (delivery, admin) and
// the tests pass nil.
//
// closer is a lifecycle hook for the auth middleware; it is currently a
// no-op but is retained so callers' teardown wiring stays stable as authn
// grows resources.
func HTTPMiddleware(ctx context.Context, log *slog.Logger, oidc *OIDCConfig) (mw func(http.Handler) http.Handler, closer func() error, err error) {
	if log == nil {
		log = slog.Default()
	}
	var oidcAuth authn.AuthFunc
	if oidc != nil && oidc.Enabled() {
		oa, oerr := newOIDCAuthFunc(ctx, *oidc)
		if oerr != nil {
			return nil, nil, oerr
		}
		oidcAuth = oa
	}
	authnMW := authn.NewMiddleware(composeAuthFunc(oidcAuth))
	stamp := principalStampHandler(log)
	mw = func(next http.Handler) http.Handler {
		return authnMW.Wrap(stamp(next))
	}
	return mw, func() error { return nil }, nil
}

// composeAuthFunc resolves the Principal mesh-mTLS-first, then (if configured)
// an OIDC bearer token. A node/operator leaf is never shadowed by a token: mesh
// wins, OIDC runs only when there is no mesh identity. A malformed verified leaf
// is a hard failure (mesh returns the error); with no mesh identity and no OIDC,
// the request is anonymous.
func composeAuthFunc(oidcAuth authn.AuthFunc) authn.AuthFunc {
	mesh := meshAuthFunc()
	return func(ctx context.Context, r *http.Request) (any, error) {
		info, err := mesh(ctx, r)
		if err != nil {
			return nil, err
		}
		if info != nil {
			return info, nil
		}
		if oidcAuth != nil {
			return oidcAuth(ctx, r)
		}
		return nil, nil
	}
}
