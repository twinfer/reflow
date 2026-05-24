package auth

import (
	"context"
	"log/slog"
	"net/http"

	"connectrpc.com/authn"
)

// PrincipalHeader is the canonical HTTP header reflow stamps with the
// server-verified principal (e.g. "node/3", "operator/alice"). Inbound
// values are stripped by the stamp handler to prevent forgery — only
// the server-stamped header survives into downstream handlers.
const PrincipalHeader = "X-Reflow-Principal"

// HTTPMiddleware builds the inbound authentication chain for the Connect-
// mounted HTTP handlers. It does authn only — authorization is enforced
// downstream by the Cedar Connect interceptor (internal/authz), which sees
// the decoded request body. The chain has two steps:
//
//  1. authn.Middleware runs an AuthFunc that tries the mTLS-leaf CN first,
//     then Bearer-JWT (when cfg.OIDC is non-empty). Verification failures
//     emit connect.CodeUnauthenticated (with an RFC 6750 challenge for bad
//     bearer tokens) before any handler or interceptor runs.
//  2. principalStampHandler reads the verified Principal, strips any forged
//     inbound X-Reflow-Principal header, stamps the server-controlled value,
//     and attaches the Principal to the request context for the authz
//     interceptor and the in-process handlers.
//
// Returns the constructed *JWTVerifier so callers can wire a
// TenantOIDCReconciler against it. Verifier is always non-nil; a snapshot-
// empty verifier means no issuers are configured and bearer auth falls
// through to anonymous (the authz interceptor then decides).
//
// closer is a lifecycle hook for the auth middleware; it is currently a
// no-op (the policy file watcher it once released is gone) but is retained
// so callers' teardown wiring stays stable as authn grows resources.
func HTTPMiddleware(cfg Config, log *slog.Logger) (mw func(http.Handler) http.Handler, closer func() error, verifier *JWTVerifier, err error) {
	if log == nil {
		log = slog.Default()
	}
	jwt, jerr := NewJWTVerifier(context.Background(), cfg.OIDC, log)
	if jerr != nil {
		return nil, nil, nil, jerr
	}
	authFunc := composeAuthFunc(jwt, log)
	authnMW := authn.NewMiddleware(authFunc)
	stamp := principalStampHandler(log)
	mw = func(next http.Handler) http.Handler {
		return authnMW.Wrap(stamp(next))
	}
	return mw, func() error { return nil }, jwt, nil
}

// composeAuthFunc chains the mesh-leaf and Bearer authenticators: mTLS wins
// when both are present (a leaked bearer cannot forge a peer-verified leaf).
// When the mesh step returns a non-anonymous Principal the bearer is not
// consulted; a debug-level log notes the override.
func composeAuthFunc(jwt *JWTVerifier, log *slog.Logger) authn.AuthFunc {
	mesh := meshAuthFunc()
	bearer := bearerAuthFunc(jwt)
	return func(ctx context.Context, r *http.Request) (any, error) {
		info, err := mesh(ctx, r)
		if err != nil {
			return nil, err
		}
		if info != nil {
			if _, hasBearer := authn.BearerToken(r); hasBearer {
				log.Debug("auth: bearer token ignored — verified mTLS leaf present",
					"path", r.URL.Path)
			}
			return info, nil
		}
		return bearer(ctx, r)
	}
}
