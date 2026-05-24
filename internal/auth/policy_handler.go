package auth

import (
	"log/slog"
	"net/http"

	"connectrpc.com/authn"
)

// principalStampHandler runs after authentication. It reads the Principal the
// authn middleware stamped into the request context, strips any forged inbound
// X-Reflow-Principal header, and re-stamps the server-controlled value so
// downstream HTTP handlers can read a trusted identity. It then attaches the
// Principal to the context via ContextWithPrincipal for the Cedar
// authorization interceptor (internal/authz) and the in-process handlers.
//
// This handler never denies: authentication failures are emitted by the authn
// middleware upstream, and authorization is enforced downstream by the Cedar
// Connect interceptor (which sees the decoded request body). Anonymous
// requests pass through here and are decided by policy at the interceptor.
func principalStampHandler(log *slog.Logger) func(http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Header.Del(PrincipalHeader)
			principal := principalFromAuthInfo(authn.GetInfo(r.Context()))
			if !principal.IsAnonymous() {
				r.Header.Set(PrincipalHeader, principal.Raw)
			}
			r = r.WithContext(ContextWithPrincipal(r.Context(), principal))
			next.ServeHTTP(w, r)
		})
	}
}

// principalFromAuthInfo extracts the Principal stamped by the authn
// middleware. Missing / wrong-type info is treated as anonymous — the Cedar
// interceptor then decides whether anonymous is acceptable for the procedure.
func principalFromAuthInfo(info any) Principal {
	if p, ok := info.(Principal); ok {
		return p
	}
	return Principal{}
}
