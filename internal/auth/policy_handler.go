package auth

import (
	"log/slog"
	"net/http"
	"sync/atomic"

	"connectrpc.com/authn"
	connect "connectrpc.com/connect"
)

// policyHandler is the authz step that runs after authentication.
// It reads the Principal stamped by the authn middleware (or treats
// the request as anonymous when none is present), enforces the path-
// glob Policy, and stamps the server-controlled X-Reflow-Principal
// header for downstream policy-aware code paths.
//
// Denial emits a Connect-coded error response via connect.ErrorWriter
// so clients see CodeUnauthenticated / CodePermissionDenied across
// all four protocols (Connect, gRPC, gRPC-Web, HTTP-JSON). Non-
// Connect HTTP requests get a plain text body with the same status
// codes the legacy middleware used.
func policyHandler(pol *atomic.Pointer[Policy], log *slog.Logger, ew *connect.ErrorWriter) func(http.Handler) http.Handler {
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
			if !pol.Load().Allow(r.URL.Path, principal) {
				log.Warn("auth: policy denied request",
					"path", r.URL.Path, "principal", principal.String())
				writeDenied(w, r, ew, principal)
				return
			}
			r = r.WithContext(ContextWithPrincipal(r.Context(), principal))
			next.ServeHTTP(w, r)
		})
	}
}

// principalFromAuthInfo extracts the Principal stamped by the authn
// middleware. Missing / wrong-type info is treated as anonymous —
// the policy then decides whether anonymous is acceptable on this
// path.
func principalFromAuthInfo(info any) Principal {
	if p, ok := info.(Principal); ok {
		return p
	}
	return Principal{}
}

// writeDenied emits the policy-denial error. Anonymous principals get
// CodeUnauthenticated (authenticate then retry); authenticated-but-
// denied principals get CodePermissionDenied (you're known, not
// allowed). The split mirrors the legacy 401 vs 403 distinction so
// monitoring can separate "no client cert presented" from
// "auth-config rejects principal X". For requests that aren't on a
// Connect-aware protocol, fall back to plain HTTP status + body.
func writeDenied(w http.ResponseWriter, r *http.Request, ew *connect.ErrorWriter, p Principal) {
	var err *connect.Error
	if p.IsAnonymous() {
		err = connect.NewError(connect.CodeUnauthenticated, errUnauthorized)
	} else {
		err = connect.NewError(connect.CodePermissionDenied, errForbidden)
	}
	if ew != nil && ew.IsSupported(r) {
		_ = ew.Write(w, r, err)
		return
	}
	status := http.StatusForbidden
	if p.IsAnonymous() {
		status = http.StatusUnauthorized
	}
	http.Error(w, "auth: "+err.Message(), status)
}

// Sentinel error values used as the wire-side error string. Opaque so
// the policy engine doesn't leak path / principal info to the caller.
var (
	errUnauthorized = newStringError("unauthorized")
	errForbidden    = newStringError("forbidden")
)

type stringError string

func (e stringError) Error() string { return string(e) }

func newStringError(s string) error { return stringError(s) }
