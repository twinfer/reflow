package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/twinfer/reflow/pkg/reflow/creds"
)

type authContextKey struct{}

// CallerPrincipal returns the verified caller principal Raw form
// (e.g. "node/1", "operator/alice") stashed by withAuth, or ("",
// false) when the request didn't pass through the middleware (auth
// disabled, or running outside the wired routes).
func CallerPrincipal(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(authContextKey{}).(*creds.Verified)
	if !ok || v == nil {
		return "", false
	}
	return v.CallerPrincipal, true
}

// withAuth wraps next with verification of the request's
// Authorization: Bearer <jwt> header against v. On any verification
// failure the request is rejected with 401, no body, and a
// WWW-Authenticate hint; the failure reason is logged at debug. On
// success the *creds.Verified is stashed in r.Context so downstream
// handlers can call CallerPrincipal.
func withAuth(v *creds.Verifier, log *slog.Logger, next http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, err := bearerToken(r.Header.Get("Authorization"))
		if err != nil {
			deny(w, log, "no_bearer", err)
			return
		}
		verified, err := v.Verify(tok)
		if err != nil {
			deny(w, log, "verify_failed", err)
			return
		}
		ctx := context.WithValue(r.Context(), authContextKey{}, verified)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func bearerToken(h string) (string, error) {
	if h == "" {
		return "", errors.New("authorization header missing")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", errors.New("authorization header not Bearer")
	}
	tok := strings.TrimSpace(strings.TrimPrefix(h, prefix))
	if tok == "" {
		return "", errors.New("bearer token empty")
	}
	return tok, nil
}

func deny(w http.ResponseWriter, log *slog.Logger, reason string, err error) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="reflow"`)
	w.WriteHeader(http.StatusUnauthorized)
	log.Debug("sdk/server: auth denied", "reason", reason, "err", err)
}
