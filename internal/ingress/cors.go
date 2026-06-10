package ingress

import (
	"net/http"
	"slices"
	"time"

	connectcors "connectrpc.com/cors"
	"github.com/rs/cors"
)

// CORSConfig drives browser cross-origin access to the ingress listener. The
// zero value (no origins) disables CORS entirely — the engine's default, since
// same-origin and non-browser clients never need it. Mirrors the public
// pkg/reflw.CORSConfig.
type CORSConfig struct {
	// AllowedOrigins is the exact-match origin allowlist (scheme+host+port),
	// e.g. "https://console.example.com". Empty disables CORS. "*" allows any
	// origin, but then credentials are not advertised (browsers forbid a
	// credentialed wildcard response).
	AllowedOrigins []string
	// AllowedHeaders are extra request headers permitted on top of the built-in
	// set (the Connect/gRPC-Web protocol headers plus Authorization and
	// Idempotency-Key). Add custom carriers here, e.g. "Reflw-Meta-Tenant".
	AllowedHeaders []string
	// MaxAgeSeconds caps how long a browser may cache a preflight. 0 → 7200 (2h).
	MaxAgeSeconds int
}

// Enabled reports whether any origin is allowlisted.
func (c CORSConfig) Enabled() bool { return len(c.AllowedOrigins) > 0 }

// corsMiddleware wraps next with a CORS handler built from the connectrpc-
// recommended header/method sets (connectrpc.com/cors) so Connect, gRPC-Web,
// and REST all work from a browser. It must sit OUTERMOST — preflight OPTIONS
// requests carry no credentials and are answered here before auth runs.
// Credentials are advertised only for a concrete origin allowlist (not "*"),
// matching the browser rule that forbids a credentialed wildcard response.
func corsMiddleware(c CORSConfig) func(http.Handler) http.Handler {
	maxAge := c.MaxAgeSeconds
	if maxAge == 0 {
		maxAge = int(2 * time.Hour / time.Second)
	}
	allowHeaders := append(connectcors.AllowedHeaders(), "Authorization", "Idempotency-Key")
	allowHeaders = append(allowHeaders, c.AllowedHeaders...)
	h := cors.New(cors.Options{
		AllowedOrigins:   c.AllowedOrigins,
		AllowedMethods:   connectcors.AllowedMethods(),
		AllowedHeaders:   allowHeaders,
		ExposedHeaders:   connectcors.ExposedHeaders(),
		AllowCredentials: !slices.Contains(c.AllowedOrigins, "*"),
		MaxAge:           maxAge,
	})
	return h.Handler
}
