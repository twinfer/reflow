// Package httpingress mounts a REST-style facade over the existing
// Connect ingress on the same listener. Each handler builds a typed
// *connect.Request and delegates to *ingress.Server, so routing,
// idempotency dedup, and Connect-level error semantics are inherited
// unchanged. The Connect surface at /reflow.ingress.v1.Ingress/* and
// this REST surface at /v1/* are two URL shapes over the same
// in-process server.
package httpingress

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/observability"
)

// defaults applied when IngressHTTPConfig leaves a knob at zero.
const (
	defaultMaxBodyBytes = 1 << 20 // 1 MiB
	defaultMaxPollMs    = 30_000  // 30s — under common LB idle timeouts
	hardMaxPollMs       = 30_000  // REST never exceeds 30s even if caller asks for more
)

// Config is the runtime subset of pkg/reflow.IngressHTTPConfig that the
// REST router actually consumes. Plumbed through ingress.Config so the
// public surface stays in pkg/reflow.
type Config struct {
	MaxBodyBytes int64
	MaxPollMs    int
}

// NewRouter builds the /v1/* REST handler bound to srv. The returned
// http.Handler is mounted as a connectserver.Route on the same listener
// as Connect; the caller is responsible for wrapping it with the same
// auth middleware Connect uses (the SPIFFE policy enforces /v1/* via the
// ingress_rest_open allow rule).
//
// Metrics may be nil — observability is opt-in.
func NewRouter(srv *ingress.Server, cfg Config, metrics *observability.Metrics) http.Handler {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultMaxBodyBytes
	}
	if cfg.MaxPollMs <= 0 || cfg.MaxPollMs > hardMaxPollMs {
		cfg.MaxPollMs = defaultMaxPollMs
	}

	h := &restHandlers{srv: srv, cfg: cfg}

	r := chi.NewRouter()
	// SyncRead inside *ingress.Server requires a context with a deadline
	// (dragonboat enforces it). The Connect ingress installs a 2s default
	// via the withDefaultDeadline interceptor; REST handlers bypass that
	// chain, so the equivalent default is installed here. The cap is sized
	// to cover the long-poll endpoints — REST callers can still send a
	// shorter ctx via the standard HTTP client semantics, and the handler
	// honors whichever fires first.
	r.Use(ensureDeadline(time.Duration(cfg.MaxPollMs+5_000) * time.Millisecond))
	r.Use(bodyCap(cfg.MaxBodyBytes))
	if metrics != nil {
		r.Use(instrument(metrics))
	}

	r.Route("/v1", func(r chi.Router) {
		// submit + await
		r.Post("/call/{service}/{handler}", h.call)
		r.Post("/call/{service}/{key}/{handler}", h.callKeyed)
		// submit only
		r.Post("/send/{service}/{handler}", h.send)
		r.Post("/send/{service}/{key}/{handler}", h.sendKeyed)
		// attach / output / cancel
		r.Get("/attach/{invocation_id}", h.attach)
		r.Get("/output/{invocation_id}", h.output)
		r.Post("/cancel/{invocation_id}", h.cancel)
		// awakeables / workflow promises
		r.Post("/awakeables/{awakeable_id}/resolve", h.resolveAwakeable)
		r.Post("/promises/{service}/{workflow_key}/{name}/resolve", h.resolveWorkflowPromise)
		// state read
		r.Get("/state/{service}/{key}/{state_key}", h.getState)
	})
	return r
}

// ensureDeadline installs a default context timeout when the inbound
// request has none. Mirrors withDefaultDeadline on the Connect path so
// REST handlers can call into *ingress.Server (which does dragonboat
// SyncRead under the hood — that rejects deadlineless contexts with
// "deadline not set"). When the caller's context already has a deadline
// it passes through untouched.
func ensureDeadline(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
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
}

// bodyCap wraps r.Body in http.MaxBytesReader so oversize requests are
// rejected with 413 Payload Too Large before handler logic runs. The
// Reader's error is surfaced to handlers via the standard io.Read
// contract, so handlers don't need to know the cap value.
func bodyCap(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// instrument increments IngressRESTRequests on every request, labeled by
// the chi route template (so cardinality stays bounded — never the raw
// path), method, and the response status family.
func instrument(m *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)
			rctx := chi.RouteContext(r.Context())
			route := "unknown"
			if rctx != nil && rctx.RoutePattern() != "" {
				route = rctx.RoutePattern()
			}
			m.IngressRESTRequests.
				WithLabelValues(route, r.Method, statusClass(sw.status)).
				Inc()
		})
	}
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (sw *statusWriter) WriteHeader(code int) {
	if !sw.wroteHeader {
		sw.status = code
		sw.wroteHeader = true
	}
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Write(b []byte) (int, error) {
	if !sw.wroteHeader {
		sw.wroteHeader = true
	}
	return sw.ResponseWriter.Write(b)
}

func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}

// parseTimeoutMs reads ?timeout_ms=… and clamps it into [0, cfg.MaxPollMs].
// Zero means "use the configured default cap" — there is no notion of
// "wait forever" on the REST surface.
func parseTimeoutMs(r *http.Request, cap int) uint32 {
	q := r.URL.Query().Get("timeout_ms")
	if q == "" {
		return uint32(cap)
	}
	v, err := strconv.Atoi(q)
	if err != nil || v <= 0 {
		return uint32(cap)
	}
	if v > cap {
		v = cap
	}
	return uint32(v)
}
