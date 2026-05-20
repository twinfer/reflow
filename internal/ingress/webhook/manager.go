// Package webhook mounts config-driven inbound vendor webhook
// endpoints on the existing ingress listener. Each configured source
// binds a URL path to a registered pkg/webhook.Verifier; on POST the
// manager pulls the secret, verifies the signature, and dispatches a
// SubmitInvocation to the in-process ingress.Server. Verified facts
// (vendor name, signed timestamp, GitHub delivery ID, …) ride
// through SubmitInvocationRequest.metadata into the durable handler's
// ctx.Metadata().
//
// The mounting flow mirrors internal/ingress/eventsource: factories
// register at package init() time, but no goroutine starts until
// cfg.Webhooks.Sources is non-empty. The escape hatch for vendors we
// don't ship is webhook.RegisterVerifier from operator main().
package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"sort"
	"strings"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/connectserver"
	"github.com/twinfer/reflow/pkg/webhook"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// Submitter is the in-process surface the manager calls. Satisfied by
// *internal/ingress.Server. Mirrors eventsource.Submitter so the same
// dispatch shape works regardless of trigger source.
type Submitter interface {
	SubmitInvocation(ctx context.Context, req *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error)
}

// SourceConfig is the per-source webhook binding the manager mounts.
// All fields are validated by NewManager — paths must be unique +
// non-empty + start with '/', service+handler must be non-empty,
// verifier must resolve via webhook.LookupVerifier.
type SourceConfig struct {
	// Path is the absolute URL path the webhook is mounted at, e.g.
	// "/webhooks/stripe". Operators choose freely; the manager
	// registers each path as its own connectserver.Route.
	Path string
	// Verifier is the registered verifier name (e.g. "stripe",
	// "github", "slack", or any operator-registered name).
	Verifier string
	// Secret is the shared secret passed to Verifier.Verify. Pulled
	// from config at startup; rotation requires restart.
	Secret []byte
	// Service / Handler / ObjectKey populate
	// SubmitInvocationRequest. ObjectKey may be empty for
	// non-keyed (singleton) handlers.
	Service   string
	Handler   string
	ObjectKey string
	// Metadata is the static fact set merged onto verifier-stamped
	// metadata before SubmitInvocation. Use for tenant tags,
	// environment labels, etc. Verifier-stamped keys win on
	// collision (the vendor's stamp is the source of truth).
	Metadata map[string]string
}

// Manager owns the configured webhook sources + their resolved
// verifiers. Stateless apart from the source list; one instance per
// process. Concurrency-safe — verifiers are looked up at construction,
// not per-request, so the hot path holds no mutex.
type Manager struct {
	sources   []resolvedSource
	submitter Submitter
	log       *slog.Logger
}

// resolvedSource is SourceConfig with the verifier resolved from the
// registry and validated. Caching the resolution avoids a registry
// lookup on every request.
type resolvedSource struct {
	cfg      SourceConfig
	verifier webhook.Verifier
}

// ValidateSources runs the field-shape + verifier-registry checks
// that NewManager would run, without requiring a Submitter. Used by
// pkg/reflow.Run as a startup pre-flight so config errors surface
// before the listener binds. Returns nil for an empty sources slice.
func ValidateSources(sources []SourceConfig) error {
	if len(sources) == 0 {
		return nil
	}
	seen := make(map[string]string, len(sources))
	for i, s := range sources {
		if err := validateSource(i, s); err != nil {
			return err
		}
		if prior, dup := seen[s.Path]; dup {
			return fmt.Errorf("webhook: sources[%d]: duplicate path %q (previously bound to verifier %q)", i, s.Path, prior)
		}
		seen[s.Path] = s.Verifier
		if _, err := webhook.LookupVerifier(s.Verifier); err != nil {
			return fmt.Errorf("webhook: sources[%d] (path=%q): %w (registered: %s)", i, s.Path, err, registeredList())
		}
	}
	return nil
}

// NewManager validates sources, resolves verifiers from the
// pkg/webhook registry, and returns a ready-to-mount Manager. Returns
// an error if any source's verifier name isn't registered, paths
// conflict, or required fields are empty. Empty sources slice returns
// a no-op Manager whose Routes() yields nil — callers can wire
// unconditionally.
func NewManager(sources []SourceConfig, submitter Submitter, log *slog.Logger) (*Manager, error) {
	if log == nil {
		log = slog.Default()
	}
	m := &Manager{submitter: submitter, log: log}
	if len(sources) == 0 {
		return m, nil
	}
	if submitter == nil {
		return nil, errors.New("webhook: NewManager requires a non-nil Submitter when sources are configured")
	}
	if err := ValidateSources(sources); err != nil {
		return nil, err
	}
	for _, s := range sources {
		v, _ := webhook.LookupVerifier(s.Verifier) // re-lookup after Validate; cannot fail
		m.sources = append(m.sources, resolvedSource{cfg: s, verifier: v})
	}
	return m, nil
}

// Routes returns one connectserver.Route per configured source. The
// caller (pkg/reflow/run.go) is responsible for wrapping each
// returned handler with the same auth middleware mounted on the
// ingress listener (matching the existing REST + Connect routes).
// Returns nil for an empty-sources manager so the caller's
// ExtraRoutes function can append unconditionally.
func (m *Manager) Routes() []connectserver.Route {
	if len(m.sources) == 0 {
		return nil
	}
	out := make([]connectserver.Route, 0, len(m.sources))
	for _, src := range m.sources {
		out = append(out, connectserver.Route{
			Path:    src.cfg.Path,
			Handler: m.handlerFor(src),
		})
	}
	return out
}

// Close is a no-op for v1 — the manager holds no goroutines or
// long-lived resources. Present for lifecycle symmetry with
// eventsource.Manager; callers can defer it unconditionally.
func (m *Manager) Close() error { return nil }

// handlerFor builds the per-source HTTP handler. Per-request flow:
//  1. enforce POST (every supported vendor uses POST)
//  2. call verifier.Verify(ctx, r, secret)
//  3. merge static + verifier-stamped metadata
//  4. SubmitInvocation via the in-process Server
//  5. 202 Accepted on success, error-coded on failure
func (m *Manager) handlerFor(src resolvedSource) http.Handler {
	errWriter := connect.NewErrorWriter()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ev, err := src.verifier.Verify(r.Context(), r, src.cfg.Secret)
		if err != nil {
			m.log.Warn("webhook: signature rejected",
				"path", src.cfg.Path, "verifier", src.cfg.Verifier, "err", err)
			writeWebhookError(w, r, errWriter, err)
			return
		}
		meta := mergeMetadata(src.cfg.Metadata, ev.Metadata)
		req := connect.NewRequest(&ingressv1.SubmitInvocationRequest{
			Service:   src.cfg.Service,
			Handler:   src.cfg.Handler,
			ObjectKey: src.cfg.ObjectKey,
			Input:     ev.Body,
			Metadata:  meta,
		})
		resp, err := m.submitter.SubmitInvocation(r.Context(), req)
		if err != nil {
			m.log.Warn("webhook: submit failed",
				"path", src.cfg.Path, "verifier", src.cfg.Verifier, "err", err)
			writeWebhookError(w, r, errWriter, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(resp.Msg.GetInvocationIdStr()))
	})
}

// mergeMetadata combines the static SourceConfig.Metadata with the
// verifier-stamped facts. Verifier values win on collision — they're
// the authoritative source for vendor-derived facts (e.g. an operator
// can't override stripe_signed_timestamp with a stale literal).
func mergeMetadata(static, stamped map[string]string) map[string]string {
	if len(static) == 0 && len(stamped) == 0 {
		return nil
	}
	out := make(map[string]string, len(static)+len(stamped))
	maps.Copy(out, static)
	maps.Copy(out, stamped)
	return out
}

// writeWebhookError emits Connect-coded errors when the request shape
// is a Connect protocol (very unlikely for webhooks, but cheap to
// support); otherwise falls back to plain HTTP status + body.
// Webhook callers are conventional HTTP clients (Stripe's
// libraries, GitHub's dispatcher, etc.) that expect plain status
// codes, so the fallback path is the hot one.
func writeWebhookError(w http.ResponseWriter, r *http.Request, ew *connect.ErrorWriter, err error) {
	if cerr, ok := err.(*connect.Error); ok {
		if ew != nil && ew.IsSupported(r) {
			_ = ew.Write(w, r, cerr)
			return
		}
		status := httpStatusFromConnectCode(cerr.Code())
		http.Error(w, "webhook: "+cerr.Message(), status)
		return
	}
	http.Error(w, "webhook: "+err.Error(), http.StatusInternalServerError)
}

// httpStatusFromConnectCode maps a Connect code to the HTTP status
// the manager surfaces on the plain-HTTP fallback path. Only the
// codes the verifier + submit paths can produce are covered; the
// fallback is 500 for anything we don't expect.
func httpStatusFromConnectCode(c connect.Code) int {
	switch c {
	case connect.CodeUnauthenticated:
		return http.StatusUnauthorized
	case connect.CodePermissionDenied:
		return http.StatusForbidden
	case connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeOutOfRange:
		return http.StatusBadRequest
	case connect.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable
	case connect.CodeResourceExhausted:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

func validateSource(i int, s SourceConfig) error {
	if s.Path == "" || !strings.HasPrefix(s.Path, "/") {
		return fmt.Errorf("webhook: sources[%d]: path %q must be absolute (start with '/')", i, s.Path)
	}
	if s.Verifier == "" {
		return fmt.Errorf("webhook: sources[%d] (path=%q): verifier is required", i, s.Path)
	}
	if s.Service == "" {
		return fmt.Errorf("webhook: sources[%d] (path=%q): invocation.service is required", i, s.Path)
	}
	if s.Handler == "" {
		return fmt.Errorf("webhook: sources[%d] (path=%q): invocation.handler is required", i, s.Path)
	}
	if len(s.Secret) == 0 {
		return fmt.Errorf("webhook: sources[%d] (path=%q): secret is required", i, s.Path)
	}
	return nil
}

func registeredList() string {
	names := webhook.RegisteredNames()
	sort.Strings(names)
	return strings.Join(names, ",")
}
