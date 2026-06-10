package ingress

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	connect "connectrpc.com/connect"
	"connectrpc.com/vanguard"

	"github.com/twinfer/reflw/internal/connectserver"
	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/proto/ingressv1/ingressv1connect"
)

// maxIngressMessageBytes caps the buffered request/response payload Vanguard
// will transcode (4 MiB). Larger payloads fail with resource-exhausted.
const maxIngressMessageBytes uint32 = 4 << 20

// Config is the minimum the ingress runtime needs. Mirrors the public
// pkg/reflw.IngressConfig but kept internal so engine packages don't
// pull in the public surface.
type Config struct {
	// Addr is the listen address. Connect content-negotiates Connect /
	// gRPC / gRPC-Web / HTTP-JSON on this single port, and Vanguard adds
	// REST+JSON for every RPC carrying a google.api.http annotation.
	Addr string
	// TLS, when non-nil, wraps the listener with TLS (HTTP/2 over TLS).
	// Nil enables h2c.
	TLS *tls.Config
	// Log is the structured logger; defaults to slog.Default.
	Log *slog.Logger
	// Middleware wraps the transcoder with the unified auth middleware
	// (auth.HTTPMiddleware). REQUIRED — Start returns an error when nil.
	// Anonymous traffic is permitted only by the policy (the foundational
	// policy opens the ingress plane to any principal), not by skipping the
	// middleware. Tests that intentionally bypass auth must pass an explicit
	// identity passthrough.
	Middleware func(http.Handler) http.Handler
	// AuthzInterceptor enforces Cedar authorization on every ingress RPC. It
	// runs inside the Connect handler, after the auth Middleware has attached
	// the verified principal — and because Vanguard transcodes REST into a
	// Connect call against the same handler, it gates the REST routes too.
	// REQUIRED — Start returns an error when nil, so the ingress data plane is
	// never served without authorization.
	AuthzInterceptor connect.Interceptor
	// ExtraRoutes builds additional connectserver.Routes mounted on the
	// same listener as the transcoder. Called once after Start constructs the
	// in-process Server, so the caller can wire handlers (notably webhook
	// receivers) that need *Server directly. Their (validated) paths out-rank
	// the "/" transcoder on the ServeMux. The caller is responsible for the
	// auth middleware on these handlers; Start does not wrap them. Webhooks
	// deliberately skip it (HMAC is their gate).
	ExtraRoutes func(srv *Server) []connectserver.Route
	// TaskSchemaResolver, when non-nil, lets GET /v1/tasks/{token} return a parked
	// task's submission JSON Schema alongside its descriptor. Resolved from the
	// active model resolver (the table-backed reflwos resolver satisfies it); nil →
	// the read returns the descriptor only. Held as an interface so this package
	// never imports reflwos.
	TaskSchemaResolver TaskSchemaResolver
	// CORS, when Enabled (non-empty AllowedOrigins), wraps the transcoder with a
	// browser CORS handler — outermost, so preflight is answered before auth. The
	// zero value disables CORS (same-origin / non-browser clients need none).
	CORS CORSConfig
	// ServeAdmin mounts the adminv1 service on this ingress listener as a second
	// Vanguard service (Connect / gRPC / gRPC-Web), making ingress a BFF: a
	// browser console reaches both the data plane and admin on one origin.
	// AdminPath + AdminHandler must be set when true. The standalone mTLS admin
	// listener is unaffected — both dispatch to the same admin.Server, and
	// authorization is the same Cedar interceptor, so cluster-admin stays
	// operator-only and app-config is group-gated.
	ServeAdmin bool
	// AdminPath / AdminHandler are the adminv1 Connect handler (from
	// admin.Server.NewHandler) mounted when ServeAdmin is set. The handler must
	// already carry its interceptor chain (authz + proposal-principal); the outer
	// auth Middleware wraps the whole transcoder.
	AdminPath    string
	AdminHandler http.Handler
}

// Runtime is a started ingress server. Close it to stop the listener
// gracefully (or via the parent context). Safe to call Close multiple
// times.
type Runtime struct {
	server *Server
	srv    *connectserver.Server
}

// Start binds the listener and serves Ingress in a background
// goroutine. Returns once the listener is accepting; the caller should
// defer rt.Close().
func Start(ctx context.Context, host *engine.Host, cfg Config) (*Runtime, error) {
	if host == nil {
		return nil, errors.New("ingress: host is required")
	}
	if cfg.Addr == "" {
		return nil, errors.New("ingress: Addr is required")
	}
	if cfg.Middleware == nil {
		return nil, errors.New("ingress: Middleware is required (authn happens here; tests should pass an explicit identity passthrough)")
	}
	if cfg.AuthzInterceptor == nil {
		return nil, errors.New("ingress: AuthzInterceptor is required (Cedar authorization happens here; tests should pass an allow-all interceptor)")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	srv := NewServer(host, cfg.Log)
	srv.schemaResolver = cfg.TaskSchemaResolver

	// One Connect handler for the ingress service, behind the Cedar authz +
	// default-deadline interceptors. Vanguard wraps it so the same handler and
	// the same interceptor chain serve Connect/gRPC/gRPC-Web AND the REST
	// routes declared by the google.api.http annotations — REST authorization
	// is the standard procedure-keyed interceptor, with no bespoke REST path.
	path, handler := ingressv1connect.NewIngressHandler(srv,
		connect.WithInterceptors(cfg.AuthzInterceptor, withDefaultDeadline(defaultLookupTimeout)),
	)
	services := []*vanguard.Service{
		vanguard.NewService(path, handler, vanguard.WithMaxMessageBufferBytes(maxIngressMessageBytes)),
	}
	// BFF: optionally serve the admin service on this same listener/transcoder so
	// a browser console hits one origin for data plane + admin. Connect-only (the
	// adminv1 protos carry no google.api.http annotations); the Cedar interceptor
	// on the handler keeps cluster-admin operator-only and app-config group-gated.
	if cfg.ServeAdmin {
		if cfg.AdminHandler == nil || cfg.AdminPath == "" {
			return nil, errors.New("ingress: ServeAdmin set but AdminPath/AdminHandler missing")
		}
		services = append(services, vanguard.NewService(cfg.AdminPath, cfg.AdminHandler, vanguard.WithMaxMessageBufferBytes(maxIngressMessageBytes)))
		cfg.Log.Info("ingress: serving admin on the ingress listener (BFF)")
	}
	transcoder, err := vanguard.NewTranscoder(services)
	if err != nil {
		return nil, fmt.Errorf("ingress: transcoder: %w", err)
	}

	// Outermost → innermost: CORS (answers browser preflight before auth) → auth
	// Middleware (stamps the verified principal) → metaLiftHandler (Reflw-Meta-*
	// headers → ctx) → transcoder. Mounted at "/" so the transcoder owns both the
	// Connect subtree and the REST paths with no ServeMux pattern conflict.
	root := cfg.Middleware(metaLiftHandler(transcoder))
	if cfg.CORS.Enabled() {
		root = corsMiddleware(cfg.CORS)(root)
	}
	routes := []connectserver.Route{{Path: "/", Handler: root}}
	if cfg.ExtraRoutes != nil {
		routes = append(routes, cfg.ExtraRoutes(srv)...)
	}
	cs, err := connectserver.New(ctx, connectserver.Config{
		Addr: cfg.Addr,
		TLS:  cfg.TLS,
		Log:  cfg.Log,
	}, routes...)
	if err != nil {
		return nil, fmt.Errorf("ingress: %w", err)
	}
	cfg.Log.Info("ingress: started", "addr", cs.Addr())
	return &Runtime{server: srv, srv: cs}, nil
}

// Addr returns the bound listener address (useful when the caller
// passed ":0" to let the kernel pick a port).
func (r *Runtime) Addr() string { return r.srv.Addr() }

// Server exposes the in-process Connect handler so in-binary subsystems
// (event-source dispatcher, embedded admin tooling) can call ingress
// methods without going through the network listener.
func (r *Runtime) Server() *Server { return r.server }

// Close stops the transport. Idempotent.
func (r *Runtime) Close() error { return r.srv.Close() }
