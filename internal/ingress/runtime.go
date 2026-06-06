package ingress

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/connectserver"
	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/observability"
	"github.com/twinfer/reflw/proto/ingressv1/ingressv1connect"
)

// Config is the minimum the ingress runtime needs. Mirrors the public
// pkg/reflw.IngressConfig but kept internal so engine packages don't
// pull in the public surface.
type Config struct {
	// Addr is the listen address. Connect content-negotiates Connect /
	// gRPC / gRPC-Web / HTTP-JSON on this single port.
	Addr string
	// TLS, when non-nil, wraps the listener with TLS (HTTP/2 over TLS).
	// Nil enables h2c.
	TLS *tls.Config
	// Log is the structured logger; defaults to slog.Default.
	Log *slog.Logger
	// Middleware wraps the Connect handler with the unified auth
	// middleware (auth.HTTPMiddleware). REQUIRED — Start returns an
	// error when nil. Anonymous traffic is permitted only by the policy
	// (the embedded starter policy includes an ingress_open allow rule
	// for /reflw.ingress.v1.Ingress/* with no principal restriction),
	// not by skipping the middleware. Tests that intentionally bypass
	// auth must pass an explicit identity passthrough.
	Middleware func(http.Handler) http.Handler
	// AuthzInterceptor enforces Cedar authorization on every ingress RPC.
	// It runs after the auth Middleware has attached the verified principal
	// to the request context. REQUIRED — Start returns an error when nil, so
	// the ingress data plane is never served without authorization.
	AuthzInterceptor connect.Interceptor
	// ExtraRoutes builds additional connectserver.Routes mounted on the
	// same listener as the Connect ingress. Called once after Start
	// constructs the in-process Server, so the caller can wire handlers
	// (notably webhook receivers) that need *Server directly. The caller
	// is responsible for wrapping each handler with the auth middleware
	// (the same instance passed in Middleware); Start does not double-wrap.
	// Webhooks deliberately skip it (HMAC is their gate).
	ExtraRoutes func(srv *Server) []connectserver.Route
	// RESTAuthorizer, when non-nil, mounts the first-class REST data-plane
	// facade (POST /v1/{service}/{handler}[/{key}], /v1/processes/{name},
	// /v1/cases/{name}) behind Middleware, authorizing each call via Cedar.
	// *authz.Interceptor satisfies it. Nil disables the REST facade (the
	// Connect RPCs still serve).
	RESTAuthorizer IngressAuthorizer
	// Metrics records IngressRESTRequests for the REST facade. Optional.
	Metrics *observability.Metrics
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
	path, handler := ingressv1connect.NewIngressHandler(srv,
		connect.WithInterceptors(cfg.AuthzInterceptor, withDefaultDeadline(defaultLookupTimeout)),
	)
	routes := []connectserver.Route{{Path: path, Handler: cfg.Middleware(handler)}}
	if cfg.RESTAuthorizer != nil {
		ic := InvokeConfig{
			Invoker:    srv,
			Starter:    srv,
			Reader:     srv,
			Authorizer: cfg.RESTAuthorizer,
			Metrics:    cfg.Metrics,
			Log:        cfg.Log,
		}
		routes = append(routes,
			connectserver.Route{Path: "POST /v1/processes/{name}", Handler: cfg.Middleware(StartProcessHTTP(ic, false))},
			connectserver.Route{Path: "POST /v1/cases/{name}", Handler: cfg.Middleware(StartProcessHTTP(ic, true))},
			connectserver.Route{Path: "GET /v1/processes/{name}/{key}/history", Handler: cfg.Middleware(GetProcessHistoryHTTP(ic))},
			connectserver.Route{Path: "POST /v1/{service}/{key}/{handler}", Handler: cfg.Middleware(InvokeHTTP(ic, true))},
			connectserver.Route{Path: "POST /v1/{service}/{handler}", Handler: cfg.Middleware(InvokeHTTP(ic, false))},
		)
	}
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
