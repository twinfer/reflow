package ingress

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/connectserver"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/proto/ingressv1/ingressv1connect"
)

// Config is the minimum the ingress runtime needs. Mirrors the public
// pkg/reflow.IngressConfig but kept internal so engine packages don't
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
	// for /reflow.ingress.v1.Ingress/* with no principal restriction),
	// not by skipping the middleware. Tests that intentionally bypass
	// auth must pass an explicit identity passthrough.
	Middleware func(http.Handler) http.Handler
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
		return nil, errors.New("ingress: Middleware is required (policy enforcement happens here; tests should pass an explicit identity passthrough)")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	srv := NewServer(host, cfg.Log)
	path, handler := ingressv1connect.NewIngressHandler(srv,
		connect.WithInterceptors(withDefaultDeadline(defaultLookupTimeout)),
	)
	cs, err := connectserver.New(ctx, connectserver.Config{
		Addr: cfg.Addr,
		TLS:  cfg.TLS,
		Log:  cfg.Log,
	}, connectserver.Route{Path: path, Handler: cfg.Middleware(handler)})
	if err != nil {
		return nil, fmt.Errorf("ingress: %w", err)
	}
	cfg.Log.Info("ingress: started", "addr", cs.Addr())
	return &Runtime{server: srv, srv: cs}, nil
}

// Addr returns the bound listener address (useful when the caller
// passed ":0" to let the kernel pick a port).
func (r *Runtime) Addr() string { return r.srv.Addr() }

// Close stops the transport. Idempotent.
func (r *Runtime) Close() error { return r.srv.Close() }
