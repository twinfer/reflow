// Package connectserver hosts one or more Connect handlers on a single
// HTTP/2 listener. Shared by ingress and admin: both want the same
// h2c-or-TLS lifecycle plus a way to mount Connect routes plus optional
// HTTP/JSON fallbacks (e.g. /metrics).
package connectserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Route is one HTTP handler bound to a path. Constructed from a Connect
// handler factory: `path, handler := xxxconnect.NewYYYHandler(impl, opts...)`.
type Route struct {
	Path    string
	Handler http.Handler
}

// Config groups constructor inputs.
type Config struct {
	// Addr is the listen address. Required.
	Addr string
	// TLS, when non-nil, wraps the listener with TLS (HTTP/2 over TLS).
	// Nil enables h2c (plaintext HTTP/2) so the listener still negotiates
	// HTTP/2 without TLS.
	TLS *tls.Config
	// Log is the structured logger; defaults to slog.Default.
	Log *slog.Logger
	// ReadHeaderTimeout bounds slowloris on header read. Defaults to 5s.
	ReadHeaderTimeout time.Duration
}

// Server wraps the *http.Server lifecycle plus the listener it serves.
type Server struct {
	srv     *http.Server
	ln      net.Listener
	addr    string
	log     *slog.Logger
	closeMu sync.Mutex
	closed  bool
	doneCh  chan struct{}
	// baseCancel cancels the http.Server BaseContext. Firing it at
	// Close surfaces ctx.Done() inside every in-flight handler so a
	// handler stuck in a blocking call (e.g. dragonboat SyncPropose)
	// returns immediately rather than holding the engine open.
	baseCancel context.CancelFunc
}

// New binds the listener and starts Serve in a background goroutine.
// Returns once the listener is accepting; the caller must Close (or
// cancel ctx) when done.
func New(ctx context.Context, cfg Config, routes ...Route) (*Server, error) {
	if cfg.Addr == "" {
		return nil, errors.New("connectserver: Addr is required")
	}
	if len(routes) == 0 {
		return nil, errors.New("connectserver: at least one route is required")
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	hdr := cfg.ReadHeaderTimeout
	if hdr <= 0 {
		hdr = 5 * time.Second
	}

	mux := http.NewServeMux()
	for _, r := range routes {
		if r.Path == "" || r.Handler == nil {
			return nil, fmt.Errorf("connectserver: route with empty path or nil handler")
		}
		mux.Handle(r.Path, r.Handler)
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("connectserver: listen %s: %w", cfg.Addr, err)
	}
	if cfg.TLS != nil {
		ln = tls.NewListener(ln, cfg.TLS)
	}

	baseCtx, baseCancel := context.WithCancel(context.Background())
	srv := &http.Server{
		Handler:           mux,
		Protocols:         new(http.Protocols),
		ReadHeaderTimeout: hdr,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
	}
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(cfg.TLS == nil)
	srv.Protocols.SetHTTP2(cfg.TLS != nil)

	s := &Server{
		srv:        srv,
		ln:         ln,
		addr:       ln.Addr().String(),
		log:        log,
		doneCh:     make(chan struct{}),
		baseCancel: baseCancel,
	}
	go func() {
		defer close(s.doneCh)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("connectserver: Serve exited", "addr", s.addr, "err", err)
		}
	}()
	if ctx != nil {
		go func() {
			<-ctx.Done()
			_ = s.Close()
		}()
	}
	return s, nil
}

// Addr returns the bound listener address (useful when Config.Addr ended
// in ":0").
func (s *Server) Addr() string { return s.addr }

// Close gracefully shuts the server down and waits for the Serve
// goroutine to exit. Idempotent. Steps:
//
//  1. Cancel BaseContext so every active handler observes ctx.Done()
//     and unwinds (handlers blocked in dragonboat SyncPropose etc.
//     return immediately on their next select).
//  2. Shutdown(2s) drains any remaining connections.
//  3. Wait for the Serve goroutine to close doneCh.
func (s *Server) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		<-s.doneCh
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	if s.baseCancel != nil {
		s.baseCancel()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(shutdownCtx)
	<-s.doneCh
	return nil
}
