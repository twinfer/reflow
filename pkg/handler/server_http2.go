package handler

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/pkg/reflow/creds"
	"github.com/twinfer/reflow/proto/handlerv1/handlerv1connect"
)

// HTTP2Server hosts a reflow handler over HTTP/2. Routes:
//   - /reflow.handler.v1.HandlerService/InvokeStream — bidi-streaming
//     session, Connect / gRPC / gRPC-Web protocols all supported on
//     this single endpoint (Connect's NewBidiStreamHandler).
//   - /discover returns a protobuf-encoded discoveryv1.DiscoveryResponse.
//
// Uses stdlib net/http's HTTP/2 (http.Server.Protocols) so deployments
// on Go 1.24+ pick up h2c without an x/net dependency.
type HTTP2Server struct {
	cfg      Config
	srv      *http.Server
	mux      *http.ServeMux
	verifier *creds.Verifier
	mu       sync.Mutex
	started  bool
	closed   bool
}

// NewHTTP2 constructs an HTTP/2 handler-side server. The default mux
// serves /discover and the Connect HandlerService route; callers that
// need additional endpoints can mount them via Mux().
//
// When cfg.RootCAs is non-nil, every InvokeStream and /discover request
// must carry an Authorization: Bearer <jwt> header whose x5c chain
// anchors at one of the configured roots and whose leaf SPIFFE URI
// appears in cfg.AllowedSPIFFE; verification failures reject with 401.
// Extra routes mounted via Mux() are NOT gated, by design (health
// probes, metrics scrapers).
func NewHTTP2(cfg Config) (*HTTP2Server, error) {
	verifier, err := validateConfig(&cfg)
	if err != nil {
		return nil, err
	}
	s := &HTTP2Server{cfg: cfg, mux: http.NewServeMux(), verifier: verifier}

	invokePath, invokeHandler := handlerv1connect.NewHandlerServiceHandler(&handlerService{
		registry: cfg.Registry,
		codec:    cfg.Codec,
	})
	discoverHandler := http.Handler(http.HandlerFunc(s.handleDiscover))
	if verifier != nil {
		invokeHandler = withAuth(verifier, nil, invokeHandler)
		discoverHandler = withAuth(verifier, nil, discoverHandler)
	}
	s.mux.Handle("/discover", discoverHandler)
	s.mux.Handle(invokePath, invokeHandler)

	s.srv = &http.Server{Handler: s.mux, Protocols: new(http.Protocols)}
	// Accept both HTTP/1.1 (for probes / health-check tools and Connect's
	// HTTP/1.1 unary fallback) and h2c (the engine's bidi path). HTTPS
	// adds plain HTTP/2 on top.
	s.srv.Protocols.SetHTTP1(true)
	s.srv.Protocols.SetUnencryptedHTTP2(true)
	s.srv.Protocols.SetHTTP2(true)
	return s, nil
}

// Mux exposes the default *http.ServeMux. Callers that want to attach
// additional handlers (health checks, metrics) mount them here before
// Serve.
func (s *HTTP2Server) Mux() *http.ServeMux { return s.mux }

// Server exposes the underlying *http.Server. Callers may tune
// timeouts, TLSConfig, etc. before Serve.
func (s *HTTP2Server) Server() *http.Server { return s.srv }

// Serve runs the HTTP server on ln until Shutdown or ln is closed.
func (s *HTTP2Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("sdk/server: HTTP2Server closed")
	}
	s.started = true
	s.mu.Unlock()
	err := s.srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the HTTP server. Idempotent.
func (s *HTTP2Server) Shutdown() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

// handleDiscover writes a protobuf-encoded discoveryv1.DiscoveryResponse
// listing every handler in the registry.
func (s *HTTP2Server) handleDiscover(w http.ResponseWriter, _ *http.Request) {
	resp := buildDiscoveryResponse(s.cfg.Registry)
	body, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, "marshal DiscoveryResponse: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
	_, _ = w.Write(body)
}
