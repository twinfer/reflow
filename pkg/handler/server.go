// Package handler is the durable-execution SDK + the handler-side Connect
// server that hosts it. Authors register handlers in a *Registry, wrap it
// in NewServer, and Serve on a listener. The reflow engine discovers the
// deployment via DiscoveryService.Discover and opens a session over
// HandlerService.InvokeStream, both Connect RPCs over HTTP/2.
package handler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/pkg/handler/wire"
	"github.com/twinfer/reflw/pkg/reflow/creds"
	"github.com/twinfer/reflw/proto/discoveryv1/discoveryv1connect"
	"github.com/twinfer/reflw/proto/handlerv1/handlerv1connect"
)

// Config groups constructor inputs. Registry is required; the others have
// sensible defaults.
type Config struct {
	// Registry holds the handlers this server is willing to serve. The
	// lookup is concurrency-safe; the same registry instance can back
	// multiple Servers (e.g. h2c and HTTPS on different ports).
	Registry *Registry

	// Codec governs inner-payload encoding for protocolv1 messages.
	// Defaults to protobuf. Both sides of the session must agree; the
	// engine's wire.Codec is the matching half.
	Codec wire.Codec

	// RootCAs, when non-nil, enables JWT verification of every
	// InvokeStream and /discover request via Authorization: Bearer <jwt>.
	// The bundle is the PEM-encoded set of CAs trusted to sign the
	// caller's leaf; the engine signs with a leaf rooted at one of these.
	RootCAs []byte

	// AllowedPrincipals is the exact-match allowlist of caller principal
	// Raw strings (e.g. "node/1", "operator/alice"). Required when
	// RootCAs is set; leave empty when RootCAs is nil.
	AllowedPrincipals []string

	// ExpectedAudience, when non-empty, requires the JWT aud claim to
	// match. Empty skips the aud check (chain + principal + exp/iat still
	// run). The SDK handler typically doesn't know its own deployment_id
	// (engine-assigned), so this is opt-in.
	ExpectedAudience string

	// MaxRecvBytes caps a single inbound InvokeRequest
	// (connect.WithReadMaxBytes) — the engine batches a whole session's
	// StartMessage + replay frames into one message. Zero uses
	// wire.DefaultMaxRecvBytes (64 MiB); raise it for handlers that receive
	// very large journals or eager state.
	MaxRecvBytes int
}

// Server hosts a reflow handler over HTTP/2. Routes:
//   - /reflow.handler.v1.HandlerService/InvokeStream — bidi-streaming
//     session over Connect.
//   - /reflow.discovery.v1.DiscoveryService/Discover — capability probe
//     over Connect.
//
// Accepts HTTP/1.1, h2c (engine's bidi path), and HTTP/2 over TLS via
// stdlib net/http's Protocols field (Go 1.24+).
type Server struct {
	cfg      Config
	srv      *http.Server
	verifier *creds.Verifier
	mu       sync.Mutex
	closed   bool
}

// NewServer constructs a handler-side server.
//
// When cfg.RootCAs is non-nil, every request must carry an
// Authorization: Bearer <jwt> header whose x5c chain anchors at one of
// the configured roots and whose leaf CN matches an entry in
// cfg.AllowedPrincipals; verification failures reject with 401.
func NewServer(cfg Config) (*Server, error) {
	verifier, err := validateConfig(&cfg)
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, verifier: verifier}

	mux := http.NewServeMux()
	invokePath, invokeHandler := handlerv1connect.NewHandlerServiceHandler(&handlerService{
		registry: cfg.Registry,
		codec:    cfg.Codec,
	}, connect.WithReadMaxBytes(cfg.MaxRecvBytes))
	discoverPath, discoverHandler := discoveryv1connect.NewDiscoveryServiceHandler(&discoveryService{
		registry: cfg.Registry,
	})
	if verifier != nil {
		invokeHandler = withAuth(verifier, nil, invokeHandler)
		discoverHandler = withAuth(verifier, nil, discoverHandler)
	}
	mux.Handle(invokePath, invokeHandler)
	mux.Handle(discoverPath, discoverHandler)

	s.srv = &http.Server{Handler: mux, Protocols: new(http.Protocols)}
	s.srv.Protocols.SetHTTP1(true)
	s.srv.Protocols.SetUnencryptedHTTP2(true)
	s.srv.Protocols.SetHTTP2(true)
	return s, nil
}

// Serve runs the HTTP server on ln until Shutdown or ln is closed.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("handler: Server closed")
	}
	s.mu.Unlock()
	err := s.srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the HTTP server. Idempotent.
func (s *Server) Shutdown() error {
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

// validateConfig fills defaults, rejects obviously broken inputs, and
// builds the verifier when RootCAs is set. Returns the verifier (nil
// when auth is disabled).
func validateConfig(cfg *Config) (*creds.Verifier, error) {
	if cfg.Registry == nil {
		return nil, fmt.Errorf("handler: Config.Registry is required")
	}
	if cfg.Codec == nil {
		cfg.Codec = wire.DefaultCodec()
	}
	if cfg.MaxRecvBytes <= 0 {
		cfg.MaxRecvBytes = wire.DefaultMaxRecvBytes
	}
	if cfg.RootCAs == nil {
		if len(cfg.AllowedPrincipals) > 0 || cfg.ExpectedAudience != "" {
			return nil, errors.New("handler: auth fields set without RootCAs; either set RootCAs to enable verification or remove the other auth fields")
		}
		return nil, nil
	}
	v, err := creds.NewVerifier(cfg.RootCAs, cfg.AllowedPrincipals, cfg.ExpectedAudience)
	if err != nil {
		return nil, fmt.Errorf("handler: build verifier: %w", err)
	}
	return v, nil
}
