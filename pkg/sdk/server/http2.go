package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// HTTP2Server hosts a reflow handler over raw HTTP/2. Wire framing:
//   - POST /invoke/<service>/<handler> with chunked
//     [8-byte BE header][payload] frames matching protocolv1.Frame.
//   - GET  /discover returns a protobuf-encoded discoveryv1.DiscoveryResponse.
//
// Uses stdlib net/http's HTTP/2 (http.Server.Protocols) so deployments
// on Go 1.24+ pick up h2c without an x/net dependency.
type HTTP2Server struct {
	cfg     Config
	srv     *http.Server
	mux     *http.ServeMux
	mu      sync.Mutex
	started bool
	closed  bool
}

// NewHTTP2 constructs an HTTP/2 handler-side server. The default mux
// serves /discover and /invoke/{service}/{handler}; callers that need
// additional endpoints can mount them via Mux().
func NewHTTP2(cfg Config) (*HTTP2Server, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	s := &HTTP2Server{cfg: cfg, mux: http.NewServeMux()}
	s.mux.HandleFunc("/discover", s.handleDiscover)
	s.mux.HandleFunc("/invoke/", s.handleInvoke)
	s.srv = &http.Server{Handler: s.mux, Protocols: new(http.Protocols)}
	// Accept both HTTP/1.1 (for probes / health-check style tools) and
	// h2c (the engine's wire path). HTTPS adds plain HTTP/2 on top.
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
	w.Header().Set("Content-Type", contentType(s.cfg.Codec))
	_, _ = w.Write(body)
}

// handleInvoke parses (service, handler) out of the URL path and drives
// one session via runSession over a stream that wraps the request body
// (engine→handler) and the response writer (handler→engine).
func (s *HTTP2Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required on /invoke", http.StatusMethodNotAllowed)
		return
	}
	service, handler, ok := parseInvokePath(r.URL.Path)
	if !ok {
		http.Error(w, "expected /invoke/<service>/<handler>", http.StatusBadRequest)
		return
	}

	// We must commit response headers BEFORE runSession's first stream
	// send so the engine's client side observes a 200 and starts reading
	// the response body. Without this, the first server-side write
	// blocks waiting for the client to issue a body read that hasn't
	// happened yet (the client's Recv loop is gated on awaitResponse,
	// which gates on response headers — classic chicken-and-egg).
	w.Header().Set("Content-Type", contentType(s.cfg.Codec))
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		// Should never happen on h2c / HTTP/2; degrade gracefully.
		http.Error(w, "responsewriter is not a Flusher", http.StatusInternalServerError)
		return
	}
	flusher.Flush()

	stream := &http2ServerStream{
		body:    r.Body,
		writer:  w,
		flusher: flusher,
	}
	if err := runSession(r.Context(), stream, s.cfg.Registry, s.cfg.Codec, handlerclient.Route{
		Service: service,
		Handler: handler,
	}); err != nil {
		// runSession's transport-level errors land here — the engine has
		// already received an ErrorMessage frame (sendError path) or the
		// session was already closed by the peer. There is nothing more
		// to do at the HTTP layer; the response is mid-stream and a 500
		// here would be a wire-format violation.
		return
	}
	// Drain any remaining request body so the HTTP/2 stack closes the
	// stream cleanly. The engine should have CloseSent by now.
	_, _ = io.Copy(io.Discard, r.Body)
}

// parseInvokePath splits "/invoke/<service>/<handler>" into its
// components. service and handler are URL-decoded.
func parseInvokePath(path string) (service, handler string, ok bool) {
	const prefix = "/invoke/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(path, prefix)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	s, err1 := url.PathUnescape(parts[0])
	h, err2 := url.PathUnescape(parts[1])
	if err1 != nil || err2 != nil {
		return "", "", false
	}
	return s, h, true
}

// contentType returns "application/vnd.reflow.invocation.v1+<codec>".
func contentType(c handlerclient.Codec) string {
	return "application/vnd.reflow.invocation.v1+" + c.Name()
}

// http2ServerStream adapts the HTTP/2 request body + response writer
// onto frameStream. Send writes a [8-byte BE header][payload] frame and
// flushes; Recv reads the same shape from the request body.
type http2ServerStream struct {
	body    io.Reader
	writer  io.Writer
	flusher http.Flusher
}

func (s *http2ServerStream) Send(f *protocolv1.Frame) error {
	if f == nil {
		return errors.New("sdk/server: nil frame")
	}
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], f.GetHeader())
	if _, err := s.writer.Write(hdr[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if len(f.GetPayload()) > 0 {
		if _, err := s.writer.Write(f.GetPayload()); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	s.flusher.Flush()
	return nil
}

func (s *http2ServerStream) Recv() (*protocolv1.Frame, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(s.body, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read header: %w", err)
	}
	h := binary.BigEndian.Uint64(hdr[:])
	_, _, length := handlerclient.UnpackHeader(h)
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(s.body, payload); err != nil {
			return nil, fmt.Errorf("read payload (%d bytes): %w", length, err)
		}
	}
	return &protocolv1.Frame{Header: h, Payload: payload}, nil
}
