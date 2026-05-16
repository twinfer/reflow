// Package http2client implements handlerclient.Client over raw HTTP/2 —
// POST <url>/invoke/<service>/<handler> with a chunked, framed envelope.
//
// Wire framing matches proto/protocolv1/protocol.proto: each frame is an
// 8-byte big-endian header (16-bit type | 16-bit flags | 32-bit payload
// length) followed by the protobuf-encoded payload. Content-Type is
// application/vnd.reflow.invocation.v1+<codec> so the handler-side knows
// which Codec to decode payloads with.
//
// Built on net/http's HTTP/2 support via http.Transport.Protocols (h2c
// selected via SetUnencryptedHTTP2; TLS via SetHTTP2). The bidi shape
// is built on io.Pipe (engine→handler) + resp.Body (handler→engine).
package http2client

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// Scheme is the registry key for plain HTTP/2 (h2c). Plain HTTP is only
// safe inside a trusted network; the engine bootstrap rejects it unless
// the creds spec is explicitly insecure.
const Scheme = "http"

// SchemeSecure is the registry key for HTTP/2 over TLS.
const SchemeSecure = "https"

// ContentType is the Content-Type prefix sent on /invoke requests. The
// codec name is appended (application/vnd.reflow.invocation.v1+protobuf).
const ContentType = "application/vnd.reflow.invocation.v1"

// Register installs both plain (h2c) and TLS dialers on r. Operators
// who need a custom TLS root bundle can supply their own Dialer via
// r.Register.
func Register(r *handlerclient.Registry) {
	r.Register(Scheme, dialH2C)
	r.Register(SchemeSecure, dialTLS)
}

func dialH2C(rawURL string, opts ...handlerclient.ClientOption) (handlerclient.Client, error) {
	return newClient(rawURL, opts, true)
}

func dialTLS(rawURL string, opts ...handlerclient.ClientOption) (handlerclient.Client, error) {
	return newClient(rawURL, opts, false)
}

// New is the constructor used by callers that want to bypass the
// registry — primarily tests. plaintextH2C selects h2c when true,
// HTTPS-over-TLS otherwise.
func New(rawURL string, plaintextH2C bool, opts ...handlerclient.ClientOption) (*Client, error) {
	c, err := newClient(rawURL, opts, plaintextH2C)
	if err != nil {
		return nil, err
	}
	return c.(*Client), nil
}

func newClient(rawURL string, optsIn []handlerclient.ClientOption, plaintextH2C bool) (handlerclient.Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("http2client: parse url %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("http2client: url %q missing host", rawURL)
	}
	cfg := handlerclient.ApplyOptions(optsIn)
	tr := &http.Transport{Protocols: new(http.Protocols)}
	if plaintextH2C {
		// http.Protocols.SetUnencryptedHTTP2 is the supported h2c path
		// in stdlib (Go 1.24+). Disabling HTTP1 prevents the transport
		// from silently downgrading when the server doesn't speak h2c.
		tr.Protocols.SetUnencryptedHTTP2(true)
		tr.Protocols.SetHTTP1(false)
	} else {
		tr.Protocols.SetHTTP2(true)
		tr.Protocols.SetHTTP1(false)
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	hc := &http.Client{Transport: tr}
	return &Client{
		baseURL: strings.TrimRight(rawURL, "/"),
		client:  hc,
		tr:      tr,
		codec:   cfg.Codec,
	}, nil
}

// Client wraps one *http.Client bound to a single deployment URL. The
// underlying http.Transport is shared across every Invoke and torn down
// by Close.
type Client struct {
	baseURL string
	client  *http.Client
	tr      *http.Transport
	codec   handlerclient.Codec

	mu     sync.Mutex
	closed bool
}

// Invoke opens a new chunked HTTP/2 POST against
// <baseURL>/invoke/<service>/<handler>. The request body is the engine →
// handler frame stream; the response body is the handler → engine frame
// stream. Returns once the request has been dispatched; the response
// arrives asynchronously and is awaited inside Stream.Recv.
func (c *Client) Invoke(ctx context.Context, route handlerclient.Route) (handlerclient.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, handlerclient.ErrClientClosed
	}
	c.mu.Unlock()
	if route.Service == "" || route.Handler == "" {
		return nil, errors.New("http2client: route.Service and route.Handler are required")
	}

	pr, pw := io.Pipe()
	target := c.baseURL + "/invoke/" + url.PathEscape(route.Service) + "/" + url.PathEscape(route.Handler)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, pr)
	if err != nil {
		_ = pw.Close()
		return nil, fmt.Errorf("http2client: build request: %w", err)
	}
	// ContentLength = -1 prevents net/http from trying to buffer the
	// request body or set a Content-Length header; the request is sent
	// chunked-style (the http2 transport already streams).
	req.ContentLength = -1
	req.Header.Set("Content-Type", ContentType+"+"+c.codec.Name())
	req.Header.Set("Accept", ContentType+"+"+c.codec.Name())

	s := &stream{
		ctx:       ctx,
		pw:        pw,
		respReady: make(chan struct{}),
	}
	go func() {
		resp, err := c.client.Do(req)
		s.respMu.Lock()
		s.resp = resp
		s.respErr = err
		s.respMu.Unlock()
		close(s.respReady)
	}()
	return s, nil
}

// Close drops the underlying HTTP/2 transport's connection pool.
// Idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	tr := c.tr
	c.mu.Unlock()
	tr.CloseIdleConnections()
	return nil
}

// stream is one /invoke session. Send writes to the request pipe (engine
// → handler); Recv reads from resp.Body (handler → engine).
type stream struct {
	ctx context.Context

	pw *io.PipeWriter // engine → handler

	respMu    sync.Mutex
	resp      *http.Response // populated once respReady closes
	respErr   error
	respReady chan struct{}

	recvMu     sync.Mutex
	recvClosed bool

	sendMu     sync.Mutex
	sendClosed bool
}

// Send serializes f as [8-byte BE header][payload] and writes it to the
// request body pipe. Frames are framed exactly as proto/protocolv1
// describes — the Frame proto envelope is NOT used on the wire here.
func (s *stream) Send(f *protocolv1.Frame) error {
	if f == nil {
		return errors.New("http2client: nil frame")
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sendClosed {
		return errors.New("http2client: send on closed stream")
	}
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], f.GetHeader())
	if _, err := s.pw.Write(hdr[:]); err != nil {
		return fmt.Errorf("http2client: write header: %w", err)
	}
	if len(f.GetPayload()) > 0 {
		if _, err := s.pw.Write(f.GetPayload()); err != nil {
			return fmt.Errorf("http2client: write payload: %w", err)
		}
	}
	return nil
}

// Recv blocks until the next frame is available on the response body.
// The first Recv call additionally waits for the response headers to
// arrive (so server-side errors surface here as a non-2xx status).
// Returns io.EOF when the handler closes the response body cleanly.
func (s *stream) Recv() (*protocolv1.Frame, error) {
	if err := s.awaitResponse(); err != nil {
		return nil, err
	}
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	if s.recvClosed {
		return nil, io.EOF
	}
	body := s.resp.Body
	var hdr [8]byte
	if _, err := io.ReadFull(body, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			s.recvClosed = true
			return nil, io.EOF
		}
		return nil, fmt.Errorf("http2client: read header: %w", err)
	}
	h := binary.BigEndian.Uint64(hdr[:])
	_, _, length := handlerclient.UnpackHeader(h)
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(body, payload); err != nil {
			return nil, fmt.Errorf("http2client: read payload (%d bytes): %w", length, err)
		}
	}
	return &protocolv1.Frame{Header: h, Payload: payload}, nil
}

// CloseSend closes the request pipe writer, signaling end-of-upload to
// the handler. The response side is unaffected; Recv continues until
// the handler closes the response body.
func (s *stream) CloseSend() error {
	s.sendMu.Lock()
	if s.sendClosed {
		s.sendMu.Unlock()
		return nil
	}
	s.sendClosed = true
	pw := s.pw
	s.sendMu.Unlock()
	return pw.Close()
}

// awaitResponse blocks until http.Client.Do returns or ctx is cancelled.
// On a non-2xx status the body is consumed for an error message; Recv
// then returns the error rather than attempting to parse frames out of
// an HTML/text body.
func (s *stream) awaitResponse() error {
	select {
	case <-s.respReady:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	s.respMu.Lock()
	respErr := s.respErr
	resp := s.resp
	s.respMu.Unlock()
	if respErr != nil {
		return respErr
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		_ = resp.Body.Close()
		return fmt.Errorf("http2client: handler returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}
