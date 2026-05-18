// Package connectclient implements handlerclient.Client over Connect
// RPC. The engine opens HandlerService.InvokeStream as a bidi stream
// of protocolv1.Frame envelopes; routing (service, handler) flows
// inside the StartMessage payload, not on the URL.
//
// Connect's NewClient defaults to the Connect protocol with binary
// protobuf, but the server side enables Connect/gRPC/gRPC-Web on the
// same handler — supplying connect.WithGRPC() here is one switch away
// if a deployment prefers gRPC semantics. h2c is selected via
// http.Protocols.SetUnencryptedHTTP2 for http:// URLs; TLS via
// SetHTTP2 for https://.
package connectclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/handler/wire"
	"github.com/twinfer/reflow/proto/handlerv1/handlerv1connect"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// Scheme is the registry key for plain HTTP/2 (h2c). Plain HTTP is only
// safe inside a trusted network; the engine bootstrap rejects it unless
// the creds spec is explicitly insecure.
const Scheme = "http"

// SchemeSecure is the registry key for HTTP/2 over TLS.
const SchemeSecure = "https"

// Register installs both plain (h2c) and TLS dialers on r. Operators
// who need a custom TLS root bundle can supply their own Dialer via
// r.Register. signer is optional: when non-nil, every dispatched
// request carries an Authorization: Bearer header minted by it; when
// nil, no auth header is set (single-node and insecure-creds posture).
func Register(r *handlerclient.Registry, signer handlerclient.Signer) {
	r.Register(Scheme, dialerFor(true, signer))
	r.Register(SchemeSecure, dialerFor(false, signer))
}

func dialerFor(plaintextH2C bool, signer handlerclient.Signer) handlerclient.Dialer {
	return func(deploymentID, rawURL string) (handlerclient.Client, error) {
		return newClient(deploymentID, rawURL, plaintextH2C, signer)
	}
}

// New is the constructor used by callers that want to bypass the
// registry — primarily tests. plaintextH2C selects h2c when true,
// HTTPS-over-TLS otherwise. signer may be nil for unauthenticated dials.
func New(deploymentID, rawURL string, plaintextH2C bool, signer handlerclient.Signer) (*Client, error) {
	c, err := newClient(deploymentID, rawURL, plaintextH2C, signer)
	if err != nil {
		return nil, err
	}
	return c.(*Client), nil
}

func newClient(deploymentID, rawURL string, plaintextH2C bool, signer handlerclient.Signer) (handlerclient.Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("connectclient: parse url %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("connectclient: url %q missing host", rawURL)
	}
	tr := &http.Transport{Protocols: new(http.Protocols)}
	if plaintextH2C {
		tr.Protocols.SetUnencryptedHTTP2(true)
		tr.Protocols.SetHTTP1(false)
	} else {
		tr.Protocols.SetHTTP2(true)
		tr.Protocols.SetHTTP1(false)
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	hc := &http.Client{Transport: tr}
	baseURL := strings.TrimRight(rawURL, "/")
	return &Client{
		deploymentID: deploymentID,
		baseURL:      baseURL,
		http:         hc,
		tr:           tr,
		connect:      handlerv1connect.NewHandlerServiceClient(hc, baseURL),
		signer:       signer,
	}, nil
}

// Client wraps one Connect HandlerServiceClient bound to a single
// deployment URL. The underlying http.Transport is shared across every
// Invoke and torn down by Close. deploymentID is captured at
// construction so the signer can stamp it as the JWT aud without
// threading it through Invoke.
type Client struct {
	deploymentID string
	baseURL      string
	http         *http.Client
	tr           *http.Transport
	connect      handlerv1connect.HandlerServiceClient
	signer       handlerclient.Signer

	mu     sync.Mutex
	closed bool
}

// Invoke opens a HandlerService.InvokeStream bidi stream. route is
// validated and forwarded as a sanity check, but Connect routing
// itself carries no per-handler addressing — the engine puts (service,
// handler) inside StartMessage and the handler-side server echoes the
// same tuple from there.
func (c *Client) Invoke(ctx context.Context, route wire.Route) (handlerclient.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, handlerclient.ErrClientClosed
	}
	c.mu.Unlock()
	if route.Service == "" || route.Handler == "" {
		return nil, errors.New("connectclient: route.Service and route.Handler are required")
	}

	bs := c.connect.InvokeStream(ctx)
	if c.signer != nil {
		tok, err := c.signer.Sign(c.deploymentID)
		if err != nil {
			_ = bs.CloseRequest()
			_ = bs.CloseResponse()
			return nil, fmt.Errorf("connectclient: sign: %w", err)
		}
		bs.RequestHeader().Set("Authorization", "Bearer "+tok)
	}
	return &stream{bs: bs}, nil
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

// stream is one InvokeStream session.
type stream struct {
	bs *connect.BidiStreamForClient[protocolv1.Frame, protocolv1.Frame]

	sendMu     sync.Mutex
	sendClosed bool

	recvMu     sync.Mutex
	recvClosed bool
}

// Send writes a frame onto the engine→handler half of the stream. The
// first Send call also commits the request headers (Connect attaches
// them on the first message).
func (s *stream) Send(f *protocolv1.Frame) error {
	if f == nil {
		return errors.New("connectclient: nil frame")
	}
	s.sendMu.Lock()
	closed := s.sendClosed
	s.sendMu.Unlock()
	if closed {
		return errors.New("connectclient: send on closed stream")
	}
	return s.bs.Send(f)
}

// Recv reads the next frame from the handler→engine half. Returns
// io.EOF when the handler closes the stream cleanly; Connect wraps
// EOF in its own error type, which errors.Is unwraps.
func (s *stream) Recv() (*protocolv1.Frame, error) {
	s.recvMu.Lock()
	closed := s.recvClosed
	s.recvMu.Unlock()
	if closed {
		return nil, io.EOF
	}
	f, err := s.bs.Receive()
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.recvMu.Lock()
			s.recvClosed = true
			s.recvMu.Unlock()
			return nil, io.EOF
		}
		return nil, fmt.Errorf("connectclient: receive: %w", err)
	}
	return f, nil
}

// CloseSend closes both halves of the bidi stream so the underlying
// HTTP/2 stream slot is reaped immediately rather than at GC time.
// Matches the engine's `defer stream.CloseSend()` teardown pattern.
func (s *stream) CloseSend() error {
	s.sendMu.Lock()
	if !s.sendClosed {
		s.sendClosed = true
		_ = s.bs.CloseRequest()
	}
	s.sendMu.Unlock()
	s.recvMu.Lock()
	if !s.recvClosed {
		s.recvClosed = true
		_ = s.bs.CloseResponse()
	}
	s.recvMu.Unlock()
	return nil
}
