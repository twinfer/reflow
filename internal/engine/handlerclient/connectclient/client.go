// Package connectclient implements handlerclient.Client over Connect RPC.
// The engine calls HandlerService.Invoke as a unary RPC carrying a batch of
// protocolv1.Frame envelopes — StartMessage + replay in the request, the
// handler's command frames + terminal frame in the response. Routing
// (service, handler) flows inside the StartMessage payload, not on the URL.
//
// Connect's NewClient defaults to the Connect protocol with binary
// protobuf, but the server side enables Connect/gRPC/gRPC-Web on the same
// handler — supplying connect.WithGRPC() here is one switch away if a
// deployment prefers gRPC semantics. h2c is selected via
// http.Protocols.SetUnencryptedHTTP2 for http:// URLs; TLS via SetHTTP2
// for https://.
//
// Request gzip is enabled via connect.WithSendGzip — meaningful for
// StartMessage.state_map and replay frames. Connect-Go's gzip codec has a
// built-in minimum-byte threshold so tiny requests stay uncompressed and
// the CPU cost only kicks in on payloads worth compressing. The
// handler-side server registers gzip by default (it ships with
// protoc-gen-connect-go) so no handler-side change is needed for decode.
package connectclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/handler/wire"
	handlerv1 "github.com/twinfer/reflow/proto/handlerv1"
	"github.com/twinfer/reflow/proto/handlerv1/handlerv1connect"
)

// Scheme is the registry key for plain HTTP/2 (h2c). Plain HTTP is only
// safe inside a trusted network; the engine bootstrap rejects it unless
// the creds spec is explicitly insecure.
const Scheme = "http"

// SchemeSecure is the registry key for HTTP/2 over TLS.
const SchemeSecure = "https"

// Register installs both plain (h2c) and TLS dialers on r. Operators who
// need a custom TLS root bundle can supply their own Dialer via r.Register.
// signer is optional: when non-nil, every dispatched request carries an
// Authorization: Bearer header minted by it; when nil, no auth header is
// set (single-node and insecure-creds posture).
func Register(r *handlerclient.Registry, signer handlerclient.Signer) {
	r.Register(Scheme, dialerFor(true, signer))
	r.Register(SchemeSecure, dialerFor(false, signer))
}

func dialerFor(plaintextH2C bool, signer handlerclient.Signer) handlerclient.Dialer {
	return func(deploymentID, rawURL string) (handlerclient.Client, error) {
		return newClient(deploymentID, rawURL, plaintextH2C, signer)
	}
}

// New is the constructor used by callers that want to bypass the registry —
// primarily tests. plaintextH2C selects h2c when true, HTTPS-over-TLS
// otherwise. signer may be nil for unauthenticated dials.
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
		connect:      handlerv1connect.NewHandlerServiceClient(hc, baseURL, connect.WithSendGzip(), connect.WithReadMaxBytes(wire.DefaultMaxRecvBytes)),
		signer:       signer,
	}, nil
}

// Client wraps one Connect HandlerServiceClient bound to a single
// deployment URL. The underlying http.Transport is shared across every
// Invoke and torn down by Close. deploymentID is captured at construction
// so the signer can stamp it as the JWT aud without threading it through
// Invoke.
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

// Invoke runs one session via the unary HandlerService.Invoke RPC. route is
// validated and forwarded as a sanity check, but Connect routing itself
// carries no per-handler addressing — the engine puts (service, handler)
// inside the StartMessage frame and the handler-side server echoes the same
// tuple from there. The caller's ctx scopes the whole session (the handler
// runs to suspension or completion); there is no separate deadline.
func (c *Client) Invoke(ctx context.Context, route wire.Route, req *handlerv1.InvokeRequest) (*handlerv1.InvokeResponse, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, handlerclient.ErrClientClosed
	}
	if route.Service == "" || route.Handler == "" {
		return nil, errors.New("connectclient: route.Service and route.Handler are required")
	}
	connReq := connect.NewRequest(req)
	if c.signer != nil {
		tok, err := c.signer.Sign(c.deploymentID)
		if err != nil {
			return nil, fmt.Errorf("connectclient: sign: %w", err)
		}
		connReq.Header().Set("Authorization", "Bearer "+tok)
	}
	resp, err := c.connect.Invoke(ctx, connReq)
	if err != nil {
		return nil, fmt.Errorf("connectclient: invoke: %w", err)
	}
	return resp.Msg, nil
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
