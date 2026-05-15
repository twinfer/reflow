// Package grpcclient implements handlerclient.Client over gRPC by
// dialing protocolv1.SessionServiceClient. The handler-side SDK
// registers a matching protocolv1.SessionServiceServer in 5e
// (pkg/sdk/server). For 5d the grpcclient is exercised by an in-test
// fake server.
//
// The Frame envelope is encoded by gRPC's default protobuf codec; the
// inner message payload encoding is governed by the handlerclient.Codec
// supplied via WithCodec — but the Codec is consulted by the wire
// session layer, not here, since this transport only moves Frame bytes.
package grpcclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// Scheme is the registry key for plain gRPC.
const Scheme = "grpc"

// SchemeSecure is the registry key for gRPC over TLS. The deployment
// URL host:port is dialed with the system root CA bundle by default.
const SchemeSecure = "grpcs"

// Register installs both plain and TLS dialers on r. The plain dialer
// uses insecure transport credentials; the TLS dialer uses the system
// root CA bundle. Operators who need a custom root bundle can supply
// their own Dialer via r.Register.
func Register(r *handlerclient.Registry) {
	r.Register(Scheme, dialInsecure)
	r.Register(SchemeSecure, dialTLS)
}

func dialInsecure(rawURL string, opts ...handlerclient.ClientOption) (handlerclient.Client, error) {
	return dial(rawURL, opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func dialTLS(rawURL string, opts ...handlerclient.ClientOption) (handlerclient.Client, error) {
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	return dial(rawURL, opts, grpc.WithTransportCredentials(creds))
}

func dial(rawURL string, _ []handlerclient.ClientOption, extra ...grpc.DialOption) (handlerclient.Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("grpcclient: parse url %q: %w", rawURL, err)
	}
	target := u.Host
	if target == "" {
		return nil, fmt.Errorf("grpcclient: url %q missing host", rawURL)
	}
	cc, err := grpc.NewClient(target, extra...)
	if err != nil {
		return nil, fmt.Errorf("grpcclient: NewClient %s: %w", target, err)
	}
	return &Client{cc: cc, sc: protocolv1.NewSessionServiceClient(cc)}, nil
}

// Client is a Codec-agnostic, transport-only wrapper. Each Invoke opens
// a fresh bidi stream. The underlying grpc.ClientConn is shared across
// every Invoke and torn down by Close.
type Client struct {
	mu     sync.Mutex
	cc     *grpc.ClientConn
	sc     protocolv1.SessionServiceClient
	closed bool
}

// New is the constructor used by callers that want to bypass the
// registry — primarily tests. Production code goes through
// handlerclient.Registry.Get.
func New(rawURL string, opts ...handlerclient.ClientOption) (*Client, error) {
	c, err := dial(rawURL, opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return c.(*Client), nil
}

// Invoke opens a new bidi stream against SessionService.Invoke. The
// returned Stream owns the stream lifecycle; closing the Stream's send
// side does not close the Client.
func (c *Client) Invoke(ctx context.Context) (handlerclient.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, handlerclient.ErrClientClosed
	}
	c.mu.Unlock()
	s, err := c.sc.Invoke(ctx)
	if err != nil {
		return nil, err
	}
	return &stream{s: s}, nil
}

// Close drops the gRPC ClientConn. Idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	cc := c.cc
	c.mu.Unlock()
	return cc.Close()
}

type stream struct {
	s grpc.BidiStreamingClient[protocolv1.Frame, protocolv1.Frame]
}

func (s *stream) Send(f *protocolv1.Frame) error {
	if f == nil {
		return errors.New("grpcclient: nil frame")
	}
	return s.s.Send(f)
}

func (s *stream) Recv() (*protocolv1.Frame, error) { return s.s.Recv() }

func (s *stream) CloseSend() error { return s.s.CloseSend() }
