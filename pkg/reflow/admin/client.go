// Package admin holds the shared admin gRPC client used by the
// reflow-cluster CLI and by integration tests. Thin wrapper over the
// generated adminv1.AdminClient that handles credential setup +
// connection cleanup.
package admin

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/twinfer/reflow/pkg/reflow/creds"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
)

// DialOptions configures Dial. Addr is host:port of the admin endpoint.
// Creds selects the transport-security driver (insecure zero spec for
// local tests, tls / tls_certprovider / oauth / … for production
// operator workflows).
type DialOptions struct {
	Addr  string
	Creds creds.Spec
}

// Client is the typed admin gRPC client plus its underlying conn so the
// caller can Close cleanly.
type Client struct {
	cc      *grpc.ClientConn
	Admin   adminv1.AdminClient
	addr    string
	closer  func() error
	closeFn func() error
}

var _ io.Closer = (*Client)(nil)

// Dial opens a gRPC connection to opts.Addr using the supplied creds
// spec. grpc.NewClient is non-blocking — the first RPC the caller
// issues is what surfaces an unreachable address, gated by the
// caller's ctx.
func Dial(_ context.Context, opts DialOptions) (*Client, error) {
	if opts.Addr == "" {
		return nil, errors.New("admin: Addr required")
	}
	lc, err := creds.Build(opts.Creds, nil)
	if err != nil {
		return nil, fmt.Errorf("admin: creds: %w", err)
	}
	cc, err := grpc.NewClient("passthrough:///"+opts.Addr, lc.ClientDial...)
	if err != nil {
		if lc.Close != nil {
			_ = lc.Close()
		}
		return nil, fmt.Errorf("admin: dial %s: %w", opts.Addr, err)
	}
	return &Client{
		cc:      cc,
		Admin:   adminv1.NewAdminClient(cc),
		addr:    opts.Addr,
		closer:  lc.Close,
		closeFn: cc.Close,
	}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	var firstErr error
	if c.closeFn != nil {
		if err := c.closeFn(); err != nil {
			firstErr = err
		}
		c.closeFn = nil
	}
	if c.closer != nil {
		if err := c.closer(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.closer = nil
	}
	return firstErr
}

// Addr returns the dialed server address.
func (c *Client) Addr() string { return c.addr }
