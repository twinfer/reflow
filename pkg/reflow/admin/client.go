// Package admin holds the shared admin gRPC client used by the
// reflow-cluster CLI and by integration tests. Thin wrapper over the
// generated adminv1.AdminClient that handles mTLS dial + connection
// cleanup. Phase 4.2.
package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/twinfer/reflow/pkg/reflow"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
)

// DialOptions configures Dial. Addr must be a host:port reachable from
// the caller. OperatorCertFile / OperatorKeyFile / CAFile drive the
// mTLS handshake (see pkg/reflow/tls.go BuildAdminClientTLS). TrustDomain
// is the SPIFFE trust domain the server's leaf URI must match; empty
// falls back to reflow.DefaultTrustDomain.
type DialOptions struct {
	Addr             string
	OperatorCertFile string
	OperatorKeyFile  string
	CAFile           string
	TrustDomain      string
	// Timeout caps the dial; zero defaults to 10s.
	Timeout time.Duration
}

// Client is the typed admin gRPC client plus its underlying conn so the
// caller can Close cleanly.
type Client struct {
	cc      *grpc.ClientConn
	Admin   adminv1.AdminClient
	addr    string
	closeFn func() error
}

var _ io.Closer = (*Client)(nil)

// Dial opens an mTLS gRPC connection to opts.Addr using the supplied
// operator cert / node CA.
func Dial(ctx context.Context, opts DialOptions) (*Client, error) {
	if opts.Addr == "" {
		return nil, errors.New("admin: Addr required")
	}
	if opts.Timeout == 0 {
		opts.Timeout = 10 * time.Second
	}
	td := opts.TrustDomain
	if td == "" {
		td = reflow.DefaultTrustDomain
	}
	tlsCfg, err := reflow.BuildAdminClientTLS(opts.OperatorCertFile, opts.OperatorKeyFile, opts.CAFile, td)
	if err != nil {
		return nil, fmt.Errorf("admin: tls: %w", err)
	}
	dialCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	cc, err := grpc.DialContext(dialCtx, opts.Addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("admin: dial %s: %w", opts.Addr, err)
	}
	return &Client{
		cc:      cc,
		Admin:   adminv1.NewAdminClient(cc),
		addr:    opts.Addr,
		closeFn: cc.Close,
	}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	if c.closeFn == nil {
		return nil
	}
	err := c.closeFn()
	c.closeFn = nil
	return err
}

// Addr returns the dialed server address.
func (c *Client) Addr() string { return c.addr }
