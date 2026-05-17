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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// CallWithLeaderRedirect invokes fn against the configured opts.Addr;
// on codes.Unavailable carrying a LeaderHint detail, it re-dials the
// hinted admin endpoint (reusing opts.Creds) and retries. Bounded at
// maxHops to break loops in degraded clusters where the hint cycles
// between non-leaders. Returns the last RPC error when hops are
// exhausted or when the error is non-Unavailable / lacks a hint.
//
// Used by:
//   - the joiner's callSelfJoin path in pkg/reflow/run.go (initial dial
//     comes from gossip-resolved leader admin endpoint; redirect is the
//     safety net for one-heartbeat-stale gossip);
//   - the reflow-cluster CLI, whose --admin flag now means "any cluster
//     node" — every mutating command wraps its RPC in this helper.
func CallWithLeaderRedirect(
	ctx context.Context,
	opts DialOptions,
	maxHops int,
	fn func(context.Context, adminv1.AdminClient) error,
) error {
	if maxHops < 1 {
		maxHops = 1
	}
	var lastErr error
	for hop := 0; hop < maxHops; hop++ {
		cli, err := Dial(ctx, opts)
		if err != nil {
			return err
		}
		err = fn(ctx, cli.Admin)
		_ = cli.Close()
		if err == nil {
			return nil
		}
		lastErr = err
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.Unavailable {
			return err
		}
		var hint *adminv1.LeaderHint
		for _, d := range st.Details() {
			if h, ok := d.(*adminv1.LeaderHint); ok && h.GetAdminEndpoint() != "" {
				hint = h
				break
			}
		}
		if hint == nil {
			return err
		}
		if hint.GetAdminEndpoint() == opts.Addr {
			// The server is hinting at itself or at the address we just
			// failed against — further redirects would loop. Surface the
			// original error.
			return err
		}
		opts.Addr = hint.GetAdminEndpoint()
	}
	return fmt.Errorf("admin: leader redirect exhausted after %d hops: %w", maxHops, lastErr)
}
