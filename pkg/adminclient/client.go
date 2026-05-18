// Package adminclient is the Connect-based admin client shared by the
// `reflowd cluster ...` CLI, the SelfJoin path in pkg/reflow/run.go, and
// integration tests. Thin wrapper over the generated
// adminv1connect.AdminClient with credential handling + connection
// cleanup.
package adminclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/twinfer/reflow/pkg/reflow/creds"
	"github.com/twinfer/reflow/proto/adminv1/adminv1connect"
)

// DialOptions configures Dial. Addr is host:port of the admin endpoint
// (no scheme — the dialer derives http:// for insecure / https:// for
// TLS). Creds selects the transport-security driver.
type DialOptions struct {
	Addr  string
	Creds creds.Spec
}

// Client is the typed Connect admin client plus the underlying
// transport so the caller can Close cleanly.
type Client struct {
	Admin   adminv1connect.AdminClient
	addr    string
	baseURL string
	tr      *http.Transport
	closer  func() error
}

var _ io.Closer = (*Client)(nil)

// Dial builds an HTTP/2 transport from opts.Creds and constructs the
// Connect client. Insecure (zero spec) → h2c; TLS / tls_certprovider →
// HTTPS over HTTP/2 with the spec's tls.Config. Connection setup is
// lazy — the first RPC the caller issues is what surfaces an
// unreachable address.
func Dial(_ context.Context, opts DialOptions) (*Client, error) {
	if opts.Addr == "" {
		return nil, errors.New("adminclient: Addr required")
	}
	lc, err := creds.Build(opts.Creds, nil)
	if err != nil {
		return nil, fmt.Errorf("adminclient: creds: %w", err)
	}
	tr := &http.Transport{Protocols: new(http.Protocols)}
	var baseURL string
	if lc.ServerTLSConfig == nil && lc.ClientTLSConfig == nil {
		tr.Protocols.SetUnencryptedHTTP2(true)
		tr.Protocols.SetHTTP1(false)
		baseURL = "http://" + opts.Addr
	} else {
		tr.Protocols.SetHTTP2(true)
		tr.Protocols.SetHTTP1(false)
		if lc.ClientTLSConfig != nil {
			tr.TLSClientConfig = lc.ClientTLSConfig
		} else {
			tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		baseURL = "https://" + opts.Addr
	}
	hc := &http.Client{Transport: tr}
	cli := adminv1connect.NewAdminClient(hc, strings.TrimRight(baseURL, "/"))
	return &Client{
		Admin:   cli,
		addr:    opts.Addr,
		baseURL: baseURL,
		tr:      tr,
		closer:  lc.Close,
	}, nil
}

// Close releases the underlying transport and any creds resources.
func (c *Client) Close() error {
	if c.tr != nil {
		c.tr.CloseIdleConnections()
		c.tr = nil
	}
	if c.closer != nil {
		err := c.closer()
		c.closer = nil
		return err
	}
	return nil
}

// Addr returns the dialed server address.
func (c *Client) Addr() string { return c.addr }

// BaseURL returns the base URL the Connect client points at (with the
// http:// or https:// scheme prefix derived from opts.Creds).
func (c *Client) BaseURL() string { return c.baseURL }
