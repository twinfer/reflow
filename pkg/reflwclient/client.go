// Package reflwclient is the Connect-based admin-port client shared
// by the `reflwd cluster` / `reflwd config` CLIs, the SelfJoin path
// in pkg/reflw/run.go, and integration tests. Thin wrapper over the
// generated adminv1connect.AdminClient with credential handling +
// connection cleanup.
//
// The merged Admin service and Ingress live on separate listeners — a
// Client built against an admin Addr can't reach Ingress and vice
// versa, so dial the listener matching the RPC you intend to call.
package reflwclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/twinfer/reflw/pkg/reflw/creds"
	"github.com/twinfer/reflw/proto/adminv1/adminv1connect"
	"github.com/twinfer/reflw/proto/ingressv1/ingressv1connect"
)

// DialOptions configures Dial. Addr is host:port of the admin endpoint
// (no scheme — the dialer derives http:// for insecure / https:// for
// TLS). Creds selects the transport-security driver.
type DialOptions struct {
	Addr  string
	Creds creds.Spec
	// ClientTLSConfig, when non-nil, is used verbatim for the HTTPS/HTTP2
	// transport and Creds is ignored. The node mesh identity passes its
	// live self-issued *tls.Config here for the SelfJoin dial, which has
	// no on-disk creds spec to rebuild from.
	ClientTLSConfig *tls.Config
}

// Client wraps the typed sub-clients over a single HTTP/2 transport plus
// credential lifecycle. Admin lives on the admin listener; Ingress lives on
// the separate ingress listener — a Client built against an admin Addr can't
// reach Ingress and vice versa, so dial the listener matching the RPC you
// intend to call.
type Client struct {
	Admin   adminv1connect.AdminClient
	Ingress ingressv1connect.IngressClient

	addr    string
	baseURL string
	tr      *http.Transport
	closer  func() error
}

var _ io.Closer = (*Client)(nil)

// Dial builds an HTTP/2 transport from opts.Creds and constructs the
// two Connect sub-clients. Insecure (zero spec) → h2c; TLS /
// tls_certprovider → HTTPS over HTTP/2 with the spec's tls.Config.
// Connection setup is lazy — the first RPC the caller issues is what
// surfaces an unreachable address.
func Dial(_ context.Context, opts DialOptions) (*Client, error) {
	if opts.Addr == "" {
		return nil, errors.New("reflwclient: Addr required")
	}
	// Live-config override: the mesh SelfJoin dial passes its self-issued
	// *tls.Config directly (no creds.Spec to rebuild from).
	if opts.ClientTLSConfig != nil {
		tr := &http.Transport{Protocols: new(http.Protocols)}
		tr.Protocols.SetHTTP2(true)
		tr.Protocols.SetHTTP1(false)
		tr.TLSClientConfig = opts.ClientTLSConfig
		baseURL := "https://" + opts.Addr
		hc := &http.Client{Transport: tr}
		trimmed := strings.TrimRight(baseURL, "/")
		return &Client{
			Admin:   adminv1connect.NewAdminClient(hc, trimmed),
			Ingress: ingressv1connect.NewIngressClient(hc, trimmed),
			addr:    opts.Addr,
			baseURL: baseURL,
			tr:      tr,
		}, nil
	}
	lc, err := creds.Build(opts.Creds, nil)
	if err != nil {
		return nil, fmt.Errorf("reflwclient: creds: %w", err)
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
	trimmed := strings.TrimRight(baseURL, "/")
	return &Client{
		Admin:   adminv1connect.NewAdminClient(hc, trimmed),
		Ingress: ingressv1connect.NewIngressClient(hc, trimmed),
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

// BaseURL returns the base URL the Connect clients point at (with the
// http:// or https:// scheme prefix derived from opts.Creds).
func (c *Client) BaseURL() string { return c.baseURL }
