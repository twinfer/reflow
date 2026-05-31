// Package reflowclient is the Connect-based admin-port client shared
// by the `reflowd cluster` / `reflowd config` CLIs, the SelfJoin path
// in pkg/reflow/run.go, and integration tests. Thin wrapper over the
// generated clusterctlv1connect.ClusterCtlClient and
// configv1connect.ConfigClient with credential handling + connection
// cleanup.
//
// Both services live on the same admin listener today, so one Dial
// yields one transport hosting both typed sub-clients. A future split
// onto separate ports would change DialOptions but not the Client
// shape — the Cluster + Config fields are stable.
package reflowclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/twinfer/reflow/pkg/reflow/creds"
	"github.com/twinfer/reflow/proto/clusterctlv1/clusterctlv1connect"
	"github.com/twinfer/reflow/proto/configv1/configv1connect"
	"github.com/twinfer/reflow/proto/ingressv1/ingressv1connect"
)

// DialOptions configures Dial. Addr is host:port of the admin endpoint
// (no scheme — the dialer derives http:// for insecure / https:// for
// TLS). Creds selects the transport-security driver.
type DialOptions struct {
	Addr  string
	Creds creds.Spec
}

// Client wraps the typed sub-clients over a single HTTP/2 transport plus
// credential lifecycle. Cluster + Config live on the admin listener;
// Ingress lives on the separate ingress listener — a Client built against
// an admin Addr can't reach Ingress and vice versa, so dial the listener
// matching the RPC you intend to call.
type Client struct {
	Cluster clusterctlv1connect.ClusterCtlClient
	Config  configv1connect.ConfigClient
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
		return nil, errors.New("reflowclient: Addr required")
	}
	lc, err := creds.Build(opts.Creds, nil)
	if err != nil {
		return nil, fmt.Errorf("reflowclient: creds: %w", err)
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
		Cluster: clusterctlv1connect.NewClusterCtlClient(hc, trimmed),
		Config:  configv1connect.NewConfigClient(hc, trimmed),
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
