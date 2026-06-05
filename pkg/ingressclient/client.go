// Package ingressclient is a typed Connect client for the reflow
// ingress service. Callers (test harness, operator tools, embedded
// programs) dial one of these against a reflow node's ingress
// listener and call SubmitInvocation / AwaitInvocation / etc.
package ingressclient

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/twinfer/reflw/proto/ingressv1/ingressv1connect"
)

// Client is a thin wrapper around the generated Connect client plus the
// underlying http.Client (so callers can Close idle connections).
type Client struct {
	ingressv1connect.IngressClient
	hc *http.Client
	tr *http.Transport
}

// Options configures Dial.
type Options struct {
	// BaseURL is the ingress endpoint, e.g. "http://127.0.0.1:8080" or
	// "https://api.example.com". Required.
	BaseURL string
	// TLS, when non-nil, replaces the default TLS config. Implies the
	// transport accepts plain HTTP/2 only (no h2c). Leave nil for h2c.
	TLS *tls.Config
}

// Dial constructs a client. The returned *Client must be Closed when
// done to release the underlying connection pool.
func Dial(opts Options) (*Client, error) {
	if opts.BaseURL == "" {
		return nil, errors.New("ingressclient: BaseURL is required")
	}
	tr := &http.Transport{Protocols: new(http.Protocols)}
	switch {
	case strings.HasPrefix(opts.BaseURL, "http://"):
		tr.Protocols.SetUnencryptedHTTP2(true)
		tr.Protocols.SetHTTP1(false)
	case strings.HasPrefix(opts.BaseURL, "https://"):
		tr.Protocols.SetHTTP2(true)
		tr.Protocols.SetHTTP1(false)
		if opts.TLS != nil {
			tr.TLSClientConfig = opts.TLS
		} else {
			tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	default:
		return nil, fmt.Errorf("ingressclient: BaseURL %q must use http:// or https://", opts.BaseURL)
	}
	hc := &http.Client{Transport: tr}
	return &Client{
		IngressClient: ingressv1connect.NewIngressClient(hc, strings.TrimRight(opts.BaseURL, "/")),
		hc:            hc,
		tr:            tr,
	}, nil
}

// Close releases the underlying connection pool.
func (c *Client) Close() error {
	c.tr.CloseIdleConnections()
	return nil
}
