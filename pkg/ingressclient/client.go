// Package ingressclient is a typed client for the reflw ingress service.
// Callers (test harness, operator tools, embedded programs) dial one of
// these against a reflw node's ingress listener. Every call is a Connect
// RPC over the single ingress listener (content-negotiated against the
// Vanguard transcoder); Submit is a thin wrapper over SubmitInvocation in
// SEND mode.
package ingressclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strings"

	connect "connectrpc.com/connect"

	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
	"github.com/twinfer/reflw/proto/ingressv1/ingressv1connect"
)

// Client is a thin wrapper around the generated Connect client plus the
// underlying http.Client (so callers can Close idle connections).
type Client struct {
	ingressv1connect.IngressClient
	hc      *http.Client
	tr      *http.Transport
	baseURL string
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
	base := strings.TrimRight(opts.BaseURL, "/")
	return &Client{
		IngressClient: ingressv1connect.NewIngressClient(hc, base),
		hc:            hc,
		tr:            tr,
		baseURL:       base,
	}, nil
}

// SubmitArgs is the input to Submit.
type SubmitArgs struct {
	Service        string
	Handler        string
	ObjectKey      string
	Input          []byte
	IdempotencyKey string
	Metadata       map[string]string
}

// Submit calls SubmitInvocation in SEND mode and returns the minted invocation
// id string without blocking for the result — use AwaitInvocation /
// AttachInvocation to wait. Errors are *connect.Error; classify with
// connect.CodeOf.
func (c *Client) Submit(ctx context.Context, a SubmitArgs) (string, error) {
	if a.Service == "" || a.Handler == "" {
		return "", errors.New("ingressclient: service and handler are required")
	}
	resp, err := c.SubmitInvocation(ctx, connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service:        a.Service,
		Handler:        a.Handler,
		ObjectKey:      a.ObjectKey,
		Input:          a.Input,
		IdempotencyKey: a.IdempotencyKey,
		Metadata:       a.Metadata,
		Mode:           ingressv1.SubmitInvocationRequest_MODE_SEND,
	}))
	if err != nil {
		return "", err
	}
	return resp.Msg.GetInvocationId(), nil
}

// Close releases the underlying connection pool.
func (c *Client) Close() error {
	c.tr.CloseIdleConnections()
	return nil
}
