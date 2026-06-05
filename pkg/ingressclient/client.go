// Package ingressclient is a typed client for the reflw ingress service.
// Callers (test harness, operator tools, embedded programs) dial one of
// these against a reflw node's ingress listener. Reads/awaits use the
// Connect RPCs (AwaitInvocation / AttachInvocation / …); submitting an
// invocation uses the REST data-plane facade (Submit → POST /v1/…).
package ingressclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/twinfer/reflw/proto/ingressv1/ingressv1connect"
)

// Client is a thin wrapper around the generated Connect client plus the
// underlying http.Client (so callers can Close idle connections and Submit
// over the REST facade).
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

// SubmitArgs is the input to Submit — the REST data-plane equivalent of the
// removed SubmitInvocation RPC request.
type SubmitArgs struct {
	Service        string
	Handler        string
	ObjectKey      string
	Input          []byte
	IdempotencyKey string
	Metadata       map[string]string
}

// HTTPStatusError is returned by Submit when the REST facade responds with a
// non-202 status. It carries the status so callers can classify the failure
// (e.g. retry on 503, give up on 4xx).
type HTTPStatusError struct {
	Status int
	Body   string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("ingressclient: submit: status %d: %s", e.Status, e.Body)
}

// Submit posts to the REST data-plane facade
// (POST /v1/{service}[/{object_key}]/{handler}?mode=send) and returns the
// minted invocation id string without blocking for the result — use
// AwaitInvocation / AttachInvocation to wait. It replaces the removed
// SubmitInvocation RPC. A non-202 response yields a *HTTPStatusError.
func (c *Client) Submit(ctx context.Context, a SubmitArgs) (string, error) {
	if a.Service == "" || a.Handler == "" {
		return "", errors.New("ingressclient: service and handler are required")
	}
	segs := []string{"v1", a.Service}
	if a.ObjectKey != "" {
		segs = append(segs, a.ObjectKey)
	}
	segs = append(segs, a.Handler)
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	endpoint := c.baseURL + "/" + strings.Join(segs, "/") + "?mode=send"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(a.Input))
	if err != nil {
		return "", err
	}
	if a.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", a.IdempotencyKey)
	}
	for k, v := range a.Metadata {
		req.Header.Set("Reflw-Meta-"+k, v)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusAccepted {
		return "", &HTTPStatusError{Status: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	var out struct {
		InvocationID string `json:"invocation_id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("ingressclient: decode submit response: %w", err)
	}
	return out.InvocationID, nil
}

// Close releases the underlying connection pool.
func (c *Client) Close() error {
	c.tr.CloseIdleConnections()
	return nil
}
