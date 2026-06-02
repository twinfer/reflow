package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
)

// githubVerifier implements Verifier for GitHub-style
// X-Hub-Signature-256 headers. Format:
//
//	X-Hub-Signature-256: sha256=<lowercase-hex>
//
// HMAC-SHA256 keyed on the webhook secret over the raw request body.
// GitHub also sends X-GitHub-Event (event type, e.g. "pull_request")
// and X-GitHub-Delivery (UUID identifying the delivery) — we stamp
// both into metadata so handlers can dispatch by event type, and we
// surface the delivery as VerifiedEvent.IdempotencyKey so the adapter
// dedups retries at submit time.
//
// GitHub has no signed timestamp, so there is no replay window at the
// verifier level; the delivery-id idempotency key is the dedup anchor.
//
// Ref: https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
type githubVerifier struct{}

// NewGitHubVerifier returns a stateless GitHub webhook verifier.
func NewGitHubVerifier() Verifier { return &githubVerifier{} }

func init() { RegisterVerifier(NewGitHubVerifier()) }

func (g *githubVerifier) Name() string { return "github" }

func (g *githubVerifier) Verify(_ context.Context, r *http.Request, secret []byte) (*VerifiedEvent, error) {
	if len(secret) == 0 {
		return nil, errUnauthenticated("github: empty secret")
	}
	header := r.Header.Get("X-Hub-Signature-256")
	if header == "" {
		return nil, errUnauthenticated("github: missing X-Hub-Signature-256 header")
	}
	const prefix = "sha256="
	if !strings.HasPrefix(header, prefix) {
		return nil, errUnauthenticated("github: signature missing sha256= prefix")
	}
	sigHex := header[len(prefix):]
	got, err := hex.DecodeString(sigHex)
	if err != nil {
		return nil, errUnauthenticated("github: signature not valid hex")
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, defaultMaxBodyBytes))
	if err != nil {
		return nil, errUnauthenticated("github: body read: " + err.Error())
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	expected := mac.Sum(nil)
	if !hmac.Equal(expected, got) {
		return nil, errUnauthenticated("github: signature mismatch")
	}
	meta := map[string]string{MetadataKeyVendor: "github"}
	if v := r.Header.Get("X-GitHub-Event"); v != "" {
		meta["github_event"] = v
	}
	delivery := r.Header.Get("X-GitHub-Delivery")
	if delivery != "" {
		meta["github_delivery"] = delivery
	}
	if v := r.Header.Get("X-GitHub-Hook-Installation-Target-Type"); v != "" {
		meta["github_hook_target_type"] = v
	}
	return &VerifiedEvent{Body: body, Metadata: meta, IdempotencyKey: delivery}, nil
}
