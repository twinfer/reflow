package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"

	"github.com/twinfer/reflow/pkg/webhook"
)

// githubVerifier implements webhook.Verifier for GitHub-style
// X-Hub-Signature-256 headers. Format:
//
//	X-Hub-Signature-256: sha256=<lowercase-hex>
//
// HMAC-SHA256 keyed on the webhook secret over the raw request body.
// GitHub also sends X-GitHub-Event (event type, e.g. "pull_request")
// and X-GitHub-Delivery (UUID identifying the delivery) — we stamp
// both into metadata so handlers can dispatch by event type and
// handlers can dedup retries by delivery ID without parsing the body.
//
// GitHub has no signed timestamp, so there is no replay window at
// the verifier level. Operators rely on the durable Reflow
// invocation graph + X-GitHub-Delivery for idempotency.
//
// Ref: https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
type githubVerifier struct{}

// NewGitHubVerifier returns a stateless GitHub webhook verifier.
func NewGitHubVerifier() webhook.Verifier { return &githubVerifier{} }

func init() { webhook.RegisterVerifier(NewGitHubVerifier()) }

func (g *githubVerifier) Name() string { return "github" }

func (g *githubVerifier) Verify(_ context.Context, r *http.Request, secret []byte) (*webhook.VerifiedEvent, error) {
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
	meta := map[string]string{webhook.MetadataKeyVendor: "github"}
	if v := r.Header.Get("X-GitHub-Event"); v != "" {
		meta["github_event"] = v
	}
	if v := r.Header.Get("X-GitHub-Delivery"); v != "" {
		meta["github_delivery"] = v
	}
	if v := r.Header.Get("X-GitHub-Hook-Installation-Target-Type"); v != "" {
		meta["github_hook_target_type"] = v
	}
	return &webhook.VerifiedEvent{Body: body, Metadata: meta}, nil
}
