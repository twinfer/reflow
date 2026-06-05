// Package webhook is the operator-importable library for verifying
// inbound vendor webhook signatures. Reflow ships built-in verifiers
// for Stripe, GitHub, and Slack (each self-registers via init);
// operators with bespoke schemes register their own implementation
// with RegisterVerifier before reflw.Run.
//
// A Verifier is pure: Verify takes the request plus the signing secret
// and returns the verified body, vendor metadata, and a best-effort
// idempotency key. It performs no I/O and holds no cluster state.
// Mounting verifiers as HTTP routes — resolving the secret from the
// secret store, calling SubmitInvocation, mapping the result to an
// HTTP status — lives in pkg/reflw's ExtraRoutes adapter, keyed off
// reflw.Config.Webhooks (see pkg/reflw/webhook.go).
package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	connect "connectrpc.com/connect"
)

// Verifier validates an inbound webhook signature for one vendor.
//
// Implementations:
//   - MUST be safe to call concurrently — verifiers are looked up at
//     startup and shared across all requests for that source.
//   - MUST use crypto/hmac's constant-time compare (or equivalent)
//     to defeat timing attacks on the signature byte string.
//   - SHOULD enforce a replay window when the vendor's scheme
//     includes a signed timestamp (Stripe, Slack); silent acceptance
//     of arbitrarily-old signatures is a known weakness.
//   - MUST NOT mutate r.Body's position visible to callers — read
//     the full body, return it via VerifiedEvent.Body, and leave
//     r.Body as-is. The adapter re-derives the invocation input from
//     VerifiedEvent.Body, so request-body state after Verify is not
//     load-bearing.
type Verifier interface {
	// Name is the config key that selects this verifier from
	// reflw.Config.Webhooks[].Provider. Must match the registered
	// name exactly. Convention: lowercase vendor name ("stripe",
	// "github", "slack", "acme-internal").
	Name() string

	// Verify checks the request signature against the supplied
	// secret. On success it returns the verified payload bytes and
	// any vendor-stamped metadata that should ride durable into
	// ctx.Metadata() on the handler side.
	//
	// Failures must surface as *connect.Error with
	// CodeUnauthenticated so the adapter emits a 401 with the
	// right status; returning a plain error works too but loses the
	// Connect-coded status.
	Verify(ctx context.Context, r *http.Request, secret []byte) (*VerifiedEvent, error)
}

// VerifiedEvent is the result of a successful signature check.
//
// Body holds the buffered, signature-verified request bytes. The
// adapter uses this as the submit Input — passing the verified bytes
// (not re-reading r.Body) ensures the payload that got dispatched is
// exactly the one whose signature passed.
//
// Metadata becomes the submit metadata and flows durable
// to the handler's ctx.Metadata() (Scheduled.metadata → JEInput.metadata
// → InputCommandMessage.headers). Convention: every verifier stamps
// MetadataKeyVendor so handlers can branch on origin without parsing
// the body.
//
// IdempotencyKey is a best-effort stable event identifier (GitHub's
// X-GitHub-Delivery, the Stripe/Slack event id). When non-empty the
// adapter sets it as the submit idempotency key so the engine dedups
// vendor retries to a single invocation. Empty means no
// submit-level dedup — correctness is unaffected.
type VerifiedEvent struct {
	Body           []byte
	Metadata       map[string]string
	IdempotencyKey string
}

// MetadataKeyVendor is the canonical metadata key stamped by every
// built-in verifier to identify the originating vendor. Operators
// writing custom verifiers should use the same key for consistency.
const MetadataKeyVendor = "webhook_vendor"

var (
	registryMu sync.RWMutex
	registry   = map[string]Verifier{}
)

// RegisterVerifier installs a Verifier under its Name(). Intended to
// be called from package init() or from operator main() before
// reflw.Run. Panics on duplicate registration — a programming error,
// not a runtime condition, since names are config-bound.
func RegisterVerifier(v Verifier) {
	if v == nil {
		panic("webhook: RegisterVerifier(nil)")
	}
	name := v.Name()
	if name == "" {
		panic("webhook: RegisterVerifier called with empty Name()")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[name]; dup {
		panic("webhook: duplicate verifier registration for " + name)
	}
	registry[name] = v
}

// LookupVerifier returns the registered verifier for the given name.
// The adapter calls this once per configured source at startup; an
// unknown name aborts reflw.Run with a config-validation error.
func LookupVerifier(name string) (Verifier, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	v, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("webhook: no verifier registered for %q", name)
	}
	return v, nil
}

// RegisteredNames returns the names of all registered verifiers,
// useful for error messages that need to list available options.
// Order is not stable.
func RegisteredNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	return names
}

// defaultMaxBodyBytes caps how much of r.Body a verifier buffers while
// checking a signature. Vendor webhook payloads are well under this;
// the cap defeats a malicious oversized body. Stripe's published max
// event size is ~256KB — 1 MiB leaves headroom for other vendors.
const defaultMaxBodyBytes int64 = 1 << 20 // 1 MiB

// errUnauthenticated wraps a message as a Connect-coded 401 so the
// adapter can map it to HTTP 401 via connect.CodeOf.
func errUnauthenticated(msg string) error {
	return connect.NewError(connect.CodeUnauthenticated, errors.New(msg))
}

// jsonStringField extracts a top-level string field from a JSON-object
// body, returning "" if the body isn't a JSON object or the field is
// absent / non-string. Used for best-effort idempotency-key derivation
// (Stripe "id", Slack "event_id"); form-encoded bodies (e.g. Slack
// slash commands) simply yield "".
func jsonStringField(body []byte, field string) string {
	var m map[string]json.RawMessage
	if json.Unmarshal(body, &m) != nil {
		return ""
	}
	raw, ok := m[field]
	if !ok {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}
