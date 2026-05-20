// Package webhook is the operator-importable extension point for
// inbound vendor webhook signature verification. Reflow ships
// built-in verifiers for Stripe, GitHub, and Slack under
// internal/ingress/webhook; operators with bespoke schemes register
// their own implementation here before reflow.Run.
//
// Like pkg/handler/wire, this package is a deliberate pkg→internal
// exception: it lives in pkg/ because operator code (in their handler
// binary's main) needs to import it to call RegisterVerifier. The
// engine's mounting / dispatch lives in internal/ingress/webhook and
// looks up registered verifiers by name at startup.
package webhook

import (
	"context"
	"fmt"
	"net/http"
	"sync"
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
//     r.Body as-is. The webhook manager re-derives the invocation
//     input from VerifiedEvent.Body, so request-body state after
//     Verify is not load-bearing.
type Verifier interface {
	// Name is the config key that selects this verifier from
	// cfg.Webhooks.Sources[].Verifier. Must match the registered
	// name exactly. Convention: lowercase vendor name ("stripe",
	// "github", "slack", "acme-internal").
	Name() string

	// Verify checks the request signature against the supplied
	// secret. On success it returns the verified payload bytes and
	// any vendor-stamped metadata that should ride durable into
	// ctx.Metadata() on the handler side.
	//
	// Failures must surface as *connect.Error with
	// CodeUnauthenticated so the manager emits a 401 with the
	// right protocol shape; returning a plain error works too but
	// loses the Connect-coded status.
	Verify(ctx context.Context, r *http.Request, secret []byte) (*VerifiedEvent, error)
}

// VerifiedEvent is the result of a successful signature check.
//
// Body holds the buffered, signature-verified request bytes. The
// webhook manager uses this as the SubmitInvocationRequest.Input —
// passing the verified bytes (not re-reading r.Body) ensures the
// payload that got dispatched is exactly the one whose signature
// passed.
//
// Metadata is merged into SubmitInvocationRequest.metadata along
// with the static facts declared in WebhookSource.Invocation.Metadata,
// then flows through the full durable path documented in CLAUDE.md:
// Scheduled.metadata → JEInput.metadata → InputCommandMessage.headers
// → handler.Context.Metadata(). Convention: every verifier stamps
// "webhook_vendor" so handlers can branch on origin without parsing
// payload bytes.
type VerifiedEvent struct {
	Body     []byte
	Metadata map[string]string
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
// reflow.Run. Panics on duplicate registration — a programming error,
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
// The manager calls this once per configured source at startup; an
// unknown name aborts reflow.Run with a config-validation error.
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
