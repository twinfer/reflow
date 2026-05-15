package auth

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// Principal is the server-trusted identity of a caller, extracted by an
// Extractor from the gRPC context (TLS peer info, metadata headers,
// future JWT claims). It is the value the auth interceptor stamps into
// the outgoing x-reflow-principal header so the policy engine can match
// on Raw via authz request.headers.
type Principal struct {
	// Kind names the principal class — "node", "operator", "user", or
	// "" for the anonymous principal.
	Kind string
	// Subject is the principal name within Kind (node id, operator
	// name, OIDC sub claim).
	Subject string
	// URI is the canonical identifier when one exists: a spiffe://
	// URL from a leaf cert, a jwt:sub: pseudo-URL from a bearer token.
	// Empty for the anonymous principal.
	URI string
	// Raw is the policy-engine match key — always "kind/subject" with
	// no whitespace; the authz policy file matches against this
	// string verbatim (request.headers["x-reflow-principal"]).
	Raw string
	// Claims is the forward-compat extension bag: raw JWT claims,
	// OPA results, etc. Empty for SPIFFE today.
	Claims map[string]string
}

// IsAnonymous reports whether the principal carries no identity.
func (p Principal) IsAnonymous() bool { return p.Kind == "" && p.Subject == "" }

// String returns Raw — the audit-log-friendly canonical form.
func (p Principal) String() string {
	if p.IsAnonymous() {
		return "anonymous"
	}
	return p.Raw
}

// Extractor turns a gRPC server context into a Principal. A nil error
// with an anonymous Principal means "no identity available from this
// source"; the interceptor decides whether that is acceptable. A
// non-nil error is a hard rejection — the interceptor surfaces it as
// Unauthenticated and does not consult further Extractors.
type Extractor interface {
	Extract(ctx context.Context) (Principal, error)
}

// SPIFFEExtractor parses the verified TLS leaf's URI SAN into a
// Principal. Replaces the legacy CertClaimMapper. The contract is the
// same: exactly one URI per leaf, scheme=spiffe, host=TrustDomain,
// path "/<kind>/<subject>" with non-empty segments.
type SPIFFEExtractor struct {
	TrustDomain string
}

// Extract implements Extractor. Returns an anonymous Principal with no
// error when the context has no TLS peer info (so an interceptor
// upstream can fall through to a JWT extractor, etc.). Malformed
// SPIFFE certs return (Principal{}, error).
func (e *SPIFFEExtractor) Extract(ctx context.Context) (Principal, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return Principal{}, nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return Principal{}, nil
	}
	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return Principal{}, nil
	}
	leaf := tlsInfo.State.VerifiedChains[0][0]
	if len(leaf.URIs) != 1 {
		return Principal{}, fmt.Errorf("auth: leaf must carry exactly one URI SAN; got %d", len(leaf.URIs))
	}
	u := leaf.URIs[0]
	if u.Scheme != "spiffe" || u.Host == "" {
		return Principal{}, fmt.Errorf("auth: unrecognised URI %q", u.String())
	}
	if u.Host != e.TrustDomain {
		return Principal{}, fmt.Errorf("auth: leaf trust domain %q; want %q", u.Host, e.TrustDomain)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Principal{}, fmt.Errorf("auth: leaf URI %q does not match /<kind>/<name>", u.String())
	}
	return Principal{
		Kind:    parts[0],
		Subject: parts[1],
		URI:     u.String(),
		Raw:     parts[0] + "/" + parts[1],
	}, nil
}

// AnonExtractor always returns the anonymous Principal. Pair with
// insecure transport when an operator opts out of identity entirely
// (single-node dev, internal-only deployments).
type AnonExtractor struct{}

// Extract implements Extractor.
func (AnonExtractor) Extract(_ context.Context) (Principal, error) {
	return Principal{}, nil
}

// principalCtxKey is the context.Value key for Principal — separate
// from claimsCtxKey so the new and legacy plumbing don't collide
// during the migration.
type principalCtxKey struct{}

// ContextWithPrincipal attaches p to ctx so handlers can recover it
// via PrincipalFromContext.
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalCtxKey{}, p)
}

// PrincipalFromContext extracts the Principal attached by an
// interceptor. The second return reports whether one was present.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(Principal)
	return p, ok
}
