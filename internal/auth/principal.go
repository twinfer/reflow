package auth

import "context"

// Principal is the server-trusted identity of a caller, materialized by
// an authentication step (mTLS-leaf SPIFFE URI or verified Bearer JWT)
// at the HTTP middleware layer. It is the value the policy handler
// stamps into the outgoing X-Reflow-Principal header so the policy
// engine matches on Raw.
type Principal struct {
	// Kind names the principal class — "node", "operator", "user", or
	// "" for the anonymous principal.
	Kind string
	// Subject is the principal name within Kind (node id, operator
	// name, OIDC sub claim).
	Subject string
	// URI is the canonical identifier when one exists: a spiffe://
	// URL from a leaf cert, an oidc:// pseudo-URL from a bearer token.
	// Empty for the anonymous principal.
	URI string
	// Raw is the policy-engine match key — always "kind/subject" with
	// no whitespace; the policy file matches against this string
	// verbatim.
	Raw string
	// Claims is the forward-compat extension bag: OIDC claims copied
	// in by the JWT verifier per OIDCIssuer.AllowedClaims, OPA results
	// later. Empty for SPIFFE.
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

// principalCtxKey is the context.Value key for Principal.
type principalCtxKey struct{}

// ContextWithPrincipal attaches p to ctx so handlers can recover it
// via PrincipalFromContext.
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalCtxKey{}, p)
}

// PrincipalFromContext extracts the Principal attached by the policy
// handler. The second return reports whether one was present.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(Principal)
	return p, ok
}
