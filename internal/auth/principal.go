package auth

import (
	"context"
	"strconv"
	"strings"
)

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

// TenantIDFromPrincipal extracts the numeric tenant id from p. Returns
// 0 (the default-tenant sentinel) when the principal does not carry a
// tenant assignment — anonymous traffic, operator/* (platform admins
// do not act on behalf of a tenant), node/* (cluster mesh), and
// pre-tenancy user/* principals all fall back to tenant 0. Recognized
// shapes:
//
//   - Kind == "tenant" and Subject parses as uint32 → that id.
//
// Anything else returns 0; the engine treats tenant 0 as "no tenant
// scoping," which is the safe default for single-tenant deployments
// and the operator surface.
func TenantIDFromPrincipal(p Principal) uint32 {
	if p.Kind != "tenant" {
		return 0
	}
	// Subject may carry a `/`-sanitized prefix from the JWT
	// principal-claim path (see jwt_authfunc); the tenant id is the
	// leading numeric segment.
	subj := p.Subject
	if i := strings.IndexByte(subj, '/'); i >= 0 {
		subj = subj[:i]
	}
	id, err := strconv.ParseUint(subj, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(id)
}
