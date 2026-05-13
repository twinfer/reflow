package auth

import (
	"context"
	"net/url"

	"google.golang.org/grpc/credentials"
)

// AuthInfo carries the raw identity material gathered by an interceptor
// before any claim-mapping. TLSConnection is populated when the call
// arrives over mTLS; AuthToken is populated when a future Authorization
// header is present. Either field may be empty — the ClaimMapper
// decides what's required.
type AuthInfo struct {
	TLSConnection *credentials.TLSInfo
	AuthToken     string // empty today; future JWT
}

// Claims is the parsed, server-trusted identity of a caller.
//
// Subject is a human-readable principal name (operator name, node id as
// a string). Kind names the principal class — "node" or "operator"
// today. URI is the underlying SPIFFE identifier when the source was an
// mTLS cert; nil when claims come from a non-cert mapper. Extensions
// is a free-form bucket reserved for non-cert mappers (e.g. raw JWT
// claims, OPA results) — nothing reads it today.
type Claims struct {
	Subject    string
	Kind       string
	URI        *url.URL
	Extensions any
}

// String renders the underlying URI when present; otherwise
// "<kind>/<subject>". Useful in audit log lines.
func (c *Claims) String() string {
	if c == nil {
		return ""
	}
	if c.URI != nil {
		return c.URI.String()
	}
	return c.Kind + "/" + c.Subject
}

// ClaimMapper converts raw AuthInfo into trusted Claims.
//
// Returning (nil, nil) means "no identity available from this source"
// and lets the interceptor decide whether to try another mapper or
// reject with Unauthenticated. Returning (nil, err) is a hard
// rejection — the interceptor surfaces it as Unauthenticated and does
// not consult further mappers.
type ClaimMapper interface {
	GetClaims(ctx context.Context, info AuthInfo) (*Claims, error)
}

// CallTarget describes the RPC being authorized.
type CallTarget struct {
	APIName string // gRPC FullMethod, e.g. "/reflow.admin.v1.Admin/AddNode"
}

// Decision is the binary outcome of an Authorize call.
type Decision int

const (
	// DecisionDeny rejects the call. The interceptor surfaces it as
	// PermissionDenied, with Result.Reason embedded in the error.
	DecisionDeny Decision = iota
	// DecisionAllow lets the call through to the handler.
	DecisionAllow
)

// Result is what an Authorizer returns. Reason is included in the
// rejection error and audit log line.
type Result struct {
	Decision Decision
	Reason   string
}

// Authorizer makes the access decision given Claims (which may be nil
// when no identity is present) and a CallTarget.
type Authorizer interface {
	Authorize(ctx context.Context, claims *Claims, target *CallTarget) (Result, error)
}

// Context plumbing ---------------------------------------------------

type claimsCtxKey struct{}

// ContextWithClaims returns a derived context carrying c. The
// interceptors install this after a successful Authorize so handlers
// can recover the caller identity via ClaimsFromContext.
func ContextWithClaims(ctx context.Context, c *Claims) context.Context {
	return context.WithValue(ctx, claimsCtxKey{}, c)
}

// ClaimsFromContext extracts the Claims attached by an interceptor.
// Returns false when no claims are present (test fixtures, unauthed
// paths).
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(claimsCtxKey{}).(*Claims)
	return c, ok && c != nil
}
