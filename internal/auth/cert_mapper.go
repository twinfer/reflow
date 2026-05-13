package auth

import (
	"context"
	"fmt"
	"strings"
)

// CertClaimMapper extracts Claims from the verified TLS leaf's SPIFFE
// URI SAN. It requires exactly one URI per leaf, scheme=spiffe, host
// matching TrustDomain, and a two-segment path "/<kind>/<subject>".
//
// A future JWTClaimMapper will mirror this shape but read
// AuthInfo.AuthToken instead. Mappers compose via ChainedClaimMapper
// (not implemented yet — first non-(nil,nil) result wins).
type CertClaimMapper struct {
	TrustDomain string
}

// GetClaims implements ClaimMapper. Returns (nil, nil) when no
// TLSConnection is present so the interceptor can try a different
// mapper or surface Unauthenticated. Malformed certs that DO present a
// URI SAN return (nil, error).
func (m *CertClaimMapper) GetClaims(_ context.Context, info AuthInfo) (*Claims, error) {
	if info.TLSConnection == nil ||
		len(info.TLSConnection.State.VerifiedChains) == 0 ||
		len(info.TLSConnection.State.VerifiedChains[0]) == 0 {
		return nil, nil
	}
	leaf := info.TLSConnection.State.VerifiedChains[0][0]
	if len(leaf.URIs) != 1 {
		return nil, fmt.Errorf("auth: leaf must carry exactly one URI SAN; got %d", len(leaf.URIs))
	}
	u := leaf.URIs[0]
	if u.Scheme != "spiffe" || u.Host == "" {
		return nil, fmt.Errorf("auth: unrecognised URI %q", u.String())
	}
	if u.Host != m.TrustDomain {
		return nil, fmt.Errorf("auth: leaf trust domain %q; want %q", u.Host, m.TrustDomain)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("auth: leaf URI %q does not match /<kind>/<name>", u.String())
	}
	return &Claims{Kind: parts[0], Subject: parts[1], URI: u}, nil
}
