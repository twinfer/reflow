package auth

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/authn"
)

// spiffeAuthFunc returns an authn.AuthFunc that extracts a SPIFFE
// Principal from the verified mTLS leaf on r.TLS. Composition rules:
//
//   - No TLS state → returns (nil, nil); the caller falls through to
//     the next authenticator (e.g. Bearer JWT).
//   - TLS verified, leaf has no URI SAN → returns (nil, nil); fall
//     through. This is the "TLS but not SPIFFE" case.
//   - TLS verified, leaf has exactly one URI SAN → must be a valid
//     spiffe://<td>/<kind>/<name> in trust domain td or returns a
//     hard CodeUnauthenticated error.
//   - TLS verified, leaf has >1 URI SANs → hard CodeUnauthenticated.
//
// Returning a non-nil *Principal stamps it onto the request context
// via authn.SetInfo; the policy handler reads it back with
// authn.GetInfo.
func spiffeAuthFunc(td string) authn.AuthFunc {
	return func(_ context.Context, r *http.Request) (any, error) {
		p, err := extractSPIFFE(td, r.TLS)
		if err != nil {
			return nil, authn.Errorf("spiffe: %v", err)
		}
		if p.IsAnonymous() {
			return nil, nil
		}
		return p, nil
	}
}

// extractSPIFFE parses the SPIFFE URI SAN out of a verified mTLS leaf
// against trust domain td. Returns (Principal{}, nil) for the fall-
// through cases (no TLS, no verified chain, no URI on leaf). Returns
// an error only when a verified leaf carries a URI SAN that fails the
// SPIFFE format checks.
func extractSPIFFE(td string, state *tls.ConnectionState) (Principal, error) {
	if state == nil {
		return Principal{}, nil
	}
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return Principal{}, nil
	}
	leaf := state.VerifiedChains[0][0]
	if len(leaf.URIs) == 0 {
		return Principal{}, nil
	}
	if len(leaf.URIs) > 1 {
		return Principal{}, fmt.Errorf("leaf must carry at most one URI SAN; got %d", len(leaf.URIs))
	}
	u := leaf.URIs[0]
	if u.Scheme != "spiffe" || u.Host == "" {
		return Principal{}, fmt.Errorf("unrecognised URI %q", u.String())
	}
	if u.Host != td {
		return Principal{}, fmt.Errorf("leaf trust domain %q; want %q", u.Host, td)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Principal{}, fmt.Errorf("leaf URI %q does not match /<kind>/<name>", u.String())
	}
	return Principal{
		Kind:    parts[0],
		Subject: parts[1],
		URI:     u.String(),
		Raw:     parts[0] + "/" + parts[1],
	}, nil
}
