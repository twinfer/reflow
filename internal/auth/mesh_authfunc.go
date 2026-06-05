package auth

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/authn"
	"github.com/twinfer/reflw/pkg/reflow/creds"
)

// meshAuthFunc returns an authn.AuthFunc that extracts a Principal
// from the verified mTLS leaf on r.TLS via the leaf CN. Composition
// rules:
//
//   - No TLS state → returns (nil, nil); the caller falls through to
//     the next authenticator (e.g. Bearer JWT).
//   - TLS verified, leaf CN is empty → returns (nil, nil); fall
//     through. This is the "TLS but not a mesh leaf" case.
//   - TLS verified, leaf CN is a well-formed <kind>/<name> → returns a
//     non-nil *Principal stamped onto the request context via
//     authn.SetInfo.
//   - TLS verified, leaf CN is set but malformed → hard
//     CodeUnauthenticated.
func meshAuthFunc() authn.AuthFunc {
	return func(_ context.Context, r *http.Request) (any, error) {
		p, err := extractMesh(r.TLS)
		if err != nil {
			return nil, authn.Errorf("mesh: %v", err)
		}
		if p.IsAnonymous() {
			return nil, nil
		}
		return p, nil
	}
}

// extractMesh parses the principal Raw form out of a verified mTLS
// leaf's CN. Returns (Principal{}, nil) for the fall-through cases
// (no TLS, no verified chain, empty CN). Returns an error only when a
// verified leaf carries a CN that does not match <kind>/<name>.
func extractMesh(state *tls.ConnectionState) (Principal, error) {
	if state == nil {
		return Principal{}, nil
	}
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return Principal{}, nil
	}
	chain := state.VerifiedChains[0]
	leaf := chain[0]
	if leaf.Subject.CommonName == "" {
		return Principal{}, nil
	}
	raw, err := creds.LeafPrincipal(leaf)
	if err != nil {
		return Principal{}, err
	}
	parts := strings.SplitN(raw, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Principal{}, fmt.Errorf("leaf CN %q does not match <kind>/<name>", leaf.Subject.CommonName)
	}
	return Principal{
		Kind:              parts[0],
		Subject:           parts[1],
		Raw:               raw,
		MeshCAFingerprint: creds.SPKIFingerprint(chain[len(chain)-1]),
	}, nil
}
