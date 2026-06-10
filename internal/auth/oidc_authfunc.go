package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/authn"
	"github.com/coreos/go-oidc/v3/oidc"
)

// OIDCConfig configures the OIDC bearer-token authenticator. Issuer is the
// provider's discovery URL; Audience is the expected `aud` (empty skips the
// audience check). GroupsClaim names the token claim holding the caller's
// groups (default "groups"); ClaimKeys lists extra string claims copied into
// Principal.Claims for audit. Mirrors the public pkg/reflw.OIDCConfig.
type OIDCConfig struct {
	Issuer      string
	Audience    string
	GroupsClaim string
	ClaimKeys   []string
}

// Enabled reports whether an issuer is configured.
func (c OIDCConfig) Enabled() bool { return c.Issuer != "" }

// verifiedClaims is the subset of a verified token the authfunc maps to a
// Principal. Extracted behind the bearerVerifier seam so the authfunc logic is
// testable without a live provider (an *oidc.IDToken's claims payload is
// unexported, so a fake token can't carry claims directly).
type verifiedClaims struct {
	subject string
	groups  []string
	extra   map[string]string
}

// bearerVerifier turns a raw bearer token into verified claims, or an error if
// the token is invalid. The production implementation wraps an
// *oidc.IDTokenVerifier; tests inject a fake.
type bearerVerifier func(ctx context.Context, rawToken string) (verifiedClaims, error)

// oidcAuthFunc builds an authn.AuthFunc from a bearerVerifier:
//   - no `Authorization: Bearer` header → (nil, nil): fall through to anonymous;
//   - a present-but-invalid token → CodeUnauthenticated (RFC 6750), never
//     silently anonymous;
//   - a valid token → a User Principal stamped with sub + groups + picked claims.
func oidcAuthFunc(verify bearerVerifier) authn.AuthFunc {
	return func(ctx context.Context, r *http.Request) (any, error) {
		raw, ok := bearerToken(r.Header)
		if !ok {
			return nil, nil
		}
		c, err := verify(ctx, raw)
		if err != nil {
			return nil, authn.Errorf("oidc: %v", err)
		}
		return Principal{
			Kind:    "user",
			Subject: c.subject,
			Raw:     "user/" + c.subject,
			Groups:  c.groups,
			Claims:  c.extra,
		}, nil
	}
}

// bearerToken extracts the token from an `Authorization: Bearer <token>`
// header. ok is false when the header is absent, not a Bearer credential, or
// carries an empty token.
func bearerToken(h http.Header) (string, bool) {
	const prefix = "Bearer "
	v := h.Get("Authorization")
	if len(v) <= len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return "", false
	}
	tok := strings.TrimSpace(v[len(prefix):])
	if tok == "" {
		return "", false
	}
	return tok, true
}

// newOIDCAuthFunc discovers the provider at cfg.Issuer (network I/O, once at
// startup), builds a JWT verifier, and returns the composed authn.AuthFunc.
func newOIDCAuthFunc(ctx context.Context, cfg OIDCConfig) (authn.AuthFunc, error) {
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("oidc: discover issuer %q: %w", cfg.Issuer, err)
	}
	oc := &oidc.Config{ClientID: cfg.Audience}
	if cfg.Audience == "" {
		oc.SkipClientIDCheck = true
	}
	verifier := provider.Verifier(oc)

	groupsClaim := cfg.GroupsClaim
	if groupsClaim == "" {
		groupsClaim = "groups"
	}
	verify := func(ctx context.Context, raw string) (verifiedClaims, error) {
		tok, err := verifier.Verify(ctx, raw)
		if err != nil {
			return verifiedClaims{}, err
		}
		var all map[string]any
		if err := tok.Claims(&all); err != nil {
			return verifiedClaims{}, err
		}
		return verifiedClaims{
			subject: tok.Subject,
			groups:  stringsFromClaim(all[groupsClaim]),
			extra:   pickClaims(all, cfg.ClaimKeys),
		}, nil
	}
	return oidcAuthFunc(verify), nil
}

// stringsFromClaim coerces a JSON claim value into a string slice, accepting a
// JSON array of strings (the on-the-wire shape), a []string, or a single
// string. Anything else → nil.
func stringsFromClaim(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	default:
		return nil
	}
}

// pickClaims copies the requested string claims into a flat map for audit.
// Non-string and absent claims are skipped; an empty result returns nil.
func pickClaims(all map[string]any, keys []string) map[string]string {
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		if s, ok := all[k].(string); ok {
			out[k] = s
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
