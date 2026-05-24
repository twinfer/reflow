package creds

import (
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"

	"github.com/golang-jwt/jwt/v5"
)

// allowedAlgs enumerates the JWT signing algorithms the verifier
// accepts; mirrors what Signer.signingMethodFor emits. "none" and the
// HS* family are deliberately absent — symmetric keys have no place on
// the engine→handler hop.
var allowedAlgs = []string{
	jwt.SigningMethodEdDSA.Alg(),
	jwt.SigningMethodES256.Alg(),
	jwt.SigningMethodES384.Alg(),
	jwt.SigningMethodES512.Alg(),
	jwt.SigningMethodRS256.Alg(),
}

// Verifier validates engine→handler JWTs minted by Signer. Roots are
// the operator-supplied trust bundle; allowedPrincipals is the
// exact-match allowlist of caller principal Raw strings (e.g.
// "node/1", "operator/alice"). audience, when non-empty, additionally
// requires the token's aud claim to match.
type Verifier struct {
	pool              *x509.CertPool
	allowedPrincipals map[string]struct{}
	audience          string
}

// Verified is the success payload from Verifier.Verify: the caller's
// principal Raw form (extracted from the leaf cert CN and bound to
// the iss claim), the audience claim from the token, and the SPKI
// fingerprint of the chain's root CA for audit.
type Verified struct {
	CallerPrincipal   string
	Audience          string
	MeshCAFingerprint string
}

// NewVerifier builds a Verifier from a PEM bundle of trusted roots and
// the principal allowlist (Raw strings, "kind/name"). audience may be
// empty to skip the aud check.
func NewVerifier(rootsPEM []byte, allowedPrincipals []string, audience string) (*Verifier, error) {
	if len(rootsPEM) == 0 {
		return nil, errors.New("reflow/creds: verifier requires non-empty rootsPEM")
	}
	if len(allowedPrincipals) == 0 {
		return nil, errors.New("reflow/creds: verifier requires at least one allowed principal")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rootsPEM) {
		return nil, errors.New("reflow/creds: verifier: rootsPEM contains no parseable PEM blocks")
	}
	set := make(map[string]struct{}, len(allowedPrincipals))
	for _, p := range allowedPrincipals {
		set[p] = struct{}{}
	}
	return &Verifier{
		pool:              pool,
		allowedPrincipals: set,
		audience:          audience,
	}, nil
}

// Verify parses bearer as a JWT, decodes its x5c header, verifies the
// chain against the configured roots, reads the leaf CN as the
// caller's principal Raw form, checks it against the allowlist, binds
// iss to the leaf principal, and lets jwt validate exp/iat. On
// success returns the caller's principal and the aud claim plus the
// chain root's SPKI fingerprint for audit.
func (v *Verifier) Verify(bearer string) (*Verified, error) {
	if v == nil {
		return nil, errors.New("reflow/creds: nil Verifier")
	}
	if bearer == "" {
		return nil, errors.New("reflow/creds: empty bearer token")
	}
	claims := &jwt.RegisteredClaims{}
	var callerPrincipal, caFingerprint string
	parsed, err := jwt.ParseWithClaims(bearer, claims, func(tok *jwt.Token) (any, error) {
		raw, ok := tok.Header["x5c"].([]any)
		if !ok || len(raw) == 0 {
			return nil, errors.New("missing x5c header")
		}
		chain := make([]*x509.Certificate, 0, len(raw))
		for i, entry := range raw {
			s, ok := entry.(string)
			if !ok {
				return nil, fmt.Errorf("x5c[%d] not a string", i)
			}
			der, derr := base64.StdEncoding.DecodeString(s)
			if derr != nil {
				return nil, fmt.Errorf("x5c[%d] base64: %w", i, derr)
			}
			c, perr := x509.ParseCertificate(der)
			if perr != nil {
				return nil, fmt.Errorf("x5c[%d] parse: %w", i, perr)
			}
			chain = append(chain, c)
		}
		leaf := chain[0]
		intermediates := x509.NewCertPool()
		for _, c := range chain[1:] {
			intermediates.AddCert(c)
		}
		verifiedChains, verr := leaf.Verify(x509.VerifyOptions{
			Roots:         v.pool,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		})
		if verr != nil {
			return nil, fmt.Errorf("chain verify: %w", verr)
		}
		principal, perr := LeafPrincipal(leaf)
		if perr != nil {
			return nil, fmt.Errorf("leaf identity: %w", perr)
		}
		if _, ok := v.allowedPrincipals[principal]; !ok {
			return nil, fmt.Errorf("caller %q not in allowlist", principal)
		}
		if iss := claims.Issuer; iss != principal {
			return nil, fmt.Errorf("iss %q != leaf principal %q", iss, principal)
		}
		callerPrincipal = principal
		if len(verifiedChains) > 0 {
			c := verifiedChains[0]
			caFingerprint = SPKIFingerprint(c[len(c)-1])
		}
		return leaf.PublicKey, nil
	},
		jwt.WithValidMethods(allowedAlgs),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
	)
	if err != nil {
		return nil, fmt.Errorf("reflow/creds: verify: %w", err)
	}
	if !parsed.Valid {
		return nil, errors.New("reflow/creds: token invalid after parse")
	}
	if v.audience != "" && !audienceContains(claims.Audience, v.audience) {
		return nil, fmt.Errorf("reflow/creds: aud %v does not contain %q", []string(claims.Audience), v.audience)
	}
	aud := ""
	if len(claims.Audience) > 0 {
		aud = claims.Audience[0]
	}
	return &Verified{
		CallerPrincipal:   callerPrincipal,
		Audience:          aud,
		MeshCAFingerprint: caFingerprint,
	}, nil
}

func audienceContains(aud jwt.ClaimStrings, want string) bool {
	return slices.Contains(aud, want)
}
