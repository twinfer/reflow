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
// the operator-supplied trust bundle; allowedSPIFFE is the exact-match
// allowlist of caller SPIFFE URIs. audience, when non-empty, additionally
// requires the token's aud claim to match.
type Verifier struct {
	pool          *x509.CertPool
	allowedSPIFFE map[string]struct{}
	trustDomain   string
	audience      string
}

// Verified is the success payload from Verifier.Verify: the caller's
// SPIFFE URI (extracted from the leaf cert and bound to the iss claim)
// and the audience claim from the token.
type Verified struct {
	CallerURI string
	Audience  string
}

// NewVerifier builds a Verifier from a PEM bundle of trusted roots and
// the SPIFFE URI allowlist. Empty trustDomain falls back to
// DefaultTrustDomain. audience may be empty to skip the aud check.
func NewVerifier(rootsPEM []byte, allowedSPIFFE []string, trustDomain, audience string) (*Verifier, error) {
	if len(rootsPEM) == 0 {
		return nil, errors.New("reflow/creds: verifier requires non-empty rootsPEM")
	}
	if len(allowedSPIFFE) == 0 {
		return nil, errors.New("reflow/creds: verifier requires at least one allowed SPIFFE URI")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rootsPEM) {
		return nil, errors.New("reflow/creds: verifier: rootsPEM contains no parseable PEM blocks")
	}
	set := make(map[string]struct{}, len(allowedSPIFFE))
	for _, uri := range allowedSPIFFE {
		set[uri] = struct{}{}
	}
	if trustDomain == "" {
		trustDomain = DefaultTrustDomain
	}
	return &Verifier{
		pool:          pool,
		allowedSPIFFE: set,
		trustDomain:   trustDomain,
		audience:      audience,
	}, nil
}

// Verify parses bearer as a JWT, decodes its x5c header, verifies the
// chain against the configured roots, checks the leaf's SPIFFE URI
// against the allowlist, binds iss to the leaf URI, verifies the
// signature with the leaf's public key, and lets jwt validate exp/iat.
// On success returns the caller's SPIFFE URI and the aud claim.
func (v *Verifier) Verify(bearer string) (*Verified, error) {
	if v == nil {
		return nil, errors.New("reflow/creds: nil Verifier")
	}
	if bearer == "" {
		return nil, errors.New("reflow/creds: empty bearer token")
	}
	claims := &jwt.RegisteredClaims{}
	var leafURI string
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
		if _, verr := leaf.Verify(x509.VerifyOptions{
			Roots:         v.pool,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		}); verr != nil {
			return nil, fmt.Errorf("chain verify: %w", verr)
		}
		uri, uerr := ExtractSPIFFEURI(leaf, v.trustDomain)
		if uerr != nil {
			return nil, fmt.Errorf("leaf SPIFFE URI: %w", uerr)
		}
		if _, ok := v.allowedSPIFFE[uri]; !ok {
			return nil, fmt.Errorf("caller %q not in allowlist", uri)
		}
		if iss := claims.Issuer; iss != uri {
			return nil, fmt.Errorf("iss %q != leaf URI %q", iss, uri)
		}
		leafURI = uri
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
	return &Verified{CallerURI: leafURI, Audience: aud}, nil
}

func audienceContains(aud jwt.ClaimStrings, want string) bool {
	return slices.Contains(aud, want)
}
