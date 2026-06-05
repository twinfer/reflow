package creds

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/credentials/tls/certprovider"
)

// DefaultSignerTTL is the exp window applied to engine→handler JWTs.
// Short enough to bound replay; long enough to tolerate typical NTP
// skew without operator pain.
const DefaultSignerTTL = 60 * time.Second

// defaultRenewMargin is the slack between a cached token's exp and the
// time we'll still serve it. Keeps the in-flight handler request
// comfortably within the JWT's validity window (the verifier runs with
// 0s leeway today).
const defaultRenewMargin = 10 * time.Second

// Signer mints short-lived JWTs signed by the engine's node-identity
// keypair — the same cert/key the cluster mTLS surfaces use, fetched
// via a certprovider.Provider so rotations are picked up on the next
// Sign call. The leaf cert travels in the x5c header so handlers
// verify the chain against the configured CA and read the principal
// Raw form from the leaf's CN — no pre-shared public key required.
//
// Signed tokens are cached per audience and reused until they approach
// expiry — engine→handler dispatch is the hottest call site, and
// per-call KeyMaterial fetch + ECDSA/Ed25519 sign was visible in
// profiles. Cache entries from a now-rotated cert are evicted on the
// next miss when the new cert's fingerprint differs.
type Signer struct {
	provider    certprovider.Provider
	ttl         time.Duration
	renewMargin time.Duration

	mu    sync.Mutex
	cache map[string]cachedToken // keyed by audience
}

// cachedToken is the per-audience cache entry. fingerprint identifies
// which leaf cert minted this token so that a cache miss on a fresh
// fingerprint can drop sibling entries from the previous cert.
type cachedToken struct {
	token       string
	exp         time.Time
	fingerprint string
}

// NewSigner constructs a Signer bound to a certprovider.Provider for
// pull-based access to the current leaf cert + private key.
func NewSigner(p certprovider.Provider) *Signer {
	return &Signer{
		provider:    p,
		ttl:         DefaultSignerTTL,
		renewMargin: defaultRenewMargin,
		cache:       make(map[string]cachedToken),
	}
}

// Close releases the underlying provider's resources. Safe to call once.
// Nil-receiver and nil-provider are no-ops so callers can defer
// unconditionally.
func (s *Signer) Close() {
	if s == nil || s.provider == nil {
		return
	}
	s.provider.Close()
}

// Sign mints (or returns a cached) JWT with iss=engine's principal Raw form,
// aud=audience, iat=now, exp=now+ttl, and the leaf chain in the x5c
// header. audience is typically the deployment_id pinned at
// handlerclient construction.
//
// Cache hit: returns the previously minted token when its exp is more
// than renewMargin from now. Cache miss: fetches KeyMaterial, mints a
// fresh token, and stores it. If the fresh leaf's fingerprint differs
// from the cached entry's, sibling entries from the prior cert are
// dropped.
func (s *Signer) Sign(audience string) (string, error) {
	if s == nil {
		return "", errors.New("reflw/creds: nil Signer")
	}
	if audience == "" {
		return "", errors.New("reflw/creds: Sign requires audience")
	}

	s.mu.Lock()
	entry, ok := s.cache[audience]
	s.mu.Unlock()
	if ok && time.Now().Before(entry.exp.Add(-s.renewMargin)) {
		return entry.token, nil
	}
	return s.signFresh(audience)
}

// signFresh fetches the current key material, mints a token, and stores
// it in the cache. Multiple racing misses on the same audience just
// last-writer-win — the duplicated mint is bounded by goroutine count,
// not request rate, and every minted token is valid.
func (s *Signer) signFresh(audience string) (string, error) {
	km, err := s.provider.KeyMaterial(context.Background())
	if err != nil {
		return "", fmt.Errorf("reflw/creds: signer KeyMaterial: %w", err)
	}
	if len(km.Certs) == 0 {
		return "", errors.New("reflw/creds: signer: provider returned no certs")
	}
	cert := km.Certs[0]
	if len(cert.Certificate) == 0 {
		return "", errors.New("reflw/creds: signer: leaf cert has no DER bytes")
	}
	leaf := cert.Leaf
	if leaf == nil {
		l, perr := x509.ParseCertificate(cert.Certificate[0])
		if perr != nil {
			return "", fmt.Errorf("reflw/creds: signer: parse leaf: %w", perr)
		}
		leaf = l
	}
	iss, err := LeafPrincipal(leaf)
	if err != nil {
		return "", fmt.Errorf("reflw/creds: signer: extract iss: %w", err)
	}
	method, err := signingMethodFor(cert.PrivateKey)
	if err != nil {
		return "", err
	}
	now := time.Now()
	exp := now.Add(s.ttl)
	claims := jwt.RegisteredClaims{
		Issuer:    iss,
		Audience:  jwt.ClaimStrings{audience},
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(exp),
	}
	tok := jwt.NewWithClaims(method, claims)
	tok.Header["x5c"] = encodeX5C(cert.Certificate)
	signed, err := tok.SignedString(cert.PrivateKey)
	if err != nil {
		return "", err
	}

	fp := leafFingerprint(cert.Certificate[0])
	s.mu.Lock()
	// Evict entries from a rotated cert so the cache reflects the
	// currently-issued key only. Same-fingerprint entries stay (they
	// would just be re-minted on next miss otherwise).
	for k, v := range s.cache {
		if v.fingerprint != fp {
			delete(s.cache, k)
		}
	}
	s.cache[audience] = cachedToken{token: signed, exp: exp, fingerprint: fp}
	s.mu.Unlock()

	return signed, nil
}

// leafFingerprint returns a stable hex fingerprint of the leaf cert's
// DER bytes. Used to detect cert rotation between cache misses.
func leafFingerprint(der []byte) string {
	h := sha256.Sum256(der)
	return hex.EncodeToString(h[:])
}

func signingMethodFor(k any) (jwt.SigningMethod, error) {
	switch key := k.(type) {
	case ed25519.PrivateKey:
		return jwt.SigningMethodEdDSA, nil
	case *ecdsa.PrivateKey:
		switch key.Curve {
		case elliptic.P256():
			return jwt.SigningMethodES256, nil
		case elliptic.P384():
			return jwt.SigningMethodES384, nil
		case elliptic.P521():
			return jwt.SigningMethodES512, nil
		default:
			return nil, fmt.Errorf("reflw/creds: signer: unsupported ECDSA curve %s", key.Curve.Params().Name)
		}
	case *rsa.PrivateKey:
		return jwt.SigningMethodRS256, nil
	default:
		return nil, fmt.Errorf("reflw/creds: signer: unsupported private-key type %T", k)
	}
}

// encodeX5C base64-STD-encodes each DER cert in the chain. RFC 7515
// §4.1.6 specifies standard (not URL-safe) base64 for x5c.
func encodeX5C(chain [][]byte) []string {
	out := make([]string, 0, len(chain))
	for _, c := range chain {
		out = append(out, base64.StdEncoding.EncodeToString(c))
	}
	return out
}
