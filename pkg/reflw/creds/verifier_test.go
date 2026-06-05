package creds

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/credentials/tls/certprovider"
)

// caBundle is the helper output for tests: a self-signed CA + a leaf
// signed by it. PEM is the CA cert as a PEM bundle the verifier accepts.
type caBundle struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caPEM  []byte
}

// makeCA builds a self-signed CA cert using ECDSA-P256. The CA is
// reusable across tests within a single t.Run; its key is held in memory.
func makeCA(t *testing.T) *caBundle {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate(CA): %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate(CA): %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &caBundle{caCert: cert, caKey: key, caPEM: pemBytes}
}

// makeSignedLeaf builds a leaf cert signed by ca with CN=principalRaw
// (the "<kind>/<name>" form the auth layer reads at peer-verify time).
// Returns a tls.Certificate consumable by fakeProvider.
func makeSignedLeaf(t *testing.T, ca *caBundle, leafKey crypto.Signer, leafPub any, principalRaw string) tls.Certificate {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: principalRaw},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.caCert, leafPub, ca.caKey)
	if err != nil {
		t.Fatalf("CreateCertificate(leaf): %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate(leaf): %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: leafKey, Leaf: leaf}
}

func newSignerFromCert(cert tls.Certificate) *Signer {
	return NewSigner(&fakeProvider{cert: cert})
}

func mustSign(t *testing.T, s *Signer, audience string) string {
	t.Helper()
	tok, err := s.Sign(audience)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return tok
}

func TestVerifier_RoundTripECDSA(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	s := newSignerFromCert(cert)
	v, err := NewVerifier(ca.caPEM, []string{"node/1"}, "dep-1")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	tok := mustSign(t, s, "dep-1")
	got, err := v.Verify(tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.CallerPrincipal != "node/1" {
		t.Errorf("CallerPrincipal = %q", got.CallerPrincipal)
	}
	if got.Audience != "dep-1" {
		t.Errorf("Audience = %q", got.Audience)
	}
	if got.MeshCAFingerprint == "" {
		t.Errorf("MeshCAFingerprint empty; want sha256:<hex>")
	}
}

func TestVerifier_RoundTripEd25519(t *testing.T) {
	ca := makeCA(t)
	leafPub, leafPriv, _ := ed25519.GenerateKey(rand.Reader)
	cert := makeSignedLeaf(t, ca, leafPriv, leafPub, "node/2")
	s := newSignerFromCert(cert)
	v, _ := NewVerifier(ca.caPEM, []string{"node/2"}, "")
	tok := mustSign(t, s, "dep-x")
	if _, err := v.Verify(tok); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifier_RoundTripRSA(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/3")
	s := newSignerFromCert(cert)
	v, _ := NewVerifier(ca.caPEM, []string{"node/3"}, "")
	tok := mustSign(t, s, "dep-r")
	if _, err := v.Verify(tok); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifier_AudOptional(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	s := newSignerFromCert(cert)
	v, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "")
	tok := mustSign(t, s, "any-audience-here")
	if _, err := v.Verify(tok); err != nil {
		t.Fatalf("Verify with empty expected aud should accept any aud: %v", err)
	}
}

func TestVerifier_AudMismatch(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	s := newSignerFromCert(cert)
	v, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "expected-aud")
	tok := mustSign(t, s, "different-aud")
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("expected aud mismatch error; got nil")
	}
}

func TestVerifier_NotInAllowlist(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/9")
	s := newSignerFromCert(cert)
	// allowlist names a different node
	v, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "")
	tok := mustSign(t, s, "dep")
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("expected allowlist rejection; got nil")
	}
}

func TestVerifier_ChainNotAnchored(t *testing.T) {
	ca := makeCA(t)
	otherCA := makeCA(t) // verifier trusts a different CA
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	s := newSignerFromCert(cert)
	v, _ := NewVerifier(otherCA.caPEM, []string{"node/1"}, "")
	tok := mustSign(t, s, "dep")
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("expected chain-not-anchored rejection; got nil")
	}
}

// TestVerifier_LeafCNMalformed replaces the old "trust domain mismatch"
// case: the new identity model has no trust domain, but a leaf whose CN
// fails the <kind>/<name> shape must still be rejected so a CA with a
// human-readable CN can't accidentally satisfy mesh-leaf checks.
func TestVerifier_LeafCNMalformed(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	// CN is not "<kind>/<name>" — single segment.
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "single-segment")
	s := newSignerFromCert(cert)
	v, _ := NewVerifier(ca.caPEM, []string{"single-segment"}, "")
	tok, err := s.Sign("dep")
	if err == nil {
		// Signer should have rejected the malformed CN during Sign.
		if _, vErr := v.Verify(tok); vErr == nil {
			t.Fatal("expected leaf-CN-malformed rejection somewhere in the round trip; got nil")
		}
	}
}

func TestVerifier_TamperedSignature(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	s := newSignerFromCert(cert)
	v, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "")
	tok := mustSign(t, s, "dep")
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token shape: %d parts", len(parts))
	}
	// flip the last byte of the signature
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode sig: %v", err)
	}
	sig[len(sig)-1] ^= 0xff
	parts[2] = base64.RawURLEncoding.EncodeToString(sig)
	tampered := strings.Join(parts, ".")
	if _, err := v.Verify(tampered); err == nil {
		t.Fatal("expected signature-tamper rejection; got nil")
	}
}

func TestVerifier_Expired(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	// Set a negative TTL so the minted token's exp is in the past.
	s := NewSigner(&fakeProvider{cert: cert})
	s.ttl = -time.Minute
	v, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "")
	tok, err := s.Sign("dep")
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("expected exp-in-past rejection; got nil")
	}
}

func TestVerifier_RejectAlgNone(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	v, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "")

	// Hand-craft a token with alg=none and x5c stamped.
	header := map[string]any{
		"alg": "none",
		"typ": "JWT",
		"x5c": []string{base64.StdEncoding.EncodeToString(cert.Certificate[0])},
	}
	claims := map[string]any{
		"iss": "node/1",
		"aud": []string{"dep"},
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Minute).Unix(),
	}
	hdrJSON, _ := json.Marshal(header)
	clmJSON, _ := json.Marshal(claims)
	tok := base64.RawURLEncoding.EncodeToString(hdrJSON) + "." +
		base64.RawURLEncoding.EncodeToString(clmJSON) + "."
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("expected alg=none rejection; got nil")
	}
}

func TestVerifier_RejectAlgHS256(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	v, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "")

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Issuer:    "node/1",
		Audience:  jwt.ClaimStrings{"dep"},
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	})
	tok.Header["x5c"] = []string{base64.StdEncoding.EncodeToString(cert.Certificate[0])}
	signed, err := tok.SignedString([]byte("shared-secret"))
	if err != nil {
		t.Fatalf("sign HS256: %v", err)
	}
	if _, err := v.Verify(signed); err == nil {
		t.Fatal("expected HS256 rejection; got nil")
	}
}

func TestVerifier_IssLeafMismatch(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	v, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "")

	// Sign a token where iss claims to be a different principal than
	// the leaf CN encodes.
	method, _ := signingMethodFor(leafKey)
	tok := jwt.NewWithClaims(method, jwt.RegisteredClaims{
		Issuer:    "node/99",
		Audience:  jwt.ClaimStrings{"dep"},
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	})
	tok.Header["x5c"] = []string{base64.StdEncoding.EncodeToString(cert.Certificate[0])}
	signed, err := tok.SignedString(leafKey)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := v.Verify(signed); err == nil {
		t.Fatal("expected iss/leaf-principal mismatch rejection; got nil")
	}
}

func TestVerifier_EmptyBearer(t *testing.T) {
	v, _ := NewVerifier([]byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"), []string{"x"}, "")
	if v != nil {
		t.Fatal("expected nil verifier for invalid PEM bundle")
	}
	// Real verifier:
	ca := makeCA(t)
	v2, _ := NewVerifier(ca.caPEM, []string{"node/1"}, "")
	if _, err := v2.Verify(""); err == nil {
		t.Fatal("expected empty-bearer rejection; got nil")
	}
}

func TestNewVerifier_RequiresAllowlist(t *testing.T) {
	ca := makeCA(t)
	if _, err := NewVerifier(ca.caPEM, nil, ""); err == nil {
		t.Fatal("expected empty-allowlist rejection; got nil")
	}
}

func TestNewVerifier_RejectsBadPEM(t *testing.T) {
	if _, err := NewVerifier([]byte("not a pem block"), []string{"x"}, ""); err == nil {
		t.Fatal("expected bad-PEM rejection; got nil")
	}
}

// Sanity check: the helper round-trip the test corpus uses still works
// against a fresh provider built from the chain helpers.
func TestVerifier_HelperSanity(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")
	prov := &fakeProvider{cert: cert}
	km, err := prov.KeyMaterial(context.Background())
	if err != nil {
		t.Fatalf("KeyMaterial: %v", err)
	}
	if len(km.Certs) != 1 {
		t.Fatalf("Certs len = %d; want 1", len(km.Certs))
	}
	if km.Certs[0].Leaf == nil {
		t.Fatal("Leaf nil — makeSignedLeaf should pre-parse")
	}
	// Build the same Verifier the production path would assemble.
	if _, err := NewVerifier(ca.caPEM, []string{"node/1"}, ""); err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	// Confirm certprovider.KeyMaterial typing doesn't drift.
	_ = certprovider.KeyMaterial{}
}
