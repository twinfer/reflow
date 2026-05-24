package creds

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// golden_test.go snapshots the post-PR-1 mesh-identity wire format:
// leaf CN encodes the principal Raw form, iss claim equals the Raw
// form, no URI SANs, no trust domain anywhere.

// TestGolden_SignerVerifier_RoundTrip is the canonical happy-path
// snapshot. It exercises every load-bearing piece: leaf signing via
// the CA, x5c chain embedding, principal extraction from CN,
// allowlist lookup, audience check.
func TestGolden_SignerVerifier_RoundTrip(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")

	s := newSignerFromCert(cert)
	v, err := NewVerifier(ca.caPEM, []string{"node/1"}, "dep-golden")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	tok := mustSign(t, s, "dep-golden")
	got, err := v.Verify(tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.CallerPrincipal != "node/1" {
		t.Errorf("CallerPrincipal = %q; want %q", got.CallerPrincipal, "node/1")
	}
	if got.Audience != "dep-golden" {
		t.Errorf("Audience = %q; want %q", got.Audience, "dep-golden")
	}
	if !strings.HasPrefix(got.MeshCAFingerprint, "sha256:") {
		t.Errorf("MeshCAFingerprint = %q; want sha256:<hex> shape", got.MeshCAFingerprint)
	}
}

// TestGolden_TokenShape locks in the wire-format shape of a minted
// token: iss == principal Raw form, x5c carries the DER chain.
func TestGolden_TokenShape(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "operator/alice")

	s := newSignerFromCert(cert)
	tok := mustSign(t, s, "dep-shape")

	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts; want 3 (header.claims.sig)", len(parts))
	}

	header, err := decodeJWTSegment(parts[0])
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if alg, _ := header["alg"].(string); alg != "ES256" {
		t.Errorf("header.alg = %q; want ES256 (P-256 key)", alg)
	}
	if _, ok := header["x5c"]; !ok {
		t.Error("header missing x5c (leaf chain)")
	}

	claims, err := decodeJWTSegment(parts[1])
	if err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	if iss, _ := claims["iss"].(string); iss != "operator/alice" {
		t.Errorf("claims.iss = %q; want %q", iss, "operator/alice")
	}
}

// TestGolden_LeafIdentityShape isolates the leaf-cert wire format.
// The post-PR-1 model puts the principal Raw form in the CN; URI SANs
// are not used.
func TestGolden_LeafIdentityShape(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "node/1")

	leaf := cert.Leaf
	if leaf == nil {
		t.Fatal("Leaf is nil — makeSignedLeaf should pre-parse")
	}
	if got := leaf.Subject.CommonName; got != "node/1" {
		t.Errorf("leaf CN = %q; want %q", got, "node/1")
	}
	if got, want := len(leaf.URIs), 0; got != want {
		t.Errorf("leaf URIs = %d; want %d (post-PR-1 mesh leaves carry no URI SANs)", got, want)
	}

	extracted, err := LeafPrincipal(leaf)
	if err != nil {
		t.Fatalf("LeafPrincipal: %v", err)
	}
	if extracted != "node/1" {
		t.Errorf("LeafPrincipal = %q; want %q", extracted, "node/1")
	}
}

// TestGolden_SPKIFingerprint locks in the mesh-CA fingerprint format
// used by the audit field MeshCAFingerprint on Principal + Verified.
func TestGolden_SPKIFingerprint(t *testing.T) {
	ca := makeCA(t)
	fp := SPKIFingerprint(ca.caCert)
	if !strings.HasPrefix(fp, "sha256:") {
		t.Errorf("fingerprint = %q; want sha256:<hex> prefix", fp)
	}
	if got, want := len(fp), len("sha256:")+64; got != want {
		t.Errorf("fingerprint length = %d; want %d", got, want)
	}
	// Stable across calls.
	if SPKIFingerprint(ca.caCert) != fp {
		t.Error("SPKIFingerprint should be deterministic")
	}
}

func decodeJWTSegment(seg string) (map[string]any, error) {
	raw, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}
