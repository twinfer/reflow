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

// golden_test.go snapshots the current SPIFFE-shaped Signer/Verifier
// wire format in one place, so PR 1 (CN + SPKI-fingerprint identity)
// produces a single readable diff against the previous identity model.
//
// PR 1 will:
//   - drop ExtractSPIFFEURI; iss is sourced from a CN-derived "kind/subject"
//   - delete trustDomain plumbing on Signer + Verifier
//   - rename pkg/handler.Config.AllowedSPIFFE → AllowedPrincipals,
//     values shaped "node/1" not "spiffe://reflow.local/node/1"
//
// The structural shape (mint a JWT carrying the leaf in x5c, verify chain
// + identity + audience round-trip) survives the rewrite.

// TestGolden_SignerVerifier_RoundTrip is the canonical happy-path snapshot.
// It exercises every load-bearing piece: leaf signing via the CA, x5c chain
// embedding, iss extraction, allowlist lookup, audience check.
func TestGolden_SignerVerifier_RoundTrip(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "reflow.local", "/node/1")

	s := newSignerFromCert(cert, "reflow.local")
	v, err := NewVerifier(ca.caPEM, []string{"spiffe://reflow.local/node/1"}, "reflow.local", "dep-golden")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	tok := mustSign(t, s, "dep-golden")
	got, err := v.Verify(tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.CallerURI != "spiffe://reflow.local/node/1" {
		t.Errorf("CallerURI = %q; want %q (PR 1 rewrites this to %q)",
			got.CallerURI, "spiffe://reflow.local/node/1", "node/1")
	}
	if got.Audience != "dep-golden" {
		t.Errorf("Audience = %q; want %q", got.Audience, "dep-golden")
	}
}

// TestGolden_TokenShape locks in the wire-format shape of a minted token:
// the iss claim is the SPIFFE URI from the leaf, x5c carries the DER chain.
// PR 1 should leave the token structure (header + claims + signature)
// unchanged but swap iss to the principal Raw form.
func TestGolden_TokenShape(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "reflow.local", "/operator/alice")

	s := newSignerFromCert(cert, "reflow.local")
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
	if iss, _ := claims["iss"].(string); iss != "spiffe://reflow.local/operator/alice" {
		t.Errorf("claims.iss = %q; want %q (PR 1 changes this to %q)",
			iss, "spiffe://reflow.local/operator/alice", "operator/alice")
	}
}

// TestGolden_LeafURIShape isolates the leaf-cert wire format. The current
// model puts the kind+name in a URI SAN; PR 1 moves it to the CN.
func TestGolden_LeafURIShape(t *testing.T) {
	ca := makeCA(t)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeSignedLeaf(t, ca, leafKey, &leafKey.PublicKey, "reflow.local", "/node/1")

	leaf := cert.Leaf
	if leaf == nil {
		t.Fatal("Leaf is nil — makeSignedLeaf should pre-parse")
	}
	if got, want := len(leaf.URIs), 1; got != want {
		t.Fatalf("leaf URIs = %d; want %d (PR 1 drops URI SAN entirely)", got, want)
	}

	uri := leaf.URIs[0]
	if uri.Scheme != "spiffe" {
		t.Errorf("URI scheme = %q; want spiffe", uri.Scheme)
	}
	if uri.Host != "reflow.local" {
		t.Errorf("URI host = %q; want reflow.local", uri.Host)
	}
	if uri.Path != "/node/1" {
		t.Errorf("URI path = %q; want /node/1", uri.Path)
	}

	extracted, err := ExtractSPIFFEURI(leaf, "reflow.local")
	if err != nil {
		t.Fatalf("ExtractSPIFFEURI: %v", err)
	}
	if extracted != "spiffe://reflow.local/node/1" {
		t.Errorf("ExtractSPIFFEURI = %q; want %q", extracted, "spiffe://reflow.local/node/1")
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
