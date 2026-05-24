package auth

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/url"
	"reflect"
	"testing"

	"github.com/twinfer/reflow/internal/pki"
)

// identity_golden_test.go snapshots the current SPIFFE-shaped identity
// pipeline end-to-end: a leaf issued by the production internal/pki.CA
// carries a spiffe://<td>/<kind>/<name> URI SAN, and extractSPIFFE parses
// it into the canonical Principal{Kind, Subject, URI, Raw}.
//
// PR 1 (CN + SPKI-fingerprint identity) will:
//   - drop URI SAN from CA.Issue; the kind/name encoding moves to the CN
//   - rewrite extractSPIFFE → extractMesh: principal sourced from CN,
//     trust anchor verified against cfg.Auth.MeshCAFingerprint
//   - delete Principal.URI; add Principal.MeshCAFingerprint
//   - delete cfg.Auth.TrustDomain
//
// When PR 1 lands, these tests' LeafOptions and assertions both change
// in lockstep with the implementation. The structural shape — issue a
// leaf via the production CA, parse a principal from it — survives.

const goldenTrustDomain = "reflow.local"

func goldenIssueLeaf(t *testing.T, ca *pki.CA, kind pki.LeafKind, role, name string) *x509.Certificate {
	t.Helper()
	uri, err := pki.BuildSPIFFEID(goldenTrustDomain, role, name)
	if err != nil {
		t.Fatalf("BuildSPIFFEID: %v", err)
	}
	mat, err := ca.Issue(pki.LeafOptions{
		Kind:  kind,
		Name:  name,
		Hosts: []string{"127.0.0.1"},
		URIs:  []*url.URL{uri},
	})
	if err != nil {
		t.Fatalf("CA.Issue: %v", err)
	}
	block, _ := pem.Decode(mat.CertPEM)
	if block == nil {
		t.Fatalf("decode leaf PEM: nil block")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf
}

func TestGolden_Identity_Node(t *testing.T) {
	ca, err := pki.NewCA("golden-ca")
	if err != nil {
		t.Fatalf("NewCA: %v", err)
	}
	leaf := goldenIssueLeaf(t, ca, pki.LeafNode, "node", "1")

	if got, want := leaf.Subject.CommonName, "1"; got != want {
		t.Errorf("leaf CN = %q; want %q (PR 1 changes this to %q)", got, want, "node/1")
	}
	if len(leaf.URIs) != 1 {
		t.Fatalf("leaf URIs = %d; want 1 (PR 1 removes URI SANs entirely)", len(leaf.URIs))
	}
	if got, want := leaf.URIs[0].String(), "spiffe://reflow.local/node/1"; got != want {
		t.Errorf("leaf URI = %q; want %q", got, want)
	}

	state := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	got, err := extractSPIFFE(goldenTrustDomain, state)
	if err != nil {
		t.Fatalf("extractSPIFFE: %v", err)
	}
	want := Principal{
		Kind:    "node",
		Subject: "1",
		URI:     "spiffe://reflow.local/node/1",
		Raw:     "node/1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Principal mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestGolden_Identity_Operator(t *testing.T) {
	ca, err := pki.NewCA("golden-ca")
	if err != nil {
		t.Fatalf("NewCA: %v", err)
	}
	leaf := goldenIssueLeaf(t, ca, pki.LeafOperator, "operator", "alice")

	if got, want := leaf.Subject.CommonName, "alice"; got != want {
		t.Errorf("leaf CN = %q; want %q (PR 1 changes this to %q)", got, want, "operator/alice")
	}
	if got, want := leaf.URIs[0].String(), "spiffe://reflow.local/operator/alice"; got != want {
		t.Errorf("leaf URI = %q; want %q", got, want)
	}

	state := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	got, err := extractSPIFFE(goldenTrustDomain, state)
	if err != nil {
		t.Fatalf("extractSPIFFE: %v", err)
	}
	want := Principal{
		Kind:    "operator",
		Subject: "alice",
		URI:     "spiffe://reflow.local/operator/alice",
		Raw:     "operator/alice",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Principal mismatch:\n got %+v\nwant %+v", got, want)
	}
}

// TestGolden_Identity_NoURI snapshots the "TLS but no SPIFFE" fall-through.
// PR 1 will replace this with "TLS but no recognizable CN" — same fall-through
// semantics, different field on the leaf.
func TestGolden_Identity_NoURI(t *testing.T) {
	leaf := &x509.Certificate{} // no URIs, no CN
	state := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	got, err := extractSPIFFE(goldenTrustDomain, state)
	if err != nil {
		t.Fatalf("extractSPIFFE: %v", err)
	}
	if !got.IsAnonymous() {
		t.Errorf("expected anonymous; got %+v", got)
	}
}
