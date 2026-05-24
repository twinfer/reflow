package auth

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"reflect"
	"strings"
	"testing"

	"github.com/twinfer/reflow/internal/pki"
)

// identity_golden_test.go snapshots the post-PR-1 mesh identity
// pipeline end-to-end: a leaf issued by the production internal/pki.CA
// encodes the principal Raw form in CN, and extractMesh parses it
// into Principal{Kind, Subject, Raw, MeshCAFingerprint}.
//
// PR 1 (CN + SPKI-fingerprint identity) replaced the prior SPIFFE
// URI shape; PR 0's earlier assertions were rewritten in lockstep.

func goldenIssueLeaf(t *testing.T, ca *pki.CA, kind pki.LeafKind, name string) (*x509.Certificate, *x509.Certificate) {
	t.Helper()
	mat, err := ca.Issue(pki.LeafOptions{
		Kind:  kind,
		Name:  name,
		Hosts: []string{"127.0.0.1"},
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
	return leaf, ca.Cert
}

func TestGolden_Identity_Node(t *testing.T) {
	ca, err := pki.NewCA("golden-ca")
	if err != nil {
		t.Fatalf("NewCA: %v", err)
	}
	leaf, caCert := goldenIssueLeaf(t, ca, pki.LeafNode, "1")

	if got, want := leaf.Subject.CommonName, "node/1"; got != want {
		t.Errorf("leaf CN = %q; want %q", got, want)
	}
	if got, want := len(leaf.URIs), 0; got != want {
		t.Errorf("leaf URIs = %d; want %d (post-PR-1 leaves carry no URI SANs)", got, want)
	}

	state := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf, caCert}}}
	got, err := extractMesh(state)
	if err != nil {
		t.Fatalf("extractMesh: %v", err)
	}
	if got.Kind != "node" || got.Subject != "1" || got.Raw != "node/1" {
		t.Errorf("identity mismatch: got %+v", got)
	}
	if !strings.HasPrefix(got.MeshCAFingerprint, "sha256:") {
		t.Errorf("MeshCAFingerprint = %q; want sha256:<hex> shape", got.MeshCAFingerprint)
	}
}

func TestGolden_Identity_Operator(t *testing.T) {
	ca, err := pki.NewCA("golden-ca")
	if err != nil {
		t.Fatalf("NewCA: %v", err)
	}
	leaf, caCert := goldenIssueLeaf(t, ca, pki.LeafOperator, "alice")

	if got, want := leaf.Subject.CommonName, "operator/alice"; got != want {
		t.Errorf("leaf CN = %q; want %q", got, want)
	}

	state := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf, caCert}}}
	got, err := extractMesh(state)
	if err != nil {
		t.Fatalf("extractMesh: %v", err)
	}
	want := Principal{
		Kind:              "operator",
		Subject:           "alice",
		Raw:               "operator/alice",
		MeshCAFingerprint: got.MeshCAFingerprint, // value-dependent on key bytes
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Principal mismatch:\n got %+v\nwant %+v", got, want)
	}
}

// TestGolden_Identity_EmptyCN snapshots the "TLS but no mesh CN"
// fall-through: empty CN yields an anonymous Principal, not an error.
func TestGolden_Identity_EmptyCN(t *testing.T) {
	leaf := &x509.Certificate{} // empty CN
	state := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	got, err := extractMesh(state)
	if err != nil {
		t.Fatalf("extractMesh: %v", err)
	}
	if !got.IsAnonymous() {
		t.Errorf("expected anonymous; got %+v", got)
	}
}
