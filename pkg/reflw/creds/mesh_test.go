package creds

import (
	"context"
	"testing"

	"github.com/twinfer/reflw/internal/certmgr"
)

// TestNodeIdentity_SelfIssuesVerifiableLeaf is the core of the
// decentralized mesh path: a node builds its identity straight from CA
// cert+key bytes (the key arrives KMS-unwrapped in production), CertMagic
// self-issues a node/<id> leaf, and the client-side mesh verifier accepts
// that leaf's chain while rejecting a leaf from a different CA.
func TestNodeIdentity_SelfIssuesVerifiableLeaf(t *testing.T) {
	ca, err := certmgr.MintCA("test-cluster-ca")
	if err != nil {
		t.Fatalf("MintCA: %v", err)
	}
	id, err := BuildNodeIdentity(context.Background(), NodeIdentityOptions{
		CACertPEM:    ca.CertPEM,
		CAKeyPEM:     ca.KeyPEM,
		NodeID:       "7",
		Principal:    "node/7",
		CertCacheDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("BuildNodeIdentity: %v", err)
	}
	defer id.Close()

	cert, err := id.mgr.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if len(cert.Certificate) == 0 || cert.Leaf == nil {
		t.Fatal("self-issued cert has no leaf")
	}
	if got, err := LeafPrincipal(cert.Leaf); err != nil || got != "node/7" {
		t.Fatalf("leaf principal = %q, err=%v; want node/7", got, err)
	}

	// The client-side mesh verifier (used with InsecureSkipVerify) must
	// accept the self-issued chain against the cluster CA pool.
	verify := verifyMeshPeer(id.caPool)
	if err := verify(cert.Certificate, nil); err != nil {
		t.Fatalf("verifyMeshPeer rejected a valid self-issued leaf: %v", err)
	}

	// A leaf from a different CA must be rejected by this pool.
	other, err := certmgr.MintCA("other-ca")
	if err != nil {
		t.Fatalf("MintCA(other): %v", err)
	}
	otherID, err := BuildNodeIdentity(context.Background(), NodeIdentityOptions{
		CACertPEM:    other.CertPEM,
		CAKeyPEM:     other.KeyPEM,
		NodeID:       "9",
		Principal:    "node/9",
		CertCacheDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("BuildNodeIdentity(other): %v", err)
	}
	defer otherID.Close()
	otherCert, err := otherID.mgr.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate(other): %v", err)
	}
	if err := verify(otherCert.Certificate, nil); err == nil {
		t.Fatal("verifyMeshPeer accepted a leaf signed by an untrusted CA")
	}
}

// TestMeshListenerCreds_Shape verifies the server config enforces mTLS
// against the cluster CA and that ListenerCreds.Close is nil (the
// NodeIdentity owns the Manager lifecycle, not the per-listener creds).
func TestMeshListenerCreds_Shape(t *testing.T) {
	ca, err := certmgr.MintCA("test-cluster-ca")
	if err != nil {
		t.Fatalf("MintCA: %v", err)
	}
	id, err := BuildNodeIdentity(context.Background(), NodeIdentityOptions{
		CACertPEM:    ca.CertPEM,
		CAKeyPEM:     ca.KeyPEM,
		NodeID:       "1",
		Principal:    "node/1",
		CertCacheDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("BuildNodeIdentity: %v", err)
	}
	defer id.Close()

	mesh := MeshListenerCreds(id, true)
	if mesh.Close != nil {
		t.Error("mesh ListenerCreds.Close must be nil (identity owns the Manager)")
	}
	if mesh.ServerTLSConfig == nil || mesh.ClientTLSConfig == nil {
		t.Fatal("mesh creds missing server or client config")
	}
	if mesh.ServerTLSConfig.ClientCAs == nil {
		t.Error("mTLS server config must set ClientCAs to the cluster CA pool")
	}
	if !mesh.ClientTLSConfig.InsecureSkipVerify {
		t.Error("mesh client config must skip hostname verification (CN-based identity)")
	}
}
