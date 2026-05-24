package certmgr

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// mintRemoteCA builds a self-signed CA cert whose private key is held
// by an in-memory crypto.Signer — emulating the "key never leaves the
// KMS" property by only ever passing the signer through to x509.
// Returns the signer (registered later as the factory output) and the
// CARootRecord pre-filled with the cert PEM.
func mintRemoteCA(t *testing.T, rowName string) (*fakeRemoteSigner, *enginev1.CARootRecord) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer := &fakeRemoteSigner{priv: priv}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "remote-ca-" + rowName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, signer.Public(), signer)
	if err != nil {
		t.Fatal(err)
	}
	rec := &enginev1.CARootRecord{
		Name:          rowName,
		CertPem:       pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeySecretName: "", // intentionally unused in remote mode
		Fingerprint:   "sha256:irrelevant-for-test",
		RotationEpoch: 1,
		CreatedAtMs:   1,
	}
	return signer, rec
}

func TestClusterIssuer_RemoteSigner_EndToEndSignsLeaf(t *testing.T) {
	const prefix = "test-kms-cluster://"
	t.Cleanup(func() { UnregisterRemoteSigner(prefix) })

	signer, rec := mintRemoteCA(t, "active")
	RegisterRemoteSigner(prefix, func(context.Context, string) (crypto.Signer, error) {
		return signer, nil
	})

	reader := &fakeReader{records: []*enginev1.CARootRecord{rec}, rev: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	issuer, err := NewClusterIssuer(ctx, ClusterOptions{
		Reader:      reader,
		SigningMode: SigningModeRemote,
		KMSKeyURI:   prefix + "ca-key-handle",
		Principal:   "node/9",
		Hosts:       []string{"localhost"},
		Validity:    5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewClusterIssuer(remote): %v", err)
	}

	m, err := New(Options{
		Dir:       filepath.Join(t.TempDir(), "cm"),
		NodeID:    "9",
		Principal: "node/9",
		Issuer:    issuer,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	if err := m.ManageLeaf(ctx); err != nil {
		t.Fatalf("ManageLeaf: %v", err)
	}
	cert, err := m.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if leaf.Subject.CommonName != "node/9" {
		t.Errorf("CN = %q; want node/9", leaf.Subject.CommonName)
	}

	caBlock, _ := pem.Decode(rec.CertPem)
	if caBlock == nil {
		t.Fatal("CARootRecord cert is not PEM")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("chain verify against KMS-rooted CA: %v", err)
	}
}

func TestClusterIssuer_RemoteSigner_RejectsCertSignerMismatch(t *testing.T) {
	const prefix = "test-kms-cluster-mismatch://"
	t.Cleanup(func() { UnregisterRemoteSigner(prefix) })

	_, rec := mintRemoteCA(t, "active")
	// Register a factory that returns a DIFFERENT signer than the one
	// the CA cert was issued against — the constructor must reject.
	other := newFakeRemoteSigner(t)
	RegisterRemoteSigner(prefix, func(context.Context, string) (crypto.Signer, error) {
		return other, nil
	})

	reader := &fakeReader{records: []*enginev1.CARootRecord{rec}, rev: 1}
	_, err := NewClusterIssuer(context.Background(), ClusterOptions{
		Reader:      reader,
		SigningMode: SigningModeRemote,
		KMSKeyURI:   prefix + "wrong-key",
		Principal:   "node/1",
	})
	if err == nil {
		t.Fatal("expected cert/remote-signer mismatch to be rejected")
	}
}

func TestClusterIssuer_RemoteSigner_RequiresKMSKeyURI(t *testing.T) {
	_, rec := mintRemoteCA(t, "active")
	reader := &fakeReader{records: []*enginev1.CARootRecord{rec}, rev: 1}
	_, err := NewClusterIssuer(context.Background(), ClusterOptions{
		Reader:      reader,
		SigningMode: SigningModeRemote,
		// KMSKeyURI omitted
		Principal: "node/1",
	})
	if err == nil {
		t.Fatal("expected error when KMSKeyURI is empty in remote mode")
	}
}

func TestClusterIssuer_LocalMode_RequiresKeys(t *testing.T) {
	_, rec := mintRemoteCA(t, "active")
	reader := &fakeReader{records: []*enginev1.CARootRecord{rec}, rev: 1}
	_, err := NewClusterIssuer(context.Background(), ClusterOptions{
		Reader:      reader,
		SigningMode: SigningModeLocal,
		// Keys omitted
		Principal: "node/1",
	})
	if err == nil {
		t.Fatal("expected error when Keys is nil in local mode")
	}
}

func TestClusterIssuer_RemoteSigner_IssueForPrincipalWorks(t *testing.T) {
	const prefix = "test-kms-cluster-issue-for-principal://"
	t.Cleanup(func() { UnregisterRemoteSigner(prefix) })

	signer, rec := mintRemoteCA(t, "active")
	RegisterRemoteSigner(prefix, func(context.Context, string) (crypto.Signer, error) {
		return signer, nil
	})

	reader := &fakeReader{records: []*enginev1.CARootRecord{rec}, rev: 1}
	issuer, err := NewClusterIssuer(context.Background(), ClusterOptions{
		Reader:      reader,
		SigningMode: SigningModeRemote,
		KMSKeyURI:   prefix + "k",
		Principal:   "node/1",
	})
	if err != nil {
		t.Fatalf("NewClusterIssuer: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "operator/alice"},
	}, leafKey)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatal(err)
	}
	csr.PublicKey = &leafKey.PublicKey

	leafPEM, err := issuer.IssueForPrincipal(csr, "operator/alice", LeafOperator, nil, 10*time.Minute)
	if err != nil {
		t.Fatalf("IssueForPrincipal: %v", err)
	}
	block, _ := pem.Decode(leafPEM)
	if block == nil {
		t.Fatal("leaf is not PEM")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if leaf.Subject.CommonName != "operator/alice" {
		t.Errorf("CN = %q; want operator/alice", leaf.Subject.CommonName)
	}

	caBlock, _ := pem.Decode(rec.CertPem)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Errorf("operator leaf chain verify: %v", err)
	}
}
