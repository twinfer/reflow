package certmgr

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/pki"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

type fakeReader struct {
	records []*enginev1.CARootRecord
	rev     uint64
	err     error
}

func (f *fakeReader) CARoots(context.Context) ([]*enginev1.CARootRecord, uint64, error) {
	return f.records, f.rev, f.err
}

type fakeKeys struct {
	bytesByName map[string][]byte
	err         error
}

func (f *fakeKeys) LookupForCASigning(name string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	b, ok := f.bytesByName[name]
	if !ok {
		return nil, errors.New("missing")
	}
	return b, nil
}

func newFakeBackends(t *testing.T, rowName, secretName string) (*fakeReader, *fakeKeys, *pki.CA) {
	t.Helper()
	ca, err := pki.NewCA("reflow-cluster-test-ca")
	if err != nil {
		t.Fatal(err)
	}
	rec := &enginev1.CARootRecord{
		Name:          rowName,
		CertPem:       ca.CertPEM,
		KeySecretName: secretName,
		Fingerprint:   "sha256:doesntmatter",
		RotationEpoch: 1,
		CreatedAtMs:   1,
	}
	return &fakeReader{records: []*enginev1.CARootRecord{rec}, rev: 1},
		&fakeKeys{bytesByName: map[string][]byte{secretName: ca.KeyPEM}},
		ca
}

func TestClusterIssuer_EndToEndManagerSignsLeaf(t *testing.T) {
	reader, keys, ca := newFakeBackends(t, "active", "ca/root/active")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	issuer, err := NewClusterIssuer(ctx, ClusterOptions{
		Reader:    reader,
		Keys:      keys,
		Principal: "node/3",
		Hosts:     []string{"localhost"},
		Validity:  10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewClusterIssuer: %v", err)
	}

	m, err := New(Options{
		Dir:       filepath.Join(t.TempDir(), "cm"),
		NodeID:    "3",
		Principal: "node/3",
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
	if leaf.Subject.CommonName != "node/3" {
		t.Errorf("CN = %q; want node/3", leaf.Subject.CommonName)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("chain verify: %v", err)
	}
}

func TestClusterIssuer_RefreshFailsWhenSigningKeyMissing(t *testing.T) {
	reader, _, _ := newFakeBackends(t, "active", "ca/root/active")
	keys := &fakeKeys{err: errors.New("not resolved yet")}
	_, err := NewClusterIssuer(context.Background(), ClusterOptions{
		Reader:    reader,
		Keys:      keys,
		Principal: "node/1",
	})
	if err == nil {
		t.Fatal("expected NewClusterIssuer to surface resolve error at startup")
	}
}

func TestClusterIssuer_CertKeyMismatchRejected(t *testing.T) {
	reader, _, _ := newFakeBackends(t, "active", "ca/root/active")
	// Resolve to a different CA's key
	other, err := pki.NewCA("other-ca")
	if err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeys{bytesByName: map[string][]byte{"ca/root/active": other.KeyPEM}}
	_, err = NewClusterIssuer(context.Background(), ClusterOptions{
		Reader:    reader,
		Keys:      keys,
		Principal: "node/1",
	})
	if err == nil {
		t.Fatal("expected cert/key mismatch to be rejected")
	}
}

func TestClusterIssuer_NoActiveRowFails(t *testing.T) {
	_, keys, _ := newFakeBackends(t, "ignored", "x")
	emptyReader := &fakeReader{}
	_, err := NewClusterIssuer(context.Background(), ClusterOptions{
		Reader:    emptyReader,
		Keys:      keys,
		Principal: "node/1",
	})
	if err == nil {
		t.Fatal("expected error when CARootTable empty")
	}
}

// TestClusterIssuer_ActiveCertPEMRoundTrip ensures the active snapshot
// is what the operator-facing inspection helper exposes.
func TestClusterIssuer_ActiveCertPEMRoundTrip(t *testing.T) {
	reader, keys, ca := newFakeBackends(t, "active", "ca/root/active")
	issuer, err := NewClusterIssuer(context.Background(), ClusterOptions{
		Reader:    reader,
		Keys:      keys,
		Principal: "node/1",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := issuer.ActiveCertPEM()
	if len(got) == 0 {
		t.Fatal("expected ActiveCertPEM to return non-empty bytes after Refresh")
	}
	// Round-trip parse to ensure it matches the source CA.
	block, _ := pem.Decode(got)
	if block == nil {
		t.Fatal("ActiveCertPEM not PEM")
	}
	parsed, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.SerialNumber.Cmp(ca.Cert.SerialNumber) != 0 {
		t.Errorf("ActiveCertPEM serial differs from source CA")
	}
}
