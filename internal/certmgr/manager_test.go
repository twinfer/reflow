package certmgr

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManager_BuiltinIssuerProducesLeafWithPrincipalCN(t *testing.T) {
	ca, err := MintCA("reflw-test-ca")
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := NewBuiltinIssuer(BuiltinOptions{
		CA:        ca,
		Principal: "node/7",
		Hosts:     []string{"localhost"},
		Validity:  5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(Options{
		Dir:       filepath.Join(t.TempDir(), "cm"),
		NodeID:    "7",
		Principal: "node/7",
		Issuer:    issuer,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := m.ManageLeaf(ctx); err != nil {
		t.Fatalf("ManageLeaf: %v", err)
	}

	cert, err := m.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if len(cert.Certificate) == 0 {
		t.Fatal("empty cert chain")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	if got := leaf.Subject.CommonName; got != "node/7" {
		t.Errorf("leaf CN = %q; want %q", got, "node/7")
	}
	if leaf.NotAfter.Sub(leaf.NotBefore) > 30*time.Minute {
		t.Errorf("leaf validity = %v; expected ~5m", leaf.NotAfter.Sub(leaf.NotBefore))
	}

	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("chain verify against CA: %v", err)
	}
}

func TestManager_ReopenSameNodeOK_DifferentNodeRefuses(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cm")
	ca, err := MintCA("reflw-test-ca")
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := NewBuiltinIssuer(BuiltinOptions{CA: ca, Principal: "node/1"})
	if err != nil {
		t.Fatal(err)
	}

	m1, err := New(Options{Dir: dir, NodeID: "1", Principal: "node/1", Issuer: issuer})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := m1.Close(); err != nil {
		t.Fatalf("close m1: %v", err)
	}

	m1again, err := New(Options{Dir: dir, NodeID: "1", Principal: "node/1", Issuer: issuer})
	if err != nil {
		t.Fatalf("reopen same node: %v", err)
	}
	t.Cleanup(func() { _ = m1again.Close() })

	if _, err := New(Options{Dir: dir, NodeID: "2", Principal: "node/2", Issuer: issuer}); err == nil {
		t.Fatal("expected lock refusal for different node id")
	} else if !strings.Contains(err.Error(), "locked by node") {
		t.Errorf("error = %v; want 'locked by node'", err)
	}
}

func TestBuiltinIssuer_RejectsMalformedPrincipal(t *testing.T) {
	ca, err := MintCA("ca")
	if err != nil {
		t.Fatal(err)
	}
	cases := []string{"", "alice", "/", "node/", "/alice", "unknown/role"}
	for _, p := range cases {
		t.Run(p, func(t *testing.T) {
			if _, err := NewBuiltinIssuer(BuiltinOptions{CA: ca, Principal: p}); err == nil {
				t.Errorf("expected error for principal %q", p)
			}
		})
	}
}
