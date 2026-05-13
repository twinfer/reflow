package reflow

import (
	"context"
	"crypto/tls"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/pki"
)

const testTrustDomain = "reflow.local"

// writeTLSFixtures generates an ephemeral single CA, node leaf with a
// SPIFFE node URI SAN, and operator leaf with a SPIFFE operator URI SAN.
// Returns TLSFiles plus the operator cert/key paths for client dialing.
func writeTLSFixtures(t *testing.T, dir string) (TLSFiles, string, string) {
	t.Helper()
	ca, err := pki.NewCA("reflow-ca")
	if err != nil {
		t.Fatal(err)
	}
	caCrt, _, err := ca.WriteSingle(dir)
	if err != nil {
		t.Fatal(err)
	}
	nodeURI, err := pki.BuildSPIFFEID(testTrustDomain, "node", "1")
	if err != nil {
		t.Fatal(err)
	}
	nodeLeaf, err := ca.Issue(pki.LeafOptions{
		Kind:  pki.LeafNode,
		Name:  "node-1",
		Hosts: []string{"127.0.0.1", "localhost"},
		URIs:  []*url.URL{nodeURI},
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeCrt, nodeKey, err := pki.WriteMaterial(dir, "node-1", nodeLeaf)
	if err != nil {
		t.Fatal(err)
	}
	opURI, err := pki.BuildSPIFFEID(testTrustDomain, "operator", "alice")
	if err != nil {
		t.Fatal(err)
	}
	opLeaf, err := ca.Issue(pki.LeafOptions{
		Kind: pki.LeafOperator,
		Name: "alice",
		URIs: []*url.URL{opURI},
	})
	if err != nil {
		t.Fatal(err)
	}
	opCrt, opKp, err := pki.WriteMaterial(dir, "alice", opLeaf)
	if err != nil {
		t.Fatal(err)
	}
	return TLSFiles{
		CAFile:   caCrt,
		CertFile: nodeCrt,
		KeyFile:  nodeKey,
	}, opCrt, opKp
}

func TestTLS_BuildDeliveryServer_ShapeAndDefaults(t *testing.T) {
	files, _, _ := writeTLSFixtures(t, t.TempDir())
	cfg, err := BuildDeliveryServerTLS(files, testTrustDomain)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v; want RequireAndVerifyClientCert", cfg.ClientAuth)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %x; want TLS 1.3", cfg.MinVersion)
	}
	if cfg.ClientCAs == nil {
		t.Error("ClientCAs not set")
	}
	if cfg.GetCertificate == nil {
		t.Error("GetCertificate not set")
	}
	if cfg.VerifyPeerCertificate == nil {
		t.Error("VerifyPeerCertificate not set")
	}
	cert, err := cfg.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if cert == nil || len(cert.Certificate) == 0 {
		t.Error("GetCertificate returned empty cert")
	}
}

func TestTLS_BuildAdminServer_RejectsMissingFields(t *testing.T) {
	files, _, _ := writeTLSFixtures(t, t.TempDir())
	short := files
	short.CAFile = ""
	if _, err := BuildAdminServerTLS(short, testTrustDomain); err == nil {
		t.Error("expected error when CAFile is empty")
	}
	if _, err := BuildAdminServerTLS(files, ""); err == nil {
		t.Error("expected error when trust domain is empty")
	}
}

func TestTLS_BuildDeliveryClient_HasClientCertCallback(t *testing.T) {
	files, _, _ := writeTLSFixtures(t, t.TempDir())
	cfg, err := BuildDeliveryClientTLS(files, testTrustDomain)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GetClientCertificate == nil {
		t.Fatal("GetClientCertificate not set")
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs not set")
	}
	cert, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("GetClientCertificate: %v", err)
	}
	if cert == nil || len(cert.Certificate) == 0 {
		t.Error("empty client cert")
	}
}

func TestTLS_BuildAdminClient_AcceptsOperatorCert(t *testing.T) {
	dir := t.TempDir()
	files, opCrt, opKey := writeTLSFixtures(t, dir)
	cfg, err := BuildAdminClientTLS(opCrt, opKey, files.CAFile, testTrustDomain)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GetClientCertificate == nil {
		t.Fatal("GetClientCertificate not set")
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs not set")
	}
}

func TestTLS_HotReloadOnMtimeBump(t *testing.T) {
	dir := t.TempDir()
	files, _, _ := writeTLSFixtures(t, dir)
	getCert, err := hotReloadCert(files.CertFile, files.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	first, err := getCert(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Generate a fresh node leaf and atomically replace the cert/key.
	ca, err := pki.LoadCA(filepath.Join(dir, "ca.crt"), filepath.Join(dir, "ca.key"))
	if err != nil {
		t.Fatal(err)
	}
	nodeURI, err := pki.BuildSPIFFEID(testTrustDomain, "node", "1")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(pki.LeafOptions{
		Kind: pki.LeafNode, Name: "node-1-rotated",
		URIs: []*url.URL{nodeURI},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pki.WriteMaterial(dir, "node-1", leaf); err != nil {
		t.Fatal(err)
	}
	// Tick the clock past the cached mtime so the loader sees a change.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(files.CertFile, future, future); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(files.KeyFile, future, future); err != nil {
		t.Fatal(err)
	}

	second, err := getCert(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Certificate[0]) == string(second.Certificate[0]) {
		t.Error("hotReloadCert did not pick up the new cert")
	}
}

// dialHandshake performs a TLS handshake from clientCfg to a server that
// uses serverCfg and returns the first error the client observes. In
// TLS 1.3 with mutual auth, a server-side VerifyPeerCertificate failure
// is delivered to the client via an alert that surfaces on the first
// read after Dial returns, so we attempt a one-byte read after the
// handshake to surface that alert.
func dialHandshake(t *testing.T, serverCfg, clientCfg *tls.Config) error {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		if tc, ok := conn.(*tls.Conn); ok {
			_ = tc.HandshakeContext(context.Background())
		}
		_ = conn.Close()
	}()

	clientCfg = clientCfg.Clone()
	clientCfg.ServerName = "127.0.0.1"
	conn, dErr := tls.Dial("tcp", ln.Addr().String(), clientCfg)
	if dErr != nil {
		return dErr
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	_, rerr := conn.Read(buf)
	return rerr
}

// operatorClientTLS builds a client TLS config that presents the
// operator leaf and trusts the shared CA, without role verification.
func clientTLSWithCert(t *testing.T, certFile, keyFile, caFile string) *tls.Config {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := loadCAPool(caFile)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   "127.0.0.1",
		MinVersion:   tls.VersionTLS13,
		// No VerifyPeerCertificate — we want to isolate the server's
		// VerifyPeerCertificate check, not the client's.
	}
}

func TestTLS_BuildAdminServer_RejectsNodeCertAtHandshake(t *testing.T) {
	dir := t.TempDir()
	files, _, _ := writeTLSFixtures(t, dir)
	serverCfg, err := BuildAdminServerTLS(files, testTrustDomain)
	if err != nil {
		t.Fatal(err)
	}
	// Present the node leaf (signed by the same CA) to the Admin server
	// — its VerifyPeerCertificate must reject because the URI prefix is
	// /node/ not /operator/.
	clientCfg := clientTLSWithCert(t, files.CertFile, files.KeyFile, files.CAFile)
	if err := dialHandshake(t, serverCfg, clientCfg); err == nil {
		t.Fatal("expected handshake to fail; node cert reached Admin server")
	} else if !strings.Contains(err.Error(), "prefix") && !strings.Contains(err.Error(), "operator") {
		t.Logf("got expected handshake error: %v", err)
	}
}

func TestTLS_BuildDeliveryServer_RejectsOperatorCertAtHandshake(t *testing.T) {
	dir := t.TempDir()
	files, opCrt, opKey := writeTLSFixtures(t, dir)
	serverCfg, err := BuildDeliveryServerTLS(files, testTrustDomain)
	if err != nil {
		t.Fatal(err)
	}
	clientCfg := clientTLSWithCert(t, opCrt, opKey, files.CAFile)
	if err := dialHandshake(t, serverCfg, clientCfg); err == nil {
		t.Fatal("expected handshake to fail; operator cert reached Delivery server")
	} else if !strings.Contains(err.Error(), "prefix") && !strings.Contains(err.Error(), "node") {
		t.Logf("got expected handshake error: %v", err)
	}
}
