package reflow

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/pki"
)

// writeTLSFixtures generates an ephemeral node CA, operator CA, node leaf,
// and operator leaf in dir. Returned TLSFiles plus the operator-cert
// paths for client-side dialing.
func writeTLSFixtures(t *testing.T, dir string) (TLSFiles, string, string) {
	t.Helper()
	nodeCA, err := pki.NewCA("reflow-node-ca")
	if err != nil {
		t.Fatal(err)
	}
	opCA, err := pki.NewCA("reflow-operator-ca")
	if err != nil {
		t.Fatal(err)
	}
	nodeCAcrt, _, err := nodeCA.Write(dir, "node")
	if err != nil {
		t.Fatal(err)
	}
	opCAcrt, _, err := opCA.Write(dir, "operator")
	if err != nil {
		t.Fatal(err)
	}
	nodeLeaf, err := nodeCA.Issue(pki.LeafOptions{
		Kind:  pki.LeafNode,
		Name:  "node-1",
		Hosts: []string{"127.0.0.1", "localhost"},
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeCrt, nodeKey, err := pki.WriteMaterial(dir, "node-1", nodeLeaf)
	if err != nil {
		t.Fatal(err)
	}
	opLeaf, err := opCA.Issue(pki.LeafOptions{
		Kind: pki.LeafOperator, Name: "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	opCrt, opKp, err := pki.WriteMaterial(dir, "alice", opLeaf)
	if err != nil {
		t.Fatal(err)
	}
	return TLSFiles{
		NodeCAFile:     nodeCAcrt,
		OperatorCAFile: opCAcrt,
		NodeCertFile:   nodeCrt,
		NodeKeyFile:    nodeKey,
	}, opCrt, opKp
}

func TestPhase4_2_TLS_BuildDeliveryServer_ShapeAndDefaults(t *testing.T) {
	files, _, _ := writeTLSFixtures(t, t.TempDir())
	cfg, err := BuildDeliveryServerTLS(files)
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
	cert, err := cfg.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if cert == nil || len(cert.Certificate) == 0 {
		t.Error("GetCertificate returned empty cert")
	}
}

func TestPhase4_2_TLS_BuildAdminServer_TrustsOperatorCAOnly(t *testing.T) {
	files, _, _ := writeTLSFixtures(t, t.TempDir())
	cfg, err := BuildAdminServerTLS(files)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientCAs == nil {
		t.Fatal("ClientCAs not set")
	}
	// We can't introspect the pool's contents directly, but we can ensure
	// the helper rejects a config that nulls out the operator CA.
	short := files
	short.OperatorCAFile = ""
	if _, err := BuildAdminServerTLS(short); err == nil {
		t.Error("expected error when OperatorCAFile is empty")
	}
}

func TestPhase4_2_TLS_BuildDeliveryClient_HasClientCertCallback(t *testing.T) {
	files, _, _ := writeTLSFixtures(t, t.TempDir())
	cfg, err := BuildDeliveryClientTLS(files)
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

func TestPhase4_2_TLS_BuildAdminClient_AcceptsOperatorCert(t *testing.T) {
	dir := t.TempDir()
	files, opCrt, opKey := writeTLSFixtures(t, dir)
	cfg, err := BuildAdminClientTLS(opCrt, opKey, files.NodeCAFile)
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

func TestPhase4_2_TLS_HotReloadOnMtimeBump(t *testing.T) {
	dir := t.TempDir()
	files, _, _ := writeTLSFixtures(t, dir)
	getCert, err := hotReloadCert(files.NodeCertFile, files.NodeKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	first, err := getCert(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Generate a fresh node leaf and atomically replace the cert/key.
	ca, err := pki.LoadCA(filepath.Join(dir, "node-ca.crt"), filepath.Join(dir, "node-ca.key"))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(pki.LeafOptions{Kind: pki.LeafNode, Name: "node-1-rotated"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pki.WriteMaterial(dir, "node-1", leaf); err != nil {
		t.Fatal(err)
	}
	// Tick the clock past the cached mtime so the loader sees a change.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(files.NodeCertFile, future, future); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(files.NodeKeyFile, future, future); err != nil {
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
