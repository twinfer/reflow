package creds

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/certmgr"
)

// TestBuild_TLSManagedIssuer end-to-ends the certmagic-managed path:
// Build returns a ListenerCreds whose ServerTLSConfig serves a leaf
// minted by the BuiltinIssuer (CN = principal), and a real TLS
// handshake against it succeeds with the client verifying CN +
// MeshCAFingerprint pin.
func TestBuild_TLSManagedIssuer(t *testing.T) {
	dir := t.TempDir()

	ca, err := certmgr.MintCA("reflow-managed-test-ca")
	if err != nil {
		t.Fatal(err)
	}
	caCertPath, caKeyPath, err := ca.WriteSingle(filepath.Join(dir, "ca"))
	if err != nil {
		t.Fatal(err)
	}
	pin := SPKIFingerprint(ca.Cert)

	lc, err := Build(Spec{
		Driver: DriverTLS,
		TLS: &TLSSpec{
			CAFile:            caCertPath,
			MeshCAFingerprint: pin,
			Issuer: &IssuerSpec{
				Type:         "builtin",
				CertCacheDir: filepath.Join(dir, "cm"),
				NodeID:       "1",
				Principal:    "node/1",
				LeafValidity: 5 * time.Minute,
				ExtraHosts:   []string{"127.0.0.1"},
				Builtin: &BuiltinIssuerSpec{
					CACertFile: caCertPath,
					CAKeyFile:  caKeyPath,
				},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Cleanup(func() { _ = CloseAll(lc) })

	if lc.ServerTLSConfig == nil || lc.ClientTLSConfig == nil {
		t.Fatal("expected both server and client TLS configs")
	}

	srv := &http.Server{
		TLSConfig: lc.ServerTLSConfig.Clone(),
		Handler:   http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }),
	}
	srv.TLSConfig.ClientAuth = tls.NoClientCert // simplify client-side
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srv.TLSConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln) //nolint:errcheck

	clientCfg := lc.ClientTLSConfig.Clone()
	clientCfg.ServerName = "127.0.0.1"
	conn, err := tls.Dial("tcp", ln.Addr().String(), clientCfg)
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		t.Fatal("no peer cert")
	}
	leaf := state.PeerCertificates[0]
	if got := leaf.Subject.CommonName; got != "node/1" {
		t.Errorf("leaf CN = %q; want %q", got, "node/1")
	}
	if len(leaf.URIs) != 0 {
		t.Errorf("leaf URIs = %v; expected none", leaf.URIs)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		t.Errorf("leaf chain verify: %v", err)
	}
}

// TestBuild_TLSMutuallyExclusiveCertFileAndIssuer rejects a spec that
// asks for both legacy hot-reload and managed mode in one go.
func TestBuild_TLSMutuallyExclusiveCertFileAndIssuer(t *testing.T) {
	dir := t.TempDir()
	caFile, certFile, keyFile := writeMeshTestPKI(t, dir, "node/1")
	_, err := Build(Spec{
		Driver: DriverTLS,
		TLS: &TLSSpec{
			CAFile:   caFile,
			CertFile: certFile,
			KeyFile:  keyFile,
			Issuer: &IssuerSpec{
				Type:         "builtin",
				CertCacheDir: filepath.Join(dir, "cm"),
				NodeID:       "1",
				Principal:    "node/1",
				Builtin:      &BuiltinIssuerSpec{CACertFile: caFile, CAKeyFile: keyFile},
			},
		},
	}, nil)
	if err == nil {
		t.Fatal("expected error when cert_file + issuer both set")
	}
}
