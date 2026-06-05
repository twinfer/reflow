//go:build e2e

package e2e

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/twinfer/reflw/pkg/reflw/creds"
)

// TestMeshCerts_OperatorMTLSHandshake mints the cluster PKI and drives a real
// mutual-TLS handshake over net.Pipe: a node/<id> server leaf vs an
// operator/e2e client leaf, both built through pkg/reflw/creds exactly as
// reflwd (delivery/admin listeners) and reflwclient build them. It asserts
// the handshake completes and the server extracts operator/e2e from the
// verified client chain — the property the admin authz interceptor depends
// on. Runs without Docker, so it verifies the cert/SAN/CN wiring before the
// containerized suite (which needs `make test-e2e`) ever runs.
func TestMeshCerts_OperatorMTLSHandshake(t *testing.T) {
	m := newMeshCerts(t)
	nodeCert, nodeKey := m.nodeLeaf(t, 1)

	serverCreds, err := creds.Build(creds.Spec{
		Driver: creds.DriverTLS,
		TLS:    &creds.TLSSpec{CAFile: m.caCertPath, CertFile: nodeCert, KeyFile: nodeKey},
	}, nil)
	if err != nil {
		t.Fatalf("build node (server) creds: %v", err)
	}
	clientCreds, err := creds.Build(m.operatorSpec(), nil)
	if err != nil {
		t.Fatalf("build operator (client) creds: %v", err)
	}

	srvConn, cliConn := net.Pipe()
	t.Cleanup(func() { _ = srvConn.Close(); _ = cliConn.Close() })

	clientCfg := clientCreds.ClientTLSConfig.Clone()
	clientCfg.ServerName = "localhost" // matches a node-leaf SAN

	errc := make(chan error, 2)
	var serverState tls.ConnectionState
	go func() {
		s := tls.Server(srvConn, serverCreds.ServerTLSConfig)
		_ = s.SetDeadline(time.Now().Add(5 * time.Second))
		if herr := s.Handshake(); herr != nil {
			errc <- herr
			return
		}
		serverState = s.ConnectionState()
		errc <- nil
	}()
	go func() {
		c := tls.Client(cliConn, clientCfg)
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		errc <- c.Handshake()
	}()
	for i := 0; i < 2; i++ {
		if e := <-errc; e != nil {
			t.Fatalf("mTLS handshake failed: %v", e)
		}
	}

	if len(serverState.PeerCertificates) == 0 {
		t.Fatal("server saw no client cert")
	}
	raw, err := creds.LeafPrincipal(serverState.PeerCertificates[0])
	if err != nil {
		t.Fatalf("LeafPrincipal(client leaf): %v", err)
	}
	if raw != "operator/e2e" {
		t.Errorf("client principal = %q; want operator/e2e", raw)
	}
}
