//go:build e2e

package e2e

import (
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/twinfer/reflw/internal/certmgr"
	"github.com/twinfer/reflw/pkg/reflow/creds"
)

// Container-side mount paths for the mesh PKI. Every node mounts its own
// node/<id> leaf at the same path, so the config.yaml creds stanza is static
// across nodes.
const (
	containerCertDir = "/etc/reflowd/certs"
	containerCAPath  = containerCertDir + "/ca.crt"
	containerCrtPath = containerCertDir + "/node.crt"
	containerKeyPath = containerCertDir + "/node.key"
)

// meshCerts is the test PKI for an mTLS e2e cluster: one CA, one operator
// client leaf (CN=operator/e2e) the test process dials the admin port with,
// and a per-node server+client leaf (CN=node/<id>) minted on demand by
// nodeLeaf for the delivery mesh + admin listener.
type meshCerts struct {
	ca         *certmgr.CA
	caCertPath string
	opCertPath string
	opKeyPath  string
}

// newMeshCerts mints the cluster CA + an operator/e2e client leaf and writes
// ca.crt + operator.{crt,key} into a temp dir. Fatal on error.
func newMeshCerts(t *testing.T) *meshCerts {
	t.Helper()
	ca, err := certmgr.MintCA("reflow-e2e-ca")
	if err != nil {
		t.Fatalf("e2e: mint CA: %v", err)
	}
	dir := t.TempDir()
	caCertPath, _, err := ca.WriteSingle(dir)
	if err != nil {
		t.Fatalf("e2e: write CA: %v", err)
	}
	opCert, opKey, err := ca.IssueLeaf(certmgr.IssueLeafOptions{Kind: certmgr.CALeafOperator, Name: "e2e"})
	if err != nil {
		t.Fatalf("e2e: issue operator leaf: %v", err)
	}
	opCertPath, opKeyPath, err := certmgr.WriteLeaf(dir, "operator", opCert, opKey)
	if err != nil {
		t.Fatalf("e2e: write operator leaf: %v", err)
	}
	return &meshCerts{ca: ca, caCertPath: caCertPath, opCertPath: opCertPath, opKeyPath: opKeyPath}
}

// nodeLeaf mints a node/<id> server+client leaf with SANs for the docker alias
// (inter-node delivery dialing) plus localhost/127.0.0.1 (the host-mapped
// admin port the test process dials), writes it to a fresh temp dir, and
// returns the host cert + key paths to mount into the node's container.
func (m *meshCerts) nodeLeaf(t *testing.T, nodeID uint64) (certPath, keyPath string) {
	t.Helper()
	alias := fmt.Sprintf("reflowd-node%d", nodeID)
	cert, key, err := m.ca.IssueLeaf(certmgr.IssueLeafOptions{
		Kind:  certmgr.CALeafNode,
		Name:  fmt.Sprintf("%d", nodeID),
		Hosts: []string{alias, "localhost", "127.0.0.1"},
	})
	if err != nil {
		t.Fatalf("e2e: issue node%d leaf: %v", nodeID, err)
	}
	certPath, keyPath, err = certmgr.WriteLeaf(t.TempDir(), "node", cert, key)
	if err != nil {
		t.Fatalf("e2e: write node%d leaf: %v", nodeID, err)
	}
	return certPath, keyPath
}

// operatorSpec is the creds.Spec the test process dials the admin port with —
// the operator/e2e client leaf + the cluster CA as the trust root. Maps to an
// operator/* principal once the admin authz interceptor evaluates the chain.
func (m *meshCerts) operatorSpec() creds.Spec {
	return creds.Spec{
		Driver: creds.DriverTLS,
		TLS: &creds.TLSSpec{
			CAFile:   m.caCertPath,
			CertFile: m.opCertPath,
			KeyFile:  m.opKeyPath,
		},
	}
}

// operatorClientTLS is the *tls.Config the ingress client dials with — the
// same operator/e2e mTLS material as operatorSpec, built through pkg/reflow/creds
// so it matches the server side exactly. Ingress runs mTLS in the e2e tier (not
// h2c) because Docker Desktop's port proxy mangles cleartext HTTP/2 request
// bodies; HTTP/2-over-TLS rides through opaquely, same as the admin port. The
// ingress-open Cedar permit accepts any principal, so submitting as operator/* is
// authorized.
func (m *meshCerts) operatorClientTLS(t *testing.T) *tls.Config {
	t.Helper()
	lc, err := creds.Build(m.operatorSpec(), nil)
	if err != nil {
		t.Fatalf("e2e: build ingress client TLS: %v", err)
	}
	return lc.ClientTLSConfig
}
