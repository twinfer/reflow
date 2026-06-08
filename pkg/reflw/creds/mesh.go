package creds

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/credentials"

	"github.com/twinfer/reflw/internal/certmgr"
)

// NodeIdentity is a node's single self-issued mesh identity: a CertMagic
// Manager that mints + renews this node's node/<id> leaf from the
// cluster CA, plus the CA cert pool every mesh peer is verified against.
// Built once at startup and shared by the admin + delivery listeners and
// the SelfJoin client, so the node presents one leaf everywhere and one
// background renewal loop runs.
type NodeIdentity struct {
	mgr    *certmgr.Manager
	caPool *x509.CertPool
	caPEM  []byte
}

// NodeIdentityOptions configures BuildNodeIdentity. The CA cert is
// public; the CA key arrives already KMS-unwrapped — the caller resolves
// it from the config blob URI (secretstore.ResolveRemoteEncrypted)
// before calling here, so creds stays free of KMS plumbing.
type NodeIdentityOptions struct {
	CACertPEM    []byte
	CAKeyPEM     []byte
	NodeID       string // "1", "2", ...
	Principal    string // "node/<id>"
	Hosts        []string
	Validity     time.Duration
	CertCacheDir string
	Logger       *slog.Logger
}

// BuildNodeIdentity assembles the CA from cert+key PEM, wires a
// BuiltinIssuer for Principal, and starts a CertMagic Manager that
// self-issues + renews the leaf. ManageLeaf runs synchronously so the
// first handshake never sees a cold cache.
func BuildNodeIdentity(ctx context.Context, opts NodeIdentityOptions) (*NodeIdentity, error) {
	if opts.CertCacheDir == "" {
		return nil, errors.New("reflw/creds: NodeIdentity CertCacheDir is required")
	}
	ca, err := certmgr.ParseCA(opts.CACertPEM, opts.CAKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("reflw/creds: cluster CA: %w", err)
	}
	issuer, err := certmgr.NewBuiltinIssuer(certmgr.BuiltinOptions{
		CA:        ca,
		Principal: opts.Principal,
		Hosts:     opts.Hosts,
		Validity:  opts.Validity,
	})
	if err != nil {
		return nil, err
	}
	mgr, err := certmgr.New(certmgr.Options{
		Dir:       opts.CertCacheDir,
		NodeID:    opts.NodeID,
		Principal: opts.Principal,
		Issuer:    issuer,
		Logger:    opts.Logger,
	})
	if err != nil {
		return nil, err
	}
	if err := mgr.ManageLeaf(ctx); err != nil {
		_ = mgr.Close()
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(opts.CACertPEM) {
		_ = mgr.Close()
		return nil, errors.New("reflw/creds: cluster CA cert has no PEM blocks")
	}
	return &NodeIdentity{
		mgr:    mgr,
		caPool: pool,
		caPEM:  append([]byte(nil), opts.CACertPEM...),
	}, nil
}

// Close stops the CertMagic Manager. Safe to call once; safe on nil.
func (n *NodeIdentity) Close() error {
	if n == nil || n.mgr == nil {
		return nil
	}
	return n.mgr.Close()
}

// CACertPEM returns the cluster CA cert PEM (the mesh trust anchor).
func (n *NodeIdentity) CACertPEM() []byte { return n.caPEM }

// ClientTLSConfig returns a mesh client config presenting this node's
// leaf and verifying peers against the cluster CA. Used by the SelfJoin
// dial before any listeners are up. Peers are addressed dynamically via
// gossip (no stable hostname to match a SAN against), so hostname
// verification is disabled and the chain + CN are checked manually
// against the cluster CA — mesh identity is the leaf CN, not the host.
func (n *NodeIdentity) ClientTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return n.mgr.GetCertificate(nil)
		},
		InsecureSkipVerify:    true, //nolint:gosec // chain+CN verified in verifyMeshPeer
		VerifyPeerCertificate: verifyMeshPeer(n.caPool),
	}
}

// MeshListenerCreds builds ListenerCreds for a mesh listener from the
// shared NodeIdentity. Server + client present the node leaf; both
// verify peers against the cluster CA and enforce the <kind>/<name> CN
// shape. clientAuth=true requires a verified client cert on the server
// side (the mesh default). Close is nil — the NodeIdentity owns the
// Manager lifecycle, so listeners must not close it.
func MeshListenerCreds(id *NodeIdentity, clientAuth bool) *ListenerCreds {
	// Server side: client certs are verified against the CA pool by the
	// standard verifier (no hostname check applies to client auth), then
	// verifyMeshIdentity enforces the CN shape on the verified chain.
	serverCfg := &tls.Config{
		MinVersion:            tls.VersionTLS13,
		GetCertificate:        id.mgr.GetCertificate,
		VerifyPeerCertificate: verifyMeshIdentity(""),
	}
	if clientAuth {
		serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
		serverCfg.ClientCAs = id.caPool
	}
	return &ListenerCreds{
		ServerTLSConfig: serverCfg,
		ClientTLSConfig: id.ClientTLSConfig(),
		Driver:          DriverTLS,
		SecurityLevel:   credentials.PrivacyAndIntegrity,
	}
}

// verifyMeshPeer verifies a peer leaf against the cluster CA pool
// (building the chain from the presented certs) and enforces the mesh CN
// shape, without any hostname check. Used on the client side where peers
// are reached at gossip-resolved addresses that won't match a leaf SAN.
func verifyMeshPeer(pool *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("reflw/creds: no peer certificate")
		}
		certs := make([]*x509.Certificate, 0, len(rawCerts))
		for _, der := range rawCerts {
			c, err := x509.ParseCertificate(der)
			if err != nil {
				return fmt.Errorf("reflw/creds: parse peer cert: %w", err)
			}
			certs = append(certs, c)
		}
		inter := x509.NewCertPool()
		for _, c := range certs[1:] {
			inter.AddCert(c)
		}
		if _, err := certs[0].Verify(x509.VerifyOptions{
			Roots:         pool,
			Intermediates: inter,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}); err != nil {
			return fmt.Errorf("reflw/creds: mesh peer chain: %w", err)
		}
		if _, err := LeafPrincipal(certs[0]); err != nil {
			return err
		}
		return nil
	}
}
