// Package pki generates a tiny ECDSA-P256-based CA and leaf certificates
// for reflow's single-CA mTLS surface. Both node and operator leaves are
// signed by the same root; role lives in each leaf's SPIFFE URI SAN
// (spiffe://<trust-domain>/node/<id>, spiffe://<trust-domain>/operator/<name>).
// Used by the reflowd pki init-ca / issue-cert / issue-operator
// subcommands and by integration tests.
//
// Stdlib-only — no external CA, no ACME, no SPIFFE workload-API runtime.
// Operators bringing their own PKI bypass this package entirely.
package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Material bundles a cert + matching private key. PEM-encoded.
type Material struct {
	CertPEM []byte
	KeyPEM  []byte
}

// CA is a self-signed certificate authority. The parsed cert + key are
// retained so leaves can be issued without re-parsing the PEM.
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
}

// LeafKind tags the certificate's intended use. Drives KeyUsage +
// ExtKeyUsage selection.
type LeafKind int

const (
	// LeafNode is a reflowd node certificate (server+client auth).
	LeafNode LeafKind = iota
	// LeafOperator is an operator certificate (client auth only).
	LeafOperator
)

// DefaultCAValidity is the lifetime of CA certificates created by NewCA.
const DefaultCAValidity = 10 * 365 * 24 * time.Hour

// DefaultLeafValidity is the default lifetime of leaf certs.
const DefaultLeafValidity = 365 * 24 * time.Hour

// NewCA creates a fresh self-signed CA with commonName.
func NewCA(commonName string) (*CA, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("pki: generate CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(DefaultCAValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("pki: create CA cert: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("pki: marshal CA key: %w", err)
	}
	return &CA{
		Cert:    cert,
		Key:     priv,
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// LeafOptions tunes Issue. Name becomes the leaf's CN. Hosts are the
// DNS / IP SAN entries (parsed automatically). URIs are the URI SANs;
// at least one SPIFFE-formatted URI is expected for production leaves
// — the TLS verifier in pkg/reflow rejects certs without one. Validity
// defaults to DefaultLeafValidity when zero.
type LeafOptions struct {
	Kind     LeafKind
	Name     string
	Hosts    []string
	URIs     []*url.URL
	Validity time.Duration
}

// Issue signs a fresh leaf certificate with ca. The returned Material is
// PEM-encoded and ready to write to disk.
func (ca *CA) Issue(opts LeafOptions) (Material, error) {
	if opts.Name == "" {
		return Material{}, errors.New("pki: leaf Name is required")
	}
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Material{}, fmt.Errorf("pki: generate leaf key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return Material{}, err
	}
	validity := opts.Validity
	if validity == 0 {
		validity = DefaultLeafValidity
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: opts.Name},
		NotBefore:    time.Now().Add(-1 * time.Minute),
		NotAfter:     time.Now().Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	switch opts.Kind {
	case LeafNode:
		template.ExtKeyUsage = []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		}
	case LeafOperator:
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	default:
		return Material{}, fmt.Errorf("pki: unknown leaf kind %d", opts.Kind)
	}
	for _, h := range opts.Hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
			continue
		}
		template.DNSNames = append(template.DNSNames, h)
	}
	if len(opts.URIs) > 0 {
		template.URIs = append(template.URIs, opts.URIs...)
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, &priv.PublicKey, ca.Key)
	if err != nil {
		return Material{}, fmt.Errorf("pki: create leaf cert: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return Material{}, fmt.Errorf("pki: marshal leaf key: %w", err)
	}
	return Material{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// Write writes the CA cert + key into dir as <prefix>-ca.crt /
// <prefix>-ca.key. Returns the absolute paths of both files. For the
// single-CA layout used by reflow, prefer WriteSingle.
func (ca *CA) Write(dir, prefix string) (certPath, keyPath string, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	certPath = filepath.Join(dir, prefix+"-ca.crt")
	keyPath = filepath.Join(dir, prefix+"-ca.key")
	if err := os.WriteFile(certPath, ca.CertPEM, 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyPath, ca.KeyPEM, 0o600); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

// WriteSingle writes the CA into dir as ca.crt / ca.key — the layout
// reflow uses now that there is one root for both node and operator
// leaves.
func (ca *CA) WriteSingle(dir string) (certPath, keyPath string, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	certPath = filepath.Join(dir, "ca.crt")
	keyPath = filepath.Join(dir, "ca.key")
	if err := os.WriteFile(certPath, ca.CertPEM, 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyPath, ca.KeyPEM, 0o600); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

// LoadCA reads a CA cert + key pair from disk.
func LoadCA(certPath, keyPath string) (*CA, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("pki: read CA cert: %w", err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("pki: read CA key: %w", err)
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, errors.New("pki: CA cert is not PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("pki: parse CA cert: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, errors.New("pki: CA key is not PEM")
	}
	priv, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("pki: parse CA key: %w", err)
	}
	return &CA{Cert: cert, Key: priv, CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// WriteMaterial writes m to dir as <name>.crt + <name>.key.
func WriteMaterial(dir, name string, m Material) (certPath, keyPath string, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	if err := os.WriteFile(certPath, m.CertPEM, 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyPath, m.KeyPEM, 0o600); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

func randomSerial() (*big.Int, error) {
	upper := new(big.Int).Lsh(big.NewInt(1), 128)
	n, err := rand.Int(rand.Reader, upper)
	if err != nil {
		return nil, fmt.Errorf("pki: serial: %w", err)
	}
	return n, nil
}

// BuildSPIFFEID builds a SPIFFE-formatted URL of the form
// spiffe://<trustDomain>/<role>/<name>. trustDomain must be non-empty,
// and role and name must not contain "/" — keeping the path strictly
// two segments lets the verifier do an unambiguous prefix match.
func BuildSPIFFEID(trustDomain, role, name string) (*url.URL, error) {
	if trustDomain == "" {
		return nil, errors.New("pki: SPIFFE trust domain is required")
	}
	if role == "" || strings.ContainsRune(role, '/') {
		return nil, fmt.Errorf("pki: invalid SPIFFE role %q", role)
	}
	if name == "" || strings.ContainsRune(name, '/') {
		return nil, fmt.Errorf("pki: invalid SPIFFE name %q", name)
	}
	return &url.URL{
		Scheme: "spiffe",
		Host:   trustDomain,
		Path:   "/" + role + "/" + name,
	}, nil
}
