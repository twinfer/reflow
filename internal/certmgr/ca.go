package certmgr

import (
	"crypto"
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
	"os"
	"path/filepath"
	"time"
)

// CA bundles a parsed self-signed root with its private key + PEM
// material. Replaces internal/pki.CA (deleted in PR 4). The CA is used
// by:
//   - operator-driven CLI: `reflwd config ca init` mints one and ships
//     it into shard 0;
//   - tests (single-binary fixtures + the engine integration suite);
//   - the BuiltinIssuer when an operator points cfg.X.Creds.TLS.Issuer
//     at a CA on disk (the historic single-node setup).
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
}

// CALeafKind tags whether a leaf is a node cert (server+client auth) or
// an operator cert (client auth only). Mirrors the role prefix encoded
// in the leaf CN.
type CALeafKind int

const (
	CALeafNode CALeafKind = iota
	CALeafOperator
)

// caDefaultValidity is the validity stamped onto fresh CAs.
const caDefaultValidity = 10 * 365 * 24 * time.Hour

// CADefaultLeafValidity is the validity stamped onto fresh leaves when
// IssueLeafOptions.Validity is zero.
const CADefaultLeafValidity = 365 * 24 * time.Hour

// MintCA creates a self-signed ECDSA-P256 CA with subject.CommonName = cn.
func MintCA(cn string) (*CA, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("certmgr: generate CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(caDefaultValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("certmgr: create CA cert: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("certmgr: marshal CA key: %w", err)
	}
	return &CA{
		Cert:    cert,
		Key:     priv,
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// LoadCA reads a PEM-encoded cert + ECDSA key pair from disk.
func LoadCA(certPath, keyPath string) (*CA, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("certmgr: read CA cert: %w", err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("certmgr: read CA key: %w", err)
	}
	return ParseCA(certPEM, keyPEM)
}

// ParseCA builds a CA from PEM-encoded cert + EC private key bytes. The
// cluster-CA startup path uses this: the cert is public config and the
// key is freshly KMS-unwrapped from a config blob URI, so both arrive as
// bytes rather than file paths.
func ParseCA(certPEM, keyPEM []byte) (*CA, error) {
	cert, err := parsePEMCertificate(certPEM)
	if err != nil {
		return nil, fmt.Errorf("certmgr: parse CA cert: %w", err)
	}
	priv, err := parsePEMECPrivateKey(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("certmgr: parse CA key: %w", err)
	}
	return &CA{Cert: cert, Key: priv, CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// IssueLeafOptions tunes IssueLeaf / IssueLeafForKey. Name is the
// principal Raw name segment (e.g. "1", "alice"); the role prefix is
// derived from Kind. Hosts are DNS / IP SANs.
type IssueLeafOptions struct {
	Kind     CALeafKind
	Name     string
	Hosts    []string
	Validity time.Duration
}

// IssueLeaf signs a fresh leaf for a newly-generated key. Returns the
// PEM-encoded cert + key.
func (ca *CA) IssueLeaf(opts IssueLeafOptions) (certPEM, keyPEM []byte, err error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("certmgr: generate leaf key: %w", err)
	}
	der, err := ca.IssueLeafForKey(opts, &priv.PublicKey)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, nil, fmt.Errorf("certmgr: marshal leaf key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		nil
}

// IssueLeafForKey signs a leaf for the caller-supplied public key,
// returning the DER-encoded cert. Used by BuiltinIssuer where CertMagic
// generates the key + CSR upstream.
func (ca *CA) IssueLeafForKey(opts IssueLeafOptions, pub crypto.PublicKey) ([]byte, error) {
	if opts.Name == "" {
		return nil, errors.New("certmgr: leaf Name is required")
	}
	role, err := rolePrefix(opts.Kind)
	if err != nil {
		return nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	validity := opts.Validity
	if validity == 0 {
		validity = CADefaultLeafValidity
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: role + "/" + opts.Name},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	switch opts.Kind {
	case CALeafNode:
		template.ExtKeyUsage = []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		}
	case CALeafOperator:
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	for _, h := range opts.Hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
			continue
		}
		template.DNSNames = append(template.DNSNames, h)
	}
	return x509.CreateCertificate(rand.Reader, template, ca.Cert, pub, ca.Key)
}

// WriteSingle writes ca.cert + ca.key into dir as ca.crt / ca.key.
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

// WriteLeaf writes cert + key into dir as <name>.crt / <name>.key.
func WriteLeaf(dir, name string, certPEM, keyPEM []byte) (certPath, keyPath string, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

func rolePrefix(k CALeafKind) (string, error) {
	switch k {
	case CALeafNode:
		return "node", nil
	case CALeafOperator:
		return "operator", nil
	default:
		return "", fmt.Errorf("certmgr: unknown leaf kind %d", k)
	}
}

func parsePEMCertificate(pemBytes []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("not PEM")
	}
	return x509.ParseCertificate(block.Bytes)
}

func parsePEMECPrivateKey(pemBytes []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("not PEM")
	}
	switch block.Type {
	case "EC PRIVATE KEY":
		return x509.ParseECPrivateKey(block.Bytes)
	case "PRIVATE KEY":
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		ec, ok := key.(*ecdsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("not an ECDSA key: %T", key)
		}
		return ec, nil
	default:
		return nil, fmt.Errorf("unsupported PEM block %q", block.Type)
	}
}

func randomSerial() (*big.Int, error) {
	upper := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, upper)
}
