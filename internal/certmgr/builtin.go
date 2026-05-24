package certmgr

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/caddyserver/certmagic"
)

// BuiltinIssuer is a certmagic.Issuer that signs leaves with a local
// CA loaded from disk. CertMagic generates the private key + CSR; this
// issuer takes the CSR's public key, ignores its SAN/CN, and writes a
// fresh leaf whose CN is the principal Raw form (<kind>/<name>) supplied
// at construction time.
//
// One BuiltinIssuer issues exactly one principal's leaf; build a new one
// per Manager rather than mutating principal at runtime.
type BuiltinIssuer struct {
	ca        *CA
	principal string // "<kind>/<name>"
	kind      CALeafKind
	name      string
	hosts     []string
	validity  time.Duration
}

// BuiltinOptions configures a BuiltinIssuer.
type BuiltinOptions struct {
	// CA is the loaded signing CA. Must be non-nil.
	CA *CA
	// Principal is the leaf's CN — "<kind>/<name>", e.g. "node/1".
	Principal string
	// Hosts are DNS / IP SANs to embed in the leaf, in addition to the
	// CN-based identity.
	Hosts []string
	// Validity is the leaf's lifetime. Zero falls back to
	// CADefaultLeafValidity.
	Validity time.Duration
}

// NewBuiltinIssuer returns a certmagic.Issuer backed by ca.
func NewBuiltinIssuer(opts BuiltinOptions) (*BuiltinIssuer, error) {
	if opts.CA == nil {
		return nil, errors.New("certmgr: builtin issuer requires CA")
	}
	kind, name, ok := splitPrincipal(opts.Principal)
	if !ok {
		return nil, fmt.Errorf("certmgr: malformed principal %q (want <kind>/<name>)", opts.Principal)
	}
	return &BuiltinIssuer{
		ca:        opts.CA,
		principal: opts.Principal,
		kind:      kind,
		name:      name,
		hosts:     append([]string(nil), opts.Hosts...),
		validity:  opts.Validity,
	}, nil
}

// Issue implements certmagic.Issuer. The CSR's public key is signed
// with the configured CA, producing a leaf whose CN is the issuer's
// principal Raw form. The CSR's DNS / IP SANs are copied through to
// the leaf so CertMagic's cache (which indexes by SAN) can find the
// resulting cert at handshake time; auth verification still keys on
// CN via creds.LeafPrincipal.
func (b *BuiltinIssuer) Issue(_ context.Context, csr *x509.CertificateRequest) (*certmagic.IssuedCertificate, error) {
	if csr == nil || csr.PublicKey == nil {
		return nil, errors.New("certmgr: builtin issuer requires CSR with public key")
	}
	hosts := append([]string(nil), b.hosts...)
	for _, n := range csr.DNSNames {
		hosts = append(hosts, n)
	}
	for _, ip := range csr.IPAddresses {
		hosts = append(hosts, ip.String())
	}
	der, err := b.ca.IssueLeafForKey(IssueLeafOptions{
		Kind:     b.kind,
		Name:     b.name,
		Hosts:    hosts,
		Validity: b.validity,
	}, csr.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("certmgr: sign leaf: %w", err)
	}
	bundle := append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		b.ca.CertPEM...,
	)
	return &certmagic.IssuedCertificate{Certificate: bundle}, nil
}

// IssuerKey implements certmagic.Issuer. A constant value is correct:
// every Manager owns a private FileStorage tree, so cross-tenant
// collisions cannot happen here.
func (b *BuiltinIssuer) IssuerKey() string { return "reflow-builtin" }

// splitPrincipal parses "<role>/<name>" into a CALeafKind + name. Returns
// ok=false on anything that doesn't match the expected shape; node and
// operator are the only roles known here.
func splitPrincipal(raw string) (CALeafKind, string, bool) {
	idx := strings.IndexByte(raw, '/')
	if idx <= 0 || idx == len(raw)-1 {
		return 0, "", false
	}
	role, name := raw[:idx], raw[idx+1:]
	switch role {
	case "node":
		return CALeafNode, name, true
	case "operator":
		return CALeafOperator, name, true
	default:
		return 0, "", false
	}
}
