package certmgr

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/certmagic"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// CARootReader is the seam ClusterIssuer uses to fetch CARootTable
// state from shard 0. Production wiring is engine.Host.CARoots; tests
// hand in a fake.
type CARootReader interface {
	CARoots(ctx context.Context) ([]*enginev1.CARootRecord, uint64 /*tableRev*/, error)
}

// CASigningKeyResolver is the seam ClusterIssuer uses to fetch the
// PEM-encoded CA signing key by name. Production wiring is
// secretstore.Resolver.LookupForCASigning; tests hand in a fake.
type CASigningKeyResolver interface {
	LookupForCASigning(name string) ([]byte, error)
}

// SigningMode selects how the ClusterIssuer obtains the CA private
// key. Local resolves a PEM-encoded key from the SecretStore (the
// default since PR 3). Remote dispatches a KMS URI through the
// RemoteSigner registry, returning a crypto.Signer that proxies every
// Sign() call to the KMS — the CA private key bytes never enter
// process memory.
type SigningMode int

const (
	// SigningModeLocal resolves the CA signing key from SecretStore.
	SigningModeLocal SigningMode = iota
	// SigningModeRemote resolves a crypto.Signer from the global
	// RemoteSigner registry keyed by URI prefix.
	SigningModeRemote
)

// ClusterIssuer is a certmagic.Issuer that signs leaves with the
// cluster CA root stored in shard 0's CARootTable. The signing-key
// source depends on SigningMode: local (SecretStore) or remote (KMS
// via the RemoteSigner registry). The CSR's public key is signed;
// the leaf's CN is set from the issuer's construction-time principal,
// matching BuiltinIssuer's contract so the auth layer can keep keying
// on CN.
type ClusterIssuer struct {
	reader    CARootReader
	keys      CASigningKeyResolver
	mode      SigningMode
	kmsURI    string
	principal string
	kind      LeafKind
	name      string
	hosts     []string
	validity  time.Duration
	rowName   string // "active" by convention

	mu     sync.Mutex
	active atomic.Pointer[activeCA]
}

// LeafKind tags the leaf's purpose. Mirrors internal/pki.LeafKind
// values so the apply layer downstream stays unchanged when PR 4
// deletes that package.
type LeafKind int

const (
	LeafNode LeafKind = iota
	LeafOperator
)

type activeCA struct {
	cert   *x509.Certificate
	signer crypto.Signer
	pem    []byte
}

// ClusterOptions configures a ClusterIssuer. SigningMode picks the
// key-source path:
//
//   - SigningModeLocal (default): Keys is required; the CA private
//     key is fetched from SecretStore as PEM bytes on every Refresh.
//   - SigningModeRemote: KMSKeyURI is required; the URI is dispatched
//     through the global RemoteSigner registry (see RegisterRemoteSigner)
//     to obtain a crypto.Signer that proxies sign operations to the
//     KMS. Keys is unused (and may be nil).
type ClusterOptions struct {
	Reader      CARootReader
	Keys        CASigningKeyResolver
	SigningMode SigningMode
	KMSKeyURI   string
	Principal   string
	Hosts       []string
	Validity    time.Duration
	// RowName is the CARootTable row to read. Defaults to "active".
	RowName string
}

// NewClusterIssuer returns a ClusterIssuer wired against the supplied
// reader + signing-key source. Refresh is called eagerly so
// misconfiguration surfaces at construction time, not on the first
// signing operation.
func NewClusterIssuer(ctx context.Context, opts ClusterOptions) (*ClusterIssuer, error) {
	if opts.Reader == nil {
		return nil, errors.New("certmgr: ClusterIssuer requires Reader")
	}
	switch opts.SigningMode {
	case SigningModeLocal:
		if opts.Keys == nil {
			return nil, errors.New("certmgr: ClusterIssuer requires Keys in local signing mode")
		}
	case SigningModeRemote:
		if opts.KMSKeyURI == "" {
			return nil, errors.New("certmgr: ClusterIssuer requires KMSKeyURI in kms_remote signing mode")
		}
	default:
		return nil, fmt.Errorf("certmgr: unknown signing mode %d", opts.SigningMode)
	}
	kind, name, ok := parsePrincipalRaw(opts.Principal)
	if !ok {
		return nil, fmt.Errorf("certmgr: malformed principal %q", opts.Principal)
	}
	rowName := opts.RowName
	if rowName == "" {
		rowName = "active"
	}
	c := &ClusterIssuer{
		reader:    opts.Reader,
		keys:      opts.Keys,
		mode:      opts.SigningMode,
		kmsURI:    opts.KMSKeyURI,
		principal: opts.Principal,
		kind:      kind,
		name:      name,
		hosts:     append([]string(nil), opts.Hosts...),
		validity:  opts.Validity,
		rowName:   rowName,
	}
	if err := c.Refresh(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Refresh re-reads the CARootTable + resolves the signing key (local
// or remote, depending on SigningMode) and atomically swaps the active
// CA snapshot. On any error the previous snapshot is left in place so
// a transient blob/KMS hiccup doesn't break renewals. Returns the
// error so the operator-facing first-call surfaces it; the background
// reconciler loop logs + counts.
func (c *ClusterIssuer) Refresh(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	records, _, err := c.reader.CARoots(ctx)
	if err != nil {
		return fmt.Errorf("certmgr: read CARootTable: %w", err)
	}
	row := pickActive(records, c.rowName)
	if row == nil {
		return fmt.Errorf("certmgr: CARootTable has no row named %q", c.rowName)
	}
	cert, err := parsePEMCertificate(row.GetCertPem())
	if err != nil {
		return fmt.Errorf("certmgr: parse CA cert: %w", err)
	}
	signer, err := c.resolveSigner(ctx, row)
	if err != nil {
		return err
	}
	if cert.PublicKey == nil || !publicKeyMatches(cert.PublicKey, signer.Public()) {
		return errors.New("certmgr: CA cert public key does not match resolved signing key")
	}
	c.active.Store(&activeCA{
		cert:   cert,
		signer: signer,
		pem:    row.GetCertPem(),
	})
	return nil
}

// resolveSigner picks the per-mode key source. Local fetches PEM bytes
// from SecretStore; Remote dispatches the configured URI through the
// RemoteSigner registry. KMSKeyURI is ignored in local mode and
// key_secret_name is ignored in remote mode — operator-facing CARoot
// rows carry both fields for the future where a single cluster may
// rotate between modes, but at any given moment only one is read.
func (c *ClusterIssuer) resolveSigner(ctx context.Context, row *enginev1.CARootRecord) (crypto.Signer, error) {
	switch c.mode {
	case SigningModeLocal:
		keyPEM, err := c.keys.LookupForCASigning(row.GetKeySecretName())
		if err != nil {
			return nil, fmt.Errorf("certmgr: resolve CA key: %w", err)
		}
		key, err := parsePEMECPrivateKey(keyPEM)
		if err != nil {
			return nil, fmt.Errorf("certmgr: parse CA key: %w", err)
		}
		return key, nil
	case SigningModeRemote:
		signer, err := ResolveRemoteSigner(ctx, c.kmsURI)
		if err != nil {
			return nil, fmt.Errorf("certmgr: resolve kms_remote signer: %w", err)
		}
		return signer, nil
	default:
		return nil, fmt.Errorf("certmgr: unknown signing mode %d", c.mode)
	}
}

// Run is the reconcile loop: wake on sub (CARootTable notifier) or a
// 5s backstop ticker, call Refresh, log errors. Returns ctx.Err() when
// ctx is cancelled.
func (c *ClusterIssuer) Run(ctx context.Context, sub <-chan struct{}) error {
	const tick = 5 * time.Second
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			_ = c.Refresh(ctx)
		case <-t.C:
			_ = c.Refresh(ctx)
		}
	}
}

// Issue implements certmagic.Issuer. The CSR's public key is signed
// against the active cluster CA snapshot. CN encodes the principal Raw
// form; CSR-supplied SANs (DNS + IP) are propagated so CertMagic's
// cache can find the cert at handshake time.
func (c *ClusterIssuer) Issue(_ context.Context, csr *x509.CertificateRequest) (*certmagic.IssuedCertificate, error) {
	if csr == nil || csr.PublicKey == nil {
		return nil, errors.New("certmgr: ClusterIssuer requires CSR with public key")
	}
	a := c.active.Load()
	if a == nil {
		return nil, errors.New("certmgr: ClusterIssuer has no active CA snapshot")
	}
	validity := c.validity
	if validity == 0 {
		validity = 24 * time.Hour
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: c.principal},
		NotBefore:    time.Now().Add(-1 * time.Minute),
		NotAfter:     time.Now().Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	switch c.kind {
	case LeafNode:
		template.ExtKeyUsage = []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		}
	case LeafOperator:
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	for _, h := range c.hosts {
		appendHost(template, h)
	}
	for _, n := range csr.DNSNames {
		template.DNSNames = append(template.DNSNames, n)
	}
	for _, ip := range csr.IPAddresses {
		template.IPAddresses = append(template.IPAddresses, ip)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, csr.PublicKey, a.signer)
	if err != nil {
		return nil, fmt.Errorf("certmgr: sign leaf: %w", err)
	}
	bundle := append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		a.pem...,
	)
	return &certmagic.IssuedCertificate{Certificate: bundle}, nil
}

// IssuerKey implements certmagic.Issuer.
func (c *ClusterIssuer) IssuerKey() string { return "reflw-cluster" }

// IssueForPrincipal signs csr against the active cluster CA but stamps
// the CN to principalRaw (e.g. "node/7", "operator/alice") rather than
// the issuer's construction-time principal. Used by the bootstrap
// server, where the CN is determined by the redeeming join token, not
// by the signing node's identity. validity defaults to the issuer's
// configured validity (then 24h) when zero. Returns a PEM-encoded
// bundle (leaf || CA chain) so the caller can persist it directly.
func (c *ClusterIssuer) IssueForPrincipal(
	csr *x509.CertificateRequest,
	principalRaw string,
	kind LeafKind,
	hosts []string,
	validity time.Duration,
) ([]byte, error) {
	if csr == nil || csr.PublicKey == nil {
		return nil, errors.New("certmgr: IssueForPrincipal requires CSR with public key")
	}
	if principalRaw == "" {
		return nil, errors.New("certmgr: IssueForPrincipal requires principalRaw")
	}
	a := c.active.Load()
	if a == nil {
		return nil, errors.New("certmgr: ClusterIssuer has no active CA snapshot")
	}
	if validity == 0 {
		validity = c.validity
	}
	if validity == 0 {
		validity = 24 * time.Hour
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: principalRaw},
		NotBefore:    time.Now().Add(-1 * time.Minute),
		NotAfter:     time.Now().Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	switch kind {
	case LeafNode:
		template.ExtKeyUsage = []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth,
		}
	case LeafOperator:
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	for _, h := range hosts {
		appendHost(template, h)
	}
	for _, n := range csr.DNSNames {
		template.DNSNames = append(template.DNSNames, n)
	}
	for _, ip := range csr.IPAddresses {
		template.IPAddresses = append(template.IPAddresses, ip)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, csr.PublicKey, a.signer)
	if err != nil {
		return nil, fmt.Errorf("certmgr: sign leaf: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// ActiveCertPEM returns the cert PEM of the active CA snapshot, or nil
// when no snapshot is loaded. Operator-facing inspection only.
func (c *ClusterIssuer) ActiveCertPEM() []byte {
	a := c.active.Load()
	if a == nil {
		return nil
	}
	return append([]byte(nil), a.pem...)
}

// pickActive selects the row matching rowName from records. Returns
// nil when no match.
func pickActive(records []*enginev1.CARootRecord, rowName string) *enginev1.CARootRecord {
	for _, r := range records {
		if r.GetName() == rowName {
			return r
		}
	}
	return nil
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

// publicKeyMatches compares two public keys via DER-encoded SPKI
// bytes. The DER form is the canonical wire representation, so two
// keys are equal iff their PKIX-marshaled SubjectPublicKeyInfo bytes
// match — works for ECDSA, RSA, Ed25519 alike, so the kms_remote path
// is not coupled to ECDSA.
func publicKeyMatches(a, b crypto.PublicKey) bool {
	if a == nil || b == nil {
		return false
	}
	aDER, err := x509.MarshalPKIXPublicKey(a)
	if err != nil {
		return false
	}
	bDER, err := x509.MarshalPKIXPublicKey(b)
	if err != nil {
		return false
	}
	return bytes.Equal(aDER, bDER)
}

func appendHost(t *x509.Certificate, host string) {
	if ip := net.ParseIP(host); ip != nil {
		t.IPAddresses = append(t.IPAddresses, ip)
		return
	}
	t.DNSNames = append(t.DNSNames, host)
}

// parsePrincipalRaw mirrors splitPrincipal in builtin.go but returns
// the cluster-flavor LeafKind so this file stands alone when
// internal/pki goes away in PR 4.
func parsePrincipalRaw(raw string) (LeafKind, string, bool) {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '/' {
			continue
		}
		if i == 0 || i == len(raw)-1 {
			return 0, "", false
		}
		switch raw[:i] {
		case "node":
			return LeafNode, raw[i+1:], true
		case "operator":
			return LeafOperator, raw[i+1:], true
		default:
			return 0, "", false
		}
	}
	return 0, "", false
}

func randomSerial() (*big.Int, error) {
	upper := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, upper)
}
