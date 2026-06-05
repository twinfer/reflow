// Package certmgr is the cert-lifecycle layer for reflw's mesh leaves.
// One Manager per node-process owns a certmagic.Cache + Config backed by
// a per-node FileStorage tree (locked against concurrent processes), a
// pluggable Issuer (the BuiltinIssuer wraps internal/pki today; PR 3+
// will add a cluster-CA-backed issuer), and a deterministic
// ECDSA-P256 KeyGenerator. After New, callers call ManageLeaf once to
// obtain (or load) the leaf for this node's principal; TLSConfig
// returns a *tls.Config whose GetCertificate delegates to the cache, so
// rotations are picked up transparently on the next handshake.
//
// CertMagic identifies certs by a "domain name" string that must pass
// idna.ToASCII; reflw principals contain '/' which fails that check,
// so the Manager maps each principal to a deterministic IDNA-clean
// mesh name (see safeMeshName). The Issuer ignores the inbound CSR's
// SAN/CN and writes the principal Raw form into the leaf's CN itself.
package certmgr

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"
)

// Options bundles the inputs a Manager needs.
type Options struct {
	// Dir is the per-node CertMagic storage root. The Manager writes a
	// lock file here at New and refuses to start if a stale lock from
	// another node id is present.
	Dir string
	// NodeID is the engine's node identifier as a string ("1", "2", ...).
	// Embedded in the lock file so a misconfiguration is caught at
	// startup, not after the second node has corrupted the renewal
	// state.
	NodeID string
	// Principal is the principal Raw form this Manager is signing for —
	// e.g. "node/1" or "operator/alice". Passed to the configured
	// Issuer and used as the lookup key in CertMagic storage.
	Principal string
	// Issuer is the certmagic.Issuer that signs leaves. Required.
	// BuiltinIssuer (this package) is the canonical implementation; PR
	// 3+ swaps in a cluster-CA-backed issuer behind the same interface.
	Issuer certmagic.Issuer
	// Logger is used for the slog → zap bridge feeding CertMagic. nil
	// falls back to slog.Default.
	Logger *slog.Logger
}

// Manager owns the CertMagic Cache + Config for a single node-process.
type Manager struct {
	cfg       *certmagic.Config
	cache     *certmagic.Cache
	release   func() error
	meshName  string // CertMagic identifier (IDNA-clean, derived from principal)
	principal string // "<kind>/<name>"
	logger    *slog.Logger
}

// New constructs a Manager, acquires the per-node lock, and wires the
// certmagic.Config with the supplied issuer + a FileStorage rooted at
// opts.Dir. The leaf is NOT obtained here — call ManageLeaf to do that.
// New returns an error if a stale lock from a different node id is
// present, mirroring Pebble's data-dir lock semantics.
func New(opts Options) (*Manager, error) {
	if opts.Dir == "" {
		return nil, errors.New("certmgr: Dir is required")
	}
	if opts.NodeID == "" {
		return nil, errors.New("certmgr: NodeID is required")
	}
	if opts.Principal == "" {
		return nil, errors.New("certmgr: Principal is required")
	}
	if opts.Issuer == nil {
		return nil, errors.New("certmgr: Issuer is required")
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	zapLogger := zap.NewNop()

	release, err := acquireLock(opts.Dir, opts.NodeID)
	if err != nil {
		return nil, err
	}

	storage := &certmagic.FileStorage{Path: filepath.Join(opts.Dir, "storage")}
	cfgTpl := certmagic.Config{
		Storage:   storage,
		Issuers:   []certmagic.Issuer{opts.Issuer},
		KeySource: certmagic.DefaultKeyGenerator, // ECDSA-P256
		Logger:    zapLogger,
	}
	var cfg *certmagic.Config
	cache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) {
			return cfg, nil
		},
		Logger: zapLogger,
	})
	cfg = certmagic.New(cache, cfgTpl)

	return &Manager{
		cfg:       cfg,
		cache:     cache,
		release:   release,
		meshName:  safeMeshName(opts.Principal),
		principal: opts.Principal,
		logger:    logger,
	}, nil
}

// ManageLeaf obtains or loads the leaf for this Manager's principal and
// caches it. CertMagic keeps renewing the leaf in the background after
// this returns — subsequent handshakes via TLSConfig pick up rotations
// from the cache.
func (m *Manager) ManageLeaf(ctx context.Context) error {
	if err := m.cfg.ManageSync(ctx, []string{m.meshName}); err != nil {
		return fmt.Errorf("certmgr: manage leaf for %s: %w", m.principal, err)
	}
	return nil
}

// TLSConfig returns a *tls.Config whose GetCertificate delegates to
// the CertMagic cache. The returned config is base — callers wanting
// mTLS / VerifyPeerCertificate / RootCAs layer those on top.
func (m *Manager) TLSConfig() *tls.Config {
	return m.cfg.TLSConfig()
}

// GetCertificate returns the cached leaf for the configured principal.
// Suitable for wiring into a custom *tls.Config that already has its
// own ClientAuth + VerifyPeerCertificate (the mesh path uses this).
func (m *Manager) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	hint := hello
	if hint == nil {
		hint = &tls.ClientHelloInfo{ServerName: m.meshName}
	} else if hint.ServerName == "" {
		clone := *hint
		clone.ServerName = m.meshName
		hint = &clone
	}
	return m.cfg.GetCertificate(hint)
}

// Close releases the lock and stops the cache. Safe to call once.
func (m *Manager) Close() error {
	m.cache.Stop()
	if m.release != nil {
		return m.release()
	}
	return nil
}
