package creds

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/credentials"

	"github.com/twinfer/reflw/internal/certmgr"
)

// TLSSpec is the TLS configuration. Two modes are supported:
//
//   - Externally-managed: set CAFile + CertFile + KeyFile. The engine
//     loads the leaf from disk and hot-reloads on mtime change. The
//     operator (or sidecar) is responsible for rotation.
//   - Managed: set Issuer instead of CertFile/KeyFile. The engine asks
//     the configured Issuer to sign a fresh leaf for Principal and
//     keeps it renewed via CertMagic.
//
// Each leaf's CN encodes the principal Raw form (<kind>/<name>); the
// transport layer verifies chain + CN shape, with role enforcement
// (node/* vs operator/*) left to the auth interceptor.
type TLSSpec struct {
	CAFile   string `koanf:"ca_file"`
	CertFile string `koanf:"cert_file"`
	KeyFile  string `koanf:"key_file"`
	// MeshCAFingerprint, when non-empty, pins the SPKI fingerprint of
	// the CA cert at the root of every verified chain. Values are
	// "sha256:<hex>" per SPKIFingerprint. A mismatch fails the
	// handshake regardless of which CA pool the chain validates
	// against — defends against a former-mesh-CA cert that was
	// rotated out but is still present in some operator's bundle.
	MeshCAFingerprint string `koanf:"mesh_ca_fingerprint"`
	// ServerName, when set, is the SNI / DNS-SAN verification target
	// the client TLS config sends to the server. Empty falls back to
	// grpc-go's default (derived from the dial target). Useful when
	// dialing by IP — the CN identity check still runs; this only
	// gates the standard DNS verification path.
	ServerName string `koanf:"server_name"`
	// ClientAuth, when true, requires and verifies client certs on the
	// server side. Default true for reflw's mTLS surfaces (Delivery,
	// Admin); set false only when fronting OAuth/JWT validation.
	ClientAuth *bool `koanf:"client_auth"`
	// Issuer, when set, enables CertMagic-managed leaves: the engine
	// generates a key, issues a CSR through the configured Issuer, and
	// keeps the leaf renewed. Mutually exclusive with CertFile/KeyFile;
	// CAFile + MeshCAFingerprint still gate trust on the peer side.
	Issuer *IssuerSpec `koanf:"issuer"`
}

// IssuerSpec selects which Issuer plugin signs CertMagic-managed leaves
// and carries plugin-specific options. Exactly one of the nested *Spec
// pointers must match Type.
type IssuerSpec struct {
	// Type names the issuer plugin. Today only "builtin" is wired.
	// PR 3+ adds "cluster" (shard-0 CA + SecretStore-resolved key) and
	// "kms_remote" (Tink KMSClient-signed).
	Type string `koanf:"type"`
	// Builtin holds the builtin-issuer settings — a CA cert + key
	// loaded from disk.
	Builtin *BuiltinIssuerSpec `koanf:"builtin"`
	// CertCacheDir is the per-node CertMagic storage root; the
	// FileStorage tree and the refuse-start lock both live here.
	// Required.
	CertCacheDir string `koanf:"cert_cache_dir"`
	// NodeID is the engine's node identifier as a string; embedded in
	// the cache-dir lock file. Required.
	NodeID string `koanf:"node_id"`
	// Principal is the principal Raw form this listener is signing for
	// (e.g. "node/1", "operator/alice"). Becomes the leaf's CN.
	// Required.
	Principal string `koanf:"principal"`
	// LeafValidity overrides the issuer's default lifetime for the
	// signed leaf. Zero falls back to the issuer default. Short
	// validities exercise CertMagic's renewal path under test.
	LeafValidity time.Duration `koanf:"leaf_validity"`
	// ExtraHosts are additional DNS / IP SANs to embed in the issued
	// leaf (the engine's CN-based identity check ignores these; useful
	// when fronted by a hostname-verifying client).
	ExtraHosts []string `koanf:"extra_hosts"`
}

// BuiltinIssuerSpec configures the builtin Issuer (signs leaves with a
// CA loaded from disk). Replaced in PR 3 by a shard-0-managed CA root +
// SecretStore-wrapped key.
type BuiltinIssuerSpec struct {
	CACertFile string `koanf:"ca_cert_file"`
	CAKeyFile  string `koanf:"ca_key_file"`
}

func (s TLSSpec) clientAuth() bool {
	if s.ClientAuth == nil {
		return true
	}
	return *s.ClientAuth
}

func buildTLS(s *TLSSpec, log *slog.Logger) (*ListenerCreds, error) {
	if s == nil {
		return nil, errMissingSpec(DriverTLS)
	}
	if s.CAFile == "" {
		return nil, errEmptyField(DriverTLS, "ca_file")
	}
	if s.Issuer != nil {
		if s.CertFile != "" || s.KeyFile != "" {
			return nil, errors.New("reflw/creds: tls.issuer and tls.cert_file/key_file are mutually exclusive")
		}
		return buildManagedTLS(s, log)
	}
	if s.CertFile == "" || s.KeyFile == "" {
		return nil, errEmptyField(DriverTLS, "cert_file/key_file")
	}

	get, err := hotReloadKeypair(s.CertFile, s.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(s.CAFile)
	if err != nil {
		return nil, err
	}

	verify := verifyMeshIdentity(s.MeshCAFingerprint)
	serverCfg := &tls.Config{
		MinVersion:            tls.VersionTLS13,
		GetCertificate:        get,
		VerifyPeerCertificate: verify,
	}
	if s.clientAuth() {
		serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
		serverCfg.ClientCAs = pool
	}
	clientCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return get(nil)
		},
		RootCAs:               pool,
		ServerName:            s.ServerName,
		VerifyPeerCertificate: verify,
	}

	return &ListenerCreds{
		ServerTLSConfig: serverCfg,
		ClientTLSConfig: clientCfg,
		Driver:          DriverTLS,
		SecurityLevel:   credentials.PrivacyAndIntegrity,
	}, nil
}

// buildManagedTLS wires a CertMagic-managed leaf. ManageLeaf is called
// synchronously so handshakes never see a cold cache; renewal runs in
// CertMagic's background and the returned tls.Configs pick up rotation
// transparently via the cache.
func buildManagedTLS(s *TLSSpec, log *slog.Logger) (*ListenerCreds, error) {
	is := s.Issuer
	if is.CertCacheDir == "" {
		return nil, errEmptyField(DriverTLS, "issuer.cert_cache_dir")
	}
	if is.NodeID == "" {
		return nil, errEmptyField(DriverTLS, "issuer.node_id")
	}
	if is.Principal == "" {
		return nil, errEmptyField(DriverTLS, "issuer.principal")
	}

	issuer, err := buildIssuer(is)
	if err != nil {
		return nil, err
	}
	mgr, err := certmgr.New(certmgr.Options{
		Dir:       is.CertCacheDir,
		NodeID:    is.NodeID,
		Principal: is.Principal,
		Issuer:    issuer,
		Logger:    log,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := mgr.ManageLeaf(ctx); err != nil {
		_ = mgr.Close()
		return nil, err
	}
	pool, err := loadCAPool(s.CAFile)
	if err != nil {
		_ = mgr.Close()
		return nil, err
	}
	verify := verifyMeshIdentity(s.MeshCAFingerprint)
	serverCfg := &tls.Config{
		MinVersion:            tls.VersionTLS13,
		GetCertificate:        mgr.GetCertificate,
		VerifyPeerCertificate: verify,
	}
	if s.clientAuth() {
		serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
		serverCfg.ClientCAs = pool
	}
	clientCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return mgr.GetCertificate(nil)
		},
		RootCAs:               pool,
		ServerName:            s.ServerName,
		VerifyPeerCertificate: verify,
	}
	return &ListenerCreds{
		ServerTLSConfig: serverCfg,
		ClientTLSConfig: clientCfg,
		Driver:          DriverTLS,
		SecurityLevel:   credentials.PrivacyAndIntegrity,
		Close:           mgr.Close,
	}, nil
}

func buildIssuer(is *IssuerSpec) (*certmgr.BuiltinIssuer, error) {
	switch is.Type {
	case "", "builtin":
		if is.Builtin == nil {
			return nil, errors.New("reflw/creds: tls.issuer.builtin is required for builtin issuer type")
		}
		ca, err := certmgr.LoadCA(is.Builtin.CACertFile, is.Builtin.CAKeyFile)
		if err != nil {
			return nil, fmt.Errorf("reflw/creds: load issuer CA: %w", err)
		}
		return certmgr.NewBuiltinIssuer(certmgr.BuiltinOptions{
			CA:        ca,
			Principal: is.Principal,
			Hosts:     is.ExtraHosts,
			Validity:  is.LeafValidity,
		})
	default:
		return nil, fmt.Errorf("reflw/creds: unknown issuer type %q", is.Type)
	}
}

// hotReloadKeypair returns a callback that re-reads certFile/keyFile
// when either's mtime advances. The first read happens inside this
// constructor so configuration errors fail fast at startup.
func hotReloadKeypair(certFile, keyFile string) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), error) {
	type snap struct {
		cert  tls.Certificate
		mtime int64
	}
	var ptr atomic.Pointer[snap]
	var mu sync.Mutex

	loadOnce := func() (*snap, error) {
		c, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("reflw/creds: load %s/%s: %w", certFile, keyFile, err)
		}
		ci, err := os.Stat(certFile)
		if err != nil {
			return nil, err
		}
		ki, err := os.Stat(keyFile)
		if err != nil {
			return nil, err
		}
		m := ci.ModTime().UnixNano()
		if k := ki.ModTime().UnixNano(); k > m {
			m = k
		}
		return &snap{cert: c, mtime: m}, nil
	}

	initial, err := loadOnce()
	if err != nil {
		return nil, err
	}
	ptr.Store(initial)

	return func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
		cur := ptr.Load()
		ci, err := os.Stat(certFile)
		if err == nil {
			ki, kerr := os.Stat(keyFile)
			if kerr == nil {
				latest := ci.ModTime().UnixNano()
				if k := ki.ModTime().UnixNano(); k > latest {
					latest = k
				}
				if latest > cur.mtime {
					mu.Lock()
					if again := ptr.Load(); again.mtime == cur.mtime {
						if next, lerr := loadOnce(); lerr == nil {
							ptr.Store(next)
						}
					}
					mu.Unlock()
					cur = ptr.Load()
				}
			}
		}
		return &cur.cert, nil
	}, nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reflw/creds: read CA %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("reflw/creds: parse CA %s: no PEM blocks", path)
	}
	return pool, nil
}

// verifyMeshIdentity returns a VerifyPeerCertificate callback that
// enforces the mesh-identity contract on every verified leaf: the CN
// must match <kind>/<name>, and (when pin is non-empty) the chain
// root's SPKI fingerprint must equal pin. Role enforcement happens in
// the auth interceptor.
func verifyMeshIdentity(pin string) func(rawCerts [][]byte, chains [][]*x509.Certificate) error {
	return func(_ [][]byte, chains [][]*x509.Certificate) error {
		if len(chains) == 0 || len(chains[0]) == 0 {
			return errors.New("reflw/creds: no verified chain")
		}
		chain := chains[0]
		if _, err := LeafPrincipal(chain[0]); err != nil {
			return err
		}
		if pin == "" {
			return nil
		}
		root := chain[len(chain)-1]
		if got := SPKIFingerprint(root); got != pin {
			return fmt.Errorf("reflw/creds: mesh CA fingerprint %s does not match pin %s", got, pin)
		}
		return nil
	}
}
