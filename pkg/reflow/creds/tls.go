package creds

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/credentials"
)

// DefaultTrustDomain is the SPIFFE trust domain used when TLSSpec
// leaves it empty. Mirrors pkg/reflow.DefaultTrustDomain so the creds
// package stands alone.
const DefaultTrustDomain = "reflow.local"

// TLSSpec is the file-driven TLS configuration. One CA signs every
// leaf; each leaf carries a SPIFFE URI SAN whose path is /<role>/<id>.
// The transport layer verifies chain + URI well-formedness; role
// enforcement (node/* vs operator/*) is left to the auth interceptor.
type TLSSpec struct {
	CAFile      string `koanf:"ca_file"`
	CertFile    string `koanf:"cert_file"`
	KeyFile     string `koanf:"key_file"`
	TrustDomain string `koanf:"trust_domain"`
	// ServerName, when set, is the SNI / DNS-SAN verification target
	// the client TLS config sends to the server. Empty falls back to
	// grpc-go's default (derived from the dial target). Useful when
	// dialing by IP — the leaf's URI SAN check still runs; this only
	// gates the standard DNS verification path.
	ServerName string `koanf:"server_name"`
	// ClientAuth, when true, requires and verifies client certs on the
	// server side. Default true for reflow's mTLS surfaces (Delivery,
	// Admin); set false only when fronting OAuth/JWT validation.
	ClientAuth *bool `koanf:"client_auth"`
}

func (s TLSSpec) trustDomain() string {
	if s.TrustDomain == "" {
		return DefaultTrustDomain
	}
	return s.TrustDomain
}

func (s TLSSpec) clientAuth() bool {
	if s.ClientAuth == nil {
		return true
	}
	return *s.ClientAuth
}

func buildTLS(s *TLSSpec, _ *slog.Logger) (*ListenerCreds, error) {
	if s == nil {
		return nil, errMissingSpec(DriverTLS)
	}
	if s.CertFile == "" || s.KeyFile == "" {
		return nil, errEmptyField(DriverTLS, "cert_file/key_file")
	}
	if s.CAFile == "" {
		return nil, errEmptyField(DriverTLS, "ca_file")
	}
	td := s.trustDomain()

	get, err := hotReloadKeypair(s.CertFile, s.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(s.CAFile)
	if err != nil {
		return nil, err
	}

	serverCfg := &tls.Config{
		MinVersion:            tls.VersionTLS13,
		GetCertificate:        get,
		VerifyPeerCertificate: verifyURISANWellFormed(td),
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
		VerifyPeerCertificate: verifyURISANWellFormed(td),
	}

	return &ListenerCreds{
		ServerTLSConfig: serverCfg,
		ClientTLSConfig: clientCfg,
		Driver:          DriverTLS,
		SecurityLevel:   credentials.PrivacyAndIntegrity,
	}, nil
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
			return nil, fmt.Errorf("reflow/creds: load %s/%s: %w", certFile, keyFile, err)
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
		return nil, fmt.Errorf("reflow/creds: read CA %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("reflow/creds: parse CA %s: no PEM blocks", path)
	}
	return pool, nil
}

// verifyURISANWellFormed enforces the SPIFFE URI SAN shape on every
// verified leaf (exactly one URI, scheme=spiffe, host=trustDomain,
// non-empty path). Role checks happen in the auth interceptor.
func verifyURISANWellFormed(trustDomain string) func(rawCerts [][]byte, chains [][]*x509.Certificate) error {
	return func(_ [][]byte, chains [][]*x509.Certificate) error {
		if len(chains) == 0 || len(chains[0]) == 0 {
			return errors.New("reflow/creds: no verified chain")
		}
		_, err := ExtractSPIFFEURI(chains[0][0], trustDomain)
		return err
	}
}

// ExtractSPIFFEURI returns the leaf's single SPIFFE URI SAN as a string,
// after validating the same shape verifyURISANWellFormed enforces during
// TLS handshakes (exactly one URI SAN, scheme=spiffe, host=trustDomain,
// non-empty path). Used by the JWT signer to derive the iss claim from
// the engine's own leaf without duplicating the validation logic.
func ExtractSPIFFEURI(leaf *x509.Certificate, trustDomain string) (string, error) {
	if leaf == nil {
		return "", errors.New("reflow/creds: nil leaf certificate")
	}
	if len(leaf.URIs) != 1 {
		return "", fmt.Errorf("reflow/creds: leaf must carry exactly one URI SAN; got %d", len(leaf.URIs))
	}
	u := leaf.URIs[0]
	if u.Scheme != "spiffe" {
		return "", fmt.Errorf("reflow/creds: leaf URI scheme %q; want spiffe", u.Scheme)
	}
	if u.Host != trustDomain {
		return "", fmt.Errorf("reflow/creds: leaf trust domain %q; want %q", u.Host, trustDomain)
	}
	if len(u.Path) <= 1 {
		return "", fmt.Errorf("reflow/creds: leaf URI %q has empty path", u.String())
	}
	return u.String(), nil
}
