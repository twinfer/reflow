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

// TLSSpec is the file-driven TLS configuration. One CA signs every
// leaf; each leaf's CN encodes the principal Raw form (<kind>/<name>).
// The transport layer verifies chain + CN shape; role enforcement
// (node/* vs operator/*) is left to the auth interceptor.
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
	// server side. Default true for reflow's mTLS surfaces (Delivery,
	// Admin); set false only when fronting OAuth/JWT validation.
	ClientAuth *bool `koanf:"client_auth"`
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

// verifyMeshIdentity returns a VerifyPeerCertificate callback that
// enforces the mesh-identity contract on every verified leaf: the CN
// must match <kind>/<name>, and (when pin is non-empty) the chain
// root's SPKI fingerprint must equal pin. Role enforcement happens in
// the auth interceptor.
func verifyMeshIdentity(pin string) func(rawCerts [][]byte, chains [][]*x509.Certificate) error {
	return func(_ [][]byte, chains [][]*x509.Certificate) error {
		if len(chains) == 0 || len(chains[0]) == 0 {
			return errors.New("reflow/creds: no verified chain")
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
			return fmt.Errorf("reflow/creds: mesh CA fingerprint %s does not match pin %s", got, pin)
		}
		return nil
	}
}
