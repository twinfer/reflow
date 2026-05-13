package reflow

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

// TLSFiles names the three PEM files that drive reflow's mTLS surface.
//
// One CA signs every leaf cert (both node and operator). Role is encoded
// in each leaf's SPIFFE URI SAN, not in which chain it validates against:
//
//   - Node leaves carry URI spiffe://<trust-domain>/node/<id>.
//   - Operator leaves carry URI spiffe://<trust-domain>/operator/<name>.
//
// The Delivery server's VerifyPeerCertificate requires node/* URIs;
// the Admin server's requires operator/*. Cross-role certs (e.g. an
// operator cert presented to the Delivery port) fail the handshake even
// though the chain validates, because the URI prefix doesn't match.
type TLSFiles struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

// requireForServer validates that every field needed to terminate TLS
// on a server is populated.
func (f TLSFiles) requireForServer() error {
	if f.CertFile == "" || f.KeyFile == "" {
		return errors.New("reflow/tls: CertFile and KeyFile are required")
	}
	if f.CAFile == "" {
		return errors.New("reflow/tls: CAFile is required")
	}
	return nil
}

// loadCAPool reads a PEM bundle and returns an x509 cert pool. An empty
// path returns (nil, nil) — callers decide whether that is fatal.
func loadCAPool(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reflow/tls: read CA %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("reflow/tls: parse CA %s: no PEM blocks", path)
	}
	return pool, nil
}

// hotReloadCert returns a tls.Config.GetCertificate callback that re-reads
// certFile / keyFile when either's mtime advances. The first read happens
// inside this constructor so configuration errors fail fast at startup.
// Subsequent reads happen on TLS handshakes; readers see consistent (cert,
// mtime) snapshots via an atomic pointer swap.
func hotReloadCert(certFile, keyFile string) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("reflow/tls: certFile and keyFile required")
	}
	type snap struct {
		cert  tls.Certificate
		mtime int64
	}
	var ptr atomic.Pointer[snap]
	var mu sync.Mutex

	loadOnce := func() (*snap, error) {
		c, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("reflow/tls: load %s/%s: %w", certFile, keyFile, err)
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
		// Cheap mtime check before re-loading.
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
					// Re-check under lock to avoid duplicate reloads.
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

// verifyURISANRole returns a VerifyPeerCertificate callback that requires
// the verified leaf's first URI SAN to match
// spiffe://<trustDomain>/<role>/<non-empty-name>. Defense-in-depth on
// top of x509 chain validation, so an operator cert can't reach the
// Delivery port (and vice versa) even though one CA signs both kinds.
func verifyURISANRole(trustDomain, role string) func(rawCerts [][]byte, chains [][]*x509.Certificate) error {
	wantPrefix := "/" + role + "/"
	return func(_ [][]byte, chains [][]*x509.Certificate) error {
		if len(chains) == 0 || len(chains[0]) == 0 {
			return errors.New("reflow/tls: no verified chain")
		}
		leaf := chains[0][0]
		if len(leaf.URIs) != 1 {
			return fmt.Errorf("reflow/tls: leaf must carry exactly one URI SAN; got %d", len(leaf.URIs))
		}
		u := leaf.URIs[0]
		if u.Scheme != "spiffe" {
			return fmt.Errorf("reflow/tls: leaf URI scheme %q; want spiffe", u.Scheme)
		}
		if u.Host != trustDomain {
			return fmt.Errorf("reflow/tls: leaf trust domain %q; want %q", u.Host, trustDomain)
		}
		if !strings.HasPrefix(u.Path, wantPrefix) || len(u.Path) <= len(wantPrefix) {
			return fmt.Errorf("reflow/tls: leaf URI %q; want prefix spiffe://%s%s<name>",
				u.String(), trustDomain, wantPrefix)
		}
		return nil
	}
}

// BuildDeliveryServerTLS produces the Delivery-port TLS config: server
// presents the node leaf cert; client certs must verify against the
// shared CA AND carry a spiffe://<trustDomain>/node/* URI SAN.
func BuildDeliveryServerTLS(f TLSFiles, trustDomain string) (*tls.Config, error) {
	if err := f.requireForServer(); err != nil {
		return nil, err
	}
	if trustDomain == "" {
		return nil, errors.New("reflow/tls: trust domain is required")
	}
	get, err := hotReloadCert(f.CertFile, f.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(f.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		GetCertificate:        get,
		ClientAuth:            tls.RequireAndVerifyClientCert,
		ClientCAs:             pool,
		VerifyPeerCertificate: verifyURISANRole(trustDomain, "node"),
		MinVersion:            tls.VersionTLS13,
	}, nil
}

// BuildDeliveryClientTLS produces the outbound Delivery client TLS
// config: presents the node leaf cert; trusts the shared CA for the
// remote server and requires the server's leaf to be a node/* SVID.
func BuildDeliveryClientTLS(f TLSFiles, trustDomain string) (*tls.Config, error) {
	if err := f.requireForServer(); err != nil {
		return nil, err
	}
	if trustDomain == "" {
		return nil, errors.New("reflow/tls: trust domain is required")
	}
	get, err := hotReloadCert(f.CertFile, f.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(f.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return get(nil)
		},
		RootCAs:               pool,
		VerifyPeerCertificate: verifyURISANRole(trustDomain, "node"),
		MinVersion:            tls.VersionTLS13,
	}, nil
}

// BuildAdminServerTLS produces the Admin-port TLS config: server presents
// the node leaf cert; client certs must verify against the shared CA AND
// carry a spiffe://<trustDomain>/operator/* URI SAN.
func BuildAdminServerTLS(f TLSFiles, trustDomain string) (*tls.Config, error) {
	if err := f.requireForServer(); err != nil {
		return nil, err
	}
	if trustDomain == "" {
		return nil, errors.New("reflow/tls: trust domain is required")
	}
	get, err := hotReloadCert(f.CertFile, f.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(f.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		GetCertificate:        get,
		ClientAuth:            tls.RequireAndVerifyClientCert,
		ClientCAs:             pool,
		VerifyPeerCertificate: verifyURISANRole(trustDomain, "operator"),
		MinVersion:            tls.VersionTLS13,
	}, nil
}

// BuildAdminClientTLS produces an operator-side TLS config for talking
// to the Admin server. The caller supplies an operator leaf cert + key
// plus the shared CA used to verify the server's node cert. The server
// presents a node/* SVID, which the verifier here checks.
//
// Used by the reflow-cluster CLI; not invoked from inside reflowd.
func BuildAdminClientTLS(operatorCertFile, operatorKeyFile, caFile, trustDomain string) (*tls.Config, error) {
	if operatorCertFile == "" || operatorKeyFile == "" || caFile == "" {
		return nil, errors.New("reflow/tls: operator cert+key and CA are required")
	}
	if trustDomain == "" {
		return nil, errors.New("reflow/tls: trust domain is required")
	}
	get, err := hotReloadCert(operatorCertFile, operatorKeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return get(nil)
		},
		RootCAs:               pool,
		VerifyPeerCertificate: verifyURISANRole(trustDomain, "node"),
		MinVersion:            tls.VersionTLS13,
	}, nil
}
