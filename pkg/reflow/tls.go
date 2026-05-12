package reflow

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// TLSFiles names the four PEM files that drive reflow's mTLS surface.
//
// Two trust anchors:
//
//   - NodeCAFile signs every node's leaf cert. The Delivery server's
//     ClientCAs and outbound Delivery clients' RootCAs both anchor on
//     this CA.
//
//   - OperatorCAFile signs every operator's leaf cert. The Admin server's
//     ClientCAs anchor on this CA. Operator certs are not trusted on the
//     Delivery port and node certs are not trusted on the Admin port.
//
// One node-leaf cert (NodeCertFile + NodeKeyFile) is presented by the
// server on both ports — operators talking to Admin verify it against
// the NodeCA they were issued by, and peer Delivery clients do the
// same. The two-CA split lets operator-cert rotation run independently
// of node-cert rotation. Phase 4.2.
type TLSFiles struct {
	NodeCAFile     string
	OperatorCAFile string
	NodeCertFile   string
	NodeKeyFile    string
}

// requireForServer validates that every field needed to terminate TLS
// on a server is populated.
func (f TLSFiles) requireForServer() error {
	if f.NodeCertFile == "" || f.NodeKeyFile == "" {
		return errors.New("reflow/tls: NodeCertFile and NodeKeyFile are required")
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
// mtime) snapshots via an atomic pointer swap. Phase 4.2.
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

// BuildDeliveryServerTLS produces the Delivery-port TLS config: server
// presents the node leaf cert; client certs must verify against the node
// CA (RequireAndVerifyClientCert). Anchored on node CA in both
// directions because the only legitimate peers are other reflowd nodes.
func BuildDeliveryServerTLS(f TLSFiles) (*tls.Config, error) {
	if err := f.requireForServer(); err != nil {
		return nil, err
	}
	if f.NodeCAFile == "" {
		return nil, errors.New("reflow/tls: NodeCAFile required for Delivery server")
	}
	get, err := hotReloadCert(f.NodeCertFile, f.NodeKeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(f.NodeCAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		GetCertificate: get,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		ClientCAs:      pool,
		MinVersion:     tls.VersionTLS13,
	}, nil
}

// BuildDeliveryClientTLS produces the outbound Delivery client TLS
// config: presents the node leaf cert; trusts the node CA for the
// remote server.
func BuildDeliveryClientTLS(f TLSFiles) (*tls.Config, error) {
	if err := f.requireForServer(); err != nil {
		return nil, err
	}
	if f.NodeCAFile == "" {
		return nil, errors.New("reflow/tls: NodeCAFile required for Delivery client")
	}
	get, err := hotReloadCert(f.NodeCertFile, f.NodeKeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(f.NodeCAFile)
	if err != nil {
		return nil, err
	}
	// Outbound dial: gRPC uses GetClientCertificate (separate hook from
	// GetCertificate). We wrap the same loader.
	return &tls.Config{
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return get(nil)
		},
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	}, nil
}

// BuildAdminServerTLS produces the Admin-port TLS config: server presents
// the node leaf cert; client certs must verify against the operator CA.
// Only operators-with-valid-operator-certs reach Admin. Phase 4.2.
func BuildAdminServerTLS(f TLSFiles) (*tls.Config, error) {
	if err := f.requireForServer(); err != nil {
		return nil, err
	}
	if f.OperatorCAFile == "" {
		return nil, errors.New("reflow/tls: OperatorCAFile required for Admin server")
	}
	get, err := hotReloadCert(f.NodeCertFile, f.NodeKeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(f.OperatorCAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		GetCertificate: get,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		ClientCAs:      pool,
		MinVersion:     tls.VersionTLS13,
	}, nil
}

// BuildAdminClientTLS produces an operator-side TLS config for talking
// to the Admin server. Caller supplies an operator leaf cert + key.
// caFile is the node CA, used to verify the server's node cert. Phase 4.2.
//
// Used by the reflow-cluster CLI; not invoked from inside reflowd.
func BuildAdminClientTLS(operatorCertFile, operatorKeyFile, nodeCAFile string) (*tls.Config, error) {
	if operatorCertFile == "" || operatorKeyFile == "" || nodeCAFile == "" {
		return nil, errors.New("reflow/tls: operator cert+key and node CA are required")
	}
	get, err := hotReloadCert(operatorCertFile, operatorKeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(nodeCAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return get(nil)
		},
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	}, nil
}
