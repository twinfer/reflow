package creds

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
)

func TestBuild_ZeroSpecIsInsecure(t *testing.T) {
	lc, err := Build(Spec{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lc.Driver != DriverInsecure {
		t.Errorf("Driver=%q; want insecure", lc.Driver)
	}
	if lc.SecurityLevel != credentials.NoSecurity {
		t.Errorf("SecurityLevel=%v; want NoSecurity", lc.SecurityLevel)
	}
	if lc.Server == nil || len(lc.ClientDial) == 0 {
		t.Error("Server or ClientDial unset")
	}
}

func TestBuild_UnknownDriverErrors(t *testing.T) {
	if _, err := Build(Spec{Driver: "bogus"}, nil); err == nil {
		t.Error("expected error for unknown driver")
	}
}

func TestBuild_MissingNestedSpecErrors(t *testing.T) {
	cases := []Driver{DriverTLS, DriverCertProvider, DriverOAuth, DriverJWT, DriverSTS}
	for _, d := range cases {
		t.Run(string(d), func(t *testing.T) {
			if _, err := Build(Spec{Driver: d}, nil); err == nil {
				t.Errorf("expected error for driver %q with nil nested spec", d)
			}
		})
	}
}

func TestBuild_TLS(t *testing.T) {
	dir := t.TempDir()
	caFile, certFile, keyFile := writeSPIFFETestPKI(t, dir, "spiffe://reflow.local/node/1")

	lc, err := Build(Spec{
		Driver: DriverTLS,
		TLS: &TLSSpec{
			CAFile:      caFile,
			CertFile:    certFile,
			KeyFile:     keyFile,
			TrustDomain: "reflow.local",
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lc.Driver != DriverTLS {
		t.Errorf("Driver=%q; want tls", lc.Driver)
	}
	if lc.SecurityLevel != credentials.PrivacyAndIntegrity {
		t.Errorf("SecurityLevel=%v; want PrivacyAndIntegrity", lc.SecurityLevel)
	}
	if lc.Server == nil || len(lc.ClientDial) == 0 {
		t.Error("Server or ClientDial unset")
	}
}

func TestBuild_TLSMissingCAErrors(t *testing.T) {
	dir := t.TempDir()
	_, certFile, keyFile := writeSPIFFETestPKI(t, dir, "spiffe://reflow.local/node/1")
	_, err := Build(Spec{
		Driver: DriverTLS,
		TLS:    &TLSSpec{CertFile: certFile, KeyFile: keyFile},
	}, nil)
	if err == nil {
		t.Error("expected error when CAFile is empty")
	}
}

func TestBuild_TransportOnlyDrivers(t *testing.T) {
	// Constructors that need no on-disk material and no environment
	// (handshake happens later on dial). Each should produce a
	// PrivacyAndIntegrity-level listener.
	cases := []struct {
		name string
		spec Spec
	}{
		{"alts", Spec{Driver: DriverALTS}},
		{"local", Spec{Driver: DriverLocal}},
		{"google", Spec{Driver: DriverGoogle}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lc, err := Build(c.spec, nil)
			if err != nil {
				t.Fatal(err)
			}
			if lc.Server == nil || len(lc.ClientDial) == 0 {
				t.Error("Server or ClientDial unset")
			}
			if lc.SecurityLevel != credentials.PrivacyAndIntegrity {
				t.Errorf("SecurityLevel=%v; want PrivacyAndIntegrity", lc.SecurityLevel)
			}
		})
	}
}

func TestBuild_OAuthStaticToken(t *testing.T) {
	lc, err := Build(Spec{
		Driver: DriverOAuth,
		OAuth:  &OAuthSpec{StaticToken: "test-token"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lc.PerRPC == nil {
		t.Error("PerRPC unset")
	}
	if lc.Driver != DriverOAuth {
		t.Errorf("Driver=%q; want oauth", lc.Driver)
	}
}

func TestBuild_STSRequiredFields(t *testing.T) {
	// Empty spec → first required-field error wins; the test asserts
	// the driver rejects rather than passes through to grpc-go.
	if _, err := Build(Spec{Driver: DriverSTS, STS: &STSSpec{}}, nil); err == nil {
		t.Error("expected error for empty STSSpec")
	}
}

// writeSPIFFETestPKI generates a self-signed ed25519 CA and a single
// leaf cert with the given SPIFFE URI SAN, returning paths to PEM files
// in dir. Used by TLS-driver tests to materialise valid SVIDs without
// pulling in the testdata fixtures from internal/pki.
func writeSPIFFETestPKI(t *testing.T, dir, spiffeURI string) (caFile, certFile, keyFile string) {
	t.Helper()

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "reflow-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caPub, caPriv)
	if err != nil {
		t.Fatal(err)
	}

	leafPub, leafPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(spiffeURI)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "reflow-test-leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{u},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caTmpl, leafPub, caPriv)
	if err != nil {
		t.Fatal(err)
	}

	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "leaf.pem")
	keyFile = filepath.Join(dir, "leaf.key")
	writePEM(t, caFile, "CERTIFICATE", caDER)
	writePEM(t, certFile, "CERTIFICATE", leafDER)
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafPriv)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, keyFile, "PRIVATE KEY", keyDER)
	return caFile, certFile, keyFile
}

func writePEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: kind, Bytes: der}); err != nil {
		t.Fatal(err)
	}
}
