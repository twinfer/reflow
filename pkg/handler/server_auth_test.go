package handler

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc/credentials/tls/certprovider"

	"github.com/twinfer/reflow/pkg/reflow/creds"
)

// testCAAndLeaf builds a self-signed CA + a leaf signed by it carrying
// a SPIFFE URI. Returns the CA PEM and a creds.Signer wrapping the
// leaf. Kept inline because the helper in pkg/reflow/creds/*_test.go
// lives in a different package.
func testCAAndLeaf(t *testing.T, spiffePath string) (caPEM []byte, signer *creds.Signer, spiffe string) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("CA cert: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	uri, _ := url.Parse("spiffe://reflow.local" + spiffePath)
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-leaf"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{uri},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	leaf, _ := x509.ParseCertificate(leafDER)
	cert := tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey, Leaf: leaf}
	signer = creds.NewSigner(&fakeCertProvider{cert: cert}, "reflow.local")
	return caPEM, signer, "spiffe://reflow.local" + spiffePath
}

// fakeCertProvider implements certprovider.Provider with a fixed leaf
// cert. The Signer never touches roots, so the Roots field stays nil.
type fakeCertProvider struct{ cert tls.Certificate }

func (p *fakeCertProvider) KeyMaterial(_ context.Context) (*certprovider.KeyMaterial, error) {
	return &certprovider.KeyMaterial{Certs: []tls.Certificate{p.cert}}, nil
}
func (p *fakeCertProvider) Close() {}

func TestWithAuth_MissingHeader(t *testing.T) {
	caPEM, _, spiffe := testCAAndLeaf(t, "/node/1")
	v, err := creds.NewVerifier(caPEM, []string{spiffe}, "reflow.local", "")
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	called := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke/S/h", nil)
	withAuth(v, nil, next).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Error("WWW-Authenticate header missing")
	}
	if called {
		t.Error("next handler was called despite auth failure")
	}
}

func TestWithAuth_BadBearer(t *testing.T) {
	caPEM, _, spiffe := testCAAndLeaf(t, "/node/1")
	v, _ := creds.NewVerifier(caPEM, []string{spiffe}, "reflow.local", "")
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke/S/h", nil)
	req.Header.Set("Authorization", "Basic abc")
	withAuth(v, nil, next).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", rec.Code)
	}
}

func TestWithAuth_BadToken(t *testing.T) {
	caPEM, _, spiffe := testCAAndLeaf(t, "/node/1")
	v, _ := creds.NewVerifier(caPEM, []string{spiffe}, "reflow.local", "")
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke/S/h", nil)
	req.Header.Set("Authorization", "Bearer not.a.real.jwt")
	withAuth(v, nil, next).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", rec.Code)
	}
}

func TestWithAuth_ValidToken_PopulatesContext(t *testing.T) {
	caPEM, signer, spiffe := testCAAndLeaf(t, "/node/1")
	v, _ := creds.NewVerifier(caPEM, []string{spiffe}, "reflow.local", "")
	tok, err := signer.Sign("dep-test")
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	var seenURI string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if got, ok := CallerURI(r.Context()); ok {
			seenURI = got
		}
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke/S/h", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	withAuth(v, nil, next).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	if seenURI != spiffe {
		t.Errorf("CallerURI in next = %q; want %q", seenURI, spiffe)
	}
}

func TestCallerURI_NoMiddleware(t *testing.T) {
	if got, ok := CallerURI(context.Background()); ok || got != "" {
		t.Errorf("CallerURI on bare ctx = (%q, %v); want empty/false", got, ok)
	}
}

func TestValidateConfig_AuthFieldsWithoutRoots(t *testing.T) {
	reg := NewRegistry()
	for _, cfg := range []Config{
		{Registry: reg, AllowedSPIFFE: []string{"x"}},
		{Registry: reg, TrustDomain: "x"},
		{Registry: reg, ExpectedAudience: "x"},
	} {
		if _, err := validateConfig(&cfg); err == nil {
			t.Errorf("expected error for partial auth config %+v; got nil", cfg)
		}
	}
}

func TestValidateConfig_RootCAsRequiresAllowlist(t *testing.T) {
	caPEM, _, _ := testCAAndLeaf(t, "/node/1")
	cfg := Config{Registry: NewRegistry(), RootCAs: caPEM}
	if _, err := validateConfig(&cfg); err == nil {
		t.Fatal("expected error: RootCAs set without AllowedSPIFFE")
	}
}

func TestValidateConfig_NoAuthIsPassthrough(t *testing.T) {
	cfg := Config{Registry: NewRegistry()}
	v, err := validateConfig(&cfg)
	if err != nil {
		t.Fatalf("validateConfig: %v", err)
	}
	if v != nil {
		t.Errorf("verifier = %v; want nil", v)
	}
}
