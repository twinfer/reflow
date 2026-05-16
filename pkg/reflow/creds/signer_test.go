package creds

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/credentials/tls/certprovider"
)

// fakeProvider is a minimal certprovider.Provider returning a fixed
// tls.Certificate. The signer never touches roots, so KeyMaterial.Roots
// stays nil.
type fakeProvider struct{ cert tls.Certificate }

func (p *fakeProvider) KeyMaterial(_ context.Context) (*certprovider.KeyMaterial, error) {
	return &certprovider.KeyMaterial{Certs: []tls.Certificate{p.cert}}, nil
}

func (p *fakeProvider) Close() {}

func makeCert(t *testing.T, key any, pub any, trustDomain, spiffePath string) tls.Certificate {
	t.Helper()
	uri, err := url.Parse("spiffe://" + trustDomain + spiffePath)
	if err != nil {
		t.Fatalf("parse spiffe url: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func TestSigner_ECDSAP256(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	cert := makeCert(t, key, &key.PublicKey, "reflow.local", "/node/1")
	s := NewSigner(&fakeProvider{cert: cert}, "reflow.local")

	tok, err := s.Sign("dep-abc")
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	verifySignedToken(t, tok, "spiffe://reflow.local/node/1", "dep-abc", &key.PublicKey, jwt.SigningMethodES256)
}

func TestSigner_Ed25519(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	cert := makeCert(t, priv, pub, "reflow.local", "/node/2")
	s := NewSigner(&fakeProvider{cert: cert}, "reflow.local")

	tok, err := s.Sign("dep-xyz")
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	verifySignedToken(t, tok, "spiffe://reflow.local/node/2", "dep-xyz", pub, jwt.SigningMethodEdDSA)
}

func TestSigner_RSA(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	cert := makeCert(t, key, &key.PublicKey, "reflow.local", "/node/3")
	s := NewSigner(&fakeProvider{cert: cert}, "reflow.local")

	tok, err := s.Sign("dep-rsa")
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	verifySignedToken(t, tok, "spiffe://reflow.local/node/3", "dep-rsa", &key.PublicKey, jwt.SigningMethodRS256)
}

func TestSigner_EmptyAudienceRejected(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeCert(t, key, &key.PublicKey, "reflow.local", "/node/1")
	s := NewSigner(&fakeProvider{cert: cert}, "reflow.local")
	if _, err := s.Sign(""); err == nil {
		t.Fatal("expected error for empty audience; got nil")
	}
}

func TestSigner_WrongTrustDomainRejected(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cert := makeCert(t, key, &key.PublicKey, "other.local", "/node/1")
	s := NewSigner(&fakeProvider{cert: cert}, "reflow.local")
	if _, err := s.Sign("dep-1"); err == nil {
		t.Fatal("expected error for trust-domain mismatch; got nil")
	}
}

func verifySignedToken(t *testing.T, tok, wantIss, wantAud string, pub any, want jwt.SigningMethod) {
	t.Helper()
	parsed, err := jwt.Parse(tok, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != want.Alg() {
			return nil, fmt.Errorf("unexpected alg %s; want %s", token.Method.Alg(), want.Alg())
		}
		return pub, nil
	}, jwt.WithValidMethods([]string{want.Alg()}))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !parsed.Valid {
		t.Fatal("token not valid")
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		t.Fatalf("unexpected claims type %T", parsed.Claims)
	}
	if got, _ := claims["iss"].(string); got != wantIss {
		t.Errorf("iss = %q; want %q", got, wantIss)
	}
	switch a := claims["aud"].(type) {
	case string:
		if a != wantAud {
			t.Errorf("aud = %q; want %q", a, wantAud)
		}
	case []any:
		if len(a) != 1 {
			t.Errorf("aud = %v; want one entry", a)
		} else if v, _ := a[0].(string); v != wantAud {
			t.Errorf("aud[0] = %q; want %q", v, wantAud)
		}
	default:
		t.Errorf("aud has unexpected type %T: %v", a, a)
	}
	if _, ok := claims["exp"].(float64); !ok {
		t.Error("exp missing or wrong type")
	}
	x5c, ok := parsed.Header["x5c"].([]any)
	if !ok || len(x5c) == 0 {
		t.Fatalf("x5c header missing or wrong shape: %v", parsed.Header["x5c"])
	}
	derStr, _ := x5c[0].(string)
	der, err := base64.StdEncoding.DecodeString(derStr)
	if err != nil {
		t.Fatalf("decode x5c: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse x5c cert: %v", err)
	}
	if len(cert.URIs) != 1 || cert.URIs[0].String() != wantIss {
		t.Errorf("x5c cert URI = %v; want %q", cert.URIs, wantIss)
	}
}
