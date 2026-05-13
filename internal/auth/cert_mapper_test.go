package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"google.golang.org/grpc/credentials"
)

const testTrustDomain = "reflow.local"

// authInfoWithURI builds an AuthInfo whose TLSConnection carries a
// single leaf certificate with the given URI SAN. Empty uri produces a
// leaf with no URIs.
func authInfoWithURI(t *testing.T, uri string) AuthInfo {
	t.Helper()
	leaf := &x509.Certificate{}
	if uri != "" {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		leaf.URIs = []*url.URL{u}
	}
	state := tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	return AuthInfo{TLSConnection: &credentials.TLSInfo{State: state}}
}

func TestCertClaimMapper_ParsesOperatorURI(t *testing.T) {
	m := &CertClaimMapper{TrustDomain: testTrustDomain}
	claims, err := m.GetClaims(context.Background(),
		authInfoWithURI(t, "spiffe://reflow.local/operator/alice"))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if claims == nil {
		t.Fatal("expected claims, got nil")
	}
	if claims.Kind != "operator" || claims.Subject != "alice" {
		t.Errorf("got Kind=%q Subject=%q; want operator/alice", claims.Kind, claims.Subject)
	}
	if claims.URI.String() != "spiffe://reflow.local/operator/alice" {
		t.Errorf("URI mismatch: %s", claims.URI)
	}
}

func TestCertClaimMapper_ParsesNodeURI(t *testing.T) {
	m := &CertClaimMapper{TrustDomain: testTrustDomain}
	claims, err := m.GetClaims(context.Background(),
		authInfoWithURI(t, "spiffe://reflow.local/node/3"))
	if err != nil {
		t.Fatal(err)
	}
	if claims.Kind != "node" || claims.Subject != "3" {
		t.Errorf("got Kind=%q Subject=%q; want node/3", claims.Kind, claims.Subject)
	}
}

func TestCertClaimMapper_NoTLSInfo_ReturnsNoIdentity(t *testing.T) {
	m := &CertClaimMapper{TrustDomain: testTrustDomain}
	claims, err := m.GetClaims(context.Background(), AuthInfo{})
	if err != nil {
		t.Fatalf("expected (nil, nil), got err: %v", err)
	}
	if claims != nil {
		t.Errorf("expected nil claims, got %+v", claims)
	}
}

func TestCertClaimMapper_EmptyVerifiedChain_ReturnsNoIdentity(t *testing.T) {
	m := &CertClaimMapper{TrustDomain: testTrustDomain}
	info := AuthInfo{TLSConnection: &credentials.TLSInfo{
		State: tls.ConnectionState{},
	}}
	claims, err := m.GetClaims(context.Background(), info)
	if err != nil {
		t.Fatalf("expected (nil, nil), got err: %v", err)
	}
	if claims != nil {
		t.Errorf("expected nil claims, got %+v", claims)
	}
}

func TestCertClaimMapper_RejectsMalformedURIs(t *testing.T) {
	cases := map[string]string{
		"no uri SAN":         "",
		"missing name":       "spiffe://reflow.local/operator",
		"empty kind":         "spiffe://reflow.local//alice",
		"empty name":         "spiffe://reflow.local/operator/",
		"wrong scheme":       "https://reflow.local/operator/alice",
		"wrong trust domain": "spiffe://elsewhere.local/operator/alice",
		"three segments":     "spiffe://reflow.local/op/extra/alice",
		"trailing slash":     "spiffe://reflow.local/operator/alice/",
	}
	m := &CertClaimMapper{TrustDomain: testTrustDomain}
	for name, uri := range cases {
		t.Run(name, func(t *testing.T) {
			claims, err := m.GetClaims(context.Background(), authInfoWithURI(t, uri))
			if err == nil {
				t.Errorf("expected error for %q (case=%s); got claims=%+v", uri, name, claims)
			}
		})
	}
}

func TestClaimsFromContext_RoundTrip(t *testing.T) {
	u, _ := url.Parse("spiffe://reflow.local/operator/bob")
	want := &Claims{Kind: "operator", Subject: "bob", URI: u}
	ctx := ContextWithClaims(context.Background(), want)
	got, ok := ClaimsFromContext(ctx)
	if !ok {
		t.Fatal("ClaimsFromContext returned !ok")
	}
	if got != want {
		t.Errorf("got %+v; want %+v", got, want)
	}
}

func TestClaimsFromContext_AbsentReturnsNotOK(t *testing.T) {
	if _, ok := ClaimsFromContext(context.Background()); ok {
		t.Error("expected ok=false for empty context")
	}
}
