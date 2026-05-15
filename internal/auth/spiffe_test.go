package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"reflect"
	"testing"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// ctxWithURI builds a context carrying a peer.Info with one verified
// chain whose leaf has the given URI SAN. Empty uri leaves the leaf
// with no URIs.
func ctxWithURI(t *testing.T, uri string) context.Context {
	t.Helper()
	leaf := &x509.Certificate{}
	if uri != "" {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		leaf.URIs = []*url.URL{u}
	}
	tlsInfo := credentials.TLSInfo{
		State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}},
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: tlsInfo, Addr: &net.TCPAddr{}})
}

func TestSPIFFEExtractor_ParsesOperator(t *testing.T) {
	e := &SPIFFEExtractor{TrustDomain: "reflow.local"}
	p, err := e.Extract(ctxWithURI(t, "spiffe://reflow.local/operator/alice"))
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != "operator" || p.Subject != "alice" {
		t.Errorf("got %+v; want operator/alice", p)
	}
	if p.Raw != "operator/alice" {
		t.Errorf("Raw=%q; want operator/alice", p.Raw)
	}
	if p.URI != "spiffe://reflow.local/operator/alice" {
		t.Errorf("URI=%q", p.URI)
	}
}

func TestSPIFFEExtractor_ParsesNode(t *testing.T) {
	e := &SPIFFEExtractor{TrustDomain: "reflow.local"}
	p, err := e.Extract(ctxWithURI(t, "spiffe://reflow.local/node/3"))
	if err != nil {
		t.Fatal(err)
	}
	if p.Raw != "node/3" {
		t.Errorf("Raw=%q; want node/3", p.Raw)
	}
}

func TestSPIFFEExtractor_NoPeerReturnsAnonymous(t *testing.T) {
	e := &SPIFFEExtractor{TrustDomain: "reflow.local"}
	p, err := e.Extract(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !p.IsAnonymous() {
		t.Errorf("got %+v; want anonymous", p)
	}
}

func TestSPIFFEExtractor_RejectsMalformedURIs(t *testing.T) {
	cases := map[string]string{
		"no uri SAN":         "",
		"missing name":       "spiffe://reflow.local/operator",
		"empty kind":         "spiffe://reflow.local//alice",
		"empty name":         "spiffe://reflow.local/operator/",
		"wrong scheme":       "https://reflow.local/operator/alice",
		"wrong trust domain": "spiffe://elsewhere.local/operator/alice",
		"three segments":     "spiffe://reflow.local/op/extra/alice",
	}
	e := &SPIFFEExtractor{TrustDomain: "reflow.local"}
	for name, uri := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := e.Extract(ctxWithURI(t, uri))
			if err == nil {
				t.Errorf("expected error for %q", uri)
			}
		})
	}
}

func TestAnonExtractor_AlwaysAnonymous(t *testing.T) {
	p, err := AnonExtractor{}.Extract(ctxWithURI(t, "spiffe://reflow.local/operator/x"))
	if err != nil {
		t.Fatal(err)
	}
	if !p.IsAnonymous() {
		t.Errorf("got %+v; want anonymous", p)
	}
}

func TestPrincipalContextRoundTrip(t *testing.T) {
	want := Principal{Kind: "node", Subject: "7", Raw: "node/7", URI: "spiffe://reflow.local/node/7"}
	ctx := ContextWithPrincipal(context.Background(), want)
	got, ok := PrincipalFromContext(ctx)
	if !ok {
		t.Fatal("PrincipalFromContext returned !ok")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v; want %+v", got, want)
	}
}

func TestPrincipalFromContext_AbsentReturnsFalse(t *testing.T) {
	if _, ok := PrincipalFromContext(context.Background()); ok {
		t.Error("expected ok=false for empty context")
	}
}
