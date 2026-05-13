package admin

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// ctxWithSpiffeURI returns a context that looks to peerIdentity / the
// audit interceptor like an mTLS-verified call from a leaf carrying uri.
func ctxWithSpiffeURI(t *testing.T, uri string) context.Context {
	t.Helper()
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{URIs: []*url.URL{u}}
	state := tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: state},
	})
}

func TestPeerIdentity_ParsesOperatorURI(t *testing.T) {
	ctx := ctxWithSpiffeURI(t, "spiffe://reflow.local/operator/alice")
	id, ok := peerIdentity(ctx)
	if !ok {
		t.Fatal("peerIdentity returned !ok for a valid operator URI")
	}
	if id.Kind != "operator" || id.Name != "alice" {
		t.Errorf("got Kind=%q Name=%q; want operator/alice", id.Kind, id.Name)
	}
	if id.URI.String() != "spiffe://reflow.local/operator/alice" {
		t.Errorf("URI mismatch: %s", id.URI)
	}
}

func TestPeerIdentity_RejectsMalformedURIs(t *testing.T) {
	cases := []string{
		"",                                      // empty
		"spiffe://reflow.local/operator",        // missing name segment
		"spiffe://reflow.local//alice",          // empty kind
		"spiffe://reflow.local/operator/",       // empty name
		"https://reflow.local/operator/alice",   // wrong scheme
		"spiffe:///operator/alice",              // empty trust domain
		"spiffe://reflow.local/op/extra/alice",  // 3 path segments rejected
		"spiffe://reflow.local/operator/alice/", // trailing slash → 3rd empty segment
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			if c == "" {
				// no URI on the leaf at all
				leaf := &x509.Certificate{}
				state := tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
				ctx := peer.NewContext(context.Background(), &peer.Peer{
					AuthInfo: credentials.TLSInfo{State: state},
				})
				if _, ok := peerIdentity(ctx); ok {
					t.Errorf("peerIdentity returned ok for leaf without URI SAN")
				}
				return
			}
			ctx := ctxWithSpiffeURI(t, c)
			if _, ok := peerIdentity(ctx); ok {
				t.Errorf("peerIdentity returned ok for malformed URI %q", c)
			}
		})
	}
}

func TestPeerIdentityFromContext_RoundTrip(t *testing.T) {
	u, _ := url.Parse("spiffe://reflow.local/operator/bob")
	want := PeerIdentity{Kind: "operator", Name: "bob", URI: u}
	ctx := context.WithValue(context.Background(), peerIdentityCtxKey{}, want)
	got, ok := PeerIdentityFromContext(ctx)
	if !ok {
		t.Fatal("not ok")
	}
	if got != want {
		t.Errorf("got %+v; want %+v", got, want)
	}
}
