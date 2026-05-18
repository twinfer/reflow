package auth

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestPolicy_StarterAllowMatrix walks the embedded starter_policy.json
// matrix: ingress is open, delivery/admin gate on operator/* or node/*,
// SelfJoin has the node/* carve-out.
func TestPolicy_StarterAllowMatrix(t *testing.T) {
	pol, err := ParsePolicy([]byte(StarterPolicyJSON))
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	operator := Principal{Kind: "operator", Subject: "alice", Raw: "operator/alice"}
	node := Principal{Kind: "node", Subject: "7", Raw: "node/7"}
	anon := Principal{}

	cases := []struct {
		name  string
		path  string
		who   Principal
		allow bool
	}{
		{"ingress-anonymous-open", "/reflow.ingress.v1.Ingress/SubmitInvocation", anon, true},
		{"ingress-operator-also-allowed", "/reflow.ingress.v1.Ingress/SubmitInvocation", operator, true},
		{"admin-operator", "/reflow.admin.v1.Admin/ListNodes", operator, true},
		{"admin-node-denied", "/reflow.admin.v1.Admin/ListNodes", node, false},
		{"admin-anonymous-denied", "/reflow.admin.v1.Admin/ListNodes", anon, false},
		{"admin-selfjoin-node-allowed", "/reflow.admin.v1.Admin/SelfJoin", node, true},
		{"admin-selfjoin-operator-also-allowed", "/reflow.admin.v1.Admin/SelfJoin", operator, true},
		{"delivery-node", "/reflow.delivery.v1.Delivery/Deliver", node, true},
		{"delivery-operator-denied", "/reflow.delivery.v1.Delivery/Deliver", operator, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := pol.Allow(c.path, c.who); got != c.allow {
				t.Errorf("Allow(%q, %s) = %v; want %v", c.path, c.who.Raw, got, c.allow)
			}
		})
	}
}

// TestHTTPMiddleware_StampsPrincipalFromTLS feeds a synthetic
// *tls.ConnectionState with a verified spiffe leaf into the middleware
// and asserts the downstream handler observes the principal both in the
// header and via PrincipalFromContext.
func TestHTTPMiddleware_StampsPrincipalFromTLS(t *testing.T) {
	td := "reflow.local"
	mw, closer, err := HTTPMiddleware(td, "", nil)
	if err != nil {
		t.Fatalf("HTTPMiddleware: %v", err)
	}
	if closer != nil {
		defer closer()
	}

	var sawHeader string
	var sawPrincipal Principal
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHeader = r.Header.Get(PrincipalHeader)
		p, _ := PrincipalFromContext(r.Context())
		sawPrincipal = p
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	// Synthesize a verified-chain leaf with spiffe URI.
	u, _ := url.Parse("spiffe://" + td + "/operator/alice")
	leaf := &x509.Certificate{
		Subject: pkix.Name{CommonName: "alice"},
		URIs:    []*url.URL{u},
	}

	r := httptest.NewRequest("POST", "/reflow.admin.v1.Admin/ListNodes", nil)
	r.TLS = &tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{leaf}},
	}
	// Forged header — middleware must drop it before extracting from TLS.
	r.Header.Set(PrincipalHeader, "operator/eve")

	w := httptest.NewRecorder()
	mw(next).ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Result().StatusCode, w.Body.String())
	}
	if sawHeader != "operator/alice" {
		t.Errorf("header = %q; want operator/alice (forged operator/eve must be replaced)", sawHeader)
	}
	if sawPrincipal.Raw != "operator/alice" {
		t.Errorf("principal.Raw = %q; want operator/alice", sawPrincipal.Raw)
	}
}

// TestHTTPMiddleware_DeniesUnauthorizedPrincipal: node/7 hitting /Admin/ListNodes
// must be 403 because the embedded policy gates admin to operator/*.
func TestHTTPMiddleware_DeniesUnauthorizedPrincipal(t *testing.T) {
	td := "reflow.local"
	mw, _, err := HTTPMiddleware(td, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	u, _ := url.Parse("spiffe://" + td + "/node/7")
	leaf := &x509.Certificate{URIs: []*url.URL{u}}
	r := httptest.NewRequest("POST", "/reflow.admin.v1.Admin/ListNodes", nil)
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	w := httptest.NewRecorder()
	mw(next).ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("status = %d; want 403", w.Result().StatusCode)
	}
	if called {
		t.Error("downstream handler should not run on policy denial")
	}
}

// TestHTTPMiddleware_AllowsAnonymousIngress: ingress allow rule has no
// principal constraint; an anonymous (no TLS) request must pass through.
func TestHTTPMiddleware_AllowsAnonymousIngress(t *testing.T) {
	mw, _, err := HTTPMiddleware("reflow.local", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	r := httptest.NewRequest("POST", "/reflow.ingress.v1.Ingress/SubmitInvocation", nil)
	w := httptest.NewRecorder()
	mw(next).ServeHTTP(w, r)
	if !called {
		t.Errorf("anonymous ingress should pass; got status=%d", w.Result().StatusCode)
	}
}

// TestHTTPMiddleware_MalformedLeafRejected returns 401 on a leaf without
// a SPIFFE URI.
func TestHTTPMiddleware_MalformedLeafRejected(t *testing.T) {
	mw, _, err := HTTPMiddleware("reflow.local", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{Subject: pkix.Name{CommonName: "noop"}}
	r := httptest.NewRequest("POST", "/reflow.admin.v1.Admin/ListNodes", nil)
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	w := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", w.Result().StatusCode)
	}
}
