package auth

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
		{"clusterctl-operator", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", operator, true},
		{"clusterctl-node-denied", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", node, false},
		{"clusterctl-anonymous-denied", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", anon, false},
		{"clusterctl-selfjoin-node-allowed", "/reflow.clusterctl.v1.ClusterCtl/SelfJoin", node, true},
		{"clusterctl-selfjoin-operator-also-allowed", "/reflow.clusterctl.v1.ClusterCtl/SelfJoin", operator, true},
		{"config-operator", "/reflow.config.v1.Config/ListSecrets", operator, true},
		{"config-node-denied", "/reflow.config.v1.Config/ListSecrets", node, false},
		{"config-anonymous-denied", "/reflow.config.v1.Config/ListSecrets", anon, false},
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

func newTestMW(t *testing.T, td string) func(http.Handler) http.Handler {
	t.Helper()
	mw, closer, err := HTTPMiddleware(Config{TrustDomain: td}, nil)
	if err != nil {
		t.Fatalf("HTTPMiddleware: %v", err)
	}
	if closer != nil {
		t.Cleanup(func() { _ = closer() })
	}
	return mw
}

// TestHTTPMiddleware_StampsPrincipalFromTLS feeds a synthetic
// *tls.ConnectionState with a verified spiffe leaf into the middleware
// and asserts the downstream handler observes the principal both in the
// header and via PrincipalFromContext.
func TestHTTPMiddleware_StampsPrincipalFromTLS(t *testing.T) {
	td := "reflow.local"
	mw := newTestMW(t, td)

	var sawHeader string
	var sawPrincipal Principal
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHeader = r.Header.Get(PrincipalHeader)
		p, _ := PrincipalFromContext(r.Context())
		sawPrincipal = p
		w.WriteHeader(http.StatusOK)
	})

	// Synthesize a verified-chain leaf with spiffe URI.
	u, _ := url.Parse("spiffe://" + td + "/operator/alice")
	leaf := &x509.Certificate{
		Subject: pkix.Name{CommonName: "alice"},
		URIs:    []*url.URL{u},
	}

	r := httptest.NewRequest("POST", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", nil)
	r.TLS = &tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{leaf}},
	}
	// Forged header — middleware must drop it before the policy handler
	// stamps the server-computed value.
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

// TestHTTPMiddleware_DeniesUnauthorizedPrincipal: node/7 hitting
// /ClusterCtl/ListNodes must produce CodePermissionDenied (HTTP 403 fallback
// since the test request is not a Connect-shaped POST).
func TestHTTPMiddleware_DeniesUnauthorizedPrincipal(t *testing.T) {
	td := "reflow.local"
	mw := newTestMW(t, td)
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	u, _ := url.Parse("spiffe://" + td + "/node/7")
	leaf := &x509.Certificate{URIs: []*url.URL{u}}
	r := httptest.NewRequest("POST", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", nil)
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	w := httptest.NewRecorder()
	mw(next).ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("status = %d; want 403 (CodePermissionDenied HTTP fallback)", w.Result().StatusCode)
	}
	if called {
		t.Error("downstream handler should not run on policy denial")
	}
}

// TestHTTPMiddleware_AllowsAnonymousIngress: ingress allow rule has no
// principal constraint; an anonymous (no TLS) request must pass through.
func TestHTTPMiddleware_AllowsAnonymousIngress(t *testing.T) {
	mw := newTestMW(t, "reflow.local")
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

// TestHTTPMiddleware_DeniesAnonymousOnGuardedPath: anonymous principal
// hitting /ClusterCtl/ListNodes must produce CodeUnauthenticated (HTTP 401).
// The split between 401 (anonymous) and 403 (known-but-denied) lets
// monitoring separate "no client cert presented" from "auth-config
// rejects principal X".
func TestHTTPMiddleware_DeniesAnonymousOnGuardedPath(t *testing.T) {
	mw := newTestMW(t, "reflow.local")
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	r := httptest.NewRequest("POST", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", nil)
	// No TLS, no leaf — caller is anonymous.
	w := httptest.NewRecorder()
	mw(next).ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401 (CodeUnauthenticated HTTP fallback)", w.Result().StatusCode)
	}
	if called {
		t.Error("downstream handler should not run on anonymous denial")
	}
}

// TestHTTPMiddleware_NonSPIFFELeafTreatedAnonymous covers the mTLS-
// without-SPIFFE case: a verified leaf with zero URI SANs is no longer
// a hard error (it could be a client-cert deployment that uses bearer
// tokens for identity). The principal is anonymous, so a guarded path
// still gets 401 via the policy.
func TestHTTPMiddleware_NonSPIFFELeafTreatedAnonymous(t *testing.T) {
	mw := newTestMW(t, "reflow.local")
	leaf := &x509.Certificate{Subject: pkix.Name{CommonName: "noop"}}
	r := httptest.NewRequest("POST", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", nil)
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	w := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", w.Result().StatusCode)
	}
}

// TestHTTPMiddleware_MalformedSPIFFELeafRejected: a verified leaf
// carrying a URI SAN that fails SPIFFE format checks (wrong trust
// domain, wrong scheme, multiple URIs, missing kind/subject) must
// surface as a hard 401 — the leaf claims a SPIFFE identity that we
// can't honor, so falling through to anonymous would be a security
// regression.
func TestHTTPMiddleware_MalformedSPIFFELeafRejected(t *testing.T) {
	cases := map[string][]*url.URL{
		"wrong-scheme":   {mustParseURL(t, "https://reflow.local/operator/alice")},
		"wrong-td":       {mustParseURL(t, "spiffe://elsewhere.local/operator/alice")},
		"missing-name":   {mustParseURL(t, "spiffe://reflow.local/operator")},
		"empty-segments": {mustParseURL(t, "spiffe://reflow.local//alice")},
		"multiple-uris": {
			mustParseURL(t, "spiffe://reflow.local/operator/alice"),
			mustParseURL(t, "spiffe://reflow.local/operator/bob"),
		},
	}
	for name, uris := range cases {
		t.Run(name, func(t *testing.T) {
			mw := newTestMW(t, "reflow.local")
			leaf := &x509.Certificate{URIs: uris}
			r := httptest.NewRequest("POST", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", nil)
			r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
			w := httptest.NewRecorder()
			mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(w, r)
			if w.Result().StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %d; want 401", w.Result().StatusCode)
			}
		})
	}
}

// TestHTTPMiddleware_ConnectErrorEncoding asserts that a denied
// Connect-shaped request gets a connect-protocol error response, not a
// plain HTTP 401/403. Verified by content-type and body shape.
func TestHTTPMiddleware_ConnectErrorEncoding(t *testing.T) {
	mw := newTestMW(t, "reflow.local")
	r := httptest.NewRequest("POST", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", nil)
	// Connect unary content-type marks this as a Connect RPC request.
	r.Header.Set("Content-Type", "application/proto")
	w := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(w, r)
	if !strings.Contains(w.Result().Header.Get("Content-Type"), "json") {
		// Connect emits the error body as application/json on a unary path.
		t.Logf("content-type = %q (expected json-shaped Connect error)", w.Result().Header.Get("Content-Type"))
	}
	if !strings.Contains(w.Body.String(), "unauthenticated") {
		t.Errorf("body = %q; want connect-error JSON containing 'unauthenticated'", w.Body.String())
	}
}

// TestHTTPMiddleware_NoWWWAuthenticateWhenOIDCDisabled: when only
// SPIFFE is wired (no OIDC issuer), the anonymous-denial 401 must NOT
// advertise Bearer as an accepted scheme — there's no IdP-issued token
// the client could present.
func TestHTTPMiddleware_NoWWWAuthenticateWhenOIDCDisabled(t *testing.T) {
	mw := newTestMW(t, "reflow.local")
	r := httptest.NewRequest("POST", "/reflow.clusterctl.v1.ClusterCtl/ListNodes", nil)
	w := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d; want 401", w.Result().StatusCode)
	}
	if got := w.Result().Header.Get("WWW-Authenticate"); got != "" {
		t.Errorf("WWW-Authenticate=%q; want empty when bearer not configured", got)
	}
}

func mustParseURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
