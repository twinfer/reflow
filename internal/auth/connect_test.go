package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The HTTP middleware does authentication + principal stamping only;
// authorization (the path-glob policy that used to live here) moved to the
// Cedar Connect interceptor in internal/authz. These tests cover what the
// middleware still owns: stamping a verified principal and hard-rejecting a
// malformed mesh leaf at the authn layer.

func newTestMW(t *testing.T) func(http.Handler) http.Handler {
	t.Helper()
	mw, closer, err := HTTPMiddleware(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("HTTPMiddleware: %v", err)
	}
	if closer != nil {
		t.Cleanup(func() { _ = closer() })
	}
	return mw
}

// TestHTTPMiddleware_StampsPrincipalFromTLS feeds a synthetic
// *tls.ConnectionState with a verified mesh leaf into the middleware and
// asserts the downstream handler observes the principal both in the stamped
// header and via PrincipalFromContext, with any forged inbound header dropped.
func TestHTTPMiddleware_StampsPrincipalFromTLS(t *testing.T) {
	mw := newTestMW(t)

	var sawHeader string
	var sawPrincipal Principal
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHeader = r.Header.Get(PrincipalHeader)
		p, _ := PrincipalFromContext(r.Context())
		sawPrincipal = p
		w.WriteHeader(http.StatusOK)
	})

	leaf := &x509.Certificate{Subject: pkix.Name{CommonName: "operator/alice"}}
	r := httptest.NewRequest("POST", "/reflw.clusterctl.v1.ClusterCtl/ListNodes", nil)
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	// Forged header — the stamp handler must drop it before stamping the
	// server-computed value.
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

// TestHTTPMiddleware_MalformedCNLeafRejected: a verified leaf carrying a CN
// that fails the <kind>/<name> shape is hard-rejected by the authn step (the
// leaf claims a mesh identity we can't honor), surfacing as 401 before any
// handler runs — independent of authorization.
func TestHTTPMiddleware_MalformedCNLeafRejected(t *testing.T) {
	cases := map[string]string{
		"single-segment":   "no-slash",
		"empty-kind":       "/alice",
		"empty-name":       "operator/",
		"too-many-slashes": "tenant/42/operator/alice",
		"only-slash":       "/",
	}
	for name, cn := range cases {
		t.Run(name, func(t *testing.T) {
			mw := newTestMW(t)
			leaf := &x509.Certificate{Subject: pkix.Name{CommonName: cn}}
			r := httptest.NewRequest("POST", "/reflw.clusterctl.v1.ClusterCtl/ListNodes", nil)
			r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
			w := httptest.NewRecorder()
			mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(w, r)
			if w.Result().StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %d; want 401", w.Result().StatusCode)
			}
		})
	}
}
