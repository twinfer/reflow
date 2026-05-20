package hostmux

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
)

func stubHandler(label string, hits *atomic.Int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("X-Stub", label)
		w.WriteHeader(http.StatusOK)
	})
}

func mustPrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatalf("netip.ParsePrefix(%q): %v", s, err)
	}
	return p
}

func TestServeHTTP_ExactMatchBeatsWildcard(t *testing.T) {
	var exactHits, wildHits atomic.Int64
	m := New(TrustPolicy{})
	m.Set(
		map[string]http.Handler{"stripe.acme.io": stubHandler("exact", &exactHits)},
		map[string]http.Handler{"*.acme.io": stubHandler("wild", &wildHits)},
		nil,
	)

	r := httptest.NewRequest(http.MethodGet, "http://stripe.acme.io/x", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)

	if got := w.Header().Get("X-Stub"); got != "exact" {
		t.Fatalf("expected exact handler, got %q", got)
	}
	if exactHits.Load() != 1 || wildHits.Load() != 0 {
		t.Fatalf("hits: exact=%d wild=%d", exactHits.Load(), wildHits.Load())
	}
}

func TestServeHTTP_WildcardMatch(t *testing.T) {
	var wildHits atomic.Int64
	m := New(TrustPolicy{})
	m.Set(nil, map[string]http.Handler{"*.acme.io": stubHandler("wild", &wildHits)}, nil)

	cases := []struct {
		host    string
		matches bool
	}{
		{"a.acme.io", true},
		{"github.acme.io", true},
		{"acme.io", false},     // bare tld - no wildcard match
		{"b.a.acme.io", false}, // two labels deep
		{"unrelated.com", false},
	}
	for _, c := range cases {
		t.Run(c.host, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "http://"+c.host+"/", nil)
			w := httptest.NewRecorder()
			m.ServeHTTP(w, r)
			matched := w.Header().Get("X-Stub") == "wild"
			if matched != c.matches {
				t.Fatalf("host=%s matched=%v want=%v (status=%d)",
					c.host, matched, c.matches, w.Code)
			}
		})
	}
}

func TestServeHTTP_PortStripping(t *testing.T) {
	var hits atomic.Int64
	m := New(TrustPolicy{})
	m.Set(map[string]http.Handler{"acme.io": stubHandler("acme", &hits)}, nil, nil)

	r := httptest.NewRequest(http.MethodGet, "http://acme.io:8443/x", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if hits.Load() != 1 {
		t.Fatalf("port-suffixed host did not match: status=%d", w.Code)
	}
}

func TestServeHTTP_CaseInsensitive(t *testing.T) {
	var hits atomic.Int64
	m := New(TrustPolicy{})
	m.Set(map[string]http.Handler{"AcMe.IO": stubHandler("acme", &hits)}, nil, nil)

	r := httptest.NewRequest(http.MethodGet, "http://ACME.io/", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if hits.Load() != 1 {
		t.Fatalf("case mismatch did not normalize: status=%d", w.Code)
	}
}

func TestServeHTTP_NoMatchReturnsJSON404(t *testing.T) {
	m := New(TrustPolicy{})
	m.Set(nil, nil, nil)

	r := httptest.NewRequest(http.MethodGet, "http://nope.example/", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=404", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type=%q want application/json", ct)
	}
	if w.Body.Len() == 0 {
		t.Fatalf("empty body in 404")
	}
}

func TestServeHTTP_DefaultHandlerCatchAll(t *testing.T) {
	var defHits atomic.Int64
	m := New(TrustPolicy{})
	m.Set(nil, nil, stubHandler("def", &defHits))

	r := httptest.NewRequest(http.MethodGet, "http://nope.example/", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if defHits.Load() != 1 {
		t.Fatalf("default handler not invoked: status=%d", w.Code)
	}
}

func TestResolveHost_UntrustedXForwardedIgnored(t *testing.T) {
	var spoof, real atomic.Int64
	m := New(TrustPolicy{}) // empty trust policy = never trust forwarded headers
	m.Set(map[string]http.Handler{
		"spoofed.acme.io": stubHandler("spoof", &spoof),
		"real.acme.io":    stubHandler("real", &real),
	}, nil, nil)

	r := httptest.NewRequest(http.MethodGet, "http://real.acme.io/", nil)
	r.RemoteAddr = "203.0.113.7:5555" // not in any trust prefix
	r.Header.Set("X-Forwarded-Host", "spoofed.acme.io")

	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)

	if spoof.Load() != 0 {
		t.Fatalf("X-Forwarded-Host was honored from untrusted peer")
	}
	if real.Load() != 1 {
		t.Fatalf("expected real handler hit, got status=%d", w.Code)
	}
}

func TestResolveHost_TrustedXForwardedHonored(t *testing.T) {
	var lbDownstream atomic.Int64
	m := New(TrustPolicy{
		Proxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")},
	})
	m.Set(map[string]http.Handler{
		"stripe.acme.io": stubHandler("stripe", &lbDownstream),
	}, nil, nil)

	r := httptest.NewRequest(http.MethodGet, "http://internal-lb/", nil)
	r.RemoteAddr = "10.1.2.3:4444" // inside trust prefix
	r.Header.Set("X-Forwarded-Host", "stripe.acme.io")

	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if lbDownstream.Load() != 1 {
		t.Fatalf("forwarded host from trusted peer not honored: status=%d", w.Code)
	}
}

func TestResolveHost_RFC7239Forwarded(t *testing.T) {
	var hits atomic.Int64
	m := New(TrustPolicy{Proxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}})
	m.Set(map[string]http.Handler{"acme.io": stubHandler("acme", &hits)}, nil, nil)

	r := httptest.NewRequest(http.MethodGet, "http://intermediate/", nil)
	r.RemoteAddr = "10.0.0.5:1111"
	r.Header.Set("Forwarded", `for=192.0.2.60;proto=http;host="acme.io"`)

	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if hits.Load() != 1 {
		t.Fatalf("RFC7239 Forwarded host=… not honored: status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestResolveHost_XForwardedHostStripsExtras(t *testing.T) {
	var hits atomic.Int64
	m := New(TrustPolicy{Proxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}})
	m.Set(map[string]http.Handler{"first.acme.io": stubHandler("first", &hits)}, nil, nil)

	r := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	r.RemoteAddr = "10.0.0.5:1234"
	r.Header.Set("X-Forwarded-Host", "first.acme.io, second.acme.io")

	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if hits.Load() != 1 {
		t.Fatalf("first hop of X-Forwarded-Host not honored: status=%d", w.Code)
	}
}

func TestHosts_ReturnsSortedNames(t *testing.T) {
	m := New(TrustPolicy{})
	var sink atomic.Int64
	m.Set(
		map[string]http.Handler{
			"b.acme.io": stubHandler("b", &sink),
			"a.acme.io": stubHandler("a", &sink),
		},
		map[string]http.Handler{
			"*.tenant.io": stubHandler("w", &sink),
		}, nil,
	)
	got := m.Hosts()
	want := []string{"*.tenant.io", "a.acme.io", "b.acme.io"}
	if len(got) != len(want) {
		t.Fatalf("hosts=%v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("hosts[%d]=%q want %q", i, got[i], want[i])
		}
	}
}

// TestSet_AtomicSwapUnderLoad runs ServeHTTP concurrently with Set; the
// race detector catches data races on the routing table, and the assertion
// confirms every request is served by *some* installed handler (never
// dropped, never seen mid-mutation).
func TestSet_AtomicSwapUnderLoad(t *testing.T) {
	m := New(TrustPolicy{})
	var hitsA, hitsB atomic.Int64
	hA := stubHandler("A", &hitsA)
	hB := stubHandler("B", &hitsB)
	m.Set(map[string]http.Handler{"acme.io": hA}, nil, nil)

	const requests = 2000
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: flip the table back and forth.
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			m.Set(map[string]http.Handler{"acme.io": hA}, nil, nil)
			m.Set(map[string]http.Handler{"acme.io": hB}, nil, nil)
		}
	})

	// Readers: hammer ServeHTTP.
	for range 8 {
		wg.Go(func() {
			for range requests {
				r := httptest.NewRequest(http.MethodGet, "http://acme.io/", nil)
				w := httptest.NewRecorder()
				m.ServeHTTP(w, r)
				if w.Code != http.StatusOK {
					t.Errorf("unexpected status during swap: %d", w.Code)
					return
				}
			}
		})
	}
	close(stop)
	wg.Wait()
	if hitsA.Load()+hitsB.Load() == 0 {
		t.Fatalf("no requests served (both counters zero)")
	}
}
