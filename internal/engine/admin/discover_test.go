package admin

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
)

// stubSigner records calls and returns a canned token.
type stubSigner struct {
	mu      sync.Mutex
	called  int
	lastAud string
}

func (s *stubSigner) Sign(aud string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.called++
	s.lastAud = aud
	return "stub-token-for-" + aud, nil
}

func (s *stubSigner) audience() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAud
}

// TestDiscoverHTTP_SignerStampsAuthorization asserts that when a Signer
// is configured, discoverHTTP stamps the Authorization header with the
// deployment URL as audience.
func TestDiscoverHTTP_SignerStampsAuthorization(t *testing.T) {
	addr, gotAuth := startH2CDiscoverServer(t)
	signer := &stubSigner{}

	rawURL := "http://" + addr
	resp, err := discoverHTTP(context.Background(), rawURL, true, signer)
	if err != nil {
		t.Fatalf("discoverHTTP: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response")
	}
	select {
	case got := <-gotAuth:
		if want := "Bearer stub-token-for-" + rawURL; got != want {
			t.Errorf("Authorization = %q; want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for /discover request")
	}
	if got := signer.audience(); got != rawURL {
		t.Errorf("signer audience = %q; want %q", got, rawURL)
	}
}

// TestDiscoverHTTP_NoSignerLeavesHeaderUnset asserts the nil-Signer
// posture (single-node and insecure-creds) sends no Authorization.
func TestDiscoverHTTP_NoSignerLeavesHeaderUnset(t *testing.T) {
	addr, gotAuth := startH2CDiscoverServer(t)
	if _, err := discoverHTTP(context.Background(), "http://"+addr, true, nil); err != nil {
		t.Fatalf("discoverHTTP: %v", err)
	}
	select {
	case got := <-gotAuth:
		if got != "" {
			t.Errorf("Authorization = %q; want empty (nil signer)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for /discover request")
	}
}

// startH2CDiscoverServer accepts GET /discover over h2c, records the
// Authorization header into the returned channel, and replies with a
// minimal DiscoveryResponse.
func startH2CDiscoverServer(t *testing.T) (addr string, gotAuth <-chan string) {
	t.Helper()
	ch := make(chan string, 1)
	body, err := proto.Marshal(&discoveryv1.DiscoveryResponse{ProtocolVersion: protocolVersion})
	if err != nil {
		t.Fatalf("marshal DiscoveryResponse: %v", err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case ch <- r.Header.Get("Authorization"):
		default:
		}
		w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	srv := &http.Server{Handler: handler, Protocols: new(http.Protocols)}
	srv.Protocols.SetUnencryptedHTTP2(true)
	srv.Protocols.SetHTTP1(false)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = ln.Close()
	})
	return ln.Addr().String(), ch
}
