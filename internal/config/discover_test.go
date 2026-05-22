package config

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	"github.com/twinfer/reflow/proto/discoveryv1/discoveryv1connect"
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

// TestDiscover_SignerStampsAuthorization asserts that when a Signer is
// configured, discoverConnect stamps the Authorization header with the
// deployment URL as audience.
func TestDiscover_SignerStampsAuthorization(t *testing.T) {
	addr, gotAuth := startH2CDiscoverServer(t)
	signer := &stubSigner{}

	rawURL := "http://" + addr
	resp, err := discoverConnect(context.Background(), rawURL, true, signer)
	if err != nil {
		t.Fatalf("discoverConnect: %v", err)
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
		t.Fatal("timeout waiting for Discover request")
	}
	if got := signer.audience(); got != rawURL {
		t.Errorf("signer audience = %q; want %q", got, rawURL)
	}
}

// TestDiscover_NoSignerLeavesHeaderUnset asserts the nil-Signer posture
// (single-node and insecure-creds) sends no Authorization header.
func TestDiscover_NoSignerLeavesHeaderUnset(t *testing.T) {
	addr, gotAuth := startH2CDiscoverServer(t)
	if _, err := discoverConnect(context.Background(), "http://"+addr, true, nil); err != nil {
		t.Fatalf("discoverConnect: %v", err)
	}
	select {
	case got := <-gotAuth:
		if got != "" {
			t.Errorf("Authorization = %q; want empty (nil signer)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Discover request")
	}
}

// fakeDiscoveryService captures the Authorization header on the first
// request and replies with a minimal DiscoveryResponse.
type fakeDiscoveryService struct {
	discoveryv1connect.UnimplementedDiscoveryServiceHandler
	authCh chan<- string
	sent   atomic.Bool
}

func (f *fakeDiscoveryService) Discover(_ context.Context, req *connect.Request[discoveryv1.DiscoveryRequest]) (*connect.Response[discoveryv1.DiscoveryResponse], error) {
	if f.sent.CompareAndSwap(false, true) {
		select {
		case f.authCh <- req.Header().Get("Authorization"):
		default:
		}
	}
	return connect.NewResponse(&discoveryv1.DiscoveryResponse{ProtocolVersion: protocolVersion}), nil
}

// startH2CDiscoverServer mounts the DiscoveryService over h2c and
// returns the listener address plus a channel that receives the first
// request's Authorization header.
func startH2CDiscoverServer(t *testing.T) (addr string, gotAuth <-chan string) {
	t.Helper()
	ch := make(chan string, 1)
	svc := &fakeDiscoveryService{authCh: ch}
	mux := http.NewServeMux()
	path, h := discoveryv1connect.NewDiscoveryServiceHandler(svc)
	mux.Handle(path, h)
	srv := &http.Server{Handler: mux, Protocols: new(http.Protocols)}
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
