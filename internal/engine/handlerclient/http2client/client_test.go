package http2client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
)

// signerStub records the audience the http2client passed and returns a
// canned token (or an error when err != nil).
type signerStub struct {
	mu      sync.Mutex
	called  int
	audArgs []string
	token   string
	err     error
}

func (s *signerStub) Sign(audience string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.called++
	s.audArgs = append(s.audArgs, audience)
	if s.err != nil {
		return "", s.err
	}
	return s.token, nil
}

func (s *signerStub) lastAudience() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.audArgs) == 0 {
		return ""
	}
	return s.audArgs[len(s.audArgs)-1]
}

func startH2CCaptureServer(t *testing.T) (addr string, gotAuth <-chan string) {
	t.Helper()
	ch := make(chan string, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case ch <- r.Header.Get("Authorization"):
		default:
		}
		// Hold the connection open so http2client.Invoke doesn't
		// observe an early EOF. Test cancels ctx to release.
		w.Header().Set("Content-Type", ContentType+"+protobuf")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	})
	srv := &http.Server{Handler: handler, Protocols: new(http.Protocols)}
	srv.Protocols.SetUnencryptedHTTP2(true)
	srv.Protocols.SetHTTP1(false)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = ln.Close()
	})
	return ln.Addr().String(), ch
}

// TestInvoke_AuthorizationHeader: a non-nil signer stamps
// Authorization: Bearer <token> using the deploymentID as the audience.
func TestInvoke_AuthorizationHeader(t *testing.T) {
	addr, gotAuth := startH2CCaptureServer(t)

	stub := &signerStub{token: "tok-xyz"}
	c, err := New("dep-test", "http://"+addr, true, stub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := c.Invoke(ctx, handlerclient.Route{Service: "S", Handler: "H"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	t.Cleanup(func() { _ = s.CloseSend() })

	select {
	case got := <-gotAuth:
		if want := "Bearer tok-xyz"; got != want {
			t.Errorf("Authorization = %q; want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Authorization header on server")
	}
	if got := stub.lastAudience(); got != "dep-test" {
		t.Errorf("signer audience = %q; want %q", got, "dep-test")
	}
}

// TestInvoke_NoSignerLeavesHeaderUnset: nil signer means no auth header
// (single-node / insecure-creds posture).
func TestInvoke_NoSignerLeavesHeaderUnset(t *testing.T) {
	addr, gotAuth := startH2CCaptureServer(t)

	c, err := New("dep-test", "http://"+addr, true, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := c.Invoke(ctx, handlerclient.Route{Service: "S", Handler: "H"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	t.Cleanup(func() { _ = s.CloseSend() })

	select {
	case got := <-gotAuth:
		if got != "" {
			t.Errorf("Authorization = %q; want empty (nil signer)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request on server")
	}
}

// TestInvoke_SignerErrorSurfaces: a signer error fails the Invoke call
// rather than silently dropping the request.
func TestInvoke_SignerErrorSurfaces(t *testing.T) {
	stub := &signerStub{err: errors.New("boom")}
	// No server needed — Invoke fails before dispatch.
	c, err := New("dep-test", "http://127.0.0.1:1", true, stub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.Invoke(ctx, handlerclient.Route{Service: "S", Handler: "H"})
	if err == nil {
		t.Fatal("expected error from Invoke when signer fails; got nil")
	}
}
