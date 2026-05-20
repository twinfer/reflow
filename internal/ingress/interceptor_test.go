package ingress

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"
)

func TestWithDefaultDeadline_InstallsDeadlineWhenAbsent(t *testing.T) {
	interceptor := withDefaultDeadline(50 * time.Millisecond)

	var observed time.Time
	wrapped := interceptor.WrapUnary(connect.UnaryFunc(func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatal("handler ran without deadline")
		}
		observed = dl
		return nil, nil
	}))

	if _, err := wrapped(context.Background(), nil); err != nil {
		t.Fatalf("interceptor returned err: %v", err)
	}
	if budget := time.Until(observed); budget <= 0 || budget > 60*time.Millisecond {
		t.Errorf("deadline budget out of range: %s", budget)
	}
}

func TestWithDefaultDeadline_RespectsCallerDeadline(t *testing.T) {
	interceptor := withDefaultDeadline(5 * time.Second)

	callerDeadline := time.Now().Add(20 * time.Millisecond)
	parent, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()

	wrapped := interceptor.WrapUnary(connect.UnaryFunc(func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatal("handler ran without deadline")
		}
		if !dl.Equal(callerDeadline) {
			t.Errorf("deadline overridden: got %v, want %v", dl, callerDeadline)
		}
		return nil, nil
	}))

	if _, err := wrapped(parent, nil); err != nil {
		t.Fatalf("interceptor returned err: %v", err)
	}
}

// TestWithDefaultDeadline_WrapsStreamingHandler ensures the streaming
// handler path inherits the same deadline behavior as unary.
func TestWithDefaultDeadline_WrapsStreamingHandler(t *testing.T) {
	interceptor := withDefaultDeadline(40 * time.Millisecond)

	var observed time.Time
	wrapped := interceptor.WrapStreamingHandler(connect.StreamingHandlerFunc(func(ctx context.Context, _ connect.StreamingHandlerConn) error {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatal("streaming handler ran without deadline")
		}
		observed = dl
		return nil
	}))

	if err := wrapped(context.Background(), nopHandlerConn{}); err != nil {
		t.Fatalf("interceptor returned err: %v", err)
	}
	if budget := time.Until(observed); budget <= 0 || budget > 60*time.Millisecond {
		t.Errorf("deadline budget out of range: %s", budget)
	}
}

// nopHandlerConn is a minimal connect.StreamingHandlerConn stub used to
// exercise WrapStreamingHandler without spinning up a real server.
type nopHandlerConn struct{}

func (nopHandlerConn) Spec() connect.Spec           { return connect.Spec{} }
func (nopHandlerConn) Peer() connect.Peer           { return connect.Peer{} }
func (nopHandlerConn) Receive(_ any) error          { return io.EOF }
func (nopHandlerConn) RequestHeader() http.Header   { return http.Header{} }
func (nopHandlerConn) Send(_ any) error             { return nil }
func (nopHandlerConn) ResponseHeader() http.Header  { return http.Header{} }
func (nopHandlerConn) ResponseTrailer() http.Header { return http.Header{} }
