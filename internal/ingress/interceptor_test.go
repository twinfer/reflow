package ingress

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestWithDefaultDeadline_InstallsDeadlineWhenAbsent(t *testing.T) {
	interceptor := withDefaultDeadline(50 * time.Millisecond)

	var observed time.Time
	handler := grpc.UnaryHandler(func(ctx context.Context, _ any) (any, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatal("handler ran without deadline")
		}
		observed = dl
		return nil, nil
	})

	if _, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler); err != nil {
		t.Fatalf("interceptor returned err: %v", err)
	}
	if budget := time.Until(observed); budget <= 0 || budget > 60*time.Millisecond {
		t.Errorf("deadline budget out of range: %s", budget)
	}
}

func TestWithHTTPDefaultDeadline_InstallsDeadlineWhenAbsent(t *testing.T) {
	var observed time.Time
	handler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		dl, ok := r.Context().Deadline()
		if !ok {
			t.Fatal("HTTP handler ran without deadline")
		}
		observed = dl
	})
	wrapped := withHTTPDefaultDeadline(handler, 50*time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if budget := time.Until(observed); budget <= 0 || budget > 60*time.Millisecond {
		t.Errorf("HTTP deadline budget out of range: %s", budget)
	}
}

func TestWithDefaultDeadline_RespectsCallerDeadline(t *testing.T) {
	interceptor := withDefaultDeadline(5 * time.Second)

	callerDeadline := time.Now().Add(20 * time.Millisecond)
	parent, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()

	handler := grpc.UnaryHandler(func(ctx context.Context, _ any) (any, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatal("handler ran without deadline")
		}
		if !dl.Equal(callerDeadline) {
			t.Errorf("deadline overridden: got %v, want %v", dl, callerDeadline)
		}
		return nil, nil
	})

	if _, err := interceptor(parent, nil, &grpc.UnaryServerInfo{}, handler); err != nil {
		t.Fatalf("interceptor returned err: %v", err)
	}
}
