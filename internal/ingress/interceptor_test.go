package ingress

import (
	"context"
	"testing"
	"time"

	connect "connectrpc.com/connect"
)

func TestWithDefaultDeadline_InstallsDeadlineWhenAbsent(t *testing.T) {
	interceptor := withDefaultDeadline(50 * time.Millisecond)

	var observed time.Time
	wrapped := interceptor(connect.UnaryFunc(func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
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

	wrapped := interceptor(connect.UnaryFunc(func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
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
