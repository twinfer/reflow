package server

import (
	"context"
	"errors"
	"testing"

	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestWireContext_InputAndID confirms the three load-bearing accessors
// return what the constructor was handed. The rest of the durable
// primitives uniformly return ErrWireNotImplemented (see
// TestWireContext_DurablePrimitivesNotImplemented).
func TestWireContext_InputAndID(t *testing.T) {
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	input := []byte("hello")
	ctx := t.Context()

	wctx := newWireContext(ctx, id, input)

	if got := wctx.Input(); string(got) != "hello" {
		t.Errorf("Input() = %q; want %q", got, "hello")
	}
	if got := wctx.InvocationID(); got != id {
		t.Errorf("InvocationID() = %+v; want %+v", got, id)
	}
	if wctx.Context() != ctx {
		t.Errorf("Context() did not round-trip the constructor ctx")
	}
}

// TestWireContext_DurablePrimitivesNotImplemented covers every durable
// primitive's stub path. Adding a new primitive without wire-protocol
// support but forgetting to stub it will surface here as a missing case.
func TestWireContext_DurablePrimitivesNotImplemented(t *testing.T) {
	wctx := newWireContext(context.Background(), &enginev1.InvocationId{}, nil)

	// Sleep / Call / Awakeable return Futures whose Result short-circuits
	// to ErrWireNotImplemented.
	for _, tc := range []struct {
		name   string
		future sdk.Future
	}{
		{"Sleep", wctx.Sleep(0)},
		{"Call", wctx.Call(sdk.Target{}, nil)},
	} {
		_, err := tc.future.Result()
		if !errors.Is(err, ErrWireNotImplemented) {
			t.Errorf("%s.Result() err = %v; want ErrWireNotImplemented", tc.name, err)
		}
	}

	_, akFuture := wctx.Awakeable()
	if _, err := akFuture.Result(); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("Awakeable.Result() err = %v; want ErrWireNotImplemented", err)
	}

	// Direct-error methods.
	if _, err := wctx.Run("x", func() ([]byte, error) { return nil, nil }); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("Run err = %v; want ErrWireNotImplemented", err)
	}
	if err := wctx.OneWayCall(sdk.Target{}, nil); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("OneWayCall err = %v; want ErrWireNotImplemented", err)
	}
	if _, _, err := wctx.GetState("k"); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("GetState err = %v; want ErrWireNotImplemented", err)
	}
	if err := wctx.SetState("k", nil); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("SetState err = %v; want ErrWireNotImplemented", err)
	}
	if err := wctx.ClearState("k"); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("ClearState err = %v; want ErrWireNotImplemented", err)
	}
	if err := wctx.ClearAllState(); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("ClearAllState err = %v; want ErrWireNotImplemented", err)
	}
	if err := wctx.SendSignal(sdk.Target{}, "s", nil); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("SendSignal err = %v; want ErrWireNotImplemented", err)
	}

	// Combinators round-trip ErrWireNotImplemented through Results/Result.
	all := wctx.All(notImplementedFuture{})
	if _, err := all.Results(); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("All.Results err = %v; want ErrWireNotImplemented", err)
	}
	any := wctx.Any(notImplementedFuture{})
	if _, err := any.Result(); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("Any.Result err = %v; want ErrWireNotImplemented", err)
	}
}
