package iflowengine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/twinfer/iflow/capability"
	"github.com/twinfer/reflow/pkg/handler"
)

func capRegistryWith(ns string, h capability.Handler) *capability.Registry {
	r := capability.NewRegistry()
	r.Register(ns, h)
	return r
}

func TestRunCapability_SuccessMarshalsOutputs(t *testing.T) {
	reg := capRegistryWith("echo", capability.HandlerFunc(func(_ context.Context, req capability.Request) (map[string]any, error) {
		return req.Vars, nil // echo inputs back
	}))

	out, err := runCapability(context.Background(), reg, BridgeInput{Ref: "echo:noop", Vars: map[string]any{"x": float64(1)}})
	if err != nil {
		t.Fatalf("runCapability: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got["x"] != float64(1) {
		t.Errorf("output = %v, want x=1", got)
	}
}

func TestRunCapability_UnresolvedIsTerminalFailure(t *testing.T) {
	reg := capability.NewRegistry() // nothing registered

	_, err := runCapability(context.Background(), reg, BridgeInput{Ref: "missing:op"})
	var f *handler.Failure
	if !errors.As(err, &f) {
		t.Fatalf("want *handler.Failure for unresolved capability, got %T: %v", err, err)
	}
}

func TestRunCapability_CodedFaultIsTerminalFailure(t *testing.T) {
	reg := capRegistryWith("pay", capability.HandlerFunc(func(_ context.Context, _ capability.Request) (map[string]any, error) {
		return nil, capability.Coded("PAYMENT_DECLINED", errors.New("card declined"))
	}))

	_, err := runCapability(context.Background(), reg, BridgeInput{Ref: "pay:charge"})
	var f *handler.Failure
	if !errors.As(err, &f) {
		t.Fatalf("want terminal *handler.Failure for coded fault, got %T: %v", err, err)
	}
}

func TestRunCapability_BareErrorIsTransient(t *testing.T) {
	sentinel := errors.New("connection refused")
	reg := capRegistryWith("svc", capability.HandlerFunc(func(_ context.Context, _ capability.Request) (map[string]any, error) {
		return nil, sentinel
	}))

	_, err := runCapability(context.Background(), reg, BridgeInput{Ref: "svc:op"})
	var f *handler.Failure
	if errors.As(err, &f) {
		t.Fatalf("bare error should stay transient (not *handler.Failure), got Failure: %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("want the original transient error, got %v", err)
	}
}
