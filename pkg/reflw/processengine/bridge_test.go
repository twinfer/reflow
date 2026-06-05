package processengine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/twinfer/reflw/pkg/handler"
	"github.com/twinfer/reflwos/capability"
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
	// The BPMN error code rides the failure message as a bridgeFault envelope so
	// eventForBPMN can route it to the matching error boundary.
	code, cause := decodeBridgeFault(f.Message)
	if code != "PAYMENT_DECLINED" {
		t.Errorf("decoded code = %q, want PAYMENT_DECLINED", code)
	}
	if cause == "" || cause == f.Message {
		t.Errorf("decoded cause = %q, want the human message split from the envelope (%q)", cause, f.Message)
	}
}

func TestBridgeFault_RoundTrip(t *testing.T) {
	// Coded → enveloped → split back into (code, cause).
	if code, cause := decodeBridgeFault(encodeBridgeFault("E_BOOM", "kaboom")); code != "E_BOOM" || cause != "kaboom" {
		t.Fatalf("round-trip = (%q, %q), want (E_BOOM, kaboom)", code, cause)
	}
	// A plain (non-enveloped) message → catch-all code "" + message unchanged.
	if code, cause := decodeBridgeFault("connection refused"); code != "" || cause != "connection refused" {
		t.Fatalf("plain decode = (%q, %q), want (\"\", connection refused)", code, cause)
	}
	// JSON that isn't a fault envelope (no code) is treated as a plain message.
	if code, _ := decodeBridgeFault(`{"foo":1}`); code != "" {
		t.Fatalf("non-fault JSON decoded a code %q, want catch-all", code)
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
