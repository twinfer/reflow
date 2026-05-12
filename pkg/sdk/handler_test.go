package sdk

import (
	"errors"
	"strings"
	"testing"
)

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := NewRegistry()
	h := func(_ Context, _ []byte) ([]byte, error) { return nil, nil }
	if err := r.Register("Greeter", "hello", h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if r.Len() != 1 {
		t.Errorf("Len = %d; want 1", r.Len())
	}
	got, ok := r.Lookup(&Target{Service: "Greeter", Handler: "hello", Key: "ignored"})
	if !ok {
		t.Fatal("Lookup: not found")
	}
	if got == nil {
		t.Fatal("Lookup: nil handler")
	}
}

func TestRegistry_RejectsBadInputs(t *testing.T) {
	r := NewRegistry()
	cases := []struct {
		name       string
		svc, hdr   string
		fn         Handler
		wantSubstr string
	}{
		{"empty service", "", "h", func(_ Context, _ []byte) ([]byte, error) { return nil, nil }, "service must be non-empty"},
		{"empty handler", "S", "", func(_ Context, _ []byte) ([]byte, error) { return nil, nil }, "handler must be non-empty"},
		{"nil fn", "S", "h", nil, "fn must be non-nil"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := r.Register(tc.svc, tc.hdr, tc.fn)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("err=%v; want substring %q", err, tc.wantSubstr)
			}
		})
	}
}

func TestRegistry_RejectsDuplicate(t *testing.T) {
	r := NewRegistry()
	h := func(_ Context, _ []byte) ([]byte, error) { return nil, nil }
	if err := r.Register("S", "h", h); err != nil {
		t.Fatal(err)
	}
	err := r.Register("S", "h", h)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Errorf("duplicate err=%v; want 'already registered'", err)
	}
}

func TestRegistry_LookupMissing(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Lookup(&Target{Service: "Nope", Handler: "x"}); ok {
		t.Error("expected miss")
	}
	if _, ok := r.Lookup(nil); ok {
		t.Error("nil target: expected miss")
	}
}

func TestFailure_Error(t *testing.T) {
	f := NewFailure(42, "boom")
	if !strings.Contains(f.Error(), "boom") || !strings.Contains(f.Error(), "42") {
		t.Errorf("Error = %q; want code+message", f.Error())
	}
	bare := NewFailure(0, "plain")
	if bare.Error() != "plain" {
		t.Errorf("zero-code Error = %q; want plain", bare.Error())
	}
}

func TestAsFailure(t *testing.T) {
	f := NewFailure(1, "x")
	if got, ok := AsFailure(f); !ok || got != f {
		t.Errorf("AsFailure direct: ok=%v got=%v", ok, got)
	}
	wrapped := errors.Join(errors.New("ctx"), f)
	if got, ok := AsFailure(wrapped); !ok || got != f {
		t.Errorf("AsFailure wrapped: ok=%v got=%v", ok, got)
	}
	if _, ok := AsFailure(errors.New("plain")); ok {
		t.Error("AsFailure plain: expected false")
	}
}

func TestTarget_String(t *testing.T) {
	if s := (Target{Service: "S", Handler: "h"}).String(); s != "S/h" {
		t.Errorf("unkeyed = %q; want S/h", s)
	}
	if s := (Target{Service: "S", Handler: "h", Key: "k"}).String(); s != "S[k]/h" {
		t.Errorf("keyed = %q; want S[k]/h", s)
	}
}
