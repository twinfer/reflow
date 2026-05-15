package sdk

import (
	"errors"
	"strings"
	"testing"
)

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := NewRegistry()
	h := func(_ Context, _ []byte) ([]byte, error) { return nil, nil }
	if err := r.RegisterService("Greeter", "hello", h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if r.Len() != 1 {
		t.Errorf("Len = %d; want 1", r.Len())
	}
	got, kind, ok := r.Lookup(&Target{Service: "Greeter", Handler: "hello", Key: "ignored"})
	if !ok {
		t.Fatal("Lookup: not found")
	}
	if got == nil {
		t.Fatal("Lookup: nil handler")
	}
	if kind != KindService {
		t.Errorf("kind = %v; want service (Register defaults to service)", kind)
	}
}

func TestRegistry_KindAwareRegistration(t *testing.T) {
	r := NewRegistry()
	h := func(_ Context, _ []byte) ([]byte, error) { return nil, nil }

	cases := []struct {
		name string
		fn   func(svc, hdr string, h Handler) error
		want Kind
	}{
		{"service", r.RegisterService, KindService},
		{"object", r.RegisterObject, KindObject},
		{"workflow", r.RegisterWorkflow, KindWorkflow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(tc.name+"Svc", "h", h); err != nil {
				t.Fatalf("Register%s: %v", tc.name, err)
			}
			_, got, ok := r.Lookup(&Target{Service: tc.name + "Svc", Handler: "h"})
			if !ok {
				t.Fatal("Lookup miss")
			}
			if got != tc.want {
				t.Errorf("kind = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestKind_String(t *testing.T) {
	cases := map[Kind]string{
		KindUnspecified: "unspecified",
		KindService:     "service",
		KindObject:      "object",
		KindWorkflow:    "workflow",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Errorf("%d.String() = %q; want %q", k, got, want)
		}
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
			err := r.RegisterService(tc.svc, tc.hdr, tc.fn)
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
	if err := r.RegisterService("S", "h", h); err != nil {
		t.Fatal(err)
	}
	// Duplicate across kinds is still a collision — (service, handler) is
	// the namespace, kind is metadata.
	err := r.RegisterObject("S", "h", h)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Errorf("duplicate err=%v; want 'already registered'", err)
	}
}

func TestRegistry_LookupMissing(t *testing.T) {
	r := NewRegistry()
	if _, _, ok := r.Lookup(&Target{Service: "Nope", Handler: "x"}); ok {
		t.Error("expected miss")
	}
	if _, _, ok := r.Lookup(nil); ok {
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

func TestInprocDeploymentID_Stability(t *testing.T) {
	h := func(_ Context, _ []byte) ([]byte, error) { return nil, nil }

	// Two registries with the same handler set in different registration
	// orders must produce the same id — Entries() sorts before hashing.
	r1 := NewRegistry()
	_ = r1.RegisterService("Greeter", "hello", h)
	_ = r1.RegisterObject("Counter", "incr", h)
	_ = r1.RegisterWorkflow("Saga", "run", h)

	r2 := NewRegistry()
	_ = r2.RegisterWorkflow("Saga", "run", h)
	_ = r2.RegisterService("Greeter", "hello", h)
	_ = r2.RegisterObject("Counter", "incr", h)

	id1 := InprocDeploymentID(r1.Entries())
	id2 := InprocDeploymentID(r2.Entries())
	if id1 != id2 {
		t.Errorf("ids differ across registration order: %s vs %s", id1, id2)
	}
	if !strings.HasPrefix(id1, "inproc-") {
		t.Errorf("id = %q; want inproc- prefix", id1)
	}

	// Changing the kind of an existing handler MUST flip the id —
	// otherwise a code edit silently keeps the old deployment_id and
	// in-flight invocations replay against a stale signature.
	r3 := NewRegistry()
	_ = r3.RegisterObject("Greeter", "hello", h) // was service in r1
	_ = r3.RegisterObject("Counter", "incr", h)
	_ = r3.RegisterWorkflow("Saga", "run", h)
	if InprocDeploymentID(r3.Entries()) == id1 {
		t.Error("kind change did not change the id")
	}

	// Removing a handler MUST flip the id.
	r4 := NewRegistry()
	_ = r4.RegisterService("Greeter", "hello", h)
	_ = r4.RegisterObject("Counter", "incr", h)
	if InprocDeploymentID(r4.Entries()) == id1 {
		t.Error("removed handler did not change the id")
	}
}

func TestRegistry_EntriesSorted(t *testing.T) {
	r := NewRegistry()
	h := func(_ Context, _ []byte) ([]byte, error) { return nil, nil }
	_ = r.RegisterService("Z", "z", h)
	_ = r.RegisterService("A", "b", h)
	_ = r.RegisterService("A", "a", h)
	_ = r.RegisterService("B", "a", h)

	got := r.Entries()
	want := []string{"A/a", "A/b", "B/a", "Z/z"}
	if len(got) != len(want) {
		t.Fatalf("len = %d; want %d", len(got), len(want))
	}
	for i, e := range got {
		if e.Service+"/"+e.Handler != want[i] {
			t.Errorf("entries[%d] = %s/%s; want %s", i, e.Service, e.Handler, want[i])
		}
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
