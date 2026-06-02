package webhook

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

type stubVerifier struct{ name string }

func (s *stubVerifier) Name() string { return s.name }
func (s *stubVerifier) Verify(_ context.Context, _ *http.Request, _ []byte) (*VerifiedEvent, error) {
	return &VerifiedEvent{Body: []byte("ok")}, nil
}

func TestRegistry_RegisterAndLookup(t *testing.T) {
	resetRegistry(t)
	RegisterVerifier(&stubVerifier{name: "test-vendor"})

	v, err := LookupVerifier("test-vendor")
	if err != nil {
		t.Fatalf("LookupVerifier: %v", err)
	}
	if v.Name() != "test-vendor" {
		t.Errorf("Name=%q; want test-vendor", v.Name())
	}
}

func TestRegistry_UnknownReturnsError(t *testing.T) {
	resetRegistry(t)
	_, err := LookupVerifier("nope")
	if err == nil {
		t.Fatal("expected error for unknown verifier")
	}
}

func TestRegistry_DuplicatePanics(t *testing.T) {
	resetRegistry(t)
	RegisterVerifier(&stubVerifier{name: "dup"})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	RegisterVerifier(&stubVerifier{name: "dup"})
}

func TestRegistry_NilPanics(t *testing.T) {
	resetRegistry(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil verifier")
		}
	}()
	RegisterVerifier(nil)
}

func TestRegistry_EmptyNamePanics(t *testing.T) {
	resetRegistry(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty name")
		}
	}()
	RegisterVerifier(&stubVerifier{name: ""})
}

func TestRegisteredNames(t *testing.T) {
	resetRegistry(t)
	RegisterVerifier(&stubVerifier{name: "a"})
	RegisterVerifier(&stubVerifier{name: "b"})
	names := RegisteredNames()
	if len(names) != 2 {
		t.Fatalf("len(names)=%d; want 2", len(names))
	}
	all := strings.Join(names, ",")
	if !strings.Contains(all, "a") || !strings.Contains(all, "b") {
		t.Errorf("names=%v; want a+b", names)
	}
}

// resetRegistry clears the global registry between tests so
// independent assertions don't interfere. Tests that mutate the
// global should call this first.
func resetRegistry(t *testing.T) {
	t.Helper()
	registryMu.Lock()
	registry = map[string]Verifier{}
	registryMu.Unlock()
}
