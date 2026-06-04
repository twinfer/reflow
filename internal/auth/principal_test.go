package auth

import (
	"context"
	"reflect"
	"testing"
)

func TestPrincipalContextRoundTrip(t *testing.T) {
	want := Principal{Kind: "node", Subject: "7", Raw: "node/7", MeshCAFingerprint: "sha256:abc"}
	ctx := ContextWithPrincipal(context.Background(), want)
	got, ok := PrincipalFromContext(ctx)
	if !ok {
		t.Fatal("PrincipalFromContext returned !ok")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v; want %+v", got, want)
	}
}

func TestPrincipalFromContext_AbsentReturnsFalse(t *testing.T) {
	if _, ok := PrincipalFromContext(context.Background()); ok {
		t.Error("expected ok=false for empty context")
	}
}

func TestPrincipalIsAnonymous(t *testing.T) {
	if !(Principal{}).IsAnonymous() {
		t.Error("zero Principal should be anonymous")
	}
	if (Principal{Kind: "user", Subject: "x"}).IsAnonymous() {
		t.Error("non-zero Principal should not be anonymous")
	}
}

func TestPrincipalString(t *testing.T) {
	if got := (Principal{}).String(); got != "anonymous" {
		t.Errorf("anonymous String()=%q; want anonymous", got)
	}
	if got := (Principal{Kind: "user", Subject: "alice", Raw: "user/alice"}).String(); got != "user/alice" {
		t.Errorf("String()=%q; want user/alice", got)
	}
}
