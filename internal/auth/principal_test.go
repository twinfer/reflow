package auth

import (
	"context"
	"reflect"
	"testing"
)

func TestPrincipalContextRoundTrip(t *testing.T) {
	want := Principal{Kind: "node", Subject: "7", Raw: "node/7", URI: "spiffe://reflow.local/node/7"}
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

func TestTenantIDFromPrincipal(t *testing.T) {
	cases := []struct {
		name string
		p    Principal
		want uint32
	}{
		{"anonymous returns 0", Principal{}, 0},
		{"operator returns 0", Principal{Kind: "operator", Subject: "alice"}, 0},
		{"node returns 0", Principal{Kind: "node", Subject: "7"}, 0},
		{"user returns 0", Principal{Kind: "user", Subject: "42"}, 0},
		{"tenant numeric", Principal{Kind: "tenant", Subject: "42"}, 42},
		{"tenant with sanitized subject prefix", Principal{Kind: "tenant", Subject: "42_x_y"}, 0},
		{"tenant with subject path", Principal{Kind: "tenant", Subject: "42/user/alice"}, 42},
		{"tenant zero is sentinel echoed", Principal{Kind: "tenant", Subject: "0"}, 0},
		{"tenant non-numeric subject returns 0", Principal{Kind: "tenant", Subject: "acme"}, 0},
		{"tenant overflow uint32 returns 0", Principal{Kind: "tenant", Subject: "4294967296"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TenantIDFromPrincipal(tc.p); got != tc.want {
				t.Fatalf("TenantIDFromPrincipal(%+v) = %d; want %d", tc.p, got, tc.want)
			}
		})
	}
}
