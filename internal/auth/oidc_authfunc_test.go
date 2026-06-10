package auth

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func bearerReq(t *testing.T, token string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodPost, "http://x/y", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

// TestOIDCAuthFunc covers the three outcomes via a fake bearerVerifier: no
// header → anonymous fall-through (nil,nil), an invalid token → hard error
// (CodeUnauthenticated upstream), and a valid token → a User principal carrying
// sub + groups + picked claims.
func TestOIDCAuthFunc(t *testing.T) {
	verify := func(_ context.Context, raw string) (verifiedClaims, error) {
		if raw != "good" {
			return verifiedClaims{}, errors.New("bad token")
		}
		return verifiedClaims{
			subject: "alice",
			groups:  []string{"reflw-admins", "eng"},
			extra:   map[string]string{"email": "alice@example.com"},
		}, nil
	}
	af := oidcAuthFunc(verify)

	t.Run("no bearer header falls through", func(t *testing.T) {
		info, err := af(context.Background(), bearerReq(t, ""))
		if err != nil || info != nil {
			t.Fatalf("got (%v, %v), want (nil, nil)", info, err)
		}
	})

	t.Run("invalid token is rejected", func(t *testing.T) {
		info, err := af(context.Background(), bearerReq(t, "nope"))
		if err == nil {
			t.Fatalf("got (%v, nil), want a non-nil error", info)
		}
	})

	t.Run("valid token maps to a User principal", func(t *testing.T) {
		info, err := af(context.Background(), bearerReq(t, "good"))
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		p, ok := info.(Principal)
		if !ok {
			t.Fatalf("info type = %T, want auth.Principal", info)
		}
		if p.Kind != "user" || p.Subject != "alice" || p.Raw != "user/alice" {
			t.Fatalf("principal = %+v", p)
		}
		if len(p.Groups) != 2 || p.Groups[0] != "reflw-admins" {
			t.Fatalf("groups = %v", p.Groups)
		}
		if p.Claims["email"] != "alice@example.com" {
			t.Fatalf("claims = %v", p.Claims)
		}
	})
}

func TestBearerToken(t *testing.T) {
	cases := []struct {
		header string
		want   string
		wantOK bool
	}{
		{"", "", false},
		{"Bearer abc", "abc", true},
		{"bearer abc", "abc", true},   // scheme is case-insensitive
		{"Bearer  abc ", "abc", true}, // surrounding whitespace trimmed
		{"Basic abc", "", false},
		{"Bearer ", "", false}, // no token
		{"Bearer", "", false},  // bare scheme
	}
	for _, tc := range cases {
		h := http.Header{}
		if tc.header != "" {
			h.Set("Authorization", tc.header)
		}
		got, ok := bearerToken(h)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("bearerToken(%q) = (%q, %v), want (%q, %v)", tc.header, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestStringsFromClaim(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want []string
	}{
		{"json array", []any{"a", "b"}, []string{"a", "b"}},
		{"json array with non-strings", []any{"a", 1, "b"}, []string{"a", "b"}},
		{"string slice", []string{"a"}, []string{"a"}},
		{"single string", "solo", []string{"solo"}},
		{"empty string", "", nil},
		{"unsupported type", 42, nil},
		{"nil", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stringsFromClaim(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d (%v), want %d (%v)", len(got), got, len(tc.want), tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}
