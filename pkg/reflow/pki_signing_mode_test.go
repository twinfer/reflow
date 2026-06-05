package reflow

import (
	"strings"
	"testing"

	"github.com/twinfer/reflw/internal/certmgr"
)

func TestCertmgrSigningMode_Mapping(t *testing.T) {
	cases := []struct {
		in      string
		want    certmgr.SigningMode
		wantErr bool
	}{
		{"", certmgr.SigningModeLocal, false},
		{"local", certmgr.SigningModeLocal, false},
		{"kms_remote", certmgr.SigningModeRemote, false},
		{"LOCAL", 0, true},
		{"kms-remote", 0, true},
		{"acme", 0, true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := certmgrSigningMode(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", c.in)
				}
				if !strings.Contains(err.Error(), "unknown signing mode") {
					t.Errorf("error missing diagnostic substring: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("certmgrSigningMode(%q) = %d; want %d", c.in, got, c.want)
			}
		})
	}
}
