package certmgr

import (
	"strings"
	"testing"
)

func TestSafeMeshName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"node id", "node/7", "node-7"},
		{"operator slug", "operator/alice", "operator-alice"},
		{"uppercase normalises", "operator/Alice", "operator-alice"},
		{"hyphenated operator", "operator/alice-bob", "operator-alice-bob"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := safeMeshName(c.in); got != c.want {
				t.Errorf("safeMeshName(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

func TestSafeMeshName_FallbackOnInvalid(t *testing.T) {
	// '_' is rejected by IDNA Lookup (strict); we must fall back to
	// the SHA-256 form so cert issuance keeps working for principals
	// the operator chose freely.
	got := safeMeshName("operator/john_doe")
	if !strings.HasPrefix(got, "reflw-") || !strings.HasSuffix(got, ".mesh") {
		t.Errorf("expected SHA-256 fallback form; got %q", got)
	}
}

func TestSafeMeshName_DeterministicAcrossCalls(t *testing.T) {
	first := safeMeshName("node/42")
	second := safeMeshName("node/42")
	if first != second {
		t.Errorf("safeMeshName should be deterministic; got %q then %q", first, second)
	}
}
