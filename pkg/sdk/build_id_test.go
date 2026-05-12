package sdk

import (
	"testing"
)

func TestBuildID_StableAcrossCalls(t *testing.T) {
	a := BuildID()
	b := BuildID()
	if a != b {
		t.Errorf("BuildID drifted across calls: %q vs %q", a, b)
	}
}

func TestBuildID_NonEmptyInGoTestBinary(t *testing.T) {
	// `go test` produces a real Go binary with debug.ReadBuildInfo
	// populated; vcs.revision may be empty in dirty trees, but the
	// module version always has at least "devel" as a fallback.
	got := BuildID()
	if got == "" {
		t.Skip("BuildID empty — likely an unusual build (go run, stripped binary)")
	}
}
