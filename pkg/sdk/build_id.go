package sdk

import (
	"fmt"
	"runtime/debug"
	"sync"
)

// BuildID returns a short string identifying the SDK build embedded in
// the running binary. Format: "<module-version>+<vcs-revision[:12]>" with
// fallbacks for partial info. Empty string when no build info is
// available (e.g. some tests, or "go run" builds).
//
// The engine logs this string on every session start so operators can
// correlate behavior changes with SDK upgrades. The value is computed
// once at process startup and cached.
func BuildID() string {
	buildIDOnce.Do(func() { buildID = computeBuildID() })
	return buildID
}

var (
	buildIDOnce sync.Once
	buildID     string
)

func computeBuildID() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info == nil {
		return ""
	}
	version := info.Main.Version
	if version == "" || version == "(devel)" {
		version = "devel"
	}
	rev := ""
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			rev = s.Value
			break
		}
	}
	if rev != "" {
		if len(rev) > 12 {
			rev = rev[:12]
		}
		return fmt.Sprintf("%s+%s", version, rev)
	}
	return version
}
