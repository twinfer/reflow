//go:build e2e

// Package e2e is the containerized end-to-end harness driving real
// reflowd binaries inside Docker. Chaos, eventsource, kms, and snapshot
// suites under internal/e2e/... share this package's primitives.
//
// All files carry the e2e build tag so the package is invisible to
// `make test`; CI runs e2e on its own job (see Makefile: test-e2e).
package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

const (
	reflowdImageRepo     = "reflow/reflowd"
	reflowdImageTag      = "e2e"
	loadhandlerImageRepo = "reflow/loadhandler"
	loadhandlerImageTag  = "e2e"
)

var (
	reflowdImageOnce sync.Once
	reflowdImageRef  string
	reflowdImageErr  error

	loadhandlerImageOnce sync.Once
	loadhandlerImageRef  string
	loadhandlerImageErr  error
)

// ReflowdImage returns a Docker image reference for the reflowd binary
// under test. The first call builds via Dockerfile.reflowd at the repo
// root and caches the resulting tag; subsequent calls in the same
// `go test` invocation reuse it. CI prebuild + REFLOW_E2E_IMAGE
// short-circuits the build entirely.
//
// Skips the test (rather than failing it) if Docker is unavailable —
// matches the behavior of SkipUnlessDocker and keeps `go test ./...`
// passing on hosts without a daemon.
func ReflowdImage(t testing.TB) string {
	t.Helper()
	if env := os.Getenv("REFLOW_E2E_IMAGE"); env != "" {
		return env
	}
	reflowdImageOnce.Do(func() {
		reflowdImageRef, reflowdImageErr = buildReflowdImage()
	})
	if reflowdImageErr != nil {
		t.Skipf("e2e: reflowd image build failed: %v", reflowdImageErr)
	}
	return reflowdImageRef
}

func buildReflowdImage() (string, error) {
	return buildImage("Dockerfile.reflowd", reflowdImageRepo, reflowdImageTag)
}

// LoadhandlerImage returns the Docker image reference for the sidecar
// handler under test. Same shape as ReflowdImage: cached behind
// sync.Once, REFLOW_E2E_LOADHANDLER_IMAGE overrides the build.
func LoadhandlerImage(t testing.TB) string {
	t.Helper()
	if env := os.Getenv("REFLOW_E2E_LOADHANDLER_IMAGE"); env != "" {
		return env
	}
	loadhandlerImageOnce.Do(func() {
		loadhandlerImageRef, loadhandlerImageErr = buildImage(
			"Dockerfile.loadhandler", loadhandlerImageRepo, loadhandlerImageTag,
		)
	})
	if loadhandlerImageErr != nil {
		t.Skipf("e2e: loadhandler image build failed: %v", loadhandlerImageErr)
	}
	return loadhandlerImageRef
}

func buildImage(dockerfile, repo, tag string) (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	// Cold build dominates here; warm builds with Docker's layer cache
	// finish in ~1-2s when go.mod hasn't changed.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return "", fmt.Errorf("docker provider: %w", err)
	}
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    root,
			Dockerfile: dockerfile,
			Repo:       repo,
			Tag:        tag,
			KeepImage:  true,
		},
	}
	out, err := provider.BuildImage(ctx, &req)
	if err != nil {
		return "", fmt.Errorf("build %s: %w", dockerfile, err)
	}
	return out, nil
}

// repoRoot walks up from this file's directory to the one containing
// go.mod. The result is stable across tests (the module layout doesn't
// move at runtime) so callers needn't cache further.
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", filepath.Dir(file))
		}
		dir = parent
	}
}
