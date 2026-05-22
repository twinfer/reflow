//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// SkipUnlessDocker calls t.Skip if the local Docker daemon is not
// reachable. Every e2e test calls this on its first line so the suite
// degrades gracefully on hosts without Docker — mirrors the pattern at
// internal/ingress/eventsource/integration_kafka_test.go:46 where
// container-init failure becomes a Skip, not a Fail.
func SkipUnlessDocker(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("e2e: docker provider unavailable: %v", err)
	}
	if err := provider.Health(ctx); err != nil {
		t.Skipf("e2e: docker daemon not reachable: %v", err)
	}
}
