//go:build e2e

package chaos_test

import (
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/e2e"
	"github.com/twinfer/reflow/internal/e2e/chaos"
)

// TestChaos_LeaderKill runs a steady-state workload, SIGKILLs the
// shard-1 leader once mid-flight, and verifies invariants hold:
// every issued invocation reaches a terminal state. The killed node
// stays down for the rest of the run (chaos.LeaderKill does the kill
// and waits for re-election but does NOT restart).
func TestChaos_LeaderKill(t *testing.T) {
	runScenario(t, scenarioConfig{
		rate:          50,
		concurrency:   16,
		duration:      30 * time.Second,
		awaitTerminal: 120 * time.Second,
		chaosAfter:    5 * time.Second,
		chaos: func(t *testing.T, c *e2e.ContainerCluster) {
			chaos.LeaderKill(t, c, 30*time.Second)
		},
	})
}
