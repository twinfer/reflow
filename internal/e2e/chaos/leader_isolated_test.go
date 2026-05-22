//go:build e2e

package chaos_test

import (
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/e2e"
	"github.com/twinfer/reflow/internal/e2e/chaos"
)

// TestChaos_LeaderIsolated runs a steady-state workload, isolates the
// shard-1 leader from every other peer for `isolateFor` mid-flight,
// then heals. Because raft + delivery flow through the per-source
// toxiproxy sidecars (see internal/e2e/toxiproxy.go), the Cut is
// surgically per-pair: survivors keep links to each other and
// re-elect among themselves; the isolated node stays online but
// cannot reach the majority.
//
// Post-run invariant: every issued invocation reaches a terminal
// state within awaitTerminal.
func TestChaos_LeaderIsolated(t *testing.T) {
	const isolateFor = 10 * time.Second
	runScenario(t, scenarioConfig{
		withToxiproxy: true,
		rate:          50,
		concurrency:   16,
		duration:      30 * time.Second,
		awaitTerminal: 120 * time.Second,
		chaosAfter:    5 * time.Second,
		chaos: func(t *testing.T, c *e2e.ContainerCluster) {
			chaos.PartitionLeader(t, c, isolateFor)
		},
	})
}
