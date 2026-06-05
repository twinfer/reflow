//go:build e2e

package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/e2e"
)

// TestChaos_RollingRestart runs a steady-state workload while every
// node is killed and restarted exactly once in sequence.
//
// Each cycle: kill node, wait settle (so traffic notices the loss),
// restart node, await re-elected leader before advancing. The settle
// pause prevents fast cycles from transitioning through a no-quorum
// window twice in a row.
func TestChaos_RollingRestart(t *testing.T) {
	const (
		settle           = 8 * time.Second // longer than in-proc (3s) — container restart is heavier
		awaitLeader      = 30 * time.Second
		settleAfterStart = 5 * time.Second
	)
	runScenario(t, scenarioConfig{
		rate:          30,
		concurrency:   8,
		duration:      90 * time.Second,
		awaitTerminal: 120 * time.Second,
		chaosAfter:    5 * time.Second,
		chaos: func(t *testing.T, c *e2e.ContainerCluster) {
			for i := range c.Nodes {
				if c.Nodes[i] == nil {
					continue
				}
				t.Logf("rolling restart: killing idx=%d node=%d", i, c.Nodes[i].NodeID())
				c.Nodes[i].Kill()
				time.Sleep(settle)

				// Surviving (n-1) replicas must hold a shard-1 leader
				// before we re-introduce this node.
				ctx, cancel := context.WithTimeout(context.Background(), awaitLeader)
				if n := c.AwaitPartitionLeader(ctx, 1, awaitLeader); n == nil {
					cancel()
					t.Fatalf("rolling restart: no shard-1 leader after killing idx=%d", i)
				}
				cancel()

				t.Logf("rolling restart: restarting idx=%d", i)
				rctx, rcancel := context.WithTimeout(context.Background(), 60*time.Second)
				if err := c.Nodes[i].Restart(rctx); err != nil {
					rcancel()
					t.Fatalf("rolling restart: Restart(%d): %v", i, err)
				}
				rcancel()
				// Wait for the cluster to stabilize before the next
				// cycle. Either the same node retains leadership or
				// the restart node rejoins as follower — either way
				// some shard-1 leader exists.
				lctx, lcancel := context.WithTimeout(context.Background(), awaitLeader)
				if n := c.AwaitPartitionLeader(lctx, 1, awaitLeader); n == nil {
					lcancel()
					t.Fatalf("rolling restart: no shard-1 leader after restart idx=%d", i)
				}
				lcancel()
				time.Sleep(settleAfterStart)
			}
		},
	})
}
