//go:build e2e

package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/e2e"
)

// TestChaos_LeaderSIGKILL runs a steady-state workload against a
// 3-node containerized cluster, SIGKILLs the leader of partition 1
// mid-flight, restarts the same container (data dir intact), and
// asserts every issued invocation reaches a terminal state.
//
// This is the test the in-proc loadgen.Cluster cannot run: an
// in-process Kill goes through Host.Close which drains the Pebble
// WAL — same observable behavior on the happy path but not the same
// code path. Docker ContainerKill with SIGKILL leaves the WAL
// un-fsynced, so recovery on restart genuinely replays the torn
// write and exercises Pebble + dragonboat's startup invariants.
//
// Historically skipped (subprocess loadnode lacked handler
// registration); now live because handler registration runs through
// the same Config admin RPC the production cluster uses, and the
// handler sidecar survives the reflowd kill.
func TestChaos_LeaderSIGKILL(t *testing.T) {
	const (
		killGrace   = 1 * time.Second
		awaitLeader = 60 * time.Second
	)
	runScenario(t, scenarioConfig{
		rate:          30,
		concurrency:   8,
		duration:      30 * time.Second,
		awaitTerminal: 120 * time.Second,
		chaosAfter:    5 * time.Second,
		chaos: func(t *testing.T, c *e2e.ContainerCluster) {
			fctx, fcancel := context.WithTimeout(context.Background(), 30*time.Second)
			leader := c.FindPartitionLeader(fctx, 1)
			fcancel()
			if leader == nil {
				t.Errorf("no shard-1 leader at SIGKILL time")
				return
			}
			var idx int = -1
			for i, n := range c.Nodes {
				if n != nil && n.NodeID() == leader.NodeID() {
					idx = i
					break
				}
			}
			if idx < 0 {
				t.Errorf("leader node %d not in cluster.Nodes", leader.NodeID())
				return
			}
			t.Logf("SIGKILL idx=%d node=%d (shard-1 leader)", idx, leader.NodeID())
			c.Nodes[idx].Kill()

			// Let the OS reap before docker start; otherwise the
			// daemon can race the container-state transition.
			time.Sleep(killGrace)

			t.Logf("restarting idx=%d from torn WAL", idx)
			rctx, rcancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer rcancel()
			if err := c.Nodes[idx].Restart(rctx); err != nil {
				t.Errorf("Restart(%d): %v", idx, err)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), awaitLeader)
			defer cancel()
			if n := c.AwaitPartitionLeader(ctx, 1, awaitLeader); n == nil {
				t.Errorf("shard-1 leader never re-elected after SIGKILL+restart")
			}
		},
	})
}
