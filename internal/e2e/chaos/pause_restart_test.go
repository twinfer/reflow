//go:build e2e

package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/e2e"
)

// TestChaos_LeaderPauseUnpause freezes the shard-1 leader's cgroup
// mid-flight via Docker ContainerPause, lets peers' raft heartbeats
// time out (a new leader gets elected on the surviving majority),
// then Unpauses. The previously-paused node rejoins as a follower
// and catches up via raft snapshot or log replication. Distinct
// from Kill in that no process restart is involved and the in-memory
// state survives the freeze — useful for testing the "node is alive
// but unreachable" shape (cgroup throttle, GC pause, host scheduler
// starvation) without simulating a crash.
//
// Invariant: every issued invocation reaches a terminal state within
// awaitTerminal.
func TestChaos_LeaderPauseUnpause(t *testing.T) {
	const (
		pauseFor    = 8 * time.Second
		awaitLeader = 30 * time.Second
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
				t.Fatal("no shard-1 leader at pause time")
			}
			var idx int = -1
			for i, n := range c.Nodes {
				if n != nil && n.NodeID() == leader.NodeID() {
					idx = i
					break
				}
			}
			if idx < 0 {
				t.Fatalf("leader node %d not in cluster.Nodes", leader.NodeID())
			}

			t.Logf("chaos: pausing idx=%d node=%d (shard-1 leader) for %s",
				idx, leader.NodeID(), pauseFor)
			pctx, pcancel := context.WithTimeout(context.Background(), 15*time.Second)
			if err := c.Nodes[idx].Pause(pctx); err != nil {
				pcancel()
				t.Fatalf("Pause(%d): %v", idx, err)
			}
			pcancel()

			// While the original leader is frozen, the surviving
			// majority must elect a new one or commits stall and the
			// workload's invariants fail post-run.
			lctx, lcancel := context.WithTimeout(context.Background(), awaitLeader)
			if n := c.AwaitPartitionLeader(lctx, 1, awaitLeader); n == nil {
				lcancel()
				t.Fatal("no replacement shard-1 leader during pause window")
			}
			lcancel()

			time.Sleep(pauseFor)

			uctx, ucancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer ucancel()
			t.Logf("chaos: unpausing idx=%d node=%d", idx, leader.NodeID())
			if err := c.Nodes[idx].Unpause(uctx); err != nil {
				t.Fatalf("Unpause(%d): %v", idx, err)
			}

			// Verify the cluster still has a (potentially different)
			// shard-1 leader after the unpause; the previously-paused
			// node may rejoin as follower or be re-elected.
			rctx, rcancel := context.WithTimeout(context.Background(), awaitLeader)
			defer rcancel()
			if n := c.AwaitPartitionLeader(rctx, 1, awaitLeader); n == nil {
				t.Fatal("no shard-1 leader after unpause")
			}
		},
	})
}
