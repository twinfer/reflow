//go:build e2e

package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/e2e"
	"github.com/twinfer/reflw/internal/e2e/chaos"
)

// TestChaos_KillRestartIdle exercises the kill+restart primitives
// against an idle cluster — the lower-bound sanity check that the
// primitives themselves work cleanly with no workload running.
//
// Two-step structure (kept simple to isolate failure mode):
//
//   - Step 1: kill the current shard-1 leader and verify the remaining
//     peers re-elect a leader. This catches bugs where kill alone
//     destabilizes unrelated shards on the surviving replicas.
//
//   - Step 2: restart the killed container (same data dir, fresh
//     process) and verify the cluster remains stable. This catches
//     bugs in the rejoin path (Pebble lock release, dragonboat
//     NodeHostDir re-open, etc.) without concurrent traffic muddying
//     the signal.
//
// Under-load chaos behavior is exercised by TestChaos_LeaderLoss
// (separate test in this directory).
func TestChaos_KillRestartIdle(t *testing.T) {
	cluster := e2e.NewContainerCluster(t, e2e.ContainerClusterOptions{N: 3, NumShards: 1})

	// Step 1: kill the shard-1 leader and verify the survivors re-elect.
	killedIdx := chaos.LeaderKill(t, cluster, 30*time.Second)
	t.Logf("kill phase complete; surviving %d-1 nodes re-elected", len(cluster.Nodes))

	// Step 2: restart the killed container in place.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := cluster.Nodes[killedIdx].Restart(ctx); err != nil {
		t.Fatalf("Restart(%d): %v", killedIdx, err)
	}
	// Verify the cluster reconverges. The restart node rejoins as a
	// follower; we only check that some shard-1 leader still exists.
	if n := cluster.AwaitPartitionLeader(ctx, 1, 30*time.Second); n == nil {
		t.Fatalf("no shard-1 leader after restart of node idx=%d", killedIdx)
	}
	t.Logf("kill+restart cycle complete; cluster stable")
}
