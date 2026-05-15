//go:build loadtest

package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/chaos"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
)

// TestChaos_KillRestartIdle exercises the kill+restart primitives
// against an idle cluster — the lower-bound sanity check that the
// primitives themselves work cleanly with no workload running.
//
// Two-step structure (kept simple to isolate failure mode):
//
//   - Step 1: kill the current metadata leader and verify every
//     partition shard still has a leader on the surviving n-1
//     replicas. This catches bugs where kill alone destabilizes
//     unrelated shards.
//   - Step 2: restart the killed node and verify the cluster
//     remains stable. This catches bugs in the rejoin path (Pebble
//     lock release, dragonboat NodeHostDir re-open, etc.) without
//     concurrent traffic muddying the signal.
//
// Under-load chaos behavior is exercised by TestChaos_LeaderLoss
// and TestChaos_RollingRestart, which currently fail by design —
// the harness surfaces a known leader-failover recovery gap
// (invocations stuck in Scheduled state) until it is fixed.
//
// Invocation:
//
//	go test -tags=loadtest -timeout=5m -run=TestChaos_KillRestartIdle \
//	    -v ./internal/chaos/...
func TestChaos_KillRestartIdle(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("noop", "fn", loadgen.HelloHandler); err != nil {
		t.Fatalf("register: %v", err)
	}

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3, Handlers: reg})
	defer cluster.Close()

	// Step 1 — kill the metadata leader and verify the surviving
	// 2-node majority can still elect leaders on every shard.
	killedIdx := chaos.LeaderKill(t, cluster, 30*time.Second)
	for sh := uint64(1); sh <= 3; sh++ {
		ctxSh, cancelSh := context.WithTimeout(context.Background(), 30*time.Second)
		if err := cluster.AwaitAnyPartitionLeader(ctxSh, sh); err != nil {
			cancelSh()
			t.Fatalf("no leader for shard %d after kill: %v", sh, err)
		}
		cancelSh()
	}
	t.Logf("kill phase complete; all shards have leaders on n-1 nodes")

	// Step 2 — restart the killed node and verify the cluster stays
	// stable (still has leaders for every shard, possibly the same
	// pre-kill ones since rejoin doesn't transfer leadership).
	if err := cluster.RestartNode(t, killedIdx); err != nil {
		t.Fatalf("RestartNode(%d): %v", killedIdx+1, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cluster.AwaitAnyMetadataLeader(ctx); err != nil {
		t.Fatalf("no metadata leader after restart: %v", err)
	}
	for sh := uint64(1); sh <= 3; sh++ {
		ctxSh, cancelSh := context.WithTimeout(context.Background(), 30*time.Second)
		if err := cluster.AwaitAnyPartitionLeader(ctxSh, sh); err != nil {
			cancelSh()
			t.Fatalf("no leader for shard %d after restart: %v", sh, err)
		}
		cancelSh()
	}
	t.Logf("kill+restart cycle complete; cluster stable")
}
