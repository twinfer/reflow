//go:build e2e

// Package chaos orchestrates fault scenarios on top of the
// internal/e2e ContainerCluster. Each scenario function performs one
// bounded sequence of faults (kill the current leader once, isolate
// a node for a window, ...) and returns when the cluster is stable
// again. Workload-side correctness is the caller's responsibility:
// pair a scenario with a running loadgen.WorkloadConfig and check
// post-run invariants with loadgen.AwaitCompletion.
//
// This is the container-shaped port of the historical internal/chaos
// package. Lifecycle chaos goes through Docker API SIGKILL
// (ContainerNode.Kill), not host signals; network chaos goes through
// the per-cluster Toxiproxy handle (e2e.ContainerCluster.Tx), not
// the bufconn PartitionMatrix.
package chaos

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/e2e"
)

// LeaderKill identifies the current shard-1 leader, kills that node
// via Docker ContainerKill (SIGKILL — the chaos signal that bypasses
// pebble WAL fsync drain), and blocks until a replacement leader is
// elected. Returns the index of the killed node so the caller can
// restart it later via cluster.Nodes[idx].Kill / restart in the
// container.
//
// Uses shard 1 instead of shard 0 because ingress.ListPartitions
// excludes shard 0 (the metadata shard); they share a NodeHost so a
// shard-1 leader exists exactly when the metadata leader is alive on
// the same node — close enough for fault-injection semantics.
func LeaderKill(t testing.TB, c *e2e.ContainerCluster, awaitNewLeader time.Duration) int {
	t.Helper()
	idx := findLeaderIdx(t, c, 1)
	if idx < 0 {
		t.Fatal("chaos: LeaderKill: no shard-1 leader to kill")
	}
	t.Logf("chaos: killing shard-1 leader idx=%d node=%d", idx, c.Nodes[idx].NodeID())
	c.Nodes[idx].Kill()

	ctx, cancel := context.WithTimeout(context.Background(), awaitNewLeader)
	defer cancel()
	if n := c.AwaitPartitionLeader(ctx, 1, awaitNewLeader); n == nil {
		t.Fatalf("chaos: no new shard-1 leader within %s after kill", awaitNewLeader)
	}
	t.Logf("chaos: new shard-1 leader elected after killing idx=%d", idx)
	return idx
}

// IsolateNode cuts the toxiproxy raft links between cluster.Nodes[idx]
// and every other live peer for `dur`, then heals them. Requires the
// cluster to have been built with WithToxiproxy=true; on a non-tx
// cluster the function fails the test rather than silently no-op.
//
// "Cut(self, peer)" is bidirectional under the toxiproxy harness, so
// each call disables both the self→peer and peer→self proxy. The
// surviving peers retain their links to each other.
func IsolateNode(t testing.TB, c *e2e.ContainerCluster, idx int, dur time.Duration) {
	t.Helper()
	if c.Tx == nil {
		t.Fatal("chaos: IsolateNode: cluster has no toxiproxy; build with WithToxiproxy=true")
	}
	if idx < 0 || idx >= len(c.Nodes) || c.Nodes[idx] == nil {
		t.Fatalf("chaos: IsolateNode: idx %d out of range", idx)
	}
	self := c.Nodes[idx].NodeID()
	peers := c.PeerIDs()
	t.Logf("chaos: isolating idx=%d node=%d from %d peers for %s", idx, self, len(peers)-1, dur)
	if err := c.Tx.Isolate(self, peers); err != nil {
		t.Fatalf("chaos: Isolate(%d): %v", self, err)
	}
	time.Sleep(dur)
	for _, p := range peers {
		if p == self {
			continue
		}
		if err := c.Tx.Heal(self, p); err != nil {
			t.Fatalf("chaos: Heal(%d, %d): %v", self, p, err)
		}
	}
	t.Logf("chaos: healed idx=%d node=%d after %s", idx, self, dur)
}

// PartitionLeader isolates the current shard-1 leader from every
// other peer for `dur`, then heals. Returns the index of the
// isolated node. Mirrors the historical internal/chaos.PartitionLeader
// semantics with the toxiproxy-backed Cut/Heal instead of the bufconn
// PartitionMatrix.
func PartitionLeader(t testing.TB, c *e2e.ContainerCluster, dur time.Duration) int {
	t.Helper()
	idx := findLeaderIdx(t, c, 1)
	if idx < 0 {
		t.Fatal("chaos: PartitionLeader: no shard-1 leader to isolate")
	}
	IsolateNode(t, c, idx, dur)
	return idx
}

// findLeaderIdx returns the slice index of the shard-1 leader, or -1.
// Polls briefly because leader election can race startup.
func findLeaderIdx(t testing.TB, c *e2e.ContainerCluster, shardID uint64) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n := c.AwaitPartitionLeader(ctx, shardID, 30*time.Second)
	if n == nil {
		return -1
	}
	for i, node := range c.Nodes {
		if node != nil && node.NodeID() == n.NodeID() {
			return i
		}
	}
	return -1
}
