// Package chaos orchestrates fault scenarios on top of
// internal/loadgen.Cluster's kill / restart primitives. Each
// scenario function performs one bounded sequence of faults
// (e.g., kill the current leader once) and returns when the
// cluster is stable again. Workload-side correctness is the
// caller's responsibility — pair a scenario with a running
// loadgen.WorkloadConfig and check post-run invariants.
//
// Phase 5: see durable-execution-go-sad.md §10.
package chaos

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/loadgen"
)

// LeaderKill identifies the current metadata-shard leader, kills
// that node, and blocks until a replacement leader is elected.
// Returns the index of the killed node so the caller can decide
// whether to restart it later.
//
// If no metadata leader is currently present, the function fails
// the test — the harness's contract is that the cluster reaches a
// stable leader state before chaos begins.
func LeaderKill(t testing.TB, c *loadgen.Cluster, awaitNewLeader time.Duration) int {
	t.Helper()
	idx := -1
	for i, n := range c.Nodes {
		if n == nil {
			continue
		}
		if mr := n.Host.MetadataRunner(); mr != nil && mr.IsLeader() {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatal("chaos: LeaderKill: no metadata leader to kill")
	}
	t.Logf("chaos: killing metadata leader node=%d", idx+1)
	c.KillNode(idx)

	ctx, cancel := context.WithTimeout(context.Background(), awaitNewLeader)
	defer cancel()
	if err := c.AwaitAnyMetadataLeader(ctx); err != nil {
		t.Fatalf("chaos: no new metadata leader within %s after kill: %v", awaitNewLeader, err)
	}
	t.Logf("chaos: new metadata leader elected after killing node=%d", idx+1)
	return idx
}

// IsolateNode cuts the bufconn raft links between cluster.Nodes[idx]
// and every other live node for dur, then heals them. The cluster
// must have been constructed with a BufconnTransportFactory backed by
// matrix; otherwise the Cut/Heal calls are no-ops on a matrix that
// nothing consults. Used to test partial-partition behavior (a node
// is unreachable for raft but the rest of the cluster still has
// quorum among itself).
func IsolateNode(t testing.TB, c *loadgen.Cluster, idx int, matrix *loadgen.PartitionMatrix, dur time.Duration) {
	t.Helper()
	if idx < 0 || idx >= len(c.Nodes) {
		t.Fatalf("chaos: IsolateNode: idx %d out of range", idx)
	}
	self := c.RaftAddr(idx)
	if self == "" {
		t.Fatalf("chaos: IsolateNode: no RaftAddr for idx %d", idx)
	}
	peers := make([]string, 0, len(c.Nodes)-1)
	for i := range c.Nodes {
		if i == idx {
			continue
		}
		if a := c.RaftAddr(i); a != "" {
			peers = append(peers, a)
		}
	}
	t.Logf("chaos: isolating node=%d (%s) from %d peers for %s", idx+1, self, len(peers), dur)
	for _, p := range peers {
		matrix.Cut(self, p)
	}
	time.Sleep(dur)
	for _, p := range peers {
		matrix.Heal(self, p)
	}
	t.Logf("chaos: healed node=%d (%s) after %s", idx+1, self, dur)
}

// PartitionLeader isolates the current metadata leader from every
// other peer for dur, then heals. Returns the index of the isolated
// node. The remaining peers retain links to each other, so they
// re-elect among themselves while the original leader is offline-
// from-raft's-perspective.
func PartitionLeader(t testing.TB, c *loadgen.Cluster, matrix *loadgen.PartitionMatrix, dur time.Duration) int {
	t.Helper()
	idx := -1
	for i, n := range c.Nodes {
		if n == nil {
			continue
		}
		if mr := n.Host.MetadataRunner(); mr != nil && mr.IsLeader() {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatal("chaos: PartitionLeader: no metadata leader to isolate")
	}
	IsolateNode(t, c, idx, matrix, dur)
	return idx
}

// RollingRestart cycles every node in order: kill, wait `settle`,
// restart, await metadata leader, wait `settle` again, advance.
// Returns when every node has been cycled exactly once.
//
// The settle pause between fault transitions gives the cluster
// time to converge before the next kill — without it a fast cycle
// can transition through a no-quorum window twice in a row.
func RollingRestart(t testing.TB, c *loadgen.Cluster, settle time.Duration) {
	t.Helper()
	for i := range c.Nodes {
		t.Logf("chaos: rolling restart — killing node %d", i+1)
		c.KillNode(i)
		time.Sleep(settle)

		// Wait for the remaining (n-1) replicas to re-elect a
		// metadata leader before re-introducing this node.
		// Otherwise StartMetadataShard on the restarting node
		// can race the surviving group's election.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := c.AwaitAnyMetadataLeader(ctx); err != nil {
			cancel()
			t.Fatalf("chaos: no metadata leader after killing node %d: %v", i+1, err)
		}
		cancel()

		t.Logf("chaos: rolling restart — restarting node %d", i+1)
		if err := c.RestartNode(t, i); err != nil {
			t.Fatalf("chaos: RestartNode(%d): %v", i+1, err)
		}

		// Wait for the cluster to stabilize before the next kill.
		ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
		if err := c.AwaitAnyMetadataLeader(ctx2); err != nil {
			cancel2()
			t.Fatalf("chaos: no metadata leader after restarting node %d: %v", i+1, err)
		}
		cancel2()
		time.Sleep(settle)
	}
}
