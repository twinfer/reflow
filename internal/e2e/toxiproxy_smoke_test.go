//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/e2e"
	"github.com/twinfer/reflow/internal/loadgen"
)

// TestSmoke_ToxiproxyLeaderIsolation is the PR-4 capstone. Goals:
//
//  1. Verify the 3-node cluster boots with the toxiproxy sidecars in
//     the routing path — every raft message and every cross-shard
//     delivery between containers traverses a toxiproxy proxy. If the
//     sidecar topology is wired up correctly the bootstrap is
//     indistinguishable from the no-toxiproxy variant, just with the
//     proxies sitting in the path.
//
//  2. Verify Cut/Heal actually mutate proxy state via the toxiproxy
//     HTTP control API — a regression in the wiring (e.g. wrong proxy
//     key, missing client) shows up here as an error from Cut.
//
//  3. Verify the behavioral consequence: isolating the partition-shard
//     leader from its peers causes a leader change. This is the
//     replacement for the in-proc PartitionMatrix-driven test the
//     bufconn harness used to do, expressed in container-shaped terms.
//
// Deliberately scoped: this test does not assert end-to-end invocation
// completion under partition. That's the chaos suite (PR 5) — here we
// only need to know the topology works at all so the chaos suite has
// a stable foundation to build on.
func TestSmoke_ToxiproxyLeaderIsolation(t *testing.T) {
	cluster := e2e.NewContainerCluster(t, e2e.ContainerClusterOptions{
		N:             3,
		NumShards:     1,
		WithToxiproxy: true,
	})
	if cluster.Tx == nil {
		t.Fatal("ContainerCluster.Tx is nil; WithToxiproxy did not wire the sidecars")
	}

	// Identify the current partition-shard leader. We can't observe
	// shard 0 (metadata) via ingress.ListPartitions, but partition
	// shard 1 is sufficient: dragonboat assigns leaders independently
	// per shard, and isolating a node from raft traffic affects every
	// shard the node participates in.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const targetShard uint64 = 1
	leader := findShardLeader(ctx, t, cluster, targetShard, 60*time.Second)
	if leader == nil {
		t.Fatal("no shard-1 leader observed within 60s after bring-up")
	}
	t.Logf("initial shard-%d leader = node %d", targetShard, leader.NodeID())

	// Build the peer list and isolate the leader from everyone. The
	// isolation is symmetric per pair, so the leader's outbound + its
	// peers' inbound are both dropped. The remaining peers retain
	// connectivity among themselves and should elect a new leader
	// within dragonboat's election timeout (default ~1s) plus a few
	// retries.
	peers := make([]uint64, 0, len(cluster.Nodes))
	for _, n := range cluster.Nodes {
		if n == nil {
			continue
		}
		peers = append(peers, n.NodeID())
	}
	if err := cluster.Tx.Isolate(leader.NodeID(), peers); err != nil {
		t.Fatalf("Isolate(leader=%d): %v", leader.NodeID(), err)
	}

	// Poll the surviving peers (anyone other than the isolated leader)
	// for a shard-1 leader != the old leader. We poll via the surviving
	// peers' ingress because the isolated leader's own
	// ListPartitions reflects its stale Leadership view.
	newLeader := waitNewShardLeader(ctx, t, cluster, targetShard, leader.NodeID(), 30*time.Second)
	if newLeader == nil {
		t.Fatalf("no new shard-%d leader observed within 30s after isolating node %d",
			targetShard, leader.NodeID())
	}
	t.Logf("new shard-%d leader after isolation = node %d", targetShard, newLeader.NodeID())

	// Heal the network. Cluster should converge; the previously
	// isolated node rejoins as a follower of the new leader. We don't
	// assert the post-heal leader identity (raft is free to keep the
	// new leader); we only require Heal to return cleanly.
	if err := cluster.Tx.HealAll(); err != nil {
		t.Fatalf("HealAll: %v", err)
	}
}

// findShardLeader polls every node's ingress.ListPartitions until one
// node reports IsLeader=true for the given shard. Returns the first
// such node, or nil on timeout.
func findShardLeader(ctx context.Context, t *testing.T, cluster *e2e.ContainerCluster, shardID uint64, timeout time.Duration) *e2e.ContainerNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range cluster.Nodes {
			if n == nil {
				continue
			}
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			parts, err := n.ListPartitions(pctx)
			cancel()
			if err != nil {
				continue
			}
			for _, p := range parts {
				if p.ShardID == shardID && p.IsLeader {
					return n
				}
			}
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// waitNewShardLeader polls every node EXCEPT excludeID until one
// reports IsLeader=true for shardID with a different node winning.
// The dragonboat ListPartitions response from any surviving peer
// reflects gossip-derived ShardView; the leader's own (isolated)
// view is stale until heal.
func waitNewShardLeader(ctx context.Context, t *testing.T, cluster *e2e.ContainerCluster, shardID, excludeID uint64, timeout time.Duration) *e2e.ContainerNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range cluster.Nodes {
			if n == nil || n.NodeID() == excludeID {
				continue
			}
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			parts, err := n.ListPartitions(pctx)
			cancel()
			if err != nil {
				continue
			}
			for _, p := range parts {
				if p.ShardID != shardID {
					continue
				}
				if p.IsLeader && n.NodeID() != excludeID {
					return n
				}
			}
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// Compile-time guard: assert loadgen.PartitionInfo is what we believe
// it is so refactors flag this test before it silently breaks.
var _ = loadgen.PartitionInfo{ShardID: 0, IsLeader: false, LeaderEpoch: 0}
