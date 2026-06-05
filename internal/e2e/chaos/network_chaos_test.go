//go:build e2e

package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/e2e"
)

// TestChaos_NetworkLatency runs a steady-state workload while a fixed
// pair of nodes (the shard-1 leader and one follower) has 200ms of
// added latency in both directions. The third peer is untouched, so
// the cluster always retains a fast majority for commits. Invariant:
// every issued invocation still reaches a terminal state — slower
// percentiles are expected but no propose should fail.
func TestChaos_NetworkLatency(t *testing.T) {
	const (
		latency  = 200 * time.Millisecond
		jitter   = 50 * time.Millisecond
		toxicFor = 15 * time.Second
	)
	runScenario(t, scenarioConfig{
		withToxiproxy: true,
		rate:          30,
		concurrency:   16,
		duration:      30 * time.Second,
		awaitTerminal: 120 * time.Second,
		chaosAfter:    5 * time.Second,
		chaos: func(t *testing.T, c *e2e.ContainerCluster) {
			leader, peer := pickLeaderAndPeer(t, c)
			t.Logf("chaos: %dms latency between leader=%d peer=%d for %s",
				latency/time.Millisecond, leader, peer, toxicFor)
			if err := c.Tx.LatencyBoth(leader, peer, latency, jitter); err != nil {
				t.Fatalf("LatencyBoth(%d,%d): %v", leader, peer, err)
			}
			time.Sleep(toxicFor)
			if err := c.Tx.ClearToxics(leader, peer); err != nil {
				t.Fatalf("ClearToxics(%d,%d): %v", leader, peer, err)
			}
			if err := c.Tx.ClearToxics(peer, leader); err != nil {
				t.Fatalf("ClearToxics(%d,%d): %v", peer, leader, err)
			}
		},
	})
}

// TestChaos_NetworkBandwidth throttles one direction of the leader↔peer
// link to a tight rate. The unthrottled peer still serves quorum, so
// the cluster keeps committing; the throttled peer falls behind and
// catches up via snapshot or replicated entries once the toxic is
// cleared. Invariant: every issued invocation reaches a terminal
// state within awaitTerminal.
func TestChaos_NetworkBandwidth(t *testing.T) {
	const (
		rateKBps = 8 // tight enough to noticeably starve AppendEntries
		toxicFor = 15 * time.Second
	)
	runScenario(t, scenarioConfig{
		withToxiproxy: true,
		rate:          30,
		concurrency:   16,
		duration:      30 * time.Second,
		awaitTerminal: 120 * time.Second,
		chaosAfter:    5 * time.Second,
		chaos: func(t *testing.T, c *e2e.ContainerCluster) {
			leader, peer := pickLeaderAndPeer(t, c)
			t.Logf("chaos: %dKB/s bandwidth cap leader=%d -> peer=%d for %s",
				rateKBps, leader, peer, toxicFor)
			if err := c.Tx.Bandwidth(leader, peer, rateKBps); err != nil {
				t.Fatalf("Bandwidth(%d,%d): %v", leader, peer, err)
			}
			time.Sleep(toxicFor)
			if err := c.Tx.ClearToxics(leader, peer); err != nil {
				t.Fatalf("ClearToxics(%d,%d): %v", leader, peer, err)
			}
		},
	})
}

// TestChaos_AsymmetricPartition cuts only the leader→follower direction.
// The follower can still send to the leader (so heartbeats from leader
// to follower fail, but follower-to-leader pre-vote works), exercising
// dragonboat's recovery from a half-open link. Invariant: every issued
// invocation reaches a terminal state — either the leader recovers
// after heal, or a new leader is elected on the still-connected
// majority side.
func TestChaos_AsymmetricPartition(t *testing.T) {
	const cutFor = 10 * time.Second
	runScenario(t, scenarioConfig{
		withToxiproxy: true,
		rate:          30,
		concurrency:   16,
		duration:      30 * time.Second,
		awaitTerminal: 120 * time.Second,
		chaosAfter:    5 * time.Second,
		chaos: func(t *testing.T, c *e2e.ContainerCluster) {
			leader, peer := pickLeaderAndPeer(t, c)
			t.Logf("chaos: asymmetric cut leader=%d -> peer=%d (peer can still reach leader) for %s",
				leader, peer, cutFor)
			if err := c.Tx.CutDir(leader, peer); err != nil {
				t.Fatalf("CutDir(%d,%d): %v", leader, peer, err)
			}
			time.Sleep(cutFor)
			if err := c.Tx.HealDir(leader, peer); err != nil {
				t.Fatalf("HealDir(%d,%d): %v", leader, peer, err)
			}
		},
	})
}

// TestChaos_NetworkSlowClose adds a slow_close delay to the leader's
// outbound raft sockets, simulating peers that linger after a FIN.
// Less destructive than partition + latency; this is mostly a smoke
// that the toxic plumbing works end-to-end (toxiproxy applies the
// toxic, the proxy still forwards traffic, no toxic state leaks
// between tests).
func TestChaos_NetworkSlowClose(t *testing.T) {
	const (
		closeDelay = 2 * time.Second
		toxicFor   = 12 * time.Second
	)
	runScenario(t, scenarioConfig{
		withToxiproxy: true,
		rate:          30,
		concurrency:   8,
		duration:      25 * time.Second,
		awaitTerminal: 90 * time.Second,
		chaosAfter:    5 * time.Second,
		chaos: func(t *testing.T, c *e2e.ContainerCluster) {
			leader, peer := pickLeaderAndPeer(t, c)
			t.Logf("chaos: slow_close %s on leader=%d -> peer=%d for %s",
				closeDelay, leader, peer, toxicFor)
			if err := c.Tx.SlowClose(leader, peer, closeDelay); err != nil {
				t.Fatalf("SlowClose(%d,%d): %v", leader, peer, err)
			}
			time.Sleep(toxicFor)
			if err := c.Tx.ClearToxics(leader, peer); err != nil {
				t.Fatalf("ClearToxics(%d,%d): %v", leader, peer, err)
			}
		},
	})
}

// pickLeaderAndPeer resolves the shard-1 leader's NodeID and returns
// (leader, peer) where peer is one of the leader's neighbors. Fails
// the test loudly when the cluster has no shard-1 leader or only one
// node.
func pickLeaderAndPeer(t *testing.T, c *e2e.ContainerCluster) (leader, peer uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n := c.AwaitPartitionLeader(ctx, 1, 30*time.Second)
	if n == nil {
		t.Fatal("no shard-1 leader")
	}
	leader = n.NodeID()
	for _, id := range c.PeerIDs() {
		if id != leader {
			peer = id
			break
		}
	}
	if peer == 0 {
		t.Fatal("no peer distinct from leader")
	}
	return leader, peer
}
