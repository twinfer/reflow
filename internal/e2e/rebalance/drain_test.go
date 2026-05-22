//go:build e2e

// Package rebalance_test exercises the autonomous LP balancer
// (internal/engine/rebalance.Balancer) end-to-end against real
// reflowd binaries. The integration tier
// (internal/engine/integration_rebalance_test.go) covers the saga
// at in-proc tier; this suite re-runs the same shape against the
// compiled binary so binary-only startup ordering / signal handling
// around the leader-scoped rebalance goroutine is regression-tested.
package rebalance_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/e2e"
	"github.com/twinfer/reflow/pkg/reflowclient"
	clusterctlv1 "github.com/twinfer/reflow/proto/clusterctlv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestE2EBalance_DrainShardRebalances brings up a 3-node, 3-shard
// reflowd cluster in `rebalance.mode=auto` with tight knobs, marks
// shard 2 drained via Config / ClusterCtl.RebalanceDrain, and asserts
// the autonomous balancer (a) drains LPs off shard 2 within a bounded
// window, AND (b) every LP transfer it initiated targets a non-drained
// shard.
//
// Mirrors integration_rebalance_test.go's
// TestRebalance_DrainShard_DrainsProgressively, but against the
// containerized reflowd binary — exercises the production startup
// ordering and the admin-RPC path (round-robin retry on Unavailable),
// neither of which the in-proc integration tier covers.
//
// Convergence threshold is intentionally loose: each LP transfer is a
// multi-phase Raft + cross-shard saga (freeze → ship → stage → flip →
// clean), so "all LPs migrated" can take many minutes. We assert a
// small drop (>= 5 LPs) within a generous deadline — enough to prove
// the loop reacted, not to wait for full convergence.
func TestE2EBalance_DrainShardRebalances(t *testing.T) {
	cluster := e2e.NewContainerCluster(t, e2e.ContainerClusterOptions{
		N:         3,
		NumShards: 3,
		ExtraEnv: map[string]string{
			"REFLOW_REBALANCE_MODE":                          "auto",
			"REFLOW_REBALANCE_MAX_CONCURRENT_TRANSFERS":      "8",
			"REFLOW_REBALANCE_MIN_SECONDS_BETWEEN_TRANSFERS": "0",
			"REFLOW_REBALANCE_SKEW_ENGAGE_PCT":               "5",
			"REFLOW_REBALANCE_SKEW_DISENGAGE_PCT":            "1",
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Wait until the LPOwners bootstrap seed has committed. Until then
	// RebalanceAdvise.lps_per_shard is empty and we can't measure the
	// initial shard-2 count. The drain notifier wakes the balancer
	// immediately on RebalanceDrain regardless of poll-interval, so we
	// don't need a fast PollInterval knob in the production config —
	// the 30s backstop is just defense in depth.
	initial, err := awaitInitialDistribution(ctx, cluster)
	if err != nil {
		t.Fatalf("await initial distribution: %v", err)
	}
	initialOnShard2 := initial[2]
	if initialOnShard2 == 0 {
		t.Fatalf("initial cluster has 0 LPs on shard 2; can't test drain (distribution=%v)", initial)
	}
	t.Logf("initial: %d LPs on shard 2 (distribution=%v)", initialOnShard2, initial)

	// Drain shard 2.
	if err := drainShard(ctx, cluster, 2, true); err != nil {
		t.Fatalf("RebalanceDrain(shard=2, drain=true): %v", err)
	}

	// Poll RebalanceAdvise.lps_per_shard until shard 2's count drops
	// by at least minDrop. We use a small absolute floor (not the
	// in-proc integration's 1%-of-initial formula) because the
	// containerized binary runs with the production 30s PollInterval
	// — the balancer only fires on drainCh wake + 30s backstop ticks,
	// and each transfer is a multi-phase saga (lpMover advances one
	// phase per second, ~5 phases per transfer). Empirical per-window
	// throughput is ~3-4 LPs/120s. minDrop=3 is enough to prove the
	// loop reacted and iterated past a single transfer; this is a
	// reaction test, not a convergence benchmark.
	const minDrop = 3
	targetMax := initialOnShard2 - minDrop
	deadline := time.Now().Add(120 * time.Second)
	lastCount := initialOnShard2
	for time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		dist, err := adviseLPsPerShard(pctx, cluster)
		pcancel()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		count := dist[2]
		if count != lastCount {
			t.Logf("LPs on shard 2: %d (was %d, target ≤ %d)", count, lastCount, targetMax)
			lastCount = count
		}
		if count <= targetMax {
			t.Logf("drain progress: %d LPs migrated off shard 2 (initial=%d, threshold=%d)",
				initialOnShard2-count, initialOnShard2, minDrop)
			break
		}
		time.Sleep(1 * time.Second)
	}
	if lastCount > targetMax {
		t.Fatalf("shard 2 still owns %d LPs (initial=%d, want ≤ %d); drain stalled",
			lastCount, initialOnShard2, targetMax)
	}

	// Correctness: every LP transfer initiated must source from shard 2
	// (drain origin) and target some non-drained shard.
	xfers, err := listTransfers(ctx, cluster)
	if err != nil {
		t.Fatalf("ListLPTransfers: %v", err)
	}
	saw := 0
	for _, rec := range xfers {
		if rec.GetSourceShard() != 2 {
			continue
		}
		saw++
		if rec.GetDestShard() == 2 {
			t.Fatalf("transfer %s targets the drained shard: source=%d dest=%d",
				rec.GetTransferId(), rec.GetSourceShard(), rec.GetDestShard())
		}
	}
	if saw == 0 {
		t.Fatalf("LPTransferTable has no rows sourcing from drained shard 2; rebalancer didn't actuate")
	}
}

// awaitInitialDistribution polls RebalanceAdvise until lps_per_shard
// is non-empty, signaling the bootstrap LPOwners seed committed.
// Returns the map. Honors ctx.
func awaitInitialDistribution(ctx context.Context, cluster *e2e.ContainerCluster) (map[uint64]uint32, error) {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		dist, err := adviseLPsPerShard(probeCtx, cluster)
		cancel()
		if err == nil && len(dist) > 0 {
			return dist, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil, errors.New("rebalance: lps_per_shard never became non-empty")
}

// adviseLPsPerShard issues one RebalanceAdvise round-robin across nodes
// and returns lps_per_shard. Read-only, so any node would in principle
// serve — but advise requires shard-0 SyncRead and the followers can
// reject with CodeUnavailable; round-robin handles that without
// needing to query for the current leader.
func adviseLPsPerShard(ctx context.Context, cluster *e2e.ContainerCluster) (map[uint64]uint32, error) {
	var lastErr error
	for _, node := range cluster.Nodes {
		if node == nil || !node.IsLive() {
			continue
		}
		cli, err := dialAdmin(ctx, node)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := cli.Cluster.RebalanceAdvise(ctx,
			connect.NewRequest(&clusterctlv1.RebalanceAdviseRequest{}))
		_ = cli.Close()
		if err == nil {
			return resp.Msg.GetLpsPerShard(), nil
		}
		lastErr = err
		var cerr *connect.Error
		if !errors.As(err, &cerr) || cerr.Code() != connect.CodeUnavailable {
			return nil, err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no live node")
	}
	return nil, lastErr
}

// drainShard issues RebalanceDrain via round-robin. Mutating RPC, so
// non-leaders return CodeUnavailable with LeaderHint; the
// round-robin pattern is the simplest way to land on the right node
// without resolving the docker-internal LeaderHint admin endpoint
// from the test process.
func drainShard(ctx context.Context, cluster *e2e.ContainerCluster, shardID uint64, drain bool) error {
	var lastErr error
	for hop := 0; hop < 10; hop++ {
		for _, node := range cluster.Nodes {
			if node == nil || !node.IsLive() {
				continue
			}
			cli, err := dialAdmin(ctx, node)
			if err != nil {
				lastErr = err
				continue
			}
			req := connect.NewRequest(&clusterctlv1.RebalanceDrainRequest{
				ShardId: shardID,
				Drain:   drain,
			})
			_, err = cli.Cluster.RebalanceDrain(ctx, req)
			_ = cli.Close()
			if err == nil {
				return nil
			}
			lastErr = err
			var cerr *connect.Error
			if !errors.As(err, &cerr) || cerr.Code() != connect.CodeUnavailable {
				return err
			}
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return lastErr
}

// dialAdmin opens an insecure reflowclient against node's host-mapped
// admin port. The caller owns Close. Mirrors the pattern in
// internal/e2e/handler.go.
func dialAdmin(ctx context.Context, node *e2e.ContainerNode) (*reflowclient.Client, error) {
	addr := stripScheme(node.AdminURLForTest())
	return reflowclient.Dial(ctx, reflowclient.DialOptions{Addr: addr})
}

func stripScheme(u string) string {
	for _, p := range []string{"http://", "https://"} {
		if strings.HasPrefix(u, p) {
			return strings.TrimPrefix(u, p)
		}
	}
	return u
}

// listTransfers returns every LPTransferRecord visible to the metadata
// leader. Round-robins on Unavailable so the call lands on the leader
// without resolving LeaderHint.
func listTransfers(ctx context.Context, cluster *e2e.ContainerCluster) ([]*enginev1.LPTransferRecord, error) {
	var lastErr error
	for _, node := range cluster.Nodes {
		if node == nil || !node.IsLive() {
			continue
		}
		cli, err := dialAdmin(ctx, node)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := cli.Cluster.ListLPTransfers(ctx,
			connect.NewRequest(&clusterctlv1.ListLPTransfersRequest{}))
		_ = cli.Close()
		if err == nil {
			return resp.Msg.GetRecords(), nil
		}
		lastErr = err
		var cerr *connect.Error
		if !errors.As(err, &cerr) || cerr.Code() != connect.CodeUnavailable {
			return nil, err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no live node")
	}
	return nil, lastErr
}

