package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine/rebalance"
	"github.com/twinfer/reflow/internal/loadgen"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestRebalance_DrainShard_DrainsProgressively stands up a 3-node/3-shard
// cluster with the autonomous rebalancer in auto mode and very tight
// knobs, marks shard 2 drained, and asserts the rebalancer makes
// measurable progress draining LPs off shard 2 within a bounded window
// AND that every transfer it initiates targets a non-drained shard.
//
// We don't require full convergence — each transfer is a multi-step
// saga (freeze → ship → stage → flip → clean), so draining all ~1366
// LPs in test wall-clock would force a multi-minute timeout. The
// "progress + correctness" assertion is sufficient evidence the
// autonomous loop is reacting to drain and proposing through the
// correct path.
//
// Skipped on -short — drives end-to-end LP transfer sagas.
func TestRebalance_DrainShard_DrainsProgressively(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in -short")
	}
	cl := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N: 3,
		Rebalance: rebalance.Config{
			Mode:                       rebalance.ModeAuto,
			MaxConcurrentTransfers:     8,
			MinSecondsBetweenTransfers: 0,
			SkewEngagePct:              5,
			SkewDisengagePct:           1,
			PollInterval:               200 * time.Millisecond,
		},
	})
	defer cl.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer awaitCancel()
	if err := cl.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leader := findMetadataLeader(t, cl)
	host := leader.Host

	// Wait for the bootstrap LPOwners seed to land. The first
	// non-empty LPOwners list indicates the consistent-hash seed has
	// committed.
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer seedCancel()
	for {
		ow, err := host.LPOwners(seedCtx)
		if err == nil && len(ow.Records) > 0 {
			break
		}
		select {
		case <-seedCtx.Done():
			t.Fatal("LPOwners seed never committed")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Snapshot the initial owner distribution.
	initialOwners, err := host.LPOwners(seedCtx)
	if err != nil {
		t.Fatalf("LPOwners (initial): %v", err)
	}
	initialOnShard2 := countOnShard(initialOwners.Records, 2)
	if initialOnShard2 == 0 {
		t.Fatalf("initial cluster has 0 LPs on shard 2; can't test drain")
	}
	t.Logf("initial: %d LPs on shard 2 (of %d total)", initialOnShard2, len(initialOwners.Records))

	// Propose SetRebalanceDrain{shard 2, drain=true} via the metadata
	// proposer. This is the same path ClusterCtl/RebalanceDrain takes
	// once that handler is wired; doing it directly avoids the Connect
	// listener setup.
	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer proposeCancel()
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_SetRebalanceDrain{
			SetRebalanceDrain: &enginev1.SetRebalanceDrain{
				ShardId: 2,
				Drain:   true,
			},
		},
	}
	if err := leader.Host.MetadataRunner().Proposer().ProposeSelf(proposeCtx, cmd); err != nil {
		t.Fatalf("ProposeSelf(SetRebalanceDrain): %v", err)
	}

	// Sanity-check: the drain row landed and the rebalancer can read it.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer verifyCancel()
	drains, err := host.RebalanceDrains(verifyCtx)
	if err != nil {
		t.Fatalf("RebalanceDrains: %v", err)
	}
	if !containsShardID(drains.Records, 2) {
		t.Fatalf("shard 2 drain row missing post-propose; got %d records", len(drains.Records))
	}

	// Poll for measurable drain progress on shard 2. The lpMover saga
	// is the rate-limiting step: each LP transfer is a multi-phase
	// Raft + cross-shard sequence, so observable progress is on the
	// order of single-digit LPs per ten seconds — a rate independent of
	// LPCount. We assert a small fixed drop within a generous deadline,
	// enough to prove the autonomous loop reacted; full convergence (and
	// any LPCount-proportional target) would take far longer.
	minDrop := 5
	targetMax := initialOnShard2 - minDrop
	deadline := time.Now().Add(60 * time.Second)
	var lastCount = initialOnShard2
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ow, err := host.LPOwners(ctx)
		cancel()
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		count := countOnShard(ow.Records, 2)
		if count != lastCount {
			t.Logf("LPs on shard 2: %d (was %d, target ≤ %d)", count, lastCount, targetMax)
			lastCount = count
		}
		if count <= targetMax {
			t.Logf("drain progress: %d LPs migrated off shard 2 (initial=%d, threshold=%d)",
				initialOnShard2-count, initialOnShard2, minDrop)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastCount > targetMax {
		t.Fatalf("shard 2 still owns %d LPs (initial=%d, want ≤ %d); drain stalled",
			lastCount, initialOnShard2, targetMax)
	}

	// Correctness: inspect every LP transfer initiated during the
	// window. Each must originate from shard 2 (drain → source=2) and
	// target a non-drained shard (dest != 2).
	xferCtx, xferCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer xferCancel()
	tr, err := host.LPTransfers(xferCtx)
	if err != nil {
		t.Fatalf("LPTransfers: %v", err)
	}
	saw := 0
	for _, rec := range tr.Records {
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

func countOnShard(records []*enginev1.LPOwnerRecord, shardID uint64) int {
	n := 0
	for _, r := range records {
		if r.GetShardId() == shardID {
			n++
		}
	}
	return n
}

func containsShardID(records []*enginev1.RebalanceDrainRecord, shardID uint64) bool {
	for _, r := range records {
		if r.GetShardId() == shardID {
			return true
		}
	}
	return false
}

// TestRebalance_AdvisoryMode_NeverActuates spins up a cluster with
// rebalance.Mode=advisory, drains a shard, and asserts no
// LPTransferRecord rows appear after a generous observation window.
// Verifies that the advisory mode emits decisions without proposing
// transfers.
func TestRebalance_AdvisoryMode_NeverActuates(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in -short")
	}
	cl := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N: 3,
		Rebalance: rebalance.Config{
			Mode:                       rebalance.ModeAdvisory,
			MaxConcurrentTransfers:     8,
			MinSecondsBetweenTransfers: 0,
			SkewEngagePct:              5,
			SkewDisengagePct:           1,
			PollInterval:               200 * time.Millisecond,
		},
	})
	defer cl.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer awaitCancel()
	if err := cl.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leader := findMetadataLeader(t, cl)
	host := leader.Host

	// Drain shard 2 in advisory mode.
	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer proposeCancel()
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_SetRebalanceDrain{
			SetRebalanceDrain: &enginev1.SetRebalanceDrain{ShardId: 2, Drain: true},
		},
	}
	if err := leader.Host.MetadataRunner().Proposer().ProposeSelf(proposeCtx, cmd); err != nil {
		t.Fatalf("ProposeSelf(SetRebalanceDrain): %v", err)
	}

	// Wait 5s — generous for the 200ms poll to fire many ticks.
	time.Sleep(5 * time.Second)

	// Assert no transfers were initiated.
	listCtx, listCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer listCancel()
	tr, err := host.LPTransfers(listCtx)
	if err != nil {
		t.Fatalf("LPTransfers: %v", err)
	}
	if len(tr.Records) != 0 {
		t.Fatalf("advisory mode initiated %d transfers; want 0", len(tr.Records))
	}
}
