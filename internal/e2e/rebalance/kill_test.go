//go:build e2e

package rebalance_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/e2e"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// TestE2EBalance_KillMidTransfer is the chaos×rebalance smoke. Brings
// up a 3-node × 3-shard cluster in `rebalance.mode=auto`, drains
// shard 2, waits until ListLPTransfers shows at least one
// non-terminal transfer row (the saga is mid-flight), then SIGKILLs
// the shard-2 leader and asserts the saga continues to make progress
// after raft re-elects.
//
// Why kill shard 2's leader specifically: it's deterministically
// findable via ContainerCluster.FindPartitionLeader, and it exercises
// the source-side recovery path — freeze state survives on the
// re-elected shard-2 replica, the lpMover (still on the unchanged
// metadata leader) re-dials the new shard-2 leader, and the saga
// resumes. Killing the metadata leader instead would also be valid
// but rotates the admin endpoint we'd need for ListLPTransfers
// polling, which adds plumbing without buying additional coverage.
//
// Assertion: at least one LP transfer reaches a TERMINAL phase
// (CLEANED or ABORTED) AFTER the kill. Either outcome is acceptable;
// CLEANED proves resume + finish, ABORTED proves the saga noticed
// the disruption and rolled back instead of hanging. A non-terminal
// row sitting forever would be the failure mode this guards.
func TestE2EBalance_KillMidTransfer(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// Wait for bootstrap LP seed; then drain shard 2 to fire the
	// balancer's drainCh wake.
	if _, err := awaitInitialDistribution(ctx, cluster); err != nil {
		t.Fatalf("await initial distribution: %v", err)
	}
	if err := drainShard(ctx, cluster, 2, true); err != nil {
		t.Fatalf("RebalanceDrain(shard=2, drain=true): %v", err)
	}

	// Wait until at least one non-terminal LP transfer exists. The
	// non-terminal set is {INIT, SHIPPING, STAGED, FLIPPED, ABORTING}.
	// 60s is generous; the balancer wakes immediately on drainCh and
	// the first transfer reaches non-terminal within seconds.
	if err := awaitNonTerminalTransfer(ctx, cluster, 60*time.Second); err != nil {
		t.Fatalf("await non-terminal transfer: %v", err)
	}

	// Find the shard-2 leader. We need a deterministic target so the
	// kill actually disrupts saga progression (rather than killing a
	// follower replica that doesn't matter to the saga).
	leaderCtx, leaderCancel := context.WithTimeout(ctx, 30*time.Second)
	leader := cluster.AwaitPartitionLeader(leaderCtx, 2, 30*time.Second)
	leaderCancel()
	if leader == nil {
		t.Fatal("shard 2 has no leader")
	}
	t.Logf("killing shard-2 leader: node %d", leader.NodeID())

	// Snapshot terminal-transfer count BEFORE the kill so we can
	// assert at least one MORE reaches terminal afterwards. Counting
	// rows in {CLEANED, ABORTED} measures forward progress on the
	// saga as a whole — robust whether the killed node was source or
	// dest of any specific in-flight transfer.
	preTerm, err := countTerminalTransfers(ctx, cluster)
	if err != nil {
		t.Fatalf("pre-kill ListLPTransfers: %v", err)
	}
	t.Logf("pre-kill terminal transfers: %d", preTerm)

	// SIGKILL the shard-2 leader. ContainerKill on a docker container;
	// the on-disk Pebble + dragonboat state survives, so a Restart
	// would bring it back — but for this test we leave it dead so the
	// saga must complete with 2 surviving nodes (replication degraded
	// but still has quorum: 2 out of 3 is a majority).
	leader.Kill()

	// Allow time for raft to notice + elect + lpMover to retry. Then
	// observe terminal-transfer count increases by at least 1 within
	// a bounded window. The 4-minute budget covers: 5-15s for raft
	// election timeout, plus a fresh saga walk (~5s per transfer over
	// up to 8 concurrent), plus headroom for the containerized tier's
	// 30s PollInterval backstop in case the lpMover's drainCh wake
	// path doesn't fire on raft transitions.
	deadline := time.Now().Add(4 * time.Minute)
	lastTerm := preTerm
	for time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		now, err := countTerminalTransfers(pctx, cluster)
		pcancel()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if now != lastTerm {
			t.Logf("terminal transfers: %d (was %d, pre-kill %d)", now, lastTerm, preTerm)
			lastTerm = now
		}
		if now > preTerm {
			t.Logf("saga resumed after kill: %d additional transfer(s) terminal",
				now-preTerm)
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("saga stalled after kill: terminal transfers stuck at %d (pre-kill %d)",
		lastTerm, preTerm)
}

// awaitNonTerminalTransfer polls ListLPTransfers until at least one
// record is in a non-terminal phase ({INIT, SHIPPING, STAGED,
// FLIPPED, ABORTING}). Times out after `timeout`.
func awaitNonTerminalTransfer(ctx context.Context, cluster *e2e.ContainerCluster, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		recs, err := listTransfers(pctx, cluster)
		pcancel()
		if err == nil {
			for _, r := range recs {
				if !isTerminalPhase(r.GetPhase()) {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return errors.New("no non-terminal LP transfer appeared within timeout")
}

// countTerminalTransfers returns the count of records in {CLEANED,
// ABORTED}. The metric is a strict step function — once terminal,
// a transfer stays terminal — so observing the count climb after
// the kill is sufficient evidence the saga made forward progress.
func countTerminalTransfers(ctx context.Context, cluster *e2e.ContainerCluster) (int, error) {
	// Use the round-robin lister; even if the killed node is the
	// admin target, listTransfers handles CodeUnavailable by
	// rotating to the next node.
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	recs, err := listTransfers(pctx, cluster)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, r := range recs {
		if isTerminalPhase(r.GetPhase()) {
			n++
		}
	}
	return n, nil
}

func isTerminalPhase(p enginev1.LPTransferPhase) bool {
	return p == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED ||
		p == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTED
}

