package engine_test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestMultiNode_JoinExistingCluster verifies the join-existing startup
// path (HostConfig.JoinExisting=true). A 3-node cluster bootstraps
// normally, then a 4th node is added via the admin-RPC flow
// (RegisterNode on shard 0 + BeginRebalanceStep PROMOTE_TO_VOTER on every
// partition shard). The 4th Host comes up with JoinExisting=true and
// catches up via dragonboat snapshot transfer + log replication. The
// test then proposes an invocation on a shard hosted by the joiner and
// confirms a linearizable read from the joiner observes the Completed
// status — proof the joining node is serving traffic.
//
// Scope note: the admin AddNode workflow extends membership for
// partition shards only (1..N). Shard 0 (metadata) membership is not
// extended through this path; the joiner does not call
// StartMetadataShard. That gap is tracked separately — this test
// exercises the dragonboat join semantics for partition shards which is
// what the issue called out.
func TestMultiNode_JoinExistingCluster(t *testing.T) {
	const svc = "JoinSvc"
	const handler = "do"
	reg := sdk.NewRegistry()
	if err := reg.Register(svc, handler, func(_ sdk.Context, in []byte) ([]byte, error) {
		return append([]byte("joined:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Phase 1 — bring up the 3-node cluster.
	c := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3, Handlers: reg})
	defer c.Close()
	rigs := asInProcess(t, c.Nodes)
	defer closeAll(rigs)
	p := c.Partitioner

	// Wait for metadata convergence and locate the leader.
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := awaitAnyMetadataLeader(awaitCtx, rigs); err != nil {
		t.Fatalf("await metadata leader: %v", err)
	}
	var metaLeader *nodeRig
	for _, r := range rigs {
		if r.Host.MetadataRunner() != nil && r.Host.MetadataRunner().IsLeader() {
			metaLeader = r
			break
		}
	}
	if metaLeader == nil {
		t.Fatal("metadata leader vanished between check and capture")
	}

	// Wait for the partition table to materialize so we know all shards
	// are bootstrapped before we try to grow them.
	ptCtx, ptCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer ptCancel()
	if err := awaitPartitionTable(ptCtx, metaLeader.Host, 3); err != nil {
		t.Fatalf("await partition table: %v", err)
	}

	// Phase 2 — allocate addresses for node 4 and bring up its NodeHost
	// with JoinExisting=true. Shards are started later, after the
	// existing cluster has admitted node 4 into each shard's Raft
	// configuration.
	const newID uint64 = 4
	newHostID := fmt.Sprintf("00000000-0000-0000-0000-%012x", newID)
	addrs := node4Addrs(t)
	dataDir4 := filepath.Join(t.TempDir(), "node4")

	peers4 := append(c.Peers(), engine.Peer{
		NodeID:     newID,
		RaftAddr:   addrs.raft,
		GossipAddr: addrs.gossip,
		NodeHostID: newHostID,
	})
	h4, err := engine.NewHost(engine.HostConfig{
		NodeID:             newID,
		RaftAddr:           addrs.raft,
		DataDir:            dataDir4,
		RTTMillisecond:     50,
		Handlers:           reg,
		GossipBindAddr:     addrs.gossip,
		GossipAdvAddr:      addrs.gossip,
		GrpcEndpoint:       addrs.delivery,
		Peers:              peers4,
		NumPartitionShards: 3,
		JoinExisting:       true,
	})
	if err != nil {
		t.Fatalf("NewHost (joiner): %v", err)
	}
	defer h4.Close()

	// Phase 3 — drive the cluster-side AddNode workflow: RegisterNode
	// for ID=4, then PROMOTE_TO_VOTER on every partition shard. The
	// rebalancer running on the metadata leader picks the steps up and
	// fires SyncRequestAddReplica against dragonboat; node 4's gossip is
	// live so the membership change resolves and commits.
	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := metaLeader.Host.MetadataRunner().Proposer().ProposeSelf(proposeCtx, &enginev1.Command{
		Kind: &enginev1.Command_RegisterNode{
			RegisterNode: &enginev1.RegisterNode{
				Member: &enginev1.NodeMembership{
					NodeId:     newID,
					RaftAddr:   addrs.raft,
					NodeHostId: newHostID,
					LastSeenMs: time.Now().UnixMilli(),
				},
			},
		},
	}); err != nil {
		proposeCancel()
		t.Fatalf("RegisterNode: %v", err)
	}
	proposeCancel()

	// Re-read the table to pick a fresh step_id per shard.
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	pt, err := metaLeader.Host.PartitionTable(readCtx)
	readCancel()
	if err != nil || pt == nil {
		t.Fatalf("read partition table: %v (pt=%v)", err, pt)
	}
	for shardID := range pt.GetShards() {
		step := &enginev1.RebalanceStep{
			ShardId:   shardID,
			Kind:      enginev1.RebalanceStep_PROMOTE_TO_VOTER,
			AddNodeId: newID,
			StepId:    nextStepID(pt.GetPending(), shardID),
		}
		pt.Pending = append(pt.Pending, step)
		stepCtx, stepCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := metaLeader.Host.MetadataRunner().Proposer().ProposeSelf(stepCtx, &enginev1.Command{
			Kind: &enginev1.Command_BeginRebalanceStep{
				BeginRebalanceStep: &enginev1.BeginRebalanceStep{Step: step},
			},
		})
		stepCancel()
		if err != nil {
			t.Fatalf("BeginRebalanceStep shard=%d: %v", shardID, err)
		}
	}

	// Phase 4 — wait for the rebalancer to drive every PROMOTE_TO_VOTER
	// step to completion. Observable via PartitionTable.Shards[sh].NodeIds
	// containing the new ID.
	growCtx, growCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer growCancel()
	if err := awaitPartitionMembership(growCtx, metaLeader.Host, newID); err != nil {
		t.Fatalf("await partition membership add: %v", err)
	}

	// Phase 5 — on the joining host, call StartPartition for each shard.
	// HostConfig.JoinExisting routes through StartOnDiskReplica(nil, true,
	// ...) so dragonboat catches the replica up rather than seeding it.
	for sh := uint64(1); sh <= 3; sh++ {
		if _, err := h4.StartPartition(sh); err != nil {
			t.Fatalf("StartPartition(%d) on joiner: %v", sh, err)
		}
	}

	// Phase 6 — verify the joiner serves traffic. Propose an invocation
	// for a target that hashes to some shard and confirm the joiner
	// returns the resulting Completed status via a linearizable read.
	target := &enginev1.InvocationTarget{ServiceName: svc, HandlerName: handler}
	shard := p.ShardForTarget(target)
	leader := findPartitionLeader(rigs, shard)
	if leader == nil {
		t.Fatalf("no leader for shard %d", shard)
	}
	id := &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(svc, ""),
		Uuid:         []byte("join-test-uuid!!"),
	}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = leader.Host.Partition(shard).Proposer().ProposeIngress(propCtx, "join-test", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("payload"),
		}},
	})
	propCancel()
	if err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	// Linearizable read from the joiner. SyncRead requires the local
	// NodeHost to be a current member of the shard — which is exactly
	// what we want to prove.
	deadline := time.Now().Add(20 * time.Second)
	var completed bool
	for time.Now().Before(deadline) {
		readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
		st, err := h4.LookupInvocationStatus(readCtx, shard, id)
		readCancel()
		if err == nil && st != nil {
			if _, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				completed = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !completed {
		t.Fatalf("joining node did not observe Completed status within deadline (shard=%d)", shard)
	}
}

// node4Addrs allocates the three TCP endpoints node 4 will bind. Kept
// as a helper so the test body stays linear.
func node4Addrs(t *testing.T) struct{ raft, gossip, delivery string } {
	t.Helper()
	return struct{ raft, gossip, delivery string }{
		raft:     loadgen.FreeLocalAddr(t),
		gossip:   loadgen.FreeLocalAddr(t),
		delivery: loadgen.FreeLocalAddr(t),
	}
}

// awaitPartitionTable polls the metadata leader until the partition
// table has shards for [1..wantShards].
func awaitPartitionTable(ctx context.Context, host *engine.Host, wantShards int) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		readCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		pt, err := host.PartitionTable(readCtx)
		cancel()
		if err == nil && pt != nil && len(pt.GetShards()) >= wantShards {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("partition table not ready: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// awaitPartitionMembership polls until every partition shard's NodeIds contains
// newID. Indicates the rebalancer's PROMOTE_TO_VOTER steps have all
// committed via CompleteRebalanceStep.
func awaitPartitionMembership(ctx context.Context, host *engine.Host, newID uint64) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		readCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		pt, err := host.PartitionTable(readCtx)
		cancel()
		if err == nil && pt != nil {
			allIn := true
			for _, rs := range pt.GetShards() {
				if !slices.Contains(rs.GetNodeIds(), newID) {
					allIn = false
					break
				}
			}
			if allIn && len(pt.GetPending()) == 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("membership not extended to node %d: %w", newID, ctx.Err())
		case <-tick.C:
		}
	}
}

// nextStepID returns one greater than the max step_id already pending
// for shardID — mirrors the admin AddNode helper that lives in
// internal/engine/admin/server.go.
func nextStepID(pending []*enginev1.RebalanceStep, shardID uint64) uint64 {
	var max uint64
	for _, p := range pending {
		if p.GetShardId() == shardID && p.GetStepId() > max {
			max = p.GetStepId()
		}
	}
	return max + 1
}
