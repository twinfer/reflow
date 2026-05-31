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
	"github.com/twinfer/reflow/pkg/handler"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestMultiNode_JoinExistingCluster_OperatorAddNode verifies the
// operator-driven join path: the existing cluster's metadata leader
// proposes RegisterNode + PROMOTE_TO_VOTER (the same proposals that
// Admin.AddNode would emit on the wire), then the joiner brings up its
// shards with HostConfig.JoinExisting=true and catches up via
// dragonboat snapshot transfer + log replication. After the refactor
// that introduced SelfJoin, the underlying FSM-driving body is shared
// (admin/server.go: addNodeInternal); this test pins the operator-side
// behavior so that refactor stays regression-safe.
//
// The companion SelfJoin path (joiner calls SelfJoin against the
// leader's admin port via gossip-discovered endpoint) requires admin
// listeners + mTLS to exercise the SPIFFE authorization check, which
// loadgen.NewCluster does not currently wire up. The SPIFFE check
// itself is unit-tested in internal/clusterctl/selfjoin_test.go
// (TestCheckSelfJoinPrincipal_*); the redirect plumbing is
// unit-tested in pkg/reflowclient/redirect_test.go
// (TestCallWithLeaderRedirect_*).
//
// The test proves the joiner is a real cluster member on every shard:
//
//   - Partition shards: propose an invocation upstream, then verify a
//     linearizable read from the joiner observes the Completed status.
//   - Metadata shard: linearizable PartitionTable/Membership SyncRead
//     from the joiner returns the expected rows. SyncRead on shard 0
//     requires the local NodeHost to be a current voting member of
//     shard 0 — proof the metadata join worked.
func TestMultiNode_JoinExistingCluster_OperatorAddNode(t *testing.T) {
	const svc = "JoinSvc"
	const handlerName = "do"
	reg := handler.NewRegistry()
	if err := reg.RegisterService(svc, handlerName, func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("joined:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Step 1 — bring up the 3-node cluster.
	c := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer c.Close()
	rigs := asInProcess(t, c.Nodes)
	defer closeAll(rigs)
	p := c.Partitioner

	// Register the SDK handler endpoint and propose RegisterDeployment so
	// dispatch resolves the (service, handler) → deployment_id mapping.
	defer loadgen.StartEmbeddedHandlers(t, c, reg)()

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

	// Step 2 — allocate addresses for node 4 and bring up its NodeHost
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
	h4, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             newID,
		RaftAddr:           addrs.raft,
		DataDir:            dataDir4,
		RTTMillisecond:     50,
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

	// Step 3 — drive the cluster-side AddNode workflow: RegisterNode
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
	// Propose PROMOTE_TO_VOTER for shard 0 (metadata) AND every partition
	// shard. Shard 0 lives on pt.MetaReplicas; partitions live on
	// pt.Shards. The rebalance pipeline now drives both uniformly.
	proposeStep := func(shardID uint64) {
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
	proposeStep(0)
	for shardID := range pt.GetShards() {
		proposeStep(shardID)
	}

	// Step 4 — wait for the rebalancer to drive every PROMOTE_TO_VOTER
	// step to completion. Observable via PartitionTable.Shards[sh].NodeIds
	// containing the new ID.
	growCtx, growCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer growCancel()
	if err := awaitPartitionMembership(growCtx, metaLeader.Host, newID); err != nil {
		t.Fatalf("await partition membership add: %v", err)
	}

	// Step 5 — on the joining host, start the metadata shard then
	// every partition shard. HostConfig.JoinExisting routes through
	// StartOnDiskReplica(nil, true, ...) so dragonboat catches the
	// replica up rather than seeding it.
	if _, err := h4.StartMetadataShard(); err != nil {
		t.Fatalf("StartMetadataShard on joiner: %v", err)
	}
	for sh := uint64(1); sh <= 3; sh++ {
		if _, err := h4.StartPartition(sh); err != nil {
			t.Fatalf("StartPartition(%d) on joiner: %v", sh, err)
		}
	}

	// Step 6 — verify the joiner serves traffic. Propose an invocation
	// for a target that hashes to some shard and confirm the joiner
	// returns the resulting Completed status via a linearizable read.
	target := &enginev1.InvocationTarget{ServiceName: svc, HandlerName: handlerName}
	shard := p.ShardForTarget(0, target)
	leader := findPartitionLeader(rigs, shard)
	if leader == nil {
		t.Fatalf("no leader for shard %d", shard)
	}
	id := &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(0, svc, ""),
		Uuid:         []byte("join-test-uuid!!"),
	}
	depLookupCtx, depLookupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	depID, err := leader.Host.LookupDeploymentIDByHandler(depLookupCtx, svc, handlerName)
	depLookupCancel()
	if err != nil || depID == "" {
		t.Fatalf("LookupDeploymentIDByHandler: id=%q err=%v", depID, err)
	}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = leader.Host.Partition(shard).Proposer().ProposeIngress(propCtx, "join-test", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("payload"), DeploymentId: depID,
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

	// Step 7 — prove the joiner is a real shard-0 member. SyncRead on
	// shard 0 (PartitionTable / Membership) only succeeds if the local
	// NodeHost is a current voter, so a successful read is itself the
	// assertion.
	metaCtx, metaCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer metaCancel()
	if err := awaitJoinerServesMetadata(metaCtx, h4, newID); err != nil {
		t.Fatalf("joiner not serving shard-0 reads: %v", err)
	}
}

// awaitJoinerServesMetadata polls until h.PartitionTable and h.Membership
// (linearizable reads on shard 0) succeed from the joiner AND the
// returned PartitionTable shows newID in MetaReplicas — proof both the
// dragonboat shard-0 membership AND the apply-state MetaReplicas record
// have caught up.
func awaitJoinerServesMetadata(ctx context.Context, h *engine.Host, newID uint64) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		readCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		pt, ptErr := h.PartitionTable(readCtx)
		_, memErr := h.Membership(readCtx)
		cancel()
		if ptErr == nil && memErr == nil && pt != nil &&
			slices.Contains(pt.GetMetaReplicas().GetNodeIds(), newID) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("joiner shard-0 SyncRead never converged (last ptErr=%v memErr=%v): %w",
				ptErr, memErr, ctx.Err())
		case <-tick.C:
		}
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

// awaitPartitionMembership polls until newID is in every partition
// shard's NodeIds AND in pt.MetaReplicas (shard 0). Indicates the
// rebalancer's PROMOTE_TO_VOTER steps have all committed via
// CompleteRebalanceStep, on both partition shards and the metadata
// shard.
func awaitPartitionMembership(ctx context.Context, host *engine.Host, newID uint64) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		readCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		pt, err := host.PartitionTable(readCtx)
		cancel()
		if err == nil && pt != nil {
			allIn := slices.Contains(pt.GetMetaReplicas().GetNodeIds(), newID)
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
// for shardID — mirrors the ClusterCtl AddNode helper that lives in
// internal/clusterctl/server.go.
func nextStepID(pending []*enginev1.RebalanceStep, shardID uint64) uint64 {
	var highest uint64
	for _, p := range pending {
		if p.GetShardId() == shardID && p.GetStepId() > highest {
			highest = p.GetStepId()
		}
	}
	return highest + 1
}
