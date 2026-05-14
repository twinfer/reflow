package engine_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/delivery"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/pkg/sdk"
	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// nodeRig owns one in-process reflowd node for Phase 4.1 multi-node
// integration tests. Each rig hosts shard 0 (metadata) plus partition
// shards 1..N (where N == cluster size), runs a Delivery gRPC server,
// and shares a pooled Delivery client used by its own outbox.
type nodeRig struct {
	host           *engine.Host
	deliveryServer *grpc.Server
	deliveryLn     net.Listener
	deliveryClient *delivery.Client
}

func (n *nodeRig) close() {
	if n.deliveryServer != nil {
		n.deliveryServer.GracefulStop()
	}
	if n.deliveryLn != nil {
		_ = n.deliveryLn.Close()
	}
	if n.deliveryClient != nil {
		_ = n.deliveryClient.Close()
	}
	if n.host != nil {
		_ = n.host.Close()
	}
}

// bringUpThreeNodeCluster wires three engine.Hosts together via static
// peer config, starts every host's Delivery gRPC server, calls
// StartMetadataShard + StartPartition(1..3) on each, and waits for shard
// 0 to elect a leader. Returns the rigs and the partitioner the tests
// can use to predict shard assignment for a given target.
func bringUpThreeNodeCluster(t *testing.T, handlers *sdk.Registry) ([]*nodeRig, routing.Partitioner) {
	t.Helper()
	const n = 3

	type addrs struct {
		raft, gossip, delivery string
	}
	rigs := make([]*nodeRig, n)
	allAddrs := make([]addrs, n)
	for i := range allAddrs {
		allAddrs[i] = addrs{
			raft:     freeLocalAddr(t),
			gossip:   freeLocalAddr(t),
			delivery: freeLocalAddr(t),
		}
	}

	// Build the symmetric Peers slice; every node feeds the same set in.
	peers := make([]engine.Peer, n)
	for i := range peers {
		peers[i] = engine.Peer{
			NodeID:     uint64(i + 1),
			RaftAddr:   allAddrs[i].raft,
			GossipAddr: allAddrs[i].gossip,
		}
	}

	dataDirs := make([]string, n)
	for i := range n {
		dataDirs[i] = filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i+1))
	}

	// Stage 1: construct Hosts (NodeHost starts here; no shards yet).
	for i := range n {
		h, err := engine.NewHost(engine.HostConfig{
			NodeID:             uint64(i + 1),
			RaftAddr:           allAddrs[i].raft,
			DataDir:            dataDirs[i],
			RTTMillisecond:     50,
			NumPartitionShards: n,
			Handlers:           handlers,
			Peers:              peers,
			GossipBindAddr:     allAddrs[i].gossip,
			GossipAdvAddr:      allAddrs[i].gossip,
			GrpcEndpoint:       allAddrs[i].delivery,
		})
		if err != nil {
			for j := range i {
				rigs[j].close()
			}
			t.Fatalf("NewHost(%d): %v", i+1, err)
		}
		rigs[i] = &nodeRig{host: h}
	}

	// Stage 2: build a Delivery client per node and wire it as the
	// CrossShardSender. Resolver is the engine.Host itself.
	for i, r := range rigs {
		dc, err := delivery.NewClient(delivery.ClientConfig{
			Resolver: r.host,
		})
		if err != nil {
			closeAll(rigs)
			t.Fatalf("delivery.NewClient(%d): %v", i+1, err)
		}
		r.deliveryClient = dc
		r.host.SetCrossShardSender(dc)
	}

	// Stage 3: start each node's Delivery gRPC server BEFORE any
	// partition can produce cross-shard envelopes.
	for i, r := range rigs {
		ln, err := net.Listen("tcp", allAddrs[i].delivery)
		if err != nil {
			closeAll(rigs)
			t.Fatalf("listen delivery(%d): %v", i+1, err)
		}
		gs := grpc.NewServer()
		deliveryv1.RegisterDeliveryServer(gs, delivery.NewServer(r.host, nil))
		r.deliveryLn = ln
		r.deliveryServer = gs
		go func() {
			if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Logf("delivery Serve(%d) exited: %v", i+1, err)
			}
		}()
	}

	// Stage 4: start shards on every node. Shard 0 first so the
	// metadata group has formed before partition shards begin emitting
	// outbox rows on first apply.
	for i, r := range rigs {
		if _, err := r.host.StartMetadataShard(); err != nil {
			closeAll(rigs)
			t.Fatalf("StartMetadataShard(%d): %v", i+1, err)
		}
	}
	for i, r := range rigs {
		for sh := uint64(1); sh <= n; sh++ {
			if _, err := r.host.StartPartition(sh); err != nil {
				closeAll(rigs)
				t.Fatalf("StartPartition(node=%d, shard=%d): %v", i+1, sh, err)
			}
		}
	}

	// Stage 5: wait for shard 0 to elect a leader on some node.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := awaitAnyMetadataLeader(ctx, rigs); err != nil {
		closeAll(rigs)
		t.Fatalf("metadata leader never elected: %v", err)
	}

	// Wait for each partition shard to have a leader on some node.
	for sh := uint64(1); sh <= n; sh++ {
		ctxSh, cancelSh := context.WithTimeout(context.Background(), 20*time.Second)
		if err := awaitAnyPartitionLeader(ctxSh, rigs, sh); err != nil {
			cancelSh()
			closeAll(rigs)
			t.Fatalf("partition shard %d leader never elected: %v", sh, err)
		}
		cancelSh()
	}

	return rigs, routing.Partitioner{NumShards: uint64(n)}
}

func closeAll(rigs []*nodeRig) {
	for _, r := range rigs {
		if r != nil {
			r.close()
		}
	}
}

func awaitAnyMetadataLeader(ctx context.Context, rigs []*nodeRig) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, r := range rigs {
			if mr := r.host.MetadataRunner(); mr != nil && mr.IsLeader() {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func awaitAnyPartitionLeader(ctx context.Context, rigs []*nodeRig, shardID uint64) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, r := range rigs {
			if pr := r.host.Partition(shardID); pr != nil && pr.IsLeader() {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// findPartitionLeader returns the rig that currently leads shardID, or
// nil if none does (caller should retry).
func findPartitionLeader(rigs []*nodeRig, shardID uint64) *nodeRig {
	for _, r := range rigs {
		if pr := r.host.Partition(shardID); pr != nil && pr.IsLeader() {
			return r
		}
	}
	return nil
}

// TestMultiNode_StaticThreeNodeBootstrap brings up a 3-node cluster from
// static peer config and asserts the steady state: shard 0 has a leader
// reachable via dragonboat gossip, every partition has a leader, and the
// per-shard NodeHostRegistry view enumerates all 3 replicas.
func TestMultiNode_StaticThreeNodeBootstrap(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)

	// Verify each rig sees the same 3-replica view of every partition.
	for _, r := range rigs {
		reg, ok := r.host.NodeHost().GetNodeHostRegistry()
		if !ok {
			t.Fatalf("node %v: GetNodeHostRegistry returned false", r.host)
		}
		for sh := uint64(0); sh <= 3; sh++ {
			// Gossip eventually populates the view; allow a short window.
			deadline := time.Now().Add(5 * time.Second)
			var view any
			var got bool
			for time.Now().Before(deadline) {
				v, ok := reg.GetShardInfo(sh)
				if ok && len(v.Replicas) == 3 {
					view, got = v, true
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if !got {
				t.Fatalf("shard %d: replica view never reached 3; last=%+v", sh, view)
			}
		}
	}
}

// TestMultiNode_GossipMetaCarriesGrpcEndpoint asserts that every node
// publishes its reflow Delivery gRPC endpoint via gossip NodeHostMeta and
// peers can resolve it via Host.NodeEndpoint.
func TestMultiNode_GossipMetaCarriesGrpcEndpoint(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)

	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for i, src := range rigs {
			for j := range rigs {
				if i == j {
					continue
				}
				ep, found := src.host.NodeEndpoint(uint64(j + 1))
				if !found || ep == "" {
					ok = false
					break
				}
			}
			if !ok {
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("gossip Meta endpoints never propagated to every peer within 10s")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestMultiNode_PartitionTableLookup confirms shard 0's leader proposes
// the static partition table on first leader election and a SyncRead from
// any node observes it.
func TestMultiNode_PartitionTableLookup(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t, sdk.NewRegistry())
	defer closeAll(rigs)

	deadline := time.Now().Add(15 * time.Second)
	var pt *enginev1.PartitionTable
	for time.Now().Before(deadline) {
		// Poll any rig; the read is linearizable.
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		got, err := rigs[0].host.PartitionTable(ctx)
		cancel()
		if err == nil && got != nil && len(got.GetShards()) == 3 {
			pt = got
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pt == nil {
		t.Fatal("partition table never observed via SyncRead within 15s")
	}
	for sh := uint64(1); sh <= 3; sh++ {
		rs, ok := pt.GetShards()[sh]
		if !ok {
			t.Errorf("partition table missing shard %d", sh)
			continue
		}
		if len(rs.GetNodeIds()) != 3 {
			t.Errorf("shard %d replica count = %d; want 3", sh, len(rs.GetNodeIds()))
		}
	}
}

// TestMultiNode_CrossPartition_CallDelivery is the integration exit
// criterion: an invocation on the partition that owns target A invokes a
// handler on the partition that owns target B (different shard), and the
// result flows back end-to-end across the cluster.
func TestMultiNode_CrossPartition_CallDelivery(t *testing.T) {
	// Caller hashes to shard 3, ServiceB to shard 1 (both with empty
	// object_key under FNV-1a + NumShards=3). The test still asserts
	// they differ at runtime — if a future routing change moves them
	// onto the same shard the assertion will surface it.
	const callerSvc = "Caller"
	const calleeSvc = "ServiceB"
	reg := sdk.NewRegistry()
	if err := reg.Register(callerSvc, "go", func(c sdk.Context, in []byte) ([]byte, error) {
		out, err := c.Call(sdk.Target{Service: calleeSvc, Handler: "do"}, in).Result()
		if err != nil {
			return nil, err
		}
		return append([]byte("a:"), out...), nil
	}); err != nil {
		t.Fatalf("Register %s: %v", callerSvc, err)
	}
	if err := reg.Register(calleeSvc, "do", func(_ sdk.Context, in []byte) ([]byte, error) {
		return append([]byte("b:"), in...), nil
	}); err != nil {
		t.Fatalf("Register %s: %v", calleeSvc, err)
	}

	rigs, p := bringUpThreeNodeCluster(t, reg)
	defer closeAll(rigs)

	shardA := p.ShardForTarget(&enginev1.InvocationTarget{ServiceName: callerSvc})
	shardB := p.ShardForTarget(&enginev1.InvocationTarget{ServiceName: calleeSvc})
	if shardA == shardB {
		t.Fatalf("%s and %s hash to same shard (%d); test setup expected distinct shards", callerSvc, calleeSvc, shardA)
	}

	// Find shard A's leader; propose the Caller invocation there.
	leaderA := findPartitionLeader(rigs, shardA)
	if leaderA == nil {
		t.Fatalf("no leader for shard %d", shardA)
	}
	callerID := &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(callerSvc, ""),
		Uuid:         []byte("phase41-caller!!"),
	}
	target := &enginev1.InvocationTarget{ServiceName: callerSvc, HandlerName: "go"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := leaderA.host.Partition(shardA).Proposer().ProposeIngress(propCtx, "test/p4-caller", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: callerID, Target: target, Input: []byte("hello"),
		}},
	})
	propCancel()
	if err != nil {
		t.Fatalf("ProposeIngress (caller): %v", err)
	}

	// Wait for the caller to reach Completed via cross-partition Call.
	deadline := time.Now().Add(30 * time.Second)
	var caller *enginev1.Completed
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		st, err := leaderA.host.LookupInvocationStatus(ctx, shardA, callerID)
		cancel()
		if err == nil && st != nil {
			if c, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				caller = c.Completed
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if caller == nil {
		t.Fatal("caller never reached Completed")
	}
	if got := string(caller.GetOutput()); got != "a:b:hello" {
		t.Errorf("caller output = %q; want a:b:hello", got)
	}
	if caller.GetFailureMessage() != "" {
		t.Errorf("caller failure_message = %q; want empty", caller.GetFailureMessage())
	}
}

// keep proto import referenced even when no direct proto.* call lands in
// this file's switch arms.
var _ = proto.Marshal
