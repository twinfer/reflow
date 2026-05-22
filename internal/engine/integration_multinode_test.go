package engine_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/handler"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// nodeRig is the engine_test alias for the concrete in-process Node
// implementation, retained so the existing test functions keep their
// familiar identifier and direct .Host field access. The subprocess
// node implementation is intentionally not exposed here — engine
// integration tests run only in-process.
type nodeRig = loadgen.InProcessNode

// bringUpThreeNodeCluster is the engine_test shim around
// loadgen.NewCluster — see internal/loadgen/cluster.go for the actual
// bootstrap. Kept here so the per-test call sites don't have to
// change their pattern.
func bringUpThreeNodeCluster(t *testing.T) ([]*nodeRig, routing.Partitioner) {
	t.Helper()
	c := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	return asInProcess(t, c.Nodes), c.Partitioner
}

// asInProcess narrows []loadgen.Node to []*loadgen.InProcessNode.
// Engine integration tests only use the in-process cluster, so the
// assertion always succeeds; a mismatch is a programming error and
// fails the test loudly.
func asInProcess(t *testing.T, nodes []loadgen.Node) []*nodeRig {
	t.Helper()
	out := make([]*nodeRig, len(nodes))
	for i, n := range nodes {
		if n == nil {
			continue
		}
		ip, ok := n.(*loadgen.InProcessNode)
		if !ok {
			t.Fatalf("expected *InProcessNode, got %T", n)
		}
		out[i] = ip
	}
	return out
}

// asNodeSlice widens []*nodeRig to []loadgen.Node for callers that
// need to pass into Cluster methods.
func asNodeSlice(rigs []*nodeRig) []loadgen.Node {
	out := make([]loadgen.Node, len(rigs))
	for i, r := range rigs {
		if r == nil {
			continue
		}
		out[i] = r
	}
	return out
}

func closeAll(rigs []*nodeRig) {
	for _, r := range rigs {
		r.Close()
	}
}

func awaitAnyMetadataLeader(ctx context.Context, rigs []*nodeRig) error {
	return (&loadgen.Cluster{Nodes: asNodeSlice(rigs)}).AwaitAnyMetadataLeader(ctx)
}

func findPartitionLeader(rigs []*nodeRig, shardID uint64) *nodeRig {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	node := (&loadgen.Cluster{Nodes: asNodeSlice(rigs)}).FindPartitionLeader(ctx, shardID)
	if node == nil {
		return nil
	}
	if ip, ok := node.(*loadgen.InProcessNode); ok {
		return ip
	}
	return nil
}

// TestMultiNode_StaticThreeNodeBootstrap brings up a 3-node cluster from
// static peer config and asserts the steady state: shard 0 has a leader
// reachable via dragonboat gossip, every partition has a leader, and the
// per-shard NodeHostRegistry view enumerates all 3 replicas.
func TestMultiNode_StaticThreeNodeBootstrap(t *testing.T) {
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

	// Verify each rig sees the same 3-replica view of every partition.
	for _, r := range rigs {
		reg, ok := r.Host.NodeHost().GetNodeHostRegistry()
		if !ok {
			t.Fatalf("node %v: GetNodeHostRegistry returned false", r.Host)
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
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for i, src := range rigs {
			for j := range rigs {
				if i == j {
					continue
				}
				ep, found := src.Host.NodeEndpoint(uint64(j + 1))
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
	rigs, _ := bringUpThreeNodeCluster(t)
	defer closeAll(rigs)

	deadline := time.Now().Add(15 * time.Second)
	var pt *enginev1.PartitionTable
	for time.Now().Before(deadline) {
		// Poll any rig; the read is linearizable.
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		got, err := rigs[0].Host.PartitionTable(ctx)
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
	reg := handler.NewRegistry()
	if err := reg.RegisterService(callerSvc, "go", func(c handler.Context, in []byte) ([]byte, error) {
		out, err := c.Call(handler.Target{Service: calleeSvc, Handler: "do"}, in).Result()
		if err != nil {
			return nil, err
		}
		return append([]byte("a:"), out...), nil
	}); err != nil {
		t.Fatalf("Register %s: %v", callerSvc, err)
	}
	if err := reg.RegisterService(calleeSvc, "do", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("b:"), in...), nil
	}); err != nil {
		t.Fatalf("Register %s: %v", calleeSvc, err)
	}

	c := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer c.Close()
	rigs := asInProcess(t, c.Nodes)
	defer closeAll(rigs)
	defer loadgen.StartEmbeddedHandlers(t, c, reg)()
	p := c.Partitioner

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
	depCtx, depCancel := context.WithTimeout(context.Background(), 5*time.Second)
	depID, err := leaderA.Host.LookupDeploymentIDByHandler(depCtx, callerSvc, "go")
	depCancel()
	if err != nil || depID == "" {
		t.Fatalf("LookupDeploymentIDByHandler: id=%q err=%v", depID, err)
	}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = leaderA.Host.Partition(shardA).Proposer().ProposeIngress(propCtx, "test/p4-caller", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: callerID, Target: target, Input: []byte("hello"), DeploymentId: depID,
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
		st, err := leaderA.Host.LookupInvocationStatus(ctx, shardA, callerID)
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
