// Package loadgen wires up reflow engine clusters for the load and
// chaos harness.
//
// Production code does not import this package. The cluster bootstrap
// here is intentionally kept agnostic of the loadtest build tag — that
// way the engine test suite (also under internal/engine/...) can
// reuse the same in-process cluster shape via a thin wrapper, and the
// load-running test files in this package can carry //go:build
// loadtest without splitting the bootstrap implementation in two.
//
// Phase 5: see durable-execution-go-sad.md §10.
package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/grpc"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/delivery"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/pkg/sdk"
	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
)

// Node owns one in-process reflowd node: the engine Host, the
// Delivery gRPC server / listener, and the pooled Delivery client
// the host's outbox uses for cross-shard dispatch.
type Node struct {
	Host           *engine.Host
	DeliveryServer *grpc.Server
	DeliveryLn     net.Listener
	DeliveryClient *delivery.Client
}

// Close gracefully tears the node down. Safe to call multiple times.
func (n *Node) Close() {
	if n == nil {
		return
	}
	if n.DeliveryServer != nil {
		n.DeliveryServer.GracefulStop()
	}
	if n.DeliveryLn != nil {
		_ = n.DeliveryLn.Close()
	}
	if n.DeliveryClient != nil {
		_ = n.DeliveryClient.Close()
	}
	if n.Host != nil {
		_ = n.Host.Close()
	}
}

// ClusterOptions configures NewCluster. N defaults to 3; Handlers is
// required (pass sdk.NewRegistry() for clusters that don't need any
// user handlers). PebbleOptions and OnSnapshotPersisted are forwarded
// verbatim to engine.HostConfig.
type ClusterOptions struct {
	N                   int
	Handlers            *sdk.Registry
	PebbleOptions       func(shardID uint64) *pebble.Options
	OnSnapshotPersisted func(shardID uint64)
}

// Cluster is the bootstrap result: the live nodes and the
// Partitioner that matches the cluster's shard count.
type Cluster struct {
	Nodes       []*Node
	Partitioner routing.Partitioner
}

// Close tears every node down. Safe even when bring-up failed
// partway through (NewCluster leaves nil entries for the slots it
// didn't reach).
func (c *Cluster) Close() {
	if c == nil {
		return
	}
	for _, n := range c.Nodes {
		n.Close()
	}
}

// NewCluster brings up an N-node in-process cluster: every node hosts
// shard 0 (metadata) plus partition shards 1..N. Each node runs its
// own Delivery gRPC server. The function blocks until every shard
// has a leader on some node. Mirrors the production bootstrap
// staging order — Hosts → Delivery clients → Delivery servers →
// shards — so the harness exercises the same wiring path as
// pkg/reflow.Run.
func NewCluster(t testing.TB, opts ClusterOptions) *Cluster {
	t.Helper()
	if opts.N <= 0 {
		opts.N = 3
	}
	if opts.Handlers == nil {
		t.Fatal("loadgen: ClusterOptions.Handlers is required (pass sdk.NewRegistry() for no handlers)")
	}
	n := opts.N

	type addrs struct {
		raft, gossip, delivery string
	}
	cluster := &Cluster{
		Nodes:       make([]*Node, n),
		Partitioner: routing.Partitioner{NumShards: uint64(n)},
	}
	allAddrs := make([]addrs, n)
	for i := range allAddrs {
		allAddrs[i] = addrs{
			raft:     FreeLocalAddr(t),
			gossip:   FreeLocalAddr(t),
			delivery: FreeLocalAddr(t),
		}
	}

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

	// Stage 1: construct Hosts.
	for i := range n {
		h, err := engine.NewHost(engine.HostConfig{
			NodeID:              uint64(i + 1),
			RaftAddr:            allAddrs[i].raft,
			DataDir:             dataDirs[i],
			RTTMillisecond:      50,
			NumPartitionShards:  uint64(n),
			Handlers:            opts.Handlers,
			Peers:               peers,
			GossipBindAddr:      allAddrs[i].gossip,
			GossipAdvAddr:       allAddrs[i].gossip,
			GrpcEndpoint:        allAddrs[i].delivery,
			PebbleOptions:       opts.PebbleOptions,
			OnSnapshotPersisted: opts.OnSnapshotPersisted,
		})
		if err != nil {
			cluster.Close()
			t.Fatalf("loadgen: NewHost(%d): %v", i+1, err)
		}
		cluster.Nodes[i] = &Node{Host: h}
	}

	// Stage 2: Delivery clients (resolver = own host) as CrossShardSender.
	for i, node := range cluster.Nodes {
		dc, err := delivery.NewClient(delivery.ClientConfig{Resolver: node.Host})
		if err != nil {
			cluster.Close()
			t.Fatalf("loadgen: delivery.NewClient(%d): %v", i+1, err)
		}
		node.DeliveryClient = dc
		node.Host.SetCrossShardSender(dc)
	}

	// Stage 3: Delivery servers — must be live before any partition
	// can produce cross-shard envelopes.
	for i, node := range cluster.Nodes {
		ln, err := net.Listen("tcp", allAddrs[i].delivery)
		if err != nil {
			cluster.Close()
			t.Fatalf("loadgen: listen delivery(%d): %v", i+1, err)
		}
		gs := grpc.NewServer()
		deliveryv1.RegisterDeliveryServer(gs, delivery.NewServer(node.Host, nil))
		node.DeliveryLn = ln
		node.DeliveryServer = gs
		go func() {
			if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Logf("loadgen: delivery Serve(%d) exited: %v", i+1, err)
			}
		}()
	}

	// Stage 4: shards — metadata first, then partitions.
	for i, node := range cluster.Nodes {
		if _, err := node.Host.StartMetadataShard(); err != nil {
			cluster.Close()
			t.Fatalf("loadgen: StartMetadataShard(%d): %v", i+1, err)
		}
	}
	for i, node := range cluster.Nodes {
		for sh := uint64(1); sh <= uint64(n); sh++ {
			if _, err := node.Host.StartPartition(sh); err != nil {
				cluster.Close()
				t.Fatalf("loadgen: StartPartition(node=%d, shard=%d): %v", i+1, sh, err)
			}
		}
	}

	// Stage 5: leader-await — metadata first, then each partition.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cluster.AwaitAnyMetadataLeader(ctx); err != nil {
		cluster.Close()
		t.Fatalf("loadgen: metadata leader never elected: %v", err)
	}
	for sh := uint64(1); sh <= uint64(n); sh++ {
		ctxSh, cancelSh := context.WithTimeout(context.Background(), 20*time.Second)
		if err := cluster.AwaitAnyPartitionLeader(ctxSh, sh); err != nil {
			cancelSh()
			cluster.Close()
			t.Fatalf("loadgen: partition shard %d leader never elected: %v", sh, err)
		}
		cancelSh()
	}

	return cluster
}

// AwaitAnyMetadataLeader blocks until some node leads shard 0.
func (c *Cluster) AwaitAnyMetadataLeader(ctx context.Context) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, node := range c.Nodes {
			if node == nil {
				continue
			}
			if mr := node.Host.MetadataRunner(); mr != nil && mr.IsLeader() {
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

// AwaitAnyPartitionLeader blocks until some node leads shardID.
func (c *Cluster) AwaitAnyPartitionLeader(ctx context.Context, shardID uint64) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, node := range c.Nodes {
			if node == nil {
				continue
			}
			if pr := node.Host.Partition(shardID); pr != nil && pr.IsLeader() {
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

// FindPartitionLeader returns the node leading shardID, or nil. The
// caller should retry; leadership can rotate at any time.
func (c *Cluster) FindPartitionLeader(shardID uint64) *Node {
	for _, node := range c.Nodes {
		if node == nil {
			continue
		}
		if pr := node.Host.Partition(shardID); pr != nil && pr.IsLeader() {
			return node
		}
	}
	return nil
}

// FreeLocalAddr returns a free 127.0.0.1:port address by opening and
// immediately closing a listener. Races with concurrent allocations
// are possible but vanishingly rare in tests.
func FreeLocalAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
