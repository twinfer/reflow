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

// nodeAddrs captures the three TCP endpoints a node binds at
// bootstrap. Persisted on Cluster so RestartNode can rebind to the
// same addresses (dragonboat's static peer config does not tolerate
// address changes).
type nodeAddrs struct {
	raft, gossip, delivery string
}

// Cluster is the bootstrap result: the live nodes and the
// Partitioner that matches the cluster's shard count.
//
// The unexported fields persist enough state that fault-injection
// methods (KillNode, RestartNode) can rebuild a node in place
// without re-running the full NewCluster bring-up.
type Cluster struct {
	Nodes       []*Node
	Partitioner routing.Partitioner

	addrs    []nodeAddrs
	peers    []engine.Peer
	dataDirs []string
	opts     ClusterOptions
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

	cluster := &Cluster{
		Nodes:       make([]*Node, n),
		Partitioner: routing.Partitioner{NumShards: uint64(n)},
		addrs:       make([]nodeAddrs, n),
		dataDirs:    make([]string, n),
		opts:        opts,
	}
	for i := range cluster.addrs {
		cluster.addrs[i] = nodeAddrs{
			raft:     FreeLocalAddr(t),
			gossip:   FreeLocalAddr(t),
			delivery: FreeLocalAddr(t),
		}
	}

	cluster.peers = make([]engine.Peer, n)
	for i := range cluster.peers {
		cluster.peers[i] = engine.Peer{
			NodeID:     uint64(i + 1),
			RaftAddr:   cluster.addrs[i].raft,
			GossipAddr: cluster.addrs[i].gossip,
		}
	}

	for i := range n {
		cluster.dataDirs[i] = filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i+1))
	}

	// Stages 1-3: per-node bring-up — same loop body as RestartNode.
	for i := range n {
		if err := cluster.bringUpNode(t, i); err != nil {
			cluster.Close()
			t.Fatalf("loadgen: bringUpNode(%d): %v", i+1, err)
		}
	}

	// Stage 4: shards — metadata first, then partitions. Cluster-
	// scope because every node must have its NodeHost ready before
	// any partition shard starts emitting outbox rows.
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

// AnyLiveNode returns any non-nil node in the cluster, or nil if
// every slot has been killed. Used by clients that need to do a
// linearizable read against the cluster without caring which node
// serves it (the lookup goes through dragonboat's leader anyway).
func (c *Cluster) AnyLiveNode() *Node {
	for _, n := range c.Nodes {
		if n != nil {
			return n
		}
	}
	return nil
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

// bringUpNode constructs (or re-constructs) the Host + Delivery
// stack for cluster.Nodes[idx]. Stages 1-3 of NewCluster's bring-up,
// idempotent against pre-existing on-disk state (dragonboat detects
// the existing raft log and resumes; Pebble re-opens the existing
// dataDir).
//
// Does NOT start any shards — callers handle that explicitly so
// NewCluster can stage shard starts cluster-wide.
func (c *Cluster) bringUpNode(t testing.TB, idx int) error {
	t.Helper()
	addrs := c.addrs[idx]
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:              uint64(idx + 1),
		RaftAddr:            addrs.raft,
		DataDir:             c.dataDirs[idx],
		RTTMillisecond:      50,
		NumPartitionShards:  uint64(len(c.Nodes)),
		Handlers:            c.opts.Handlers,
		Peers:               c.peers,
		GossipBindAddr:      addrs.gossip,
		GossipAdvAddr:       addrs.gossip,
		GrpcEndpoint:        addrs.delivery,
		PebbleOptions:       c.opts.PebbleOptions,
		OnSnapshotPersisted: c.opts.OnSnapshotPersisted,
	})
	if err != nil {
		return fmt.Errorf("NewHost: %w", err)
	}

	dc, err := delivery.NewClient(delivery.ClientConfig{Resolver: h})
	if err != nil {
		_ = h.Close()
		return fmt.Errorf("delivery.NewClient: %w", err)
	}
	h.SetCrossShardSender(dc)

	ln, err := listenWithRetry(addrs.delivery, 2*time.Second)
	if err != nil {
		_ = dc.Close()
		_ = h.Close()
		return fmt.Errorf("listen delivery: %w", err)
	}
	gs := grpc.NewServer()
	deliveryv1.RegisterDeliveryServer(gs, delivery.NewServer(h, nil))
	go func() {
		if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("loadgen: delivery Serve(%d) exited: %v", idx+1, err)
		}
	}()

	c.Nodes[idx] = &Node{
		Host:           h,
		DeliveryServer: gs,
		DeliveryLn:     ln,
		DeliveryClient: dc,
	}
	return nil
}

// listenWithRetry retries net.Listen briefly to ride out TIME_WAIT
// or other transient bind failures that can follow a recent Close.
// Returns immediately on success; gives up after timeout.
func listenWithRetry(addr string, timeout time.Duration) (net.Listener, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln, nil
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	return nil, lastErr
}

// KillNode closes cluster.Nodes[idx] without waiting for graceful
// shutdown beyond what Host.Close already provides. The node slot
// is left as nil so subsequent leader queries skip it. Idempotent.
//
// Concurrent fault: callers may KillNode while the workload is
// running; the workload's leader-pick falls through to a surviving
// node on the next attempt.
func (c *Cluster) KillNode(idx int) {
	if idx < 0 || idx >= len(c.Nodes) {
		return
	}
	node := c.Nodes[idx]
	if node == nil {
		return
	}
	c.Nodes[idx] = nil
	node.Close()
}

// RestartNode re-bootstraps cluster.Nodes[idx] on its original
// addresses + dataDir, restarts its metadata shard and every
// partition shard, and returns when the dragonboat NodeHost is
// live. Caller is responsible for awaiting any leader-stability
// invariants needed by the scenario.
//
// Requires KillNode to have been called first (or the original
// node to be nil); returns an error otherwise so accidental
// double-bring-up is loud.
func (c *Cluster) RestartNode(t testing.TB, idx int) error {
	t.Helper()
	if idx < 0 || idx >= len(c.Nodes) {
		return fmt.Errorf("loadgen: RestartNode: idx %d out of range", idx)
	}
	if c.Nodes[idx] != nil {
		return fmt.Errorf("loadgen: RestartNode: node %d still running (call KillNode first)", idx+1)
	}
	if err := c.bringUpNode(t, idx); err != nil {
		return fmt.Errorf("bringUpNode: %w", err)
	}
	node := c.Nodes[idx]
	if _, err := node.Host.StartMetadataShard(); err != nil {
		return fmt.Errorf("StartMetadataShard: %w", err)
	}
	for sh := uint64(1); sh <= uint64(len(c.Nodes)); sh++ {
		if _, err := node.Host.StartPartition(sh); err != nil {
			return fmt.Errorf("StartPartition(%d): %w", sh, err)
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
