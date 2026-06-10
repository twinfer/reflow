// Package loadgen wires up reflw engine clusters for the load and
// chaos harness.
//
// Production code does not import this package. The cluster bootstrap
// here is intentionally kept agnostic of the loadtest build tag — that
// way the engine test suite (also under internal/engine/...) can
// reuse the same in-process cluster shape via a thin wrapper, and the
// load-running test files in this package can carry //go:build
// loadtest without splitting the bootstrap implementation in two.
package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/twinfer/reflw/internal/apimap"
	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/engine/cluster"
	"github.com/twinfer/reflw/internal/engine/delivery"
	"github.com/twinfer/reflw/internal/engine/rebalance"
	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage"
	apiv1 "github.com/twinfer/reflw/proto/apiv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// loadgenPebbleCacheBytes is the per-node Pebble block-cache budget for
// in-process clusters. Deliberately small (vs the 256 MiB production
// default) to keep the many clusters a full integration run spins up
// within reasonable memory — the cache *size* does not affect the
// write-amp / L0 numbers the loadtests measure (those are write-path),
// only read latency. The shared-cache + tuned-threshold *path* is what
// the loadtests exercise, matching production.
const loadgenPebbleCacheBytes int64 = 32 << 20 // 32 MiB

// PartitionInfo summarizes one shard's leadership state from a single
// node's point of view. Mirrors apiv1.PartitionLeadershipView but lives
// in the loadgen package so callers don't need to import the proto for
// the bring-up await loop.
type PartitionInfo struct {
	ShardID     uint64
	IsLeader    bool
	LeaderEpoch uint64
}

// Node is the abstraction the workload and invariant checker use to
// drive one cluster member. The in-process implementation lives here;
// the containerized e2e harness (internal/e2e) provides its own impl
// against the same surface, so the same workload runner drives both.
//
// Callers that need engine-internal access (Pebble metrics sampler,
// in-process leader probes inside chaos primitives) type-assert to
// the in-process concrete type and skip the node if the assertion
// fails. The Node interface itself is the minimum surface the
// workload needs.
type Node interface {
	// SubmitInvocation enqueues a fresh invocation. The destination
	// shard is derived from (service, objectKey) by the implementation.
	// Returns the minted invocation id on success.
	SubmitInvocation(ctx context.Context, service, handler, objectKey string, input []byte) (*enginev1.InvocationId, error)

	// DescribeInvocation returns the current status of an invocation by
	// id. Returns (nil, nil) if the invocation is not yet visible — the
	// poller treats that as "still pending."
	DescribeInvocation(ctx context.Context, id *enginev1.InvocationId) (*apiv1.InvocationStatusView, error)

	// ListPartitions returns leadership state for every partition shard
	// hosted by this node. Used by the bring-up leader-await loop and by
	// chaos primitives that need to find a partition leader without
	// reaching into engine internals.
	ListPartitions(ctx context.Context) ([]PartitionInfo, error)

	// RaftAddr identifies the node within the dragonboat peer config.
	// Stable for the node's lifetime.
	RaftAddr() string

	// Close shuts the node down gracefully. Idempotent.
	Close()

	// Kill terminates the node abruptly. For in-process nodes Kill is
	// identical to Close — there's no separate process to SIGKILL.
	// The containerized e2e harness implements the real SIGKILL
	// semantics by going through the Docker API.
	Kill()
}

// InProcessNode owns one in-process reflwd node: the engine Host, the
// Delivery Connect server / listener, and the pooled Delivery client the
// host's outbox uses for cross-shard dispatch. Implements Node.
type InProcessNode struct {
	Host           *engine.Host
	DeliveryServer *http.Server
	DeliveryLn     net.Listener
	DeliveryClient *delivery.Client
	// deliveryCancel cancels the BaseContext of DeliveryServer, which
	// is inherited by every handler request. Firing it at shutdown
	// surfaces ctx.Done() inside any in-flight ProposeIngress so the
	// handler returns immediately — otherwise its SyncPropose would
	// hold the local dragonboat NodeHost open and deadlock nh.Close.
	deliveryCancel context.CancelFunc
	raftAddr       string
	// pebbleCache / pebbleFileCache are this node's shared Pebble caches
	// (nil when the test supplied its own PebbleOptions). Unref'd in
	// Close after Host.Close, mirroring the production pkg/reflw path.
	pebbleCache     *pebble.Cache
	pebbleFileCache *pebble.FileCache
}

// SubmitInvocation routes (service, objectKey) through the host's
// Partitioner and proposes an InvokeCommand via the owning partition's
// ingress proposer — mirrors what ingress.Server.SubmitInvocation
// does in-process.
func (n *InProcessNode) SubmitInvocation(ctx context.Context, service, handler, objectKey string, input []byte) (*enginev1.InvocationId, error) {
	target := &enginev1.InvocationTarget{
		ServiceName: service,
		HandlerName: handler,
		ObjectKey:   objectKey,
	}
	shardID := n.Host.Partitioner().ShardForTarget(target)
	pr := n.Host.Partition(shardID)
	if pr == nil {
		return nil, fmt.Errorf("loadgen: no partition for shard %d", shardID)
	}
	id := mintInvocationID(target)
	cmd := &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id,
		Target:       target,
		Input:        input,
	}}}
	producerID := "loadgen/" + formatInvocationKey(id)
	if err := pr.Proposer().ProposeIngress(ctx, producerID, 1, cmd); err != nil {
		return nil, err
	}
	return id, nil
}

// DescribeInvocation looks up the invocation's current status via the
// host's SyncRead path.
func (n *InProcessNode) DescribeInvocation(ctx context.Context, id *enginev1.InvocationId) (*apiv1.InvocationStatusView, error) {
	pk := id.GetPartitionKey()
	if pk == 0 {
		return nil, fmt.Errorf("loadgen: invocation id has no partition_key")
	}
	shardID := n.Host.Partitioner().ShardForKey(pk)
	st, err := n.Host.LookupInvocationStatus(ctx, shardID, id)
	if err != nil {
		return nil, err
	}
	// Match the ingress RPC path the ContainerNode helper uses (apiv1 view), so
	// the Node interface is uniform across in-proc and containerized harnesses.
	return apimap.InvocationStatusView(st), nil
}

// ListPartitions returns leadership state for every partition shard
// hosted by this node. Mirrors ClusterCtl/NodeLeadership (the in-process
// counterpart of the containerized harness's admin-port probe).
func (n *InProcessNode) ListPartitions(_ context.Context) ([]PartitionInfo, error) {
	parts := n.Host.Partitions()
	out := make([]PartitionInfo, 0, len(parts))
	for shardID, runner := range parts {
		out = append(out, PartitionInfo{
			ShardID:     shardID,
			IsLeader:    runner.Leadership().IsLeader(),
			LeaderEpoch: runner.Leadership().LeaderEpoch(),
		})
	}
	return out, nil
}

// RaftAddr returns the dragonboat raft endpoint the host advertises.
func (n *InProcessNode) RaftAddr() string { return n.raftAddr }

// Close gracefully tears the node down. Safe to call multiple times.
func (n *InProcessNode) Close() {
	if n == nil {
		return
	}
	if n.deliveryCancel != nil {
		// Cancel BaseContext first so in-flight handler ProposeIngress
		// calls observe ctx.Done() and return. Then close the server
		// (immediate); the handler goroutines drain without blocking
		// the subsequent host.Close on dragonboat NodeHost.Close.
		n.deliveryCancel()
	}
	if n.DeliveryServer != nil {
		_ = n.DeliveryServer.Close()
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
	// Unref the shared Pebble caches after Host.Close has closed every
	// shard DB (each dropped its own ref). nil when the test supplied
	// its own PebbleOptions.
	if n.pebbleCache != nil {
		n.pebbleCache.Unref()
		n.pebbleCache = nil
	}
	if n.pebbleFileCache != nil {
		n.pebbleFileCache.Unref()
		n.pebbleFileCache = nil
	}
}

// Kill is identical to Close for in-process nodes — there is no
// separate process to SIGKILL. Real torn-WAL recovery semantics live
// in internal/e2e/chaos via Docker ContainerKill.
func (n *InProcessNode) Kill() { n.Close() }

// ClusterOptions configures NewCluster. N defaults to 3. PebbleOptions
// and OnSnapshotPersisted are forwarded verbatim to engine.HostConfig.
type ClusterOptions struct {
	N                   int
	PebbleOptions       func(shardID uint64) *pebble.Options
	OnSnapshotPersisted func(shardID uint64)

	// Rebalance forwards to engine.HostConfig.Rebalance on every
	// in-process node. Zero value disables the autonomous LP
	// rebalancer entirely (production default). Tests that exercise
	// auto-rebalance fill this in with Mode="auto" and tight knobs.
	Rebalance rebalance.Config
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
	Nodes       []Node
	Partitioner routing.Partitioner

	addrs    []nodeAddrs
	peers    []engine.Peer
	dataDirs []string
	opts     ClusterOptions
}

// Peers returns a copy of the static peer list this cluster bootstrapped
// with. Useful for tests that grow the cluster mid-run (the join-existing
// path) and need to derive the joiner's gossip seed list from the
// existing peers' addresses.
func (c *Cluster) Peers() []engine.Peer {
	if c == nil {
		return nil
	}
	out := make([]engine.Peer, len(c.peers))
	copy(out, c.peers)
	return out
}

// Close tears every node down. Safe even when bring-up failed
// partway through (NewCluster leaves nil entries for the slots it
// didn't reach).
func (c *Cluster) Close() {
	if c == nil {
		return
	}
	for _, n := range c.Nodes {
		if n != nil {
			n.Close()
		}
	}
}

// NewCluster brings up an N-node in-process cluster: every node hosts
// shard 0 (metadata) plus partition shards 1..N. Each node runs its
// own Delivery gRPC server. The function blocks until every shard
// has a leader on some node. Mirrors the production bootstrap
// staging order — Hosts → Delivery clients → Delivery servers →
// shards — so the harness exercises the same wiring path as
// pkg/reflw.Run.
func NewCluster(t testing.TB, opts ClusterOptions) *Cluster {
	t.Helper()
	if opts.N <= 0 {
		opts.N = 3
	}
	n := opts.N

	cluster := &Cluster{
		Nodes:       make([]Node, n),
		Partitioner: *routing.NewPartitioner(uint64(n)),
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

	// Stage 4: shards — metadata first, then partitions. Every NodeHost
	// must be ready before any partition emits outbox rows.
	for i, node := range cluster.Nodes {
		host := inProcessHost(node)
		if host == nil {
			continue
		}
		if _, err := host.StartMetadataShard(); err != nil {
			cluster.Close()
			t.Fatalf("loadgen: StartMetadataShard(%d): %v", i+1, err)
		}
	}
	for i, node := range cluster.Nodes {
		host := inProcessHost(node)
		if host == nil {
			continue
		}
		for sh := uint64(1); sh <= uint64(n); sh++ {
			if _, err := host.StartPartition(sh); err != nil {
				cluster.Close()
				t.Fatalf("loadgen: StartPartition(node=%d, shard=%d): %v", i+1, sh, err)
			}
		}
	}

	// Stage 5: leader-await — each partition.
	awaitTimeout := 20 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), awaitTimeout)
	defer cancel()
	if err := cluster.AwaitAnyMetadataLeader(ctx); err != nil {
		cluster.Close()
		t.Fatalf("loadgen: metadata leader never elected: %v", err)
	}
	for sh := uint64(1); sh <= uint64(n); sh++ {
		ctxSh, cancelSh := context.WithTimeout(context.Background(), awaitTimeout)
		if err := cluster.AwaitAnyPartitionLeader(ctxSh, sh); err != nil {
			cancelSh()
			cluster.Close()
			t.Fatalf("loadgen: partition shard %d leader never elected: %v", sh, err)
		}
		cancelSh()
	}

	return cluster
}

// AwaitAnyMetadataLeader blocks until some node leads shard 0. Reaches
// into the engine-internal MetadataRunner on in-process nodes; non
// in-process nodes are skipped, and callers in that path rely on
// AwaitAnyPartitionLeader as the convergence proxy — pkg/reflw.Run
// sequences metadata → partitions internally, so any partition leader
// implies metadata has converged.
func (c *Cluster) AwaitAnyMetadataLeader(ctx context.Context) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, node := range c.Nodes {
			host := inProcessHost(node)
			if host == nil {
				continue
			}
			if mr := host.MetadataRunner(); mr != nil && mr.IsLeader() {
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

// AwaitAnyPartitionLeader blocks until some node leads shardID. Uses
// the Node interface.
func (c *Cluster) AwaitAnyPartitionLeader(ctx context.Context, shardID uint64) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, node := range c.Nodes {
			if node == nil {
				continue
			}
			parts, err := node.ListPartitions(ctx)
			if err != nil {
				continue
			}
			for _, p := range parts {
				if p.ShardID == shardID && p.IsLeader {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// AnyLiveNode returns any non-nil node in the cluster, or nil if every
// slot has been killed.
func (c *Cluster) AnyLiveNode() Node {
	for _, n := range c.Nodes {
		if n != nil {
			return n
		}
	}
	return nil
}

// RaftAddr returns the RaftAddress of cluster.Nodes[idx]. Returns "" for
// out-of-range indices.
func (c *Cluster) RaftAddr(idx int) string {
	if idx < 0 || idx >= len(c.addrs) {
		return ""
	}
	return c.addrs[idx].raft
}

// FindPartitionLeader returns the node leading shardID, or nil. The
// caller should retry; leadership can rotate at any time.
func (c *Cluster) FindPartitionLeader(ctx context.Context, shardID uint64) Node {
	for _, node := range c.Nodes {
		if node == nil {
			continue
		}
		parts, err := node.ListPartitions(ctx)
		if err != nil {
			continue
		}
		for _, p := range parts {
			if p.ShardID == shardID && p.IsLeader {
				return node
			}
		}
	}
	return nil
}

// bringUpNode constructs (or re-constructs) the in-process Host +
// Delivery stack for cluster.Nodes[idx]. Stages 1-3 of NewCluster's
// bring-up, idempotent against pre-existing on-disk state (dragonboat
// detects the existing raft log and resumes; Pebble re-opens the
// existing dataDir).
//
// Does NOT start any shards — callers handle that explicitly so
// NewCluster can stage shard starts cluster-wide.
func (c *Cluster) bringUpNode(t testing.TB, idx int) error {
	t.Helper()
	addrs := c.addrs[idx]

	// Default to the production Pebble tuning (shared cache + tuned
	// write-stall / flush knobs) so the loadtests measure the same path
	// production runs. A test that supplies its own PebbleOptions (chaos
	// slow-disk FS injection) overrides verbatim and gets no shared
	// cache. No EventListener: tests must never crash on a simulated
	// slow disk. The caches are adopted by the InProcessNode on success;
	// the deferred cleanup Unrefs them on any early-return error (no
	// shard DBs are open yet at bring-up, so our ref is the only one).
	pebbleOpts := c.opts.PebbleOptions
	var nodeCache *pebble.Cache
	var nodeFileCache *pebble.FileCache
	if pebbleOpts == nil {
		nodeCache, nodeFileCache = storage.NewSharedCaches(loadgenPebbleCacheBytes, len(c.Nodes))
		pebbleOpts = storage.PebbleTuning{Cache: nodeCache, FileCache: nodeFileCache}.Options
	}
	cachesAdopted := false
	defer func() {
		if !cachesAdopted && nodeCache != nil {
			nodeCache.Unref()
			nodeFileCache.Unref()
		}
	}()

	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:              uint64(idx + 1),
		RaftAddr:            addrs.raft,
		DataDir:             c.dataDirs[idx],
		RTTMillisecond:      50,
		NumPartitionShards:  uint64(len(c.Nodes)),
		Peers:               c.peers,
		GossipBindAddr:      addrs.gossip,
		GossipAdvAddr:       addrs.gossip,
		GrpcEndpoint:        addrs.delivery,
		PebbleOptions:       pebbleOpts,
		OnSnapshotPersisted: c.opts.OnSnapshotPersisted,
		Rebalance:           c.opts.Rebalance,
		ClusterNotifiers:    cluster.Notifiers{RebalanceDrainTable: cluster.NewTableNotifier()},
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
	h.SetLPSSTUploader(delivery.NewLPSSTUploader(h, dc, nil))

	ln, err := listenWithRetry(addrs.delivery, 2*time.Second)
	if err != nil {
		_ = dc.Close()
		_ = h.Close()
		return fmt.Errorf("listen delivery: %w", err)
	}
	gs, deliveryCancel := newDeliveryHTTPServer(delivery.NewServer(h, nil))
	go func() {
		if err := gs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("loadgen: delivery Serve(%d) exited: %v", idx+1, err)
		}
	}()

	cachesAdopted = true
	c.Nodes[idx] = &InProcessNode{
		Host:            h,
		DeliveryServer:  gs,
		DeliveryLn:      ln,
		DeliveryClient:  dc,
		deliveryCancel:  deliveryCancel,
		raftAddr:        addrs.raft,
		pebbleCache:     nodeCache,
		pebbleFileCache: nodeFileCache,
	}
	return nil
}

// newDeliveryHTTPServer builds an h2c http.Server hosting the Delivery
// Connect handler. Returns the server + a cancel func that cancels its
// BaseContext (and therefore every in-flight handler's context). The
// chaos / loadgen harness runs without TLS or auth middleware — auth is
// exercised by dedicated tests.
func newDeliveryHTTPServer(srv *delivery.Server) (*http.Server, context.CancelFunc) {
	baseCtx, cancel := context.WithCancel(context.Background())
	mux := http.NewServeMux()
	path, handler := srv.NewHandler()
	mux.Handle(path, handler)
	hs := &http.Server{
		Handler:     mux,
		Protocols:   new(http.Protocols),
		BaseContext: func(net.Listener) context.Context { return baseCtx },
	}
	hs.Protocols.SetUnencryptedHTTP2(true)
	hs.Protocols.SetHTTP1(false)
	return hs, cancel
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
// shutdown beyond what the node's Kill method provides. The node slot
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
	node.Kill()
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
	host := inProcessHost(c.Nodes[idx])
	if host == nil {
		return fmt.Errorf("loadgen: RestartNode: expected *InProcessNode after bringUpNode")
	}
	if _, err := host.StartMetadataShard(); err != nil {
		return fmt.Errorf("StartMetadataShard: %w", err)
	}
	for sh := uint64(1); sh <= uint64(len(c.Nodes)); sh++ {
		if _, err := host.StartPartition(sh); err != nil {
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

// inProcessHost returns the engine.Host backing node, or nil if node
// is not in-process (or is nil). Used by code paths that need
// engine-internal access (Pebble metrics, metadata-leader probe).
func inProcessHost(node Node) *engine.Host {
	if ip, ok := node.(*InProcessNode); ok && ip != nil {
		return ip.Host
	}
	return nil
}
