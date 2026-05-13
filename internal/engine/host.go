package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/invoker"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// HostConfig configures a reflow node (a process hosting one or more
// partitions). Phase 1 single-node deployments use NodeID=1 with Peers
// empty; Phase 4.1 multi-node deployments populate Peers with every
// cluster member (including self) and supply gossip + Delivery endpoint
// addresses.
type HostConfig struct {
	// NodeID identifies this node in the cluster. Must be > 0.
	NodeID uint64
	// RaftAddr is the address dragonboat advertises for inter-node Raft
	// traffic (host:port). For single-node tests use a localhost port.
	RaftAddr string
	// DataDir holds per-partition state and dragonboat's own state.
	// Layout: <DataDir>/raft/, <DataDir>/p{shardID}/state/.
	DataDir string
	// Log is the structured logger; defaults to slog.Default().
	Log *slog.Logger
	// EnableMetrics turns on dragonboat's Prometheus collectors.
	EnableMetrics bool
	// RTTMillisecond is the dragonboat logical-clock tick. Defaults to 200ms.
	RTTMillisecond uint64
	// Handlers is the public SDK registry the leader-side Invoker resolves
	// against on ActInvoke dispatch. Nil is acceptable — the partition
	// builds an empty registry and any ActInvoke falls through with a
	// "no handler" warning. Phase 2.
	Handlers *sdk.Registry

	// Peers is the static cluster membership known at bootstrap. When
	// non-empty, the Host runs multi-node: dragonboat gossip is enabled,
	// NodeHostID-keyed targets are used, and every shard the node hosts
	// starts with initialMembers covering the full peer set. The current
	// node's NodeID must appear in Peers. When empty the Host runs
	// single-node (Phase 1-3.5 behavior: initialMembers={self}). Phase 4.1.
	Peers []Peer
	// GossipBindAddr is the address dragonboat's gossip layer binds to
	// (host:port). Required when Peers is non-empty.
	GossipBindAddr string
	// GossipAdvAddr is the address advertised to other peers for NAT
	// traversal. Falls back to GossipBindAddr when empty.
	GossipAdvAddr string
	// GrpcEndpoint is this node's reflow Delivery gRPC endpoint. It is
	// published via gossip NodeHostMeta so peers can dial it for
	// cross-partition outbox dispatch. Required when Peers is non-empty.
	GrpcEndpoint string

	// CrossShardSender is the dispatcher partition runners hand to their
	// OutboxService for envelopes whose destination_shard_id is non-
	// local. Wired up to an *internal/engine/delivery.Client in
	// multi-node deployments; nil in single-node deployments (where no
	// outbox row ever targets a remote shard). Phase 4.1.
	CrossShardSender CrossShardSender
}

// Peer is a static cluster member known at bootstrap. NodeHostID may be
// left empty; reflow then derives a deterministic ID from NodeID. The
// derivation is identical on every node, so the static map of
// NodeID → NodeHostID is consistent cluster-wide without coordination.
// Phase 4.1.
type Peer struct {
	NodeID     uint64
	RaftAddr   string
	NodeHostID string
	// GossipAddr is the peer's GossipAdvAddr; collected here so a node
	// can populate dragonboat's gossip Seed list from the same Peers
	// slice that drives Raft membership. Empty for self; required for
	// every other peer when Peers is non-empty.
	GossipAddr string
}

// resolvedNodeHostID returns the explicit override or a deterministic
// derivation from NodeID. Both peers and self agree on this without
// coordination, so static bootstrap needs no NodeHostID exchange.
//
// dragonboat validates NodeHostID against google/uuid.Parse, so the
// derived form has to be a syntactically valid UUID. We embed NodeID
// in the last 12 hex chars of an all-zero UUID — readable and stable.
func (p Peer) resolvedNodeHostID() string {
	if p.NodeHostID != "" {
		return p.NodeHostID
	}
	return fmt.Sprintf("00000000-0000-0000-0000-%012x", p.NodeID)
}

// Host owns the NodeHost and the per-partition runners.
type Host struct {
	cfg HostConfig
	nh  *dragonboat.NodeHost
	log *slog.Logger

	mu              sync.RWMutex
	partitions      map[uint64]*PartitionRunner
	metadataRunners map[uint64]*MetadataRunner
}

// NewHost constructs a Host but does not start any partitions; call
// StartPartition for each shard the node should host.
func NewHost(cfg HostConfig) (*Host, error) {
	if cfg.NodeID == 0 {
		return nil, errors.New("host: NodeID must be > 0")
	}
	if cfg.RaftAddr == "" {
		return nil, errors.New("host: RaftAddr is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("host: DataDir is required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.RTTMillisecond == 0 {
		cfg.RTTMillisecond = 200
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("host: create data dir: %w", err)
	}

	h := &Host{
		cfg:        cfg,
		log:        cfg.Log,
		partitions: make(map[uint64]*PartitionRunner),
	}

	nhConfig := config.NodeHostConfig{
		NodeHostDir:       filepath.Join(cfg.DataDir, "raft"),
		RaftAddress:       cfg.RaftAddr,
		RTTMillisecond:    cfg.RTTMillisecond,
		EnableMetrics:     cfg.EnableMetrics,
		RaftEventListener: &raftEventListener{host: h},
	}
	if len(cfg.Peers) > 0 {
		if err := applyMultiNodeConfig(&nhConfig, &cfg); err != nil {
			return nil, err
		}
	}
	nh, err := dragonboat.NewNodeHost(nhConfig)
	if err != nil {
		return nil, fmt.Errorf("host: NewNodeHost: %w", err)
	}
	h.nh = nh
	return h, nil
}

// applyMultiNodeConfig wires the dragonboat NodeHostConfig fields that
// turn on cross-host gossip + NodeHostID-keyed targets, and packs the
// reflow gRPC Delivery endpoint into the gossip Meta blob so peers can
// resolve it via INodeHostRegistry.GetMeta. Phase 4.1.
func applyMultiNodeConfig(nhConfig *config.NodeHostConfig, cfg *HostConfig) error {
	self := findPeer(cfg.Peers, cfg.NodeID)
	if self == nil {
		return fmt.Errorf("host: NodeID %d not present in Peers", cfg.NodeID)
	}
	if cfg.GossipBindAddr == "" {
		return errors.New("host: GossipBindAddr required when Peers is non-empty")
	}
	if cfg.GrpcEndpoint == "" {
		return errors.New("host: GrpcEndpoint required when Peers is non-empty")
	}
	// dragonboat parses every NodeHostID via google/uuid.Parse before
	// admitting a NodeHost — fail fast here with a peer-attributed error
	// rather than letting a malformed override (or a future change to the
	// derived form) surface as an opaque NewNodeHost failure.
	for _, p := range cfg.Peers {
		nhID := p.resolvedNodeHostID()
		if _, err := uuid.Parse(nhID); err != nil {
			return fmt.Errorf("host: peer NodeID=%d NodeHostID %q is not a valid UUID: %w", p.NodeID, nhID, err)
		}
	}
	adv := cfg.GossipAdvAddr
	if adv == "" {
		adv = cfg.GossipBindAddr
	}
	metaBytes, err := proto.Marshal(&enginev1.NodeHostMeta{GrpcEndpoint: cfg.GrpcEndpoint})
	if err != nil {
		return fmt.Errorf("host: marshal NodeHostMeta: %w", err)
	}
	seeds := make([]string, 0, len(cfg.Peers)-1)
	for _, p := range cfg.Peers {
		if p.NodeID == cfg.NodeID {
			continue
		}
		if p.GossipAddr == "" {
			return fmt.Errorf("host: peer NodeID=%d missing GossipAddr", p.NodeID)
		}
		seeds = append(seeds, p.GossipAddr)
	}
	nhConfig.DefaultNodeRegistryEnabled = true
	nhConfig.NodeHostID = self.resolvedNodeHostID()
	nhConfig.Gossip = config.GossipConfig{
		BindAddress:      cfg.GossipBindAddr,
		AdvertiseAddress: adv,
		Seed:             seeds,
		Meta:             metaBytes,
	}
	return nil
}

func findPeer(peers []Peer, nodeID uint64) *Peer {
	for i := range peers {
		if peers[i].NodeID == nodeID {
			return &peers[i]
		}
	}
	return nil
}

// NodeHost returns the underlying dragonboat NodeHost. Exposed for tests and
// administrative tools (status queries, manual membership changes in later
// phases). Production code should prefer Host-provided methods.
func (h *Host) NodeHost() *dragonboat.NodeHost { return h.nh }

// SetCrossShardSender installs the cross-shard dispatcher every later
// StartPartition call will hand to its OutboxService. Must be called
// before StartPartition for multi-node deployments — partitions that
// fail to receive a Sender will refuse to dispatch any cross-shard
// outbox row at runtime. No-op when Peers is empty (single-node). Phase 4.1.
func (h *Host) SetCrossShardSender(s CrossShardSender) {
	h.mu.Lock()
	h.cfg.CrossShardSender = s
	h.mu.Unlock()
}

// StartMetadataShard opens the metadata-shard store, registers the cluster
// FSM with dragonboat, and wires the metadata runner. Phase 4.1: callers
// pass shardID=0. Only valid when HostConfig.Peers is non-empty; single-
// node deployments have no need for the metadata group.
func (h *Host) StartMetadataShard() (*MetadataRunner, error) {
	const shardID uint64 = 0
	if len(h.cfg.Peers) == 0 {
		return nil, errors.New("host: StartMetadataShard requires Peers to be populated")
	}
	h.mu.Lock()
	if _, ok := h.metadataRunners[shardID]; ok {
		h.mu.Unlock()
		return nil, fmt.Errorf("host: metadata shard %d already started", shardID)
	}
	h.mu.Unlock()

	dataDir := filepath.Join(h.cfg.DataDir, "meta", "state")
	snap, err := NewSnapshotter(dataDir, func(p string) (storage.Store, error) {
		return storage.OpenPebbleWithFormatGuard(p, nil, storage.StorageFormatVersion)
	})
	if err != nil {
		return nil, fmt.Errorf("host: open metadata store: %w", err)
	}

	// Seed leadership from persisted latest_announced_epoch so a restarted
	// node bumps past prior leaders' dedup state.
	var initialEpoch uint64
	if m, err := (cluster.MetaTable{S: snap.Store()}).Get(); err == nil {
		initialEpoch = m.GetLatestAnnouncedEpoch()
	}

	proposer := NewRaftProposer(h.nh, shardID)
	leadership := NewLeadership(LeadershipConfig{
		NodeID:       h.cfg.NodeID,
		Announcer:    proposer,
		Log:          h.log,
		InitialEpoch: initialEpoch,
	})

	runner := &MetadataRunner{
		ShardID:     shardID,
		snapshotter: snap,
		proposer:    proposer,
		leadership:  leadership,
		log:         h.log,
		peers:       append([]Peer(nil), h.cfg.Peers...),
		host:        h,
	}
	leadership.SetCallbacks(runner.onBecomeLeader, runner.onStepDown)

	fsmCfg := cluster.Config{
		Snapshotter: snap,
		Leadership:  leadership,
		Log:         h.log,
	}
	raftCfg := config.Config{
		ReplicaID:          h.cfg.NodeID,
		ShardID:            shardID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		SnapshotEntries:    10_000,
		CompactionOverhead: 5_000,
		CheckQuorum:        true,
	}
	h.mu.Lock()
	if h.metadataRunners == nil {
		h.metadataRunners = make(map[uint64]*MetadataRunner)
	}
	h.metadataRunners[shardID] = runner
	h.mu.Unlock()

	initial := h.initialMembers()
	if err := h.nh.StartOnDiskReplica(initial, false, fsmCfg.Factory(), raftCfg); err != nil {
		h.mu.Lock()
		delete(h.metadataRunners, shardID)
		h.mu.Unlock()
		_ = snap.Close()
		return nil, fmt.Errorf("host: StartOnDiskReplica (metadata): %w", err)
	}
	return runner, nil
}

// MetadataRunner returns the metadata-shard runner, or nil if shard 0 is
// not started on this Host.
func (h *Host) MetadataRunner() *MetadataRunner {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.metadataRunners == nil {
		return nil
	}
	return h.metadataRunners[0]
}

// PartitionTable performs a linearizable read of the cluster's partition
// table from shard 0. Returns nil with no error when the table has not yet
// been written (i.e. shard 0 has not bootstrapped). Phase 4.1.
func (h *Host) PartitionTable(ctx context.Context) (*enginev1.PartitionTable, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupPartitionTable{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	pt, ok := res.(*enginev1.PartitionTable)
	if !ok {
		return nil, fmt.Errorf("host: PartitionTable: unexpected lookup type %T", res)
	}
	return pt, nil
}

// NodeID returns this Host's configured NodeID. Exposed for admin RPCs
// that need to refuse self-eviction.
func (h *Host) NodeID() uint64 { return h.cfg.NodeID }

// SnapshotPartitionToDir asks dragonboat to materialize an Exported
// snapshot of shardID into dstDir and returns the Raft index of the
// resulting snapshot. The export dir convention follows dragonboat's
// SnapshotOption{Exported=true, ExportPath=dir}: a single sub-directory
// is created at dstDir holding the snapshot files.
//
// Wraps nh.SyncRequestSnapshot so callers (admin RPCs, snapshot
// producers) do not import dragonboat. Phase 4.2.
func (h *Host) SnapshotPartitionToDir(ctx context.Context, shardID uint64, dstDir string) (uint64, error) {
	if h.nh == nil {
		return 0, errors.New("host: NodeHost not initialized")
	}
	opt := dragonboat.SnapshotOption{Exported: true, ExportPath: dstDir}
	return h.nh.SyncRequestSnapshot(ctx, shardID, opt)
}

// Membership performs a linearizable read of shard 0's membership table.
// Returns an empty slice (not nil) when no rows have been registered yet.
// Phase 4.2.
func (h *Host) Membership(ctx context.Context) ([]*enginev1.NodeMembership, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupMembership{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	out, ok := res.([]*enginev1.NodeMembership)
	if !ok {
		return nil, fmt.Errorf("host: Membership: unexpected lookup type %T", res)
	}
	return out, nil
}

// AwaitMetadataLeader blocks until shard 0 has a stable leader.
func (h *Host) AwaitMetadataLeader(ctx context.Context) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if r := h.MetadataRunner(); r != nil && r.IsLeader() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// StartPartition opens the per-partition store, registers the IOnDiskStateMachine
// with dragonboat, and wires the leader-side runner.
func (h *Host) StartPartition(shardID uint64) (*PartitionRunner, error) {
	if shardID == 0 {
		return nil, errors.New("host: shardID must be > 0")
	}
	h.mu.Lock()
	if _, ok := h.partitions[shardID]; ok {
		h.mu.Unlock()
		return nil, fmt.Errorf("host: partition %d already started", shardID)
	}
	h.mu.Unlock()

	dataDir := filepath.Join(h.cfg.DataDir, fmt.Sprintf("p%d", shardID), "state")
	snap, err := NewSnapshotter(dataDir, func(p string) (storage.Store, error) {
		return storage.OpenPebbleWithFormatGuard(p, nil, storage.StorageFormatVersion)
	})
	if err != nil {
		return nil, fmt.Errorf("host: open partition store: %w", err)
	}

	// Seed the leadership epoch from MetaTable so a restarted node bumps
	// past any epoch the prior leader already emitted (whose dedup records
	// still live in Pebble).
	var initialEpoch uint64
	if m, err := (tables.MetaTable{S: snap.Store()}).Get(); err == nil {
		initialEpoch = m.GetLatestAnnouncedEpoch()
	}

	collector := &ActionCollector{}
	proposer := NewRaftProposer(h.nh, shardID)
	leadership := NewLeadership(LeadershipConfig{
		NodeID:       h.cfg.NodeID,
		Announcer:    proposer,
		Log:          h.log,
		InitialEpoch: initialEpoch,
	})

	registry := h.cfg.Handlers
	if registry == nil {
		registry = sdk.NewRegistry()
	}

	runner := &PartitionRunner{
		ShardID:     shardID,
		snapshotter: snap,
		proposer:    proposer,
		leadership:  leadership,
		collector:   collector,
		sender:      h.cfg.CrossShardSender,
		log:         h.log,
	}
	// The Invoker is constructed once and survives leader gain/loss
	// cycles via Start/Stop; table views rebind on each leader gain so
	// they track the snapshotter's current store after recovery. The
	// TimerService and OutboxService are recreated per leader gain in
	// onBecomeLeader — their `done` channels are single-use so reusing
	// the same instance across promotions would panic.
	runner.invoker = invoker.New(invoker.Config{
		Registry:        invoker.NewRegistry(registry),
		JournalTable:    tables.JournalTable{S: snap.Store()},
		InvocationTable: tables.InvocationTable{S: snap.Store()},
		StateTable:      tables.StateTable{S: snap.Store()},
		Proposer:        proposer,
		Log:             h.log,
	})

	leadership.SetCallbacks(runner.onBecomeLeader, runner.onStepDown)

	pc := PartitionConfig{
		Snapshotter: snap,
		Leadership:  leadership,
		Collector:   collector,
		NowFn:       func() uint64 { return uint64(time.Now().UnixMilli()) },
		Log:         h.log,
		OnActions:   runner.dispatchActions,
		Partitioner: routing.Partitioner{NumShards: uint64(len(h.cfg.Peers))},
	}
	raftCfg := config.Config{
		ReplicaID:          h.cfg.NodeID,
		ShardID:            shardID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		SnapshotEntries:    10_000,
		CompactionOverhead: 5_000,
		CheckQuorum:        true,
	}
	// Register the runner BEFORE StartOnDiskReplica so any LeaderUpdated
	// event that fires during catch-up reaches our leadership state.
	h.mu.Lock()
	h.partitions[shardID] = runner
	h.mu.Unlock()

	initial := h.initialMembers()
	if err := h.nh.StartOnDiskReplica(initial, false, pc.Factory(), raftCfg); err != nil {
		h.mu.Lock()
		delete(h.partitions, shardID)
		h.mu.Unlock()
		_ = snap.Close()
		return nil, fmt.Errorf("host: StartOnDiskReplica: %w", err)
	}
	return runner, nil
}

// Partition returns the runner for the given shard or nil if none is started.
func (h *Host) Partition(shardID uint64) *PartitionRunner {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.partitions[shardID]
}

// RunnerView is the small-interface view of a *PartitionRunner used by
// the Phase 4.1 Delivery server. Declared here so the delivery package
// can consume it without importing the heavy PartitionRunner type.
type RunnerView interface {
	IsLeader() bool
	Proposer() *RaftProposer
}

// PartitionRunner is the small-interface accessor used by Phase 4.1's
// Delivery server (it accepts a runner satisfying a narrow IsLeader +
// Proposer surface). Returns nil when shardID is not hosted on this node;
// callers must treat nil as "not leader" so the sender re-resolves via
// gossip.
func (h *Host) PartitionRunner(shardID uint64) RunnerView {
	r := h.Partition(shardID)
	if r == nil {
		return nil
	}
	return r
}

// Partitions returns a snapshot of the runners hosted on this node, keyed by
// shard ID. The map is freshly allocated; mutating it does not affect the
// host. Order is not stable across calls. Used by ingress admin endpoints.
func (h *Host) Partitions() map[uint64]*PartitionRunner {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[uint64]*PartitionRunner, len(h.partitions))
	maps.Copy(out, h.partitions)
	return out
}

// Close stops every partition and the NodeHost. Idempotent.
func (h *Host) Close() error {
	h.mu.Lock()
	partitions := h.partitions
	metadataRunners := h.metadataRunners
	h.partitions = nil
	h.metadataRunners = nil
	h.mu.Unlock()
	for _, p := range partitions {
		p.onStepDown()
	}
	for _, r := range metadataRunners {
		r.onStepDown()
	}
	if h.nh != nil {
		h.nh.Close()
		h.nh = nil
	}
	return nil
}

// LookupInvocationStatus performs a linearizable read of an invocation's
// status. Convenience wrapper around dragonboat SyncRead.
func (h *Host) LookupInvocationStatus(ctx context.Context, shardID uint64, id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	res, err := h.nh.SyncRead(ctx, shardID, LookupInvocation{ID: id})
	if err != nil {
		return nil, err
	}
	s, ok := res.(*enginev1.InvocationStatus)
	if !ok {
		return nil, fmt.Errorf("lookup returned unexpected type %T", res)
	}
	return s, nil
}

// AwaitLeader blocks until shardID has a stable leader (LeaderUpdated event
// fired) or ctx expires.
func (h *Host) AwaitLeader(ctx context.Context, shardID uint64) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if r := h.Partition(shardID); r != nil && r.leadership.IsLeader() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// initialMembers builds the dragonboat StartOnDiskReplica seed map.
// Multi-node clusters (Peers populated) use NodeHostID targets so
// dragonboat's gossip can resolve them to live RaftAddresses; single-node
// clusters keep the Phase 1 behavior of self-only RaftAddress targets.
func (h *Host) initialMembers() map[uint64]dragonboat.Target {
	if len(h.cfg.Peers) == 0 {
		return map[uint64]dragonboat.Target{h.cfg.NodeID: dragonboat.Target(h.cfg.RaftAddr)}
	}
	out := make(map[uint64]dragonboat.Target, len(h.cfg.Peers))
	for _, p := range h.cfg.Peers {
		out[p.NodeID] = dragonboat.Target(p.resolvedNodeHostID())
	}
	return out
}

// PartitionLeaderHint returns the believed leader's NodeID for the given
// shard, sourced from dragonboat gossip (ShardView). Returns (0, false)
// when gossip is off (single-node) or no leader is known yet. The result
// is advisory — callers must still tolerate NotLeader responses from
// the Delivery RPC and re-resolve. Phase 4.1.
func (h *Host) PartitionLeaderHint(shardID uint64) (uint64, bool) {
	if h.nh == nil {
		return 0, false
	}
	reg, ok := h.nh.GetNodeHostRegistry()
	if !ok {
		return 0, false
	}
	view, ok := reg.GetShardInfo(shardID)
	if !ok || view.LeaderID == 0 {
		return 0, false
	}
	return view.LeaderID, true
}

// NodeEndpoint returns the reflow Delivery gRPC endpoint advertised via
// gossip NodeHostMeta by the peer with the given NodeID. Returns
// ("", false) when gossip is off, the peer is unknown, or its Meta blob
// has not yet propagated. Phase 4.1.
func (h *Host) NodeEndpoint(nodeID uint64) (string, bool) {
	if h.nh == nil {
		return "", false
	}
	reg, ok := h.nh.GetNodeHostRegistry()
	if !ok {
		return "", false
	}
	nhID := h.nodeHostIDOf(nodeID)
	if nhID == "" {
		return "", false
	}
	raw, ok := reg.GetMeta(nhID)
	if !ok {
		return "", false
	}
	var meta enginev1.NodeHostMeta
	if err := proto.Unmarshal(raw, &meta); err != nil {
		return "", false
	}
	return meta.GetGrpcEndpoint(), meta.GetGrpcEndpoint() != ""
}

// nodeHostIDOf returns the resolved NodeHostID for a peer NodeID, or
// "" if the node is not in this Host's static peer set.
func (h *Host) nodeHostIDOf(nodeID uint64) string {
	for _, p := range h.cfg.Peers {
		if p.NodeID == nodeID {
			return p.resolvedNodeHostID()
		}
	}
	return ""
}

// raftEventListener implements raftio.IRaftEventListener for dragonboat. It
// must NOT block — handed off to the runner's leadership which is also
// non-blocking.
type raftEventListener struct{ host *Host }

func (l *raftEventListener) LeaderUpdated(info raftio.LeaderInfo) {
	l.host.log.Debug("raftEventListener: LeaderUpdated",
		"shard", info.ShardID,
		"replica", info.ReplicaID,
		"term", info.Term,
		"leader_id", info.LeaderID,
	)
	if mr := l.host.MetadataRunner(); mr != nil && mr.ShardID == info.ShardID {
		mr.leadership.OnRaftLeaderChange(info.LeaderID)
		return
	}
	r := l.host.Partition(info.ShardID)
	if r == nil {
		l.host.log.Warn("raftEventListener: no runner for shard", "shard", info.ShardID)
		return
	}
	r.leadership.OnRaftLeaderChange(info.LeaderID)
}
