package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/engine/invoker"
	"github.com/twinfer/reflow/internal/engine/rebalance"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/observability"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// HostConfig configures a reflow node (a process hosting one or more
// partitions). Single-node deployments use NodeID=1 with Peers empty;
// multi-node deployments populate Peers with every cluster member
// (including self) and supply gossip + Delivery endpoint addresses.
type HostConfig struct {
	// NodeID identifies this node in the cluster. Must be > 0.
	NodeID uint64
	// RaftAddr is the bind address dragonboat listens on for inter-node
	// Raft traffic (host:port). For single-node tests use a localhost
	// port. When RaftAdvertisedAddr is empty (the production default),
	// RaftAddr also serves as the address gossiped to peers — dragonboat
	// treats them as the same in its NodeHostConfig.RaftAddress.
	RaftAddr string
	// RaftAdvertisedAddr, when non-empty, is the address gossiped to
	// peers (dragonboat's NodeHostConfig.RaftAddress). RaftAddr then
	// drops to a pure bind via NodeHostConfig.ListenAddress. Mirrors
	// GossipBindAddr/GossipAdvAddr below — needed when the node sits
	// behind NAT, a load balancer, or (in the e2e chaos harness) a
	// Toxiproxy listener. Empty preserves today's combined bind+advertise
	// behavior.
	RaftAdvertisedAddr string
	// DataDir holds per-partition state and dragonboat's own state.
	// Layout: <DataDir>/raft/, <DataDir>/p{shardID}/state/.
	DataDir string
	// Log is the structured logger; defaults to slog.Default().
	Log *slog.Logger
	// EnableMetrics turns on dragonboat's Prometheus collectors.
	EnableMetrics bool
	// RTTMillisecond is the dragonboat logical-clock tick. Defaults to 200ms.
	RTTMillisecond uint64

	// Peers is the static cluster membership known at bootstrap. NewHost
	// normalizes an empty slice to a single self-entry so the rest of the
	// engine sees one uniform "1+ node cluster" shape. Multi-node
	// (len(Peers) > 1) turns on dragonboat gossip and NodeHostID-keyed
	// targets; solo (len(Peers) == 1) uses the self RaftAddr directly
	// because there's no gossip layer to resolve NodeHostIDs.
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
	// AdminEndpoint is this node's reflow Admin Connect endpoint.
	// Published via gossip NodeHostMeta so peers (notably joiners
	// calling SelfJoin and the `reflowd cluster ...` CLI following
	// LeaderHint redirects) can
	// dial the metadata leader without preconfiguration. Optional but
	// recommended when Peers is non-empty; an empty value disables
	// gossip-based admin discovery for this node (the joiner path then
	// has no way to find the leader's admin port and SelfJoin will fail
	// at boot until gossip Meta is updated).
	AdminEndpoint string

	// CrossShardSender is the dispatcher partition runners hand to their
	// OutboxService for envelopes whose destination_shard_id is non-
	// local. Wired up to an *internal/engine/delivery.Client in
	// multi-node deployments; nil in single-node deployments (where no
	// outbox row ever targets a remote shard).
	CrossShardSender CrossShardSender

	// LPSSTUploader is the side-channel SST shipper handed to each
	// partition's LPTransferService. Wired up in pkg/reflow over the
	// delivery Connect listener; nil in single-node deployments.
	LPSSTUploader LPSSTUploader

	// NumPartitionShards is the total number of partition shards in the
	// cluster (the routing modulus). Independent of replication factor
	// and of peer count: a deployment can host N shards on M peers in any
	// combination. Required: NewHost returns an error when zero.
	NumPartitionShards uint64

	// Metrics carries the Prometheus collectors observed by the partition
	// apply path and the timer service. nil disables observation; the
	// engine never constructs its own — wiring is owned by the caller
	// (pkg/reflow) to keep the registry decision out of internal/engine.
	Metrics *observability.Metrics

	// OnSnapshotPersisted, when non-nil, is invoked after a successful
	// Partition.SaveSnapshot. The shardID identifies which partition just
	// snapshotted. Runs on the dragonboat snapshot goroutine — MUST be
	// non-blocking (the conventional implementation is a non-blocking send
	// into a buffered-1 channel consumed by the snapshot archive producer).
	OnSnapshotPersisted func(shardID uint64)

	// PebbleOptions, when non-nil, is consulted on each per-shard Pebble
	// open (both metadata shard 0 and partition shards 1..N). The
	// returned *pebble.Options is passed straight to pebble.Open; nil
	// means "use Pebble defaults" (the production path). The hook exists
	// to let the load/chaos harness inject a vfs.FS wrapper (slow-disk
	// fault injection) or non-default tunables without forking the engine.
	PebbleOptions func(shardID uint64) *pebble.Options

	// JoinExisting, when true, starts every shard with
	// StartOnDiskReplica(nil, join=true, ...) — the dragonboat semantics
	// for a replica that is joining an already-running Raft group rather
	// than bootstrapping it. The cluster-side `admin AddNode` workflow
	// must have run first against an existing leader so the new
	// ReplicaID is a known member of each shard's configuration; this
	// node then catches up via Raft snapshot + log replication. Default
	// false preserves the static bootstrap path: every shard seeds with
	// the full Peers set via initialMembers().
	JoinExisting bool

	// HandlerSigner, when non-nil, stamps every engine→handler HTTP/2
	// request with an Authorization: Bearer JWT minted by the engine's
	// node-identity keypair. nil disables signing (single-node and
	// insecure-creds deployments).
	HandlerSigner handlerclient.Signer

	// EagerStateMaxBytes caps the eager-state snapshot the invoker ships
	// in StartMessage.state_map. Larger object states fall back to lazy
	// fetch (StartMessage.partial_state=true). Zero means "use
	// invoker.DefaultEagerStateMaxBytes" (64 KiB). Operators tune via
	// Config.Handlers.EagerStateMaxBytes when state-heavy handlers want
	// fewer lazy round-trips, or when capping per-session memory matters
	// more than read latency.
	EagerStateMaxBytes uint32

	// ClusterNotifiers carries per-table change signals fired from the
	// shard-0 FSM apply path after batch commit. Each notifier is
	// per-table; consumers (the eventsource Reconciler, etc.) Subscribe
	// for the receive end. Zero value disables all notifications (the
	// FSM no-ops on nil notifier handles). pkg/reflow wires this up
	// before NewHost.
	ClusterNotifiers cluster.Notifiers

	// Rebalance configures the autonomous LP rebalancer started on
	// metadata-shard leadership. Zero value (Mode unset) disables the
	// loop entirely; the runner skips the goroutine spawn in
	// onBecomeLeader. pkg/reflow.withDefaults defaults Mode to "off",
	// so the production default is also disabled.
	Rebalance rebalance.Config
}

// Peer is a static cluster member known at bootstrap. NodeHostID may be
// left empty; reflow then derives a deterministic ID from NodeID. The
// derivation is identical on every node, so the static map of
// NodeID → NodeHostID is consistent cluster-wide without coordination.
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

	// partitioner is the per-Host routing singleton. Constructed once in
	// NewHost so every value-copy returned by Partitioner() shares the
	// same atomic LPOwners snapshot slot. The routing reconciler swaps
	// the slot via SetLPOwnersSnapshot on each TableNotifier wake.
	partitioner *routing.Partitioner

	mu              sync.RWMutex
	partitions      map[uint64]*PartitionRunner
	metadataRunners map[uint64]*MetadataRunner
	// startMu serializes StartPartition calls per shardID so concurrent
	// callers — typically an explicit boot loop racing the
	// PartitionTable reconciler — cannot both attempt to open the same
	// per-shard Pebble directory (pebble fails the second open with
	// "lock held"). The second caller waits, observes the partition is
	// already running, and returns the existing runner.
	startMu map[uint64]*sync.Mutex
	// handlerRegistry caches remote-deployment clients keyed by
	// deployment_id. Populated lazily by the wire dispatcher on first
	// use of a deployment; entries are evicted when a DeploymentRecord
	// is overwritten with a different URL.
	handlerRegistry *handlerclient.Registry
	// closed turns Host.Close into a single-shot operation. The
	// ctx-watcher goroutine and explicit shutdown paths both call
	// Close; the second observes closed=true and returns without
	// re-stopping the NodeHost (dragonboat panics on double-stop).
	closed bool
	// asyncStarts tracks goroutines spawned by ReconcilePartitionTable
	// that call StartPartition out-of-band. Close waits on this so a
	// pebble.Open mid-flight can't race with t.TempDir cleanup or
	// nh.Close().
	asyncStarts sync.WaitGroup
}

// NewHost constructs a Host but does not start any partitions; call
// StartPartition for each shard the node should host.
//
// ctx is a lifecycle context: when cancelled, the Host self-closes in a
// background goroutine. This is a safety net for leaked Hosts (e.g.
// tests that forget to defer Close, leaving dragonboat goroutines alive
// to race with t.TempDir cleanup). Production code should still call
// Close explicitly for deterministic teardown ordering; the watcher is
// belt-and-suspenders.
func NewHost(ctx context.Context, cfg HostConfig) (*Host, error) {
	if cfg.NodeID == 0 {
		return nil, errors.New("host: NodeID must be > 0")
	}
	if cfg.RaftAddr == "" {
		return nil, errors.New("host: RaftAddr is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("host: DataDir is required")
	}
	if cfg.NumPartitionShards == 0 {
		return nil, errors.New("host: NumPartitionShards must be > 0")
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

	// Normalize solo deployments to a 1-peer cluster so metadata-shard
	// bootstrap, initialMembers, and the runner's peer-iteration logic
	// all see one uniform shape (no len(Peers) == 0 short-circuits). The
	// self-peer carries the advertised address (what other peers would
	// dial); when RaftAdvertisedAddr is empty it falls back to RaftAddr.
	if len(cfg.Peers) == 0 {
		adv := cfg.RaftAdvertisedAddr
		if adv == "" {
			adv = cfg.RaftAddr
		}
		cfg.Peers = []Peer{{NodeID: cfg.NodeID, RaftAddr: adv}}
	}

	h := &Host{
		cfg:             cfg,
		log:             cfg.Log,
		partitioner:     routing.NewPartitioner(cfg.NumPartitionShards),
		partitions:      make(map[uint64]*PartitionRunner),
		startMu:         make(map[uint64]*sync.Mutex),
		handlerRegistry: newHandlerRegistry(cfg.HandlerSigner),
	}

	advertisedRaft, listenOverride := raftBindAndAdvertise(&cfg)
	nhConfig := config.NodeHostConfig{
		NodeHostDir:       filepath.Join(cfg.DataDir, "raft"),
		RaftAddress:       advertisedRaft,
		ListenAddress:     listenOverride,
		RTTMillisecond:    cfg.RTTMillisecond,
		EnableMetrics:     cfg.EnableMetrics,
		RaftEventListener: &raftEventListener{host: h},
	}
	if len(cfg.Peers) > 1 {
		if err := applyMultiNodeConfig(&nhConfig, &cfg); err != nil {
			return nil, err
		}
	}
	nh, err := dragonboat.NewNodeHost(nhConfig)
	if err != nil {
		return nil, fmt.Errorf("host: NewNodeHost: %w", err)
	}
	h.nh = nh
	// Safety-net lifecycle watcher: when ctx is cancelled, self-close
	// even if no explicit Close was called. Catches leaks in tests that
	// forget to defer Close; production code (pkg/reflow.Run) closes
	// explicitly so this never fires there.
	if ctx != nil {
		go func() {
			<-ctx.Done()
			_ = h.Close()
		}()
	}
	return h, nil
}

// applyMultiNodeConfig wires the dragonboat NodeHostConfig fields that
// turn on cross-host gossip + NodeHostID-keyed targets, and packs the
// reflow gRPC Delivery endpoint into the gossip Meta blob so peers can
// resolve it via INodeHostRegistry.GetMeta.
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
	metaBytes, err := proto.Marshal(&enginev1.NodeHostMeta{
		GrpcEndpoint:  cfg.GrpcEndpoint,
		AdminEndpoint: cfg.AdminEndpoint,
	})
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

// raftBindAndAdvertise resolves the bind/advertise split for the Raft
// transport. The advertise value is what dragonboat publishes via gossip
// (NodeHostConfig.RaftAddress); the listenOverride is the bind address
// (NodeHostConfig.ListenAddress) and is only set when it differs from the
// advertise — leaving ListenAddress empty makes dragonboat listen on
// RaftAddress, preserving today's combined behavior when
// RaftAdvertisedAddr is unset.
func raftBindAndAdvertise(cfg *HostConfig) (advertise, listenOverride string) {
	advertise = cfg.RaftAdvertisedAddr
	if advertise == "" {
		advertise = cfg.RaftAddr
	}
	if advertise != cfg.RaftAddr {
		listenOverride = cfg.RaftAddr
	}
	return advertise, listenOverride
}

// NodeHost returns the underlying dragonboat NodeHost. Exposed for tests and
// administrative tooling. Production code should prefer Host-provided methods.
func (h *Host) NodeHost() *dragonboat.NodeHost { return h.nh }

// pebbleOptionsFor returns the per-shard Pebble options, or nil when
// the caller did not supply a hook (the production default — Pebble
// applies its own defaults).
func (h *Host) pebbleOptionsFor(shardID uint64) *pebble.Options {
	if h.cfg.PebbleOptions == nil {
		return nil
	}
	return h.cfg.PebbleOptions(shardID)
}

// SetCrossShardSender installs the cross-shard dispatcher every later
// StartPartition call will hand to its OutboxService. Must be called
// before StartPartition for multi-node deployments — partitions that
// fail to receive a Sender will refuse to dispatch any cross-shard
// outbox row at runtime. No-op when Peers is empty (single-node).
func (h *Host) SetCrossShardSender(s CrossShardSender) {
	h.mu.Lock()
	h.cfg.CrossShardSender = s
	h.mu.Unlock()
}

// SetLPSSTUploader installs the LP-transfer SST shipper every later
// StartPartition call will hand to its LPTransferService. Must be
// called before StartPartition for multi-node deployments — partitions
// that fail to receive an uploader will fall back to a "no uploader"
// scan error and the lpMover saga will stall + abort.
func (h *Host) SetLPSSTUploader(u LPSSTUploader) {
	h.mu.Lock()
	h.cfg.LPSSTUploader = u
	h.mu.Unlock()
}

// StartMetadataShard opens the metadata-shard store, registers the cluster
// FSM with dragonboat, and wires the metadata runner. Callers pass
// shardID=0. Single-node deployments run a 1-replica metadata Raft group
// just like a 1-of-1 multi-node cluster; the deployment registry lives
// on shard 0 regardless of replica count.
func (h *Host) StartMetadataShard() (*MetadataRunner, error) {
	const shardID uint64 = 0
	h.mu.Lock()
	if _, ok := h.metadataRunners[shardID]; ok {
		h.mu.Unlock()
		return nil, fmt.Errorf("host: metadata shard %d already started", shardID)
	}
	h.mu.Unlock()

	dataDir := filepath.Join(h.cfg.DataDir, "meta", "state")
	snap, err := NewSnapshotter(dataDir, func(p string) (storage.Store, error) {
		raw, oerr := storage.OpenPebbleWithFormatGuard(p, h.pebbleOptionsFor(shardID), storage.StorageFormatVersion)
		if oerr != nil {
			return nil, oerr
		}
		return raw, nil
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
		ShardID:            shardID,
		snapshotter:        snap,
		proposer:           proposer,
		leadership:         leadership,
		log:                h.log,
		peers:              append([]Peer(nil), h.cfg.Peers...),
		numPartitionShards: h.cfg.NumPartitionShards,
		host:               h,
	}
	leadership.SetCallbacks(runner.onBecomeLeader, runner.onStepDown)

	fsmCfg := cluster.Config{
		Snapshotter: snap,
		Leadership:  leadership,
		Log:         h.log,
		Notifiers:   h.cfg.ClusterNotifiers,
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

	initial, join := h.startMembers()
	if err := h.nh.StartOnDiskReplica(initial, join, fsmCfg.Factory(), raftCfg); err != nil {
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
// been written (i.e. shard 0 has not bootstrapped).
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
// producers) do not import dragonboat.
func (h *Host) SnapshotPartitionToDir(ctx context.Context, shardID uint64, dstDir string) (uint64, error) {
	if h.nh == nil {
		return 0, errors.New("host: NodeHost not initialized")
	}
	opt := dragonboat.SnapshotOption{Exported: true, ExportPath: dstDir}
	return h.nh.SyncRequestSnapshot(ctx, shardID, opt)
}

// Membership performs a linearizable read of shard 0's membership table.
// Returns an empty slice (not nil) when no rows have been registered yet.
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

// Deployment SyncReads the named DeploymentRecord from shard 0. Returns
// (nil, nil) when no deployment claims the id. Used by the Config
// DescribeDeployment RPC; the invoker path uses resolveDeployment for
// its retry-on-leader-race semantics.
func (h *Host) Deployment(ctx context.Context, id string) (*enginev1.DeploymentRecord, error) {
	if id == "" {
		return nil, nil
	}
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupDeployment{ID: id})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	out, ok := res.(*enginev1.DeploymentRecord)
	if !ok {
		return nil, fmt.Errorf("host: Deployment: unexpected lookup type %T", res)
	}
	return out, nil
}

// Deployments SyncReads every DeploymentRecord from shard 0 plus the
// table's CAS revision. Used by the Config ListDeployments RPC.
func (h *Host) Deployments(ctx context.Context) (*cluster.DeploymentList, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupDeploymentList{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return &cluster.DeploymentList{}, nil
	}
	out, ok := res.(*cluster.DeploymentList)
	if !ok {
		return nil, fmt.Errorf("host: Deployments: unexpected lookup type %T", res)
	}
	return out, nil
}

// Secrets SyncReads every SecretRecord from shard 0 plus the table's
// CAS revision. Used by the admin RPCs and the per-node SecretStore
// Reconciler.
func (h *Host) Secrets(ctx context.Context) (*cluster.SecretList, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupSecrets{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return &cluster.SecretList{}, nil
	}
	out, ok := res.(*cluster.SecretList)
	if !ok {
		return nil, fmt.Errorf("host: Secrets: unexpected lookup type %T", res)
	}
	return out, nil
}

// ClusterAuthzPolicy SyncReads the PlatformConfigRecord singleton from shard 0
// plus the platform-config table's CAS revision. Used by the Config admin RPCs
// and the per-node authz Reconciler.
func (h *Host) ClusterAuthzPolicy(ctx context.Context) (*cluster.PlatformConfigResult, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupPlatformConfig{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return &cluster.PlatformConfigResult{}, nil
	}
	out, ok := res.(*cluster.PlatformConfigResult)
	if !ok {
		return nil, fmt.Errorf("host: ClusterAuthzPolicy: unexpected lookup type %T", res)
	}
	return out, nil
}

// CARoots SyncReads every CARootRecord from shard 0 plus the table's
// CAS revision. Used by the admin RPCs and the per-node
// certmgr.ClusterIssuer to refresh the active CA snapshot.
func (h *Host) CARoots(ctx context.Context) (*cluster.CARootList, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupCARoots{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return &cluster.CARootList{}, nil
	}
	out, ok := res.(*cluster.CARootList)
	if !ok {
		return nil, fmt.Errorf("host: CARoots: unexpected lookup type %T", res)
	}
	return out, nil
}

// JoinTokens SyncReads every JoinTokenRecord from shard 0 plus the
// table's CAS revision. Used by the bootstrap server's MeshSign path
// to locate a redeemed token by sha256 hash.
func (h *Host) JoinTokens(ctx context.Context) (*cluster.JoinTokenList, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupJoinTokens{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return &cluster.JoinTokenList{}, nil
	}
	out, ok := res.(*cluster.JoinTokenList)
	if !ok {
		return nil, fmt.Errorf("host: JoinTokens: unexpected lookup type %T", res)
	}
	return out, nil
}

// LPOwners SyncReads every LPOwnerRecord from shard 0 plus the table's
// CAS revision. Used by the per-node routing Reconciler to refresh the
// Partitioner's atomic snapshot. Returns an empty list with revision 0
// before the metadata-leader bootstrap seed commits.
func (h *Host) LPOwners(ctx context.Context) (*cluster.LPOwnersList, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupLPOwners{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return &cluster.LPOwnersList{}, nil
	}
	out, ok := res.(*cluster.LPOwnersList)
	if !ok {
		return nil, fmt.Errorf("host: LPOwners: unexpected lookup type %T", res)
	}
	return out, nil
}

// LPTransfers SyncReads every LPTransferRecord from shard 0 plus the
// table's CAS revision. Used by the lpMover to advance in-flight LP
// transfer sagas one phase per tick.
func (h *Host) LPTransfers(ctx context.Context) (*cluster.LPTransfersList, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupLPTransfers{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return &cluster.LPTransfersList{}, nil
	}
	out, ok := res.(*cluster.LPTransfersList)
	if !ok {
		return nil, fmt.Errorf("host: LPTransfers: unexpected lookup type %T", res)
	}
	return out, nil
}

// RebalanceDrains SyncReads every RebalanceDrainRecord from shard 0
// plus the table's CAS revision. Used by the autonomous rebalancer's
// advisor on each tick to subtract drained shards from the planner's
// input set, and by the operator-facing RebalanceAdvise RPC.
func (h *Host) RebalanceDrains(ctx context.Context) (*cluster.RebalanceDrainList, error) {
	res, err := h.nh.SyncRead(ctx, 0, cluster.LookupRebalanceDrains{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return &cluster.RebalanceDrainList{}, nil
	}
	out, ok := res.(*cluster.RebalanceDrainList)
	if !ok {
		return nil, fmt.Errorf("host: RebalanceDrains: unexpected lookup type %T", res)
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
// with dragonboat, and wires the leader-side runner. Idempotent: a second
// call for an already-running shard returns the existing runner. Serialized
// per shardID so concurrent callers (e.g. boot loop racing the
// PartitionTable reconciler) cannot collide on the per-shard Pebble open.
func (h *Host) StartPartition(shardID uint64) (*PartitionRunner, error) {
	if shardID == 0 {
		return nil, errors.New("host: shardID must be > 0")
	}
	h.mu.Lock()
	if h.partitions == nil {
		h.mu.Unlock()
		return nil, errors.New("host: closed")
	}
	sm := h.startMu[shardID]
	if sm == nil {
		sm = &sync.Mutex{}
		h.startMu[shardID] = sm
	}
	h.mu.Unlock()
	sm.Lock()
	defer sm.Unlock()

	h.mu.Lock()
	if h.partitions == nil {
		h.mu.Unlock()
		return nil, errors.New("host: closed")
	}
	if r, ok := h.partitions[shardID]; ok {
		h.mu.Unlock()
		return r, nil
	}
	h.mu.Unlock()

	dataDir := filepath.Join(h.cfg.DataDir, fmt.Sprintf("p%d", shardID), "state")
	snap, err := NewSnapshotter(dataDir, func(p string) (storage.Store, error) {
		raw, oerr := storage.OpenPebbleWithFormatGuard(p, h.pebbleOptionsFor(shardID), storage.StorageFormatVersion)
		if oerr != nil {
			return nil, oerr
		}
		return raw, nil
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

	// Reap orphan LP-transfer staging dirs (`<dataDir>.lpstage_{in,out}/<id>/`)
	// whose transfer_id is not referenced by a durable LPFreezeTable /
	// LPStagingTable row. A crash mid-transfer can leave half-uploaded SSTs
	// behind; the in-flight saga's Rebuild on leader gain will re-upload /
	// re-Ingest under the same transfer_id and the dirs we keep here serve
	// that retry.
	if err := reapLPTransferStagingDirs(dataDir, snap.Store(), h.log); err != nil {
		h.log.Warn("host: lp-transfer staging cleanup failed; continuing",
			"shard", shardID, "err", err)
	}

	collector := &ActionCollector{}
	proposer := NewRaftProposer(h.nh, shardID)
	leadership := NewLeadership(LeadershipConfig{
		NodeID:       h.cfg.NodeID,
		Announcer:    proposer,
		Log:          h.log,
		InitialEpoch: initialEpoch,
	})

	runner := &PartitionRunner{
		ShardID:     shardID,
		snapshotter: snap,
		proposer:    proposer,
		leadership:  leadership,
		collector:   collector,
		sender:      h.cfg.CrossShardSender,
		lpUploader:  h.cfg.LPSSTUploader,
		log:         h.log,
	}
	// The Invoker is constructed once and survives leader gain/loss
	// cycles via Start/Stop; table views rebind on each leader gain so
	// they track the snapshotter's current store after recovery. The
	// TimerService and OutboxService are recreated per leader gain in
	// onBecomeLeader — their `done` channels are single-use so reusing
	// the same instance across promotions would panic.
	runner.invoker = invoker.New(invoker.Config{
		JournalTable:       tables.JournalTable{S: snap.Store()},
		InvocationTable:    tables.InvocationTable{S: snap.Store()},
		StateTable:         tables.StateTable{S: snap.Store()},
		Proposer:           proposer,
		Deployments:        invoker.DeploymentResolverFunc(h.resolveDeployment),
		HandlerLookup:      h.LookupDeploymentIDByHandler,
		WireDispatcher:     hostWireDispatcher{h: h},
		EagerStateMaxBytes: h.cfg.EagerStateMaxBytes,
		Log:                h.log,
	})

	leadership.SetCallbacks(runner.onBecomeLeader, runner.onStepDown)

	pc := PartitionConfig{
		Snapshotter: snap,
		Leadership:  leadership,
		Collector:   collector,
		Log:         h.log,
		OnActions:   runner.dispatchActions,
		Partitioner: *h.partitioner,
		Metrics:     h.cfg.Metrics,
	}
	if hook := h.cfg.OnSnapshotPersisted; hook != nil {
		pc.OnSnapshotPersisted = func() { hook(shardID) }
	}
	runner.metrics = h.cfg.Metrics
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
	if h.partitions == nil {
		h.mu.Unlock()
		_ = snap.Close()
		return nil, errors.New("host: closed")
	}
	h.partitions[shardID] = runner
	h.mu.Unlock()

	initial, join := h.startMembers()
	if err := h.nh.StartOnDiskReplica(initial, join, pc.Factory(), raftCfg); err != nil {
		h.mu.Lock()
		if h.partitions != nil {
			delete(h.partitions, shardID)
		}
		h.mu.Unlock()
		_ = snap.Close()
		return nil, fmt.Errorf("host: StartOnDiskReplica: %w", err)
	}
	return runner, nil
}

// PartitionReplicas returns the node IDs of every replica hosting
// shardID, per shard 0's PartitionTable. Used by the LP-transfer SST
// uploader to fan out to all replicas in parallel (Pebble Ingest is
// replica-local, so every replica needs the file before the apply
// arm Ingests it).
func (h *Host) PartitionReplicas(ctx context.Context, shardID uint64) ([]uint64, error) {
	pt, err := h.PartitionTable(ctx)
	if err != nil {
		return nil, err
	}
	if pt == nil {
		return nil, nil
	}
	rs := pt.GetShards()[shardID]
	if rs == nil {
		return nil, nil
	}
	return append([]uint64(nil), rs.GetNodeIds()...), nil
}

// PartitionDataDir returns the per-shard on-disk dataDir used by Pebble
// (and the sibling staging dirs <dataDir>.lpstage_{in,out}/). The path
// exists as soon as StartPartition has been called for shardID; the
// LP-transfer upload server resolves <transfer_id>/<namespace>.sst.tmp
// under it. Returns ("", false) when shardID is not hosted on this node.
func (h *Host) PartitionDataDir(shardID uint64) (string, bool) {
	if shardID == 0 {
		return "", false
	}
	h.mu.RLock()
	_, ok := h.partitions[shardID]
	h.mu.RUnlock()
	if !ok {
		return "", false
	}
	return filepath.Join(h.cfg.DataDir, fmt.Sprintf("p%d", shardID), "state"), true
}

// Partition returns the runner for the given shard or nil if none is started.
func (h *Host) Partition(shardID uint64) *PartitionRunner {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.partitions[shardID]
}

// Partitioner returns the cluster's routing partitioner. Value-copy of
// the per-Host singleton — every copy shares the same atomic LPOwners
// snapshot slot, so a single SetLPOwnersSnapshot call (made by the
// routing reconciler) is visible to every reader.
func (h *Host) Partitioner() routing.Partitioner {
	return *h.partitioner
}

// PartitionerRef returns the per-Host routing partitioner singleton.
// The routing reconciler holds this reference to call
// SetLPOwnersSnapshot on each TableNotifier wake.
func (h *Host) PartitionerRef() *routing.Partitioner {
	return h.partitioner
}

// ReconcilePartitionTable converges this node's running-shard set with
// the given PartitionTable snapshot: starts any locally-owned shards
// not yet running, and logs ownership losses that need an explicit
// StopPartition follow-up from the rebalancer.
//
// Called by RunPartitionTableReconciler on its own goroutine — wakes on
// the cluster.Notifiers.PartitionTable bump or the 5s ticker, never on
// the FSM apply path. StartPartition is still offloaded to a per-shard
// goroutine so per-shard Pebble open + dragonboat StartOnDiskReplica
// do not stall further reconcile passes. StopPartition for ownership
// loss is deferred (logged as a warning); the rebalancer leaves drained
// replicas in place until an explicit StopPartition lands as a
// follow-up.
func (h *Host) ReconcilePartitionTable(pt *enginev1.PartitionTable) {
	if pt == nil {
		return
	}
	self := h.cfg.NodeID
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return
	}
	var toStart []uint64
	for shardID, rs := range pt.GetShards() {
		if shardID == 0 {
			continue
		}
		if !slices.Contains(rs.GetNodeIds(), self) {
			continue
		}
		if _, running := h.partitions[shardID]; running {
			continue
		}
		toStart = append(toStart, shardID)
	}
	var notOwned []uint64
	for shardID := range h.partitions {
		rs := pt.GetShards()[shardID]
		if rs == nil || !slices.Contains(rs.GetNodeIds(), self) {
			notOwned = append(notOwned, shardID)
		}
	}
	// Increment asyncStarts while still holding RLock so the Add is
	// atomic with the h.closed check: a concurrent Host.Close must wait
	// behind our RLock before flipping h.closed and reaching
	// asyncStarts.Wait. Without this, Close could move past Wait between
	// our RUnlock and the Add, and the spawned goroutines would
	// pebble.Open after Close returned (observed as "directory not
	// empty" on TempDir teardown in TestSoloBootstrap).
	h.asyncStarts.Add(len(toStart))
	h.mu.RUnlock()

	for _, shardID := range toStart {
		go func(sh uint64) {
			defer h.asyncStarts.Done()
			if _, err := h.StartPartition(sh); err != nil {
				h.log.Warn("host: ReconcilePartitionTable: StartPartition failed",
					"shard", sh, "err", err)
				return
			}
			h.log.Info("host: ReconcilePartitionTable: started shard", "shard", sh)
		}(shardID)
	}
	for _, shardID := range notOwned {
		h.log.Warn("host: ReconcilePartitionTable: shard no longer locally owned; StopPartition deferred",
			"shard", shardID)
	}
}

// RunnerView is the small-interface view of a *PartitionRunner used by
// the Delivery server. Declared here so the delivery package can consume
// it without importing the heavy PartitionRunner type.
type RunnerView interface {
	IsLeader() bool
	Proposer() *RaftProposer
}

// PartitionRunner returns the small-interface view used by the Delivery
// server (it accepts a runner satisfying a narrow IsLeader + Proposer
// surface). Returns nil when shardID is not hosted on this node; callers
// must treat nil as "not leader" so the sender re-resolves via gossip.
func (h *Host) PartitionRunner(shardID uint64) RunnerView {
	r := h.Partition(shardID)
	if r == nil {
		return nil
	}
	return r
}

// MetadataRunnerView returns the small-interface view of shard 0's
// metadata runner used by the Delivery server. Returns nil when shard 0
// is not hosted on this node; callers map nil to "not leader" so the
// sender re-resolves via gossip. Distinct from MetadataRunner() — which
// returns the concrete *MetadataRunner — so the Delivery package can
// depend on the narrow interface only.
func (h *Host) MetadataRunnerView() RunnerView {
	r := h.MetadataRunner()
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

// Close stops every partition and the NodeHost. Idempotent and
// concurrency-safe: callers race here via the ctx-watcher goroutine
// installed by NewHost and an explicit Close from the test or
// pkg/reflow.Run shutdown path. The first call wins; subsequent calls
// observe the closed state under h.mu and return immediately.
func (h *Host) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()
	// Drain in-flight ReconcilePartitionTable spawns before tearing down —
	// a goroutine mid-StartPartition (between the partitions==nil guard
	// and NewSnapshotter) would otherwise race pebble.Open against
	// t.TempDir cleanup or nh.Close.
	h.asyncStarts.Wait()
	h.mu.Lock()
	partitions := h.partitions
	metadataRunners := h.metadataRunners
	hr := h.handlerRegistry
	h.partitions = nil
	h.metadataRunners = nil
	h.mu.Unlock()
	// Drain partitions before nilling handlerRegistry. onStepDown waits
	// for in-flight invoker sessions to exit, and those sessions read
	// h.handlerRegistry via openWireStream — clearing the field first
	// would race with the read.
	for _, p := range partitions {
		p.onStepDown()
	}
	for _, r := range metadataRunners {
		r.onStepDown()
	}
	h.mu.Lock()
	h.handlerRegistry = nil
	h.mu.Unlock()
	if hr != nil {
		_ = hr.Close()
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


// NumPartitionShards returns the routing modulus the host was started
// with. Quota and other cross-shard reconcilers iterate 1..N when fanning
// out reads.
func (h *Host) NumPartitionShards() uint64 { return h.cfg.NumPartitionShards }

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
// Multi-node clusters (len(Peers) > 1) use NodeHostID targets so
// dragonboat's gossip can resolve them to live RaftAddresses; solo
// deployments use the self RaftAddr because gossip is off.
func (h *Host) initialMembers() map[uint64]dragonboat.Target {
	if len(h.cfg.Peers) <= 1 {
		return map[uint64]dragonboat.Target{h.cfg.NodeID: dragonboat.Target(h.cfg.RaftAddr)}
	}
	out := make(map[uint64]dragonboat.Target, len(h.cfg.Peers))
	for _, p := range h.cfg.Peers {
		out[p.NodeID] = dragonboat.Target(p.resolvedNodeHostID())
	}
	return out
}

// startMembers returns the (initialMembers, join) pair passed to
// dragonboat StartOnDiskReplica. When HostConfig.JoinExisting is true the
// node is catching up to a Raft group that already has this ReplicaID in
// its configuration (added via the admin AddNode workflow), and dragonboat
// requires (nil, true). Bootstrap is (full peer set, false).
func (h *Host) startMembers() (map[uint64]dragonboat.Target, bool) {
	if h.cfg.JoinExisting {
		return nil, true
	}
	return h.initialMembers(), false
}

// PartitionLeaderHint returns the believed leader's NodeID for the given
// shard, sourced from dragonboat gossip (ShardView). Returns (0, false)
// when gossip is off (single-node) or no leader is known yet. The result
// is advisory — callers must still tolerate NotLeader responses from
// the Delivery RPC and re-resolve.
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
// has not yet propagated.
func (h *Host) NodeEndpoint(nodeID uint64) (string, bool) {
	meta, ok := h.lookupNodeHostMeta(nodeID)
	if !ok {
		return "", false
	}
	return meta.GetGrpcEndpoint(), meta.GetGrpcEndpoint() != ""
}

// NodeAdminEndpoint mirrors NodeEndpoint, returning the admin gRPC
// endpoint advertised by the peer's NodeHostMeta. Used by the SelfJoin
// boot path and the admin server's LeaderHint redirect to resolve the
// metadata leader's admin port via gossip.
func (h *Host) NodeAdminEndpoint(nodeID uint64) (string, bool) {
	meta, ok := h.lookupNodeHostMeta(nodeID)
	if !ok {
		return "", false
	}
	return meta.GetAdminEndpoint(), meta.GetAdminEndpoint() != ""
}

// lookupNodeHostMeta resolves nodeID → NodeHostID → gossip Meta blob,
// unmarshals into NodeHostMeta. Returns (nil, false) when gossip is off,
// the peer is unknown, or the Meta blob has not yet propagated.
func (h *Host) lookupNodeHostMeta(nodeID uint64) (*enginev1.NodeHostMeta, bool) {
	if h.nh == nil {
		return nil, false
	}
	reg, ok := h.nh.GetNodeHostRegistry()
	if !ok {
		return nil, false
	}
	nhID := h.nodeHostIDOf(nodeID)
	if nhID == "" {
		return nil, false
	}
	raw, ok := reg.GetMeta(nhID)
	if !ok {
		return nil, false
	}
	var meta enginev1.NodeHostMeta
	if err := proto.Unmarshal(raw, &meta); err != nil {
		return nil, false
	}
	return &meta, true
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
// reapLPTransferStagingDirs deletes subdirectories of <dataDir>.lpstage_in/
// and <dataDir>.lpstage_out/ whose transfer_id is not present in the
// corresponding LP-transfer table. Both sibling roots are best-effort:
// a missing root is not an error; a present-but-unreadable entry is
// logged and skipped so a bad permission doesn't block partition open.
func reapLPTransferStagingDirs(dataDir string, store storage.Store, log *slog.Logger) error {
	live := map[string]map[string]struct{}{
		dataDir + ".lpstage_out": {},
		dataDir + ".lpstage_in":  {},
	}
	freezes, err := (tables.LPFreezeTable{S: store}).List(context.Background())
	if err != nil {
		return fmt.Errorf("list lp-freezes: %w", err)
	}
	for _, e := range freezes {
		live[dataDir+".lpstage_out"][e.Row.GetTransferId()] = struct{}{}
	}
	stagings, err := (tables.LPStagingTable{S: store}).All(context.Background())
	if err != nil {
		return fmt.Errorf("list lp-staging: %w", err)
	}
	for _, r := range stagings {
		live[dataDir+".lpstage_in"][r.GetTransferId()] = struct{}{}
	}
	for root, keep := range live {
		entries, err := os.ReadDir(root)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			log.Warn("host: read staging root", "root", root, "err", err)
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if _, ok := keep[e.Name()]; ok {
				continue
			}
			path := filepath.Join(root, e.Name())
			if err := os.RemoveAll(path); err != nil {
				log.Warn("host: remove orphan staging dir", "path", path, "err", err)
				continue
			}
			log.Info("host: removed orphan lp-transfer staging dir", "path", path)
		}
	}
	return nil
}

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
