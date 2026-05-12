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

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"

	"github.com/twinfer/reflow/internal/engine/invoker"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// HostConfig configures a reflow node (a process hosting one or more
// partitions). Phase 1 single-node deployments use NodeID=1.
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
}

// Host owns the NodeHost and the per-partition runners.
type Host struct {
	cfg HostConfig
	nh  *dragonboat.NodeHost
	log *slog.Logger

	mu         sync.RWMutex
	partitions map[uint64]*PartitionRunner
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
	nh, err := dragonboat.NewNodeHost(nhConfig)
	if err != nil {
		return nil, fmt.Errorf("host: NewNodeHost: %w", err)
	}
	h.nh = nh
	return h, nil
}

// NodeHost returns the underlying dragonboat NodeHost. Exposed for tests and
// administrative tools (status queries, manual membership changes in later
// phases). Production code should prefer Host-provided methods.
func (h *Host) NodeHost() *dragonboat.NodeHost { return h.nh }

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
		return storage.OpenPebble(p, nil)
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

	initial := map[uint64]dragonboat.Target{h.cfg.NodeID: h.cfg.RaftAddr}
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
	h.partitions = nil
	h.mu.Unlock()
	for _, p := range partitions {
		p.onStepDown()
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
	r := l.host.Partition(info.ShardID)
	if r == nil {
		l.host.log.Warn("raftEventListener: no runner for shard", "shard", info.ShardID)
		return
	}
	r.leadership.OnRaftLeaderChange(info.LeaderID)
}
