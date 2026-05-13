package cluster

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// LeadershipObserver is the subset of engine.Leadership behavior the
// metadata FSM uses. Implemented by *engine.Leadership; redeclared here to
// avoid pulling the engine package into cluster (cluster sits below engine
// in the dependency graph).
type LeadershipObserver interface {
	IsLeader() bool
	OnAnnounceLeader(cmd *enginev1.AnnounceLeader)
}

// SnapshotterRef is the subset of *engine.Snapshotter the FSM consumes. It
// gives the FSM a handle on the current storage.Store across snapshot
// recovery without depending on engine.
type SnapshotterRef interface {
	Store() storage.Store
	SaveSnapshot(w io.Writer) error
	RecoverFromSnapshot(r io.Reader) error
}

// Config is the inert configuration for a metadata FSM instance.
type Config struct {
	Snapshotter SnapshotterRef
	Leadership  LeadershipObserver
	Log         *slog.Logger
	// OnBecomeLeaderTable, if non-nil, is invoked after each Update batch
	// commits whenever the metadata shard's PartitionTable has been
	// observed for the first time on this replica. Phase 4.2 will use
	// this to drive ownership-driven shard start/stop. Phase 4.1 it is
	// unused except by tests.
	OnPartitionTable func(*enginev1.PartitionTable)
}

// FSM is the dragonboat IOnDiskStateMachine for shard 0. It accepts only
// AnnounceLeader, RegisterNode, and UpdatePartitionTable commands; every
// other variant is logged and dropped (forward-compat).
type FSM struct {
	shardID   uint64
	replicaID uint64
	cfg       Config
}

// New constructs an FSM. In production the factory closure (see Factory)
// is what dragonboat calls; this constructor exists for direct unit tests.
func New(shardID, replicaID uint64, cfg Config) *FSM {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &FSM{shardID: shardID, replicaID: replicaID, cfg: cfg}
}

// Factory returns a dragonboat-compatible factory closure.
func (cfg Config) Factory() statemachine.CreateOnDiskStateMachineFunc {
	return func(shardID, replicaID uint64) statemachine.IOnDiskStateMachine {
		return New(shardID, replicaID, cfg)
	}
}

// Open returns the highest Raft index already applied to the on-disk store.
func (f *FSM) Open(_ <-chan struct{}) (uint64, error) {
	store := f.cfg.Snapshotter.Store()
	if store == nil {
		return 0, errors.New("cluster: snapshotter has no current store")
	}
	m, err := (MetaTable{S: store}).Get()
	if err != nil {
		return 0, fmt.Errorf("cluster: read applied_index: %w", err)
	}
	return m.GetAppliedIndex(), nil
}

// Update applies a batch of committed Raft entries.
//
// Same shape as engine.Partition.Update: one Pebble batch per call,
// applied-index bumped atomically with side effects. Unknown command
// variants are warned-and-dropped (returning error halts the shard).
func (f *FSM) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	store := f.cfg.Snapshotter.Store()
	if store == nil {
		return nil, errors.New("cluster: snapshotter has no current store")
	}
	batch := store.NewBatch()
	defer batch.Close()

	metaT := MetaTable{S: store}
	meta, err := metaT.Get()
	if err != nil {
		return nil, fmt.Errorf("cluster: load meta: %w", err)
	}

	var newTable *enginev1.PartitionTable

	for i, ent := range entries {
		entries[i].Result = statemachine.Result{Value: uint64(len(ent.Cmd))}

		var env enginev1.Envelope
		if err := proto.Unmarshal(ent.Cmd, &env); err != nil {
			f.cfg.Log.Warn("cluster: malformed envelope; advancing applied index only",
				"raft_index", ent.Index, "err", err)
			meta.AppliedIndex = ent.Index
			continue
		}

		// Shard 0 accepts only SelfProposal dedup (AnnounceLeader from
		// runCandidate). RegisterNode / UpdatePartitionTable in Phase 4.1
		// are also proposed as SelfProposal by the metadata leader, so
		// dedup is uniformly self-epoch + seq. We don't import the dedup
		// table here: there's no cross-shard ingress on shard 0 in 4.1.
		// Phase 4.2 introduces arbitrary-source proposals (admin CLI).

		applied, err := f.applyCommand(batch, &env, ent.Index, store)
		if err != nil {
			return nil, err
		}
		if applied != nil {
			newTable = applied
		}
		meta.AppliedIndex = ent.Index
	}

	if err := metaT.Put(batch, meta); err != nil {
		return nil, fmt.Errorf("cluster: write meta: %w", err)
	}
	if err := batch.Commit(true); err != nil {
		return nil, fmt.Errorf("cluster: commit batch: %w", err)
	}

	if newTable != nil && f.cfg.OnPartitionTable != nil {
		f.cfg.OnPartitionTable(newTable)
	}
	return entries, nil
}

// applyCommand dispatches a single envelope. Returns the freshly-applied
// PartitionTable when the variant was UpdatePartitionTable, nil otherwise.
// This lets Update notify the OnPartitionTable hook AFTER the batch
// commits (so observers can read durable state).
func (f *FSM) applyCommand(
	batch storage.Batch,
	env *enginev1.Envelope,
	raftIndex uint64,
	store storage.Store,
) (*enginev1.PartitionTable, error) {
	cmd := env.GetCommand()
	switch k := cmd.GetKind().(type) {
	case *enginev1.Command_AnnounceLeader:
		f.cfg.Leadership.OnAnnounceLeader(k.AnnounceLeader)
		return nil, nil
	case *enginev1.Command_RegisterNode:
		m := k.RegisterNode.GetMember()
		if m == nil || m.GetNodeId() == 0 {
			f.cfg.Log.Warn("cluster: RegisterNode missing member or NodeId; ignoring",
				"raft_index", raftIndex)
			return nil, nil
		}
		if err := (MembershipTable{S: store}).Put(batch, m); err != nil {
			return nil, fmt.Errorf("cluster: write membership: %w", err)
		}
		return nil, nil
	case *enginev1.Command_UpdatePartitionTable:
		pt := k.UpdatePartitionTable.GetTable()
		if pt == nil {
			f.cfg.Log.Warn("cluster: UpdatePartitionTable missing table; ignoring",
				"raft_index", raftIndex)
			return nil, nil
		}
		// Clone so the in-memory hook observes an isolated value.
		applied := proto.Clone(pt).(*enginev1.PartitionTable)
		if err := (PartitionTableTable{S: store}).Put(batch, applied); err != nil {
			return nil, fmt.Errorf("cluster: write partition table: %w", err)
		}
		return applied, nil
	case *enginev1.Command_EvictNode:
		return f.applyEvictNode(batch, store, k.EvictNode, raftIndex)
	case *enginev1.Command_BeginRebalanceStep:
		return f.applyBeginRebalanceStep(batch, store, k.BeginRebalanceStep, raftIndex)
	case *enginev1.Command_CompleteRebalanceStep:
		return f.applyCompleteRebalanceStep(batch, store, k.CompleteRebalanceStep, raftIndex)
	case nil:
		f.cfg.Log.Warn("cluster: envelope has no command kind", "raft_index", raftIndex)
		return nil, nil
	default:
		// Forward-compat: unknown variants log + no-op. NEVER returns error
		// — that would halt the shard (dragonboat disk.go:113).
		f.cfg.Log.Warn("cluster: unknown command kind; no-op",
			"raft_index", raftIndex, "kind", fmt.Sprintf("%T", k))
		return nil, nil
	}
}

// applyEvictNode marks the named node logically dead (last_seen_ms = 0)
// and appends a DELETE_REPLICA RebalanceStep to PartitionTable.pending
// for every shard whose ReplicaSet still contains node_id. Idempotent:
// re-applying for an already-evicted node is a no-op (the membership
// check below short-circuits).
func (f *FSM) applyEvictNode(
	batch storage.Batch,
	store storage.Store,
	cmd *enginev1.EvictNode,
	raftIndex uint64,
) (*enginev1.PartitionTable, error) {
	nodeID := cmd.GetNodeId()
	if nodeID == 0 {
		f.cfg.Log.Warn("cluster: EvictNode missing node_id; ignoring",
			"raft_index", raftIndex)
		return nil, nil
	}
	m, err := (MembershipTable{S: store}).Get(nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: load membership: %w", err)
	}
	if m == nil {
		f.cfg.Log.Warn("cluster: EvictNode for unknown node; ignoring",
			"raft_index", raftIndex, "node_id", nodeID)
		return nil, nil
	}
	if m.GetLastSeenMs() == 0 {
		// Already evicted (last_seen_ms=0 is the eviction marker).
		return nil, nil
	}
	m.LastSeenMs = 0
	if err := (MembershipTable{S: store}).Put(batch, m); err != nil {
		return nil, fmt.Errorf("cluster: write membership: %w", err)
	}

	pt, err := (PartitionTableTable{S: store}).Get()
	if err != nil {
		return nil, fmt.Errorf("cluster: load partition table: %w", err)
	}
	if pt == nil {
		// No partition table yet (pre-bootstrap eviction is meaningless).
		return nil, nil
	}
	// Iterate shards in sorted order so the appended pending steps land in
	// the same byte sequence on every replica — required for Raft Apply
	// determinism (snapshot bytes diverge otherwise).
	shards := pt.GetShards()
	shardIDs := make([]uint64, 0, len(shards))
	for shardID := range shards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)
	for _, shardID := range shardIDs {
		rs := shards[shardID]
		if !replicaSetContains(rs, nodeID) {
			continue
		}
		step := &enginev1.RebalanceStep{
			ShardId:      shardID,
			Kind:         enginev1.RebalanceStep_DELETE_REPLICA,
			RemoveNodeId: nodeID,
			StepId:       nextStepID(pt.GetPending(), shardID),
		}
		pt.Pending = append(pt.Pending, step)
	}
	if err := (PartitionTableTable{S: store}).Put(batch, pt); err != nil {
		return nil, fmt.Errorf("cluster: write partition table: %w", err)
	}
	return proto.Clone(pt).(*enginev1.PartitionTable), nil
}

// applyBeginRebalanceStep appends the requested step to
// PartitionTable.pending, unless an entry with the same (shard_id,
// step_id) already exists (idempotency on retry).
func (f *FSM) applyBeginRebalanceStep(
	batch storage.Batch,
	store storage.Store,
	cmd *enginev1.BeginRebalanceStep,
	raftIndex uint64,
) (*enginev1.PartitionTable, error) {
	step := cmd.GetStep()
	if step == nil || step.GetShardId() == 0 || step.GetStepId() == 0 {
		f.cfg.Log.Warn("cluster: BeginRebalanceStep malformed; ignoring",
			"raft_index", raftIndex)
		return nil, nil
	}
	pt, err := (PartitionTableTable{S: store}).Get()
	if err != nil {
		return nil, fmt.Errorf("cluster: load partition table: %w", err)
	}
	if pt == nil {
		f.cfg.Log.Warn("cluster: BeginRebalanceStep before partition table bootstrap; ignoring",
			"raft_index", raftIndex)
		return nil, nil
	}
	for _, p := range pt.GetPending() {
		if p.GetShardId() == step.GetShardId() && p.GetStepId() == step.GetStepId() {
			// Already present — caller retried; drop silently.
			return nil, nil
		}
	}
	pt.Pending = append(pt.Pending, proto.Clone(step).(*enginev1.RebalanceStep))
	if err := (PartitionTableTable{S: store}).Put(batch, pt); err != nil {
		return nil, fmt.Errorf("cluster: write partition table: %w", err)
	}
	return proto.Clone(pt).(*enginev1.PartitionTable), nil
}

// applyCompleteRebalanceStep removes the matching pending entry, bumps
// assignment_epoch, and (for DELETE_REPLICA / PROMOTE_TO_VOTER) updates
// the relevant ReplicaSet. ADD_NON_VOTING does not appear in the
// voting set so the ReplicaSet is untouched; the entry is just popped.
// Idempotent: if no entry matches, no-op.
func (f *FSM) applyCompleteRebalanceStep(
	batch storage.Batch,
	store storage.Store,
	cmd *enginev1.CompleteRebalanceStep,
	raftIndex uint64,
) (*enginev1.PartitionTable, error) {
	shardID := cmd.GetShardId()
	stepID := cmd.GetStepId()
	if shardID == 0 || stepID == 0 {
		f.cfg.Log.Warn("cluster: CompleteRebalanceStep malformed; ignoring",
			"raft_index", raftIndex)
		return nil, nil
	}
	pt, err := (PartitionTableTable{S: store}).Get()
	if err != nil {
		return nil, fmt.Errorf("cluster: load partition table: %w", err)
	}
	if pt == nil {
		return nil, nil
	}
	var matched *enginev1.RebalanceStep
	kept := pt.Pending[:0]
	for _, p := range pt.GetPending() {
		if matched == nil && p.GetShardId() == shardID && p.GetStepId() == stepID {
			matched = p
			continue
		}
		kept = append(kept, p)
	}
	if matched == nil {
		// Already completed on a prior apply, or never proposed; no-op.
		return nil, nil
	}
	pt.Pending = kept

	switch matched.GetKind() {
	case enginev1.RebalanceStep_DELETE_REPLICA:
		if rs := pt.GetShards()[shardID]; rs != nil {
			rs.NodeIds = removeNodeID(rs.NodeIds, matched.GetRemoveNodeId())
		}
	case enginev1.RebalanceStep_PROMOTE_TO_VOTER:
		rs := pt.GetShards()[shardID]
		if rs == nil {
			rs = &enginev1.ReplicaSet{}
			if pt.Shards == nil {
				pt.Shards = make(map[uint64]*enginev1.ReplicaSet)
			}
			pt.Shards[shardID] = rs
		}
		if !replicaSetContains(rs, matched.GetAddNodeId()) {
			rs.NodeIds = append(rs.NodeIds, matched.GetAddNodeId())
		}
	case enginev1.RebalanceStep_ADD_NON_VOTING:
		// No ReplicaSet change — non-voting members are tracked by
		// dragonboat directly and are invisible to PartitionTable's
		// voting view.
	default:
		f.cfg.Log.Warn("cluster: CompleteRebalanceStep on unknown step kind",
			"raft_index", raftIndex, "kind", matched.GetKind())
	}
	pt.AssignmentEpoch++

	if err := (PartitionTableTable{S: store}).Put(batch, pt); err != nil {
		return nil, fmt.Errorf("cluster: write partition table: %w", err)
	}
	return proto.Clone(pt).(*enginev1.PartitionTable), nil
}

// replicaSetContains reports whether nodeID is present in rs.
func replicaSetContains(rs *enginev1.ReplicaSet, nodeID uint64) bool {
	return slices.Contains(rs.GetNodeIds(), nodeID)
}

// removeNodeID returns ids with the first occurrence of nodeID removed.
func removeNodeID(ids []uint64, nodeID uint64) []uint64 {
	for i, id := range ids {
		if id == nodeID {
			return append(ids[:i], ids[i+1:]...)
		}
	}
	return ids
}

// nextStepID is the smallest step_id that does not appear in the
// pending list for the requested shard. The pending list is bounded by
// active rebalances so the scan is cheap.
func nextStepID(pending []*enginev1.RebalanceStep, shardID uint64) uint64 {
	var max uint64
	for _, p := range pending {
		if p.GetShardId() == shardID && p.GetStepId() > max {
			max = p.GetStepId()
		}
	}
	return max + 1
}

// Lookup query types. Each query returns a typed result.
type (
	// LookupPartitionTable returns the persisted *enginev1.PartitionTable
	// or nil if none has been written.
	LookupPartitionTable struct{}

	// LookupMembership returns []*enginev1.NodeMembership sorted by NodeID.
	LookupMembership struct{}

	// LookupAppliedIndex returns uint64.
	LookupAppliedIndex struct{}
)

// Lookup implements statemachine.IOnDiskStateMachine.
func (f *FSM) Lookup(query any) (any, error) {
	store := f.cfg.Snapshotter.Store()
	if store == nil {
		return nil, errors.New("cluster: snapshotter has no current store")
	}
	switch query.(type) {
	case LookupPartitionTable:
		return (PartitionTableTable{S: store}).Get()
	case LookupMembership:
		return (MembershipTable{S: store}).List()
	case LookupAppliedIndex:
		m, err := (MetaTable{S: store}).Get()
		if err != nil {
			return nil, err
		}
		return m.GetAppliedIndex(), nil
	default:
		return nil, fmt.Errorf("cluster: unknown lookup type %T", query)
	}
}

// Sync flushes pending writes (no-op when Commit was called with sync=true).
func (f *FSM) Sync() error {
	store := f.cfg.Snapshotter.Store()
	if store == nil {
		return nil
	}
	return store.Flush()
}

// PrepareSnapshot returns nil — Pebble Checkpoint is itself the cookie.
func (f *FSM) PrepareSnapshot() (any, error) { return nil, nil }

// SaveSnapshot writes a tar of a fresh Pebble checkpoint to w.
func (f *FSM) SaveSnapshot(_ any, w io.Writer, _ <-chan struct{}) error {
	return f.cfg.Snapshotter.SaveSnapshot(w)
}

// RecoverFromSnapshot replaces the on-disk state with the snapshot stream.
func (f *FSM) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	return f.cfg.Snapshotter.RecoverFromSnapshot(r)
}

// Close releases FSM-owned resources. The snapshotter's underlying store is
// managed by the host, not closed here.
func (f *FSM) Close() error { return nil }
