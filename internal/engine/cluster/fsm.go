package cluster

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

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
