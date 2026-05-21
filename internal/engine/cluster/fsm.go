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
	Close() error
}

// Config is the inert configuration for a metadata FSM instance.
type Config struct {
	Snapshotter SnapshotterRef
	Leadership  LeadershipObserver
	Log         *slog.Logger
	// OnPartitionTable, if non-nil, is invoked after each Update batch
	// commits when that batch contained an UpdatePartitionTable command.
	// The argument is the freshly applied table. Intended to drive
	// ownership-based shard start/stop on the metadata-leader's host.
	OnPartitionTable func(*enginev1.PartitionTable)
	// Notifiers carry per-table change signals into local subsystems
	// (event-source Reconciler, etc.). Each one fires at most once per
	// Update batch and only when an apply arm actually touched the
	// underlying table. Non-blocking; subscribers wake, SyncRead the
	// table, and converge local state.
	Notifiers Notifiers
}

// Notifiers groups the per-table TableNotifier handles the FSM signals
// after commit. Add a new field here when migrating another subsystem
// to shard 0; the apply arm for the new command flips it on via
// applyResult.notify.
type Notifiers struct {
	EventSourceTable   *TableNotifier
	WebhookSourceTable *TableNotifier
	SecretTable        *TableNotifier
	LPOwnersTable      *TableNotifier
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

// Result.Value sentinels surfaced by Update. The default Value (when no
// apply arm explicitly stamps one) is uint64(len(ent.Cmd)), preserved
// from the original FSM contract — callers that don't read it stay
// happy. CAS-aware proposers explicitly check for
// ResultValueFailedPrecondition to translate apply-time CAS rejection
// into connect.CodeFailedPrecondition.
const (
	// ResultValueFailedPrecondition signals that an apply arm refused to
	// mutate state because Envelope.precondition.if_table_revision_eq
	// did not match the current TableRevision singleton. The row is
	// untouched; applied_index still advances; no notifiers fire.
	ResultValueFailedPrecondition uint64 = 1
)

// applyResult is the return value of applyCommand. It carries the
// optional freshly-applied PartitionTable (for the OnPartitionTable
// callback) plus a bitmap-style set of TableNotifier handles the
// caller should Bump after batch.Commit succeeds. nil means "no
// observable side effect for callbacks" (still committed to disk —
// the row mutation already happened against the batch).
type applyResult struct {
	partitionTable *enginev1.PartitionTable
	notify         []*TableNotifier
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
	// notifySet dedups notifier pointers across entries in this batch:
	// multiple Upserts in one batch should still produce exactly one
	// post-commit Bump per touched table.
	var notifySet []*TableNotifier
	noteOnce := func(n *TableNotifier) {
		if n == nil {
			return
		}
		if slices.Contains(notifySet, n) {
			return
		}
		notifySet = append(notifySet, n)
	}

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
		// runCandidate). RegisterNode / UpdatePartitionTable are also
		// proposed as SelfProposal by the metadata leader, so dedup is
		// uniformly self-epoch + seq. The dedup table is not imported here:
		// there is no cross-shard ingress on shard 0 currently; admin CLI
		// proposals would require arbitrary-source dedup if introduced.

		applied, err := f.applyCommand(batch, &env, ent.Index)
		if err != nil {
			return nil, err
		}
		if applied != nil {
			if applied.partitionTable != nil {
				newTable = applied.partitionTable
			}
			for _, n := range applied.notify {
				noteOnce(n)
			}
		} else if env.GetPrecondition() != nil &&
			env.GetPrecondition().GetIfTableRevisionEq() != 0 {
			// applyCommand returned nil because precondition failed.
			// Surface the sentinel to the proposer; row state and
			// notifiers remain untouched. applied_index still advances
			// (entry was committed in Raft; the FSM just chose to no-op
			// the user-visible effect).
			entries[i].Result = statemachine.Result{Value: ResultValueFailedPrecondition}
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
	// Notifier fan-out runs on the FSM apply goroutine, post-commit,
	// non-blocking (TableNotifier.Bump drops when the buffer is full).
	// Subscribers wake on their own goroutines, SyncRead, and converge.
	for _, n := range notifySet {
		n.Bump()
	}
	return entries, nil
}

// applyCommand dispatches a single envelope. Returns:
//
//   - non-nil applyResult on success (carries the freshly-applied
//     PartitionTable when the variant was UpdatePartitionTable, plus
//     any per-table notifiers the apply arm wants Update to Bump
//     after commit).
//   - nil applyResult on no-op (unknown variants, missing required
//     fields, precondition failure). Callers distinguish "no-op
//     because of failed precondition" from "no-op because nothing
//     happened" by inspecting Envelope.precondition themselves.
//
// All table views are bound to the in-flight batch (which satisfies
// storage.Reader via pebble.IndexedBatch). This gives read-your-writes
// coherence across multi-entry apply batches — required because shard 0
// can apply (EvictNode → BeginRebalanceStep) or
// (BeginRebalanceStep → CompleteRebalanceStep) for the same shard in one
// batch under bootstrap / rebalance churn. Reading from the underlying
// store would miss the earlier entry's pending-step append and the
// Complete arm would log "no matching pending step" without progressing.
// Same bug class as the partition-heal stranding documented in
// partition.go:124-133.
func (f *FSM) applyCommand(
	batch storage.Batch,
	env *enginev1.Envelope,
	raftIndex uint64,
) (*applyResult, error) {
	cmd := env.GetCommand()
	switch k := cmd.GetKind().(type) {
	case *enginev1.Command_AnnounceLeader:
		f.cfg.Leadership.OnAnnounceLeader(k.AnnounceLeader)
		return &applyResult{}, nil
	case *enginev1.Command_RegisterNode:
		m := k.RegisterNode.GetMember()
		if m == nil || m.GetNodeId() == 0 {
			f.cfg.Log.Warn("cluster: RegisterNode missing member or NodeId; ignoring",
				"raft_index", raftIndex)
			return &applyResult{}, nil
		}
		if err := (MembershipTable{S: batch}).Put(batch, m); err != nil {
			return nil, fmt.Errorf("cluster: write membership: %w", err)
		}
		return &applyResult{}, nil
	case *enginev1.Command_UpdatePartitionTable:
		pt := k.UpdatePartitionTable.GetTable()
		if pt == nil {
			f.cfg.Log.Warn("cluster: UpdatePartitionTable missing table; ignoring",
				"raft_index", raftIndex)
			return &applyResult{}, nil
		}
		// Clone so the in-memory hook observes an isolated value.
		// UpdatePartitionTable is a full overwrite; proposers are
		// responsible for sending the complete desired state, including
		// MetaReplicas. The metadata-runner bootstrap reads existing
		// MetaReplicas off disk before proposing so re-runs on leader
		// gain don't wipe runtime-added members.
		applied := proto.Clone(pt).(*enginev1.PartitionTable)
		if err := (PartitionTableTable{S: batch}).Put(batch, applied); err != nil {
			return nil, fmt.Errorf("cluster: write partition table: %w", err)
		}
		return &applyResult{partitionTable: applied}, nil
	case *enginev1.Command_RegisterDeployment:
		rec := k.RegisterDeployment.GetRecord()
		if rec == nil || rec.GetId() == "" {
			f.cfg.Log.Warn("cluster: RegisterDeployment missing record or id; ignoring",
				"raft_index", raftIndex)
			return &applyResult{}, nil
		}
		// Upsert: re-registering the same id (e.g. metadata-leader
		// bootstrap re-runs on leader gain for an unchanged handler set)
		// just overwrites the row. Operators registering a remote
		// deployment with a new url should mint a fresh id, not reuse.
		if err := (DeploymentTable{S: batch}).Put(batch, rec); err != nil {
			return nil, fmt.Errorf("cluster: write deployment: %w", err)
		}
		// Maintain the (service, handler) → id index so ingress can
		// resolve an unpinned invocation to a deployment in O(1).
		// Newer registrations overwrite older ones; pinned invocations
		// continue to find their deployment via DeploymentTable.Get directly.
		idx := DeploymentIndexTable{S: batch}
		for _, h := range rec.GetHandlers() {
			if h.GetService() == "" || h.GetHandler() == "" {
				continue
			}
			if err := idx.Put(batch, h.GetService(), h.GetHandler(), rec.GetId()); err != nil {
				return nil, fmt.Errorf("cluster: write deployment index: %w", err)
			}
		}
		return &applyResult{}, nil
	case *enginev1.Command_UpsertEventSource:
		return f.applyUpsertEventSource(batch, env, k.UpsertEventSource, raftIndex)
	case *enginev1.Command_DeleteEventSource:
		return f.applyDeleteEventSource(batch, env, k.DeleteEventSource, raftIndex)
	case *enginev1.Command_UpsertWebhookSource:
		return f.applyUpsertWebhookSource(batch, env, k.UpsertWebhookSource, raftIndex)
	case *enginev1.Command_DeleteWebhookSource:
		return f.applyDeleteWebhookSource(batch, env, k.DeleteWebhookSource, raftIndex)
	case *enginev1.Command_UpsertSecret:
		return f.applyUpsertSecret(batch, env, k.UpsertSecret, raftIndex)
	case *enginev1.Command_DeleteSecret:
		return f.applyDeleteSecret(batch, env, k.DeleteSecret, raftIndex)
	case *enginev1.Command_UpsertLpOwner:
		return f.applyUpsertLPOwner(batch, env, k.UpsertLpOwner, raftIndex)
	case *enginev1.Command_DeleteLpOwner:
		return f.applyDeleteLPOwner(batch, env, k.DeleteLpOwner, raftIndex)
	case *enginev1.Command_BulkUpsertLpOwners:
		return f.applyBulkUpsertLPOwners(batch, env, k.BulkUpsertLpOwners, raftIndex)
	case *enginev1.Command_EvictNode:
		return f.applyEvictNode(batch, k.EvictNode, raftIndex)
	case *enginev1.Command_BeginRebalanceStep:
		return f.applyBeginRebalanceStep(batch, k.BeginRebalanceStep, raftIndex)
	case *enginev1.Command_CompleteRebalanceStep:
		return f.applyCompleteRebalanceStep(batch, k.CompleteRebalanceStep, raftIndex)
	case nil:
		f.cfg.Log.Warn("cluster: envelope has no command kind", "raft_index", raftIndex)
		return &applyResult{}, nil
	default:
		// Forward-compat: unknown variants log + no-op. NEVER returns error
		// — that would halt the shard (dragonboat disk.go:113).
		f.cfg.Log.Warn("cluster: unknown command kind; no-op",
			"raft_index", raftIndex, "kind", fmt.Sprintf("%T", k))
		return &applyResult{}, nil
	}
}

// checkPrecondition returns (true, nil) when the envelope's precondition
// is satisfied (or absent), (false, nil) when the precondition is set
// and the current TableRevision does not match (caller should bail out
// with a nil applyResult so Update stamps ResultValueFailedPrecondition),
// or (false, err) on a storage error.
func (f *FSM) checkPrecondition(batch storage.Batch, env *enginev1.Envelope, tableName string) (bool, error) {
	pre := env.GetPrecondition()
	if pre == nil || pre.GetIfTableRevisionEq() == 0 {
		return true, nil
	}
	cur, err := (RevisionTable{S: batch}).Get(tableName)
	if err != nil {
		return false, err
	}
	return cur == pre.GetIfTableRevisionEq(), nil
}

// applyUpsertEventSource writes the EventSourceRecord and bumps the
// table's CAS revision. Honors Envelope.precondition: on mismatch
// returns (nil, nil) so Update stamps ResultValueFailedPrecondition.
// Fires Notifiers.EventSourceTable post-commit via the returned slice.
func (f *FSM) applyUpsertEventSource(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpsertEventSource,
	raftIndex uint64,
) (*applyResult, error) {
	rec := cmd.GetRecord()
	if rec == nil || rec.GetName() == "" {
		f.cfg.Log.Warn("cluster: UpsertEventSource missing record or name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableEventSource)
	if err != nil {
		return nil, fmt.Errorf("cluster: load eventsrc revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (EventSourceTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write event source: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableEventSource, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump eventsrc revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.EventSourceTable}}, nil
}

// applyDeleteEventSource removes the named row (no-op if absent) and
// bumps the table revision. Same CAS semantics as Upsert. The revision
// bump happens even on delete-of-absent so the operator's CAS-roundtrip
// CLI observes that the proposal landed.
func (f *FSM) applyDeleteEventSource(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.DeleteEventSource,
	raftIndex uint64,
) (*applyResult, error) {
	name := cmd.GetName()
	if name == "" {
		f.cfg.Log.Warn("cluster: DeleteEventSource missing name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableEventSource)
	if err != nil {
		return nil, fmt.Errorf("cluster: load eventsrc revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (EventSourceTable{S: batch}).Delete(batch, name); err != nil {
		return nil, fmt.Errorf("cluster: delete event source: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableEventSource, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump eventsrc revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.EventSourceTable}}, nil
}

// applyUpsertWebhookSource writes the WebhookSourceRecord and bumps
// the table revision. CAS + notifier semantics mirror
// applyUpsertEventSource exactly.
func (f *FSM) applyUpsertWebhookSource(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpsertWebhookSource,
	raftIndex uint64,
) (*applyResult, error) {
	rec := cmd.GetRecord()
	if rec == nil || rec.GetName() == "" {
		f.cfg.Log.Warn("cluster: UpsertWebhookSource missing record or name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableWebhookSource)
	if err != nil {
		return nil, fmt.Errorf("cluster: load webhooksrc revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (WebhookSourceTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write webhook source: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableWebhookSource, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump webhooksrc revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.WebhookSourceTable}}, nil
}

// applyDeleteWebhookSource removes the named row (no-op if absent)
// and bumps the table revision. Same CAS semantics as Upsert.
func (f *FSM) applyDeleteWebhookSource(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.DeleteWebhookSource,
	raftIndex uint64,
) (*applyResult, error) {
	name := cmd.GetName()
	if name == "" {
		f.cfg.Log.Warn("cluster: DeleteWebhookSource missing name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableWebhookSource)
	if err != nil {
		return nil, fmt.Errorf("cluster: load webhooksrc revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (WebhookSourceTable{S: batch}).Delete(batch, name); err != nil {
		return nil, fmt.Errorf("cluster: delete webhook source: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableWebhookSource, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump webhooksrc revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.WebhookSourceTable}}, nil
}

// applyUpsertSecret writes the SecretRecord and bumps the table
// revision. CAS + notifier semantics mirror the event-source / webhook
// arms.
func (f *FSM) applyUpsertSecret(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpsertSecret,
	raftIndex uint64,
) (*applyResult, error) {
	rec := cmd.GetRecord()
	if rec == nil || rec.GetName() == "" {
		f.cfg.Log.Warn("cluster: UpsertSecret missing record or name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableSecret)
	if err != nil {
		return nil, fmt.Errorf("cluster: load secret revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (SecretTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write secret: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableSecret, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump secret revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.SecretTable}}, nil
}

// applyDeleteSecret removes the named row (no-op if absent) and bumps
// the table revision. Same CAS semantics as Upsert.
//
// Deliberately does NOT cascade-check consumer references (webhook rows
// that name this secret): such validation requires a cross-table scan
// on the apply path, which we want to avoid. Consumer reconcilers see
// the missing name on next reconcile and preserve-prev or log + skip.
func (f *FSM) applyDeleteSecret(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.DeleteSecret,
	raftIndex uint64,
) (*applyResult, error) {
	name := cmd.GetName()
	if name == "" {
		f.cfg.Log.Warn("cluster: DeleteSecret missing name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableSecret)
	if err != nil {
		return nil, fmt.Errorf("cluster: load secret revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (SecretTable{S: batch}).Delete(batch, name); err != nil {
		return nil, fmt.Errorf("cluster: delete secret: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableSecret, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump secret revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.SecretTable}}, nil
}

// applyUpsertLPOwner writes one LPOwnerRecord and bumps the table
// revision. CAS + notifier semantics mirror the event-source / webhook
// / secret arms. Reserved for the future per-LP transfer protocol
// (PR 3); PR 1's bootstrap seed uses applyBulkUpsertLPOwners instead.
func (f *FSM) applyUpsertLPOwner(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpsertLPOwner,
	raftIndex uint64,
) (*applyResult, error) {
	rec := cmd.GetRecord()
	if rec == nil || rec.GetShardId() == 0 {
		f.cfg.Log.Warn("cluster: UpsertLPOwner missing record or shard_id; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableLPOwners)
	if err != nil {
		return nil, fmt.Errorf("cluster: load lpowners revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (LPOwnersTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write lpowner: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableLPOwners, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump lpowners revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.LPOwnersTable}}, nil
}

// applyDeleteLPOwner removes the row for lp (no-op if absent) and bumps
// the table revision. Same CAS semantics as Upsert. Defensive; not used
// by PR 1 or PR 2.
func (f *FSM) applyDeleteLPOwner(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.DeleteLPOwner,
	raftIndex uint64,
) (*applyResult, error) {
	ok, err := f.checkPrecondition(batch, env, RevisionTableLPOwners)
	if err != nil {
		return nil, fmt.Errorf("cluster: load lpowners revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (LPOwnersTable{S: batch}).Delete(batch, cmd.GetLp()); err != nil {
		return nil, fmt.Errorf("cluster: delete lpowner: %w", err)
	}
	_ = raftIndex
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableLPOwners, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump lpowners revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.LPOwnersTable}}, nil
}

// applyBulkUpsertLPOwners writes every record in one batch and bumps
// the table revision exactly once. One notifier fan-out for the whole
// batch (subscribers will re-Snapshot the entire table on wake — there
// is no benefit to fanning out per-row). Used by the metadata-leader
// bootstrap to seed the identity assignment for all 4096 LPs.
func (f *FSM) applyBulkUpsertLPOwners(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.BulkUpsertLPOwners,
	raftIndex uint64,
) (*applyResult, error) {
	records := cmd.GetRecords()
	if len(records) == 0 {
		f.cfg.Log.Warn("cluster: BulkUpsertLPOwners empty records; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableLPOwners)
	if err != nil {
		return nil, fmt.Errorf("cluster: load lpowners revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	tab := LPOwnersTable{S: batch}
	for _, rec := range records {
		if rec == nil || rec.GetShardId() == 0 {
			f.cfg.Log.Warn("cluster: BulkUpsertLPOwners record missing or zero shard_id; skipping",
				"raft_index", raftIndex, "lp", rec.GetLp())
			continue
		}
		if err := tab.Put(batch, rec); err != nil {
			return nil, fmt.Errorf("cluster: write lpowner lp=%d: %w", rec.GetLp(), err)
		}
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableLPOwners, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump lpowners revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.LPOwnersTable}}, nil
}

// applyEvictNode marks the named node logically dead (last_seen_ms = 0)
// and appends a DELETE_REPLICA RebalanceStep to PartitionTable.pending
// for every shard whose ReplicaSet still contains node_id. Idempotent:
// re-applying for an already-evicted node is a no-op (the membership
// check below short-circuits).
func (f *FSM) applyEvictNode(
	batch storage.Batch,
	cmd *enginev1.EvictNode,
	raftIndex uint64,
) (*applyResult, error) {
	nodeID := cmd.GetNodeId()
	if nodeID == 0 {
		f.cfg.Log.Warn("cluster: EvictNode missing node_id; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	m, err := (MembershipTable{S: batch}).Get(nodeID)
	if err != nil {
		return nil, fmt.Errorf("cluster: load membership: %w", err)
	}
	if m == nil {
		f.cfg.Log.Warn("cluster: EvictNode for unknown node; ignoring",
			"raft_index", raftIndex, "node_id", nodeID)
		return &applyResult{}, nil
	}
	if m.GetLastSeenMs() == 0 {
		// Already evicted (last_seen_ms=0 is the eviction marker).
		return &applyResult{}, nil
	}
	m.LastSeenMs = 0
	if err := (MembershipTable{S: batch}).Put(batch, m); err != nil {
		return nil, fmt.Errorf("cluster: write membership: %w", err)
	}

	pt, err := (PartitionTableTable{S: batch}).Get()
	if err != nil {
		return nil, fmt.Errorf("cluster: load partition table: %w", err)
	}
	if pt == nil {
		// No partition table yet (pre-bootstrap eviction is meaningless).
		return &applyResult{}, nil
	}
	// Iterate shards in sorted order so the appended pending steps land in
	// the same byte sequence on every replica — required for Raft Apply
	// determinism (snapshot bytes diverge otherwise). Shard 0 is appended
	// last when the evicted node is also a metadata voter so the
	// resulting byte sequence stays canonical (partitions first, meta
	// second).
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
	if replicaSetContains(pt.GetMetaReplicas(), nodeID) {
		pt.Pending = append(pt.Pending, &enginev1.RebalanceStep{
			ShardId:      0,
			Kind:         enginev1.RebalanceStep_DELETE_REPLICA,
			RemoveNodeId: nodeID,
			StepId:       nextStepID(pt.GetPending(), 0),
		})
	}
	if err := (PartitionTableTable{S: batch}).Put(batch, pt); err != nil {
		return nil, fmt.Errorf("cluster: write partition table: %w", err)
	}
	return &applyResult{partitionTable: proto.Clone(pt).(*enginev1.PartitionTable)}, nil
}

// applyBeginRebalanceStep appends the requested step to
// PartitionTable.pending, unless an entry with the same (shard_id,
// step_id) already exists (idempotency on retry).
func (f *FSM) applyBeginRebalanceStep(
	batch storage.Batch,
	cmd *enginev1.BeginRebalanceStep,
	raftIndex uint64,
) (*applyResult, error) {
	step := cmd.GetStep()
	if step == nil || step.GetStepId() == 0 {
		f.cfg.Log.Warn("cluster: BeginRebalanceStep malformed; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	// shard_id=0 is the metadata Raft group itself — same step kinds,
	// applied against pt.meta_replicas instead of pt.shards[shard_id]
	// in applyCompleteRebalanceStep.
	pt, err := (PartitionTableTable{S: batch}).Get()
	if err != nil {
		return nil, fmt.Errorf("cluster: load partition table: %w", err)
	}
	if pt == nil {
		f.cfg.Log.Warn("cluster: BeginRebalanceStep before partition table bootstrap; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	for _, p := range pt.GetPending() {
		if p.GetShardId() == step.GetShardId() && p.GetStepId() == step.GetStepId() {
			// Already present — caller retried; drop silently.
			return &applyResult{}, nil
		}
	}
	pt.Pending = append(pt.Pending, proto.Clone(step).(*enginev1.RebalanceStep))
	if err := (PartitionTableTable{S: batch}).Put(batch, pt); err != nil {
		return nil, fmt.Errorf("cluster: write partition table: %w", err)
	}
	return &applyResult{partitionTable: proto.Clone(pt).(*enginev1.PartitionTable)}, nil
}

// applyCompleteRebalanceStep removes the matching pending entry and
// updates the relevant ReplicaSet (pt.shards[shard_id] for partitions,
// pt.meta_replicas for shard 0). ADD_NON_VOTING does not appear in any
// voting set so the entry is just popped. AssignmentEpoch bumps only on
// partition-shard completions — routing decisions don't depend on
// metadata-shard membership. Idempotent: if no entry matches, no-op.
func (f *FSM) applyCompleteRebalanceStep(
	batch storage.Batch,
	cmd *enginev1.CompleteRebalanceStep,
	raftIndex uint64,
) (*applyResult, error) {
	shardID := cmd.GetShardId()
	stepID := cmd.GetStepId()
	if stepID == 0 {
		f.cfg.Log.Warn("cluster: CompleteRebalanceStep malformed; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	pt, err := (PartitionTableTable{S: batch}).Get()
	if err != nil {
		return nil, fmt.Errorf("cluster: load partition table: %w", err)
	}
	if pt == nil {
		return &applyResult{}, nil
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
		return &applyResult{}, nil
	}
	pt.Pending = kept

	// Pick the voting set to mutate: shard 0 routes to MetaReplicas,
	// partition shards route to pt.Shards[shardID].
	targetRS, setTargetRS := pickRebalanceTarget(pt, shardID)
	switch matched.GetKind() {
	case enginev1.RebalanceStep_DELETE_REPLICA:
		if targetRS != nil {
			targetRS.NodeIds = removeNodeID(targetRS.NodeIds, matched.GetRemoveNodeId())
		}
	case enginev1.RebalanceStep_PROMOTE_TO_VOTER:
		if targetRS == nil {
			targetRS = &enginev1.ReplicaSet{}
			setTargetRS(targetRS)
		}
		if !replicaSetContains(targetRS, matched.GetAddNodeId()) {
			targetRS.NodeIds = append(targetRS.NodeIds, matched.GetAddNodeId())
		}
	case enginev1.RebalanceStep_ADD_NON_VOTING:
		// No ReplicaSet change — non-voting members are tracked by
		// dragonboat directly and are invisible to PartitionTable's
		// voting view.
	default:
		f.cfg.Log.Warn("cluster: CompleteRebalanceStep on unknown step kind",
			"raft_index", raftIndex, "kind", matched.GetKind())
	}
	// AssignmentEpoch tracks partition-ownership generation for routing
	// clients; metadata-shard membership doesn't affect routing.
	if shardID != 0 {
		pt.AssignmentEpoch++
	}

	if err := (PartitionTableTable{S: batch}).Put(batch, pt); err != nil {
		return nil, fmt.Errorf("cluster: write partition table: %w", err)
	}
	return &applyResult{partitionTable: proto.Clone(pt).(*enginev1.PartitionTable)}, nil
}

// pickRebalanceTarget returns the ReplicaSet to mutate for a step with
// the given shardID, plus a setter that lazily inserts a fresh ReplicaSet
// at that location (used when promoting into an empty set).
func pickRebalanceTarget(pt *enginev1.PartitionTable, shardID uint64) (*enginev1.ReplicaSet, func(*enginev1.ReplicaSet)) {
	if shardID == 0 {
		return pt.GetMetaReplicas(), func(rs *enginev1.ReplicaSet) { pt.MetaReplicas = rs }
	}
	return pt.GetShards()[shardID], func(rs *enginev1.ReplicaSet) {
		if pt.Shards == nil {
			pt.Shards = make(map[uint64]*enginev1.ReplicaSet)
		}
		pt.Shards[shardID] = rs
	}
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
	var highest uint64
	for _, p := range pending {
		if p.GetShardId() == shardID && p.GetStepId() > highest {
			highest = p.GetStepId()
		}
	}
	return highest + 1
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

	// LookupDeployment returns *enginev1.DeploymentRecord (or nil) for
	// the named id.
	LookupDeployment struct{ ID string }

	// LookupDeployments returns []*enginev1.DeploymentRecord sorted by
	// id (lex byte order).
	LookupDeployments struct{}

	// LookupDeploymentByHandler returns the deployment_id (string) that
	// (service, handler) currently routes to, or "" if no deployment
	// claims that handler. Resolves via the (service, handler) → id
	// index maintained by the RegisterDeployment apply arm.
	LookupDeploymentByHandler struct{ Service, Handler string }

	// LookupHandlerInfo returns *HandlerInfo (or nil) for (service, handler) —
	// both deployment_id and the handler's protocolv1.Kind (encoded as uint32
	// to avoid pulling protocolv1 into cluster). Combines the index read with
	// a record lookup; saves callers a second SyncRead.
	LookupHandlerInfo struct{ Service, Handler string }

	// LookupEventSources returns *EventSourceList — every
	// EventSourceRecord on shard 0 plus the table's CAS revision in one
	// SyncRead. The Reconciler calls this on each TableNotifier wake to
	// converge local dispatcher state.
	LookupEventSources struct{}

	// LookupWebhookSources returns *WebhookSourceList — every
	// WebhookSourceRecord on shard 0 plus the table's CAS revision in
	// one SyncRead. The Reconciler calls this on each TableNotifier wake.
	LookupWebhookSources struct{}

	// LookupSecrets returns *SecretList — every SecretRecord on shard 0
	// plus the table's CAS revision in one SyncRead. The SecretStore
	// Reconciler calls this on each TableNotifier wake.
	LookupSecrets struct{}

	// LookupLPOwners returns *LPOwnersList — every LPOwnerRecord on
	// shard 0 plus the table's CAS revision in one SyncRead. The
	// per-node routing Reconciler calls this on each TableNotifier wake.
	LookupLPOwners struct{}
)

// EventSourceList bundles every row in EventSourceTable with the
// table's CAS revision, atomic w.r.t. the read snapshot (single
// IndexedBatch view).
type EventSourceList struct {
	Sources       []*enginev1.EventSourceRecord
	TableRevision uint64
}

// WebhookSourceList bundles every row in WebhookSourceTable with the
// table's CAS revision, atomic w.r.t. the read snapshot.
type WebhookSourceList struct {
	Sources       []*enginev1.WebhookSourceRecord
	TableRevision uint64
}

// SecretList bundles every row in SecretTable with the table's CAS
// revision, atomic w.r.t. the read snapshot.
type SecretList struct {
	Records       []*enginev1.SecretRecord
	TableRevision uint64
}

// LPOwnersList bundles every row in LPOwnersTable with the table's CAS
// revision, atomic w.r.t. the read snapshot.
type LPOwnersList struct {
	Records       []*enginev1.LPOwnerRecord
	TableRevision uint64
}

// HandlerInfo is the result of LookupHandlerInfo — the (service, handler)
// tuple's current deployment_id plus the kind the deployment advertises
// for this handler. Kind is encoded as uint32 (protocolv1.Kind values) so
// cluster doesn't depend on protocolv1.
type HandlerInfo struct {
	DeploymentID string
	Kind         uint32
}

// Lookup implements statemachine.IOnDiskStateMachine.
func (f *FSM) Lookup(query any) (any, error) {
	store := f.cfg.Snapshotter.Store()
	if store == nil {
		return nil, errors.New("cluster: snapshotter has no current store")
	}
	switch q := query.(type) {
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
	case LookupDeployment:
		return (DeploymentTable{S: store}).Get(q.ID)
	case LookupDeployments:
		return (DeploymentTable{S: store}).List()
	case LookupDeploymentByHandler:
		return (DeploymentIndexTable{S: store}).Get(q.Service, q.Handler)
	case LookupHandlerInfo:
		id, err := (DeploymentIndexTable{S: store}).Get(q.Service, q.Handler)
		if err != nil {
			return nil, err
		}
		if id == "" {
			return (*HandlerInfo)(nil), nil
		}
		rec, err := (DeploymentTable{S: store}).Get(id)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			// Dangling index — the record was deleted but the index row
			// outlived it. Treat as miss; ingress maps to FailedPrecondition.
			return (*HandlerInfo)(nil), nil
		}
		var kind uint32
		for _, h := range rec.GetHandlers() {
			if h.GetService() == q.Service && h.GetHandler() == q.Handler {
				kind = h.GetKind()
				break
			}
		}
		return &HandlerInfo{DeploymentID: id, Kind: kind}, nil
	case LookupEventSources:
		sources, err := (EventSourceTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableEventSource)
		if err != nil {
			return nil, err
		}
		return &EventSourceList{Sources: sources, TableRevision: rev}, nil
	case LookupWebhookSources:
		sources, err := (WebhookSourceTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableWebhookSource)
		if err != nil {
			return nil, err
		}
		return &WebhookSourceList{Sources: sources, TableRevision: rev}, nil
	case LookupSecrets:
		records, err := (SecretTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableSecret)
		if err != nil {
			return nil, err
		}
		return &SecretList{Records: records, TableRevision: rev}, nil
	case LookupLPOwners:
		records, err := (LPOwnersTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableLPOwners)
		if err != nil {
			return nil, err
		}
		return &LPOwnersList{Records: records, TableRevision: rev}, nil
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

// Close releases the underlying snapshotter (and its Pebble store).
// Mirrors partition.Partition.Close: dragonboat calls this when the
// NodeHost shuts the FSM down, and without it the metadata shard's
// per-shard Pebble lock leaks for the lifetime of the process —
// blocking any in-process restart of the host.
func (f *FSM) Close() error {
	if f.cfg.Snapshotter != nil {
		return f.cfg.Snapshotter.Close()
	}
	return nil
}
