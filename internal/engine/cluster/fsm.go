package cluster

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
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
	PartitionTable      *TableNotifier
	SecretTable         *TableNotifier
	ModelTable          *TableNotifier
	LPOwnersTable       *TableNotifier
	LPTransfersTable    *TableNotifier
	RebalanceDrainTable *TableNotifier
	CARootTable         *TableNotifier
	JoinTokenTable      *TableNotifier
	PlatformConfigTable *TableNotifier
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

// applyResult is the return value of applyCommand. It carries a
// bitmap-style set of TableNotifier handles the caller should Bump
// after batch.Commit succeeds. nil means "no observable side effect
// for subscribers" (still committed to disk — the row mutation already
// happened against the batch).
type applyResult struct {
	notify []*TableNotifier
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
		// Clone so the persisted bytes are independent of the inbound
		// command (proposers may retain it). UpdatePartitionTable is a
		// full overwrite; proposers are responsible for sending the
		// complete desired state, including MetaReplicas. The
		// metadata-runner bootstrap reads existing MetaReplicas off disk
		// before proposing so re-runs on leader gain don't wipe
		// runtime-added members.
		applied := proto.Clone(pt).(*enginev1.PartitionTable)
		if err := (PartitionTableTable{S: batch}).Put(batch, applied); err != nil {
			return nil, fmt.Errorf("cluster: write partition table: %w", err)
		}
		return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.PartitionTable}}, nil
	case *enginev1.Command_RegisterDeployment:
		return f.applyRegisterDeployment(batch, env, k.RegisterDeployment, raftIndex)
	case *enginev1.Command_DeleteDeployment:
		return f.applyDeleteDeployment(batch, env, k.DeleteDeployment, raftIndex)
	case *enginev1.Command_UpsertPlatformConfig:
		return f.applyUpsertPlatformConfig(batch, env, k.UpsertPlatformConfig, raftIndex)
	case *enginev1.Command_UpsertSecret:
		return f.applyUpsertSecret(batch, env, k.UpsertSecret, raftIndex)
	case *enginev1.Command_DeleteSecret:
		return f.applyDeleteSecret(batch, env, k.DeleteSecret, raftIndex)
	case *enginev1.Command_UpsertModelSet:
		return f.applyUpsertModelSet(batch, env, k.UpsertModelSet, raftIndex)
	case *enginev1.Command_DeleteModel:
		return f.applyDeleteModel(batch, env, k.DeleteModel, raftIndex)
	case *enginev1.Command_UpsertLpOwner:
		return f.applyUpsertLPOwner(batch, env, k.UpsertLpOwner, raftIndex)
	case *enginev1.Command_BulkUpsertLpOwners:
		return f.applyBulkUpsertLPOwners(batch, env, k.BulkUpsertLpOwners, raftIndex)
	case *enginev1.Command_InitiateLpTransfer:
		return f.applyInitiateLPTransfer(batch, env, k.InitiateLpTransfer, raftIndex)
	case *enginev1.Command_UpdateLpTransferPhase:
		return f.applyUpdateLPTransferPhase(batch, env, k.UpdateLpTransferPhase, raftIndex)
	case *enginev1.Command_RemoveLpTransfer:
		return f.applyRemoveLPTransfer(batch, env, k.RemoveLpTransfer, raftIndex)
	case *enginev1.Command_EvictNode:
		return f.applyEvictNode(batch, k.EvictNode, raftIndex)
	case *enginev1.Command_BeginRebalanceStep:
		return f.applyBeginRebalanceStep(batch, k.BeginRebalanceStep, raftIndex)
	case *enginev1.Command_CompleteRebalanceStep:
		return f.applyCompleteRebalanceStep(batch, k.CompleteRebalanceStep, raftIndex)
	case *enginev1.Command_SetRebalanceDrain:
		return f.applySetRebalanceDrain(batch, env, k.SetRebalanceDrain, raftIndex)
	case *enginev1.Command_UpsertCaRoot:
		return f.applyUpsertCARoot(batch, env, k.UpsertCaRoot, raftIndex)
	case *enginev1.Command_DeleteCaRoot:
		return f.applyDeleteCARoot(batch, env, k.DeleteCaRoot, raftIndex)
	case *enginev1.Command_UpsertJoinToken:
		return f.applyUpsertJoinToken(batch, env, k.UpsertJoinToken, raftIndex)
	case *enginev1.Command_ConsumeJoinToken:
		return f.applyConsumeJoinToken(batch, env, k.ConsumeJoinToken, raftIndex)
	case *enginev1.Command_DeleteJoinToken:
		return f.applyDeleteJoinToken(batch, env, k.DeleteJoinToken, raftIndex)
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

// applyRegisterDeployment writes the DeploymentRecord and maintains the
// (service, handler) → id index, then bumps RevisionTableDeployment.
// Honors Envelope.precondition (CAS off when if_table_revision_eq=0).
// Idempotent re-registers (e.g. metadata-leader bootstrap re-runs on
// leader gain for an unchanged handler set) just overwrite the row.
// Operators registering a remote deployment at a new URL should mint a
// fresh id, not reuse.
func (f *FSM) applyRegisterDeployment(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.RegisterDeployment,
	raftIndex uint64,
) (*applyResult, error) {
	rec := cmd.GetRecord()
	if rec == nil || rec.GetId() == "" {
		f.cfg.Log.Warn("cluster: RegisterDeployment missing record or id; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableDeployment)
	if err != nil {
		return nil, fmt.Errorf("cluster: load deployment revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (DeploymentTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write deployment: %w", err)
	}
	// Maintain the (service, handler) → id index so ingress can resolve
	// an unpinned invocation to a deployment in O(1). Newer registrations
	// overwrite older ones; pinned invocations still find their record
	// via DeploymentTable.Get directly.
	idx := DeploymentIndexTable{S: batch}
	for _, h := range rec.GetHandlers() {
		if h.GetService() == "" || h.GetHandler() == "" {
			continue
		}
		if err := idx.Put(batch, h.GetService(), h.GetHandler(), rec.GetId()); err != nil {
			return nil, fmt.Errorf("cluster: write deployment index: %w", err)
		}
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableDeployment, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump deployment revision: %w", err)
	}
	return &applyResult{}, nil
}

// applyDeleteDeployment removes the named DeploymentRecord and evicts
// any DeploymentIndexTable rows that still point to this id. Other
// deployments may have taken over a (service, handler) since this row
// was written, so we only delete index entries whose current value
// matches the deleted id.
func (f *FSM) applyDeleteDeployment(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.DeleteDeployment,
	raftIndex uint64,
) (*applyResult, error) {
	id := cmd.GetId()
	if id == "" {
		f.cfg.Log.Warn("cluster: DeleteDeployment missing id; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableDeployment)
	if err != nil {
		return nil, fmt.Errorf("cluster: load deployment revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	// Load first so we can find the (service, handler) pairs to evict
	// from the index. Read-your-writes against the in-flight batch.
	rec, err := (DeploymentTable{S: batch}).Get(id)
	if err != nil {
		return nil, fmt.Errorf("cluster: load deployment for delete: %w", err)
	}
	if err := (DeploymentTable{S: batch}).Delete(batch, id); err != nil {
		return nil, fmt.Errorf("cluster: delete deployment: %w", err)
	}
	if rec != nil {
		idx := DeploymentIndexTable{S: batch}
		for _, h := range rec.GetHandlers() {
			if h.GetService() == "" || h.GetHandler() == "" {
				continue
			}
			cur, err := idx.Get(h.GetService(), h.GetHandler())
			if err != nil {
				return nil, fmt.Errorf("cluster: load deployment index: %w", err)
			}
			if cur != id {
				continue
			}
			if err := idx.Delete(batch, h.GetService(), h.GetHandler()); err != nil {
				return nil, fmt.Errorf("cluster: delete deployment index: %w", err)
			}
		}
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableDeployment, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump deployment revision: %w", err)
	}
	return &applyResult{}, nil
}

// applyUpsertPlatformConfig replaces the PlatformConfigRecord singleton and
// bumps the table's CAS revision. Honors Envelope.precondition: on mismatch
// returns (nil, nil) so Update stamps ResultValueFailedPrecondition. Fires
// Notifiers.PlatformConfigTable post-commit so each node's authz Reconciler
// recompiles the live Cedar policy set.
func (f *FSM) applyUpsertPlatformConfig(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpsertPlatformConfig,
	raftIndex uint64,
) (*applyResult, error) {
	rec := cmd.GetRecord()
	if rec == nil {
		f.cfg.Log.Warn("cluster: UpsertPlatformConfig missing record; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTablePlatformConfig)
	if err != nil {
		return nil, fmt.Errorf("cluster: load platformconfig revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (PlatformConfigTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write platform config: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTablePlatformConfig, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump platformconfig revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.PlatformConfigTable}}, nil
}

// applyUpsertSecret writes the SecretRecord and bumps the table
// revision. CAS + notifier semantics mirror the other shard-0 config
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

// applyUpsertModelSet writes N ModelRecords (a model + its dependency closure)
// atomically: one CAS check against RevisionTableModel, N Puts into the in-flight
// batch, one revision Bump, one ModelTable notifier. All-or-nothing via the
// single per-apply batch.Commit. Mirrors applyBulkUpsertLPOwners. Malformed
// records are skipped (warn-and-continue, per the apply-path contract); a genuine
// storage failure halts the shard.
func (f *FSM) applyUpsertModelSet(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpsertModelSet,
	raftIndex uint64,
) (*applyResult, error) {
	records := cmd.GetRecords()
	if len(records) == 0 {
		f.cfg.Log.Warn("cluster: UpsertModelSet with no records; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableModel)
	if err != nil {
		return nil, fmt.Errorf("cluster: load model revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	wrote := false
	for _, rec := range records {
		ref := rec.GetModelRef()
		if ref.GetKind() == "" || ref.GetName() == "" {
			f.cfg.Log.Warn("cluster: UpsertModelSet record missing model_ref kind/name; skipping",
				"raft_index", raftIndex)
			continue
		}
		rec.RegisteredAtMs = nowMs // deterministic stamp from the proposer's header clock
		if err := (ModelTable{S: batch}).Put(batch, rec); err != nil {
			return nil, fmt.Errorf("cluster: write model %s/%s/%s: %w",
				ref.GetKind(), ref.GetName(), ref.GetVersion(), err)
		}
		wrote = true
	}
	if !wrote {
		// Every record was malformed; nothing written, so don't bump or notify.
		return &applyResult{}, nil
	}
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableModel, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump model revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.ModelTable}}, nil
}

func (f *FSM) applyDeleteModel(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.DeleteModel,
	raftIndex uint64,
) (*applyResult, error) {
	ref := cmd.GetModelRef()
	if ref.GetKind() == "" || ref.GetName() == "" {
		f.cfg.Log.Warn("cluster: DeleteModel missing model_ref kind/name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableModel)
	if err != nil {
		return nil, fmt.Errorf("cluster: load model revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (ModelTable{S: batch}).Delete(batch, ref.GetKind(), ref.GetName(), ref.GetVersion()); err != nil {
		return nil, fmt.Errorf("cluster: delete model: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableModel, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump model revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.ModelTable}}, nil
}

// applyUpsertCARoot writes the CARootRecord and bumps the table
// revision. CAS + notifier semantics mirror the secret arm. The signing
// key referenced by record.key_secret_name is not validated on the
// apply path; per-node ClusterIssuer Reconcilers surface missing-key
// failures via secretstore metrics and preserve their in-memory active
// CA snapshot.
func (f *FSM) applyUpsertCARoot(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpsertCARoot,
	raftIndex uint64,
) (*applyResult, error) {
	rec := cmd.GetRecord()
	if rec == nil || rec.GetName() == "" {
		f.cfg.Log.Warn("cluster: UpsertCARoot missing record or name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableCARoot)
	if err != nil {
		return nil, fmt.Errorf("cluster: load caroot revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (CARootTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write caroot: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableCARoot, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump caroot revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.CARootTable}}, nil
}

// applyDeleteCARoot removes the named row (no-op if absent) and bumps
// the table revision. Same CAS semantics as Upsert. Deleting the active
// CA breaks renewal on the next pass; the operator-facing CLI gates on
// --force in its higher layer.
func (f *FSM) applyDeleteCARoot(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.DeleteCARoot,
	raftIndex uint64,
) (*applyResult, error) {
	name := cmd.GetName()
	if name == "" {
		f.cfg.Log.Warn("cluster: DeleteCARoot missing name; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableCARoot)
	if err != nil {
		return nil, fmt.Errorf("cluster: load caroot revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (CARootTable{S: batch}).Delete(batch, name); err != nil {
		return nil, fmt.Errorf("cluster: delete caroot: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableCARoot, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump caroot revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.CARootTable}}, nil
}

// applyUpsertJoinToken writes the JoinTokenRecord and bumps the table
// revision. CAS + notifier semantics mirror the CARoot arm. The token
// plaintext never traverses the FSM; the proposer ships sha256 already.
func (f *FSM) applyUpsertJoinToken(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpsertJoinToken,
	raftIndex uint64,
) (*applyResult, error) {
	rec := cmd.GetRecord()
	if rec == nil || len(rec.GetTokenHash()) == 0 {
		f.cfg.Log.Warn("cluster: UpsertJoinToken missing record or token_hash; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableJoinToken)
	if err != nil {
		return nil, fmt.Errorf("cluster: load jointoken revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (JoinTokenTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write jointoken: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableJoinToken, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump jointoken revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.JoinTokenTable}}, nil
}

// applyConsumeJoinToken atomically marks a single_use token as consumed.
// Beyond the standard CAS guard on Envelope.precondition, the apply arm
// also rejects (via the same ResultValueFailedPrecondition sentinel) when
// the row is absent, already used, or expired — so the bootstrap server's
// SignCSR call can treat the proposer's success as proof that the token
// was both valid and is now spent.
func (f *FSM) applyConsumeJoinToken(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.ConsumeJoinToken,
	raftIndex uint64,
) (*applyResult, error) {
	hash := cmd.GetTokenHash()
	if len(hash) == 0 {
		f.cfg.Log.Warn("cluster: ConsumeJoinToken missing token_hash; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableJoinToken)
	if err != nil {
		return nil, fmt.Errorf("cluster: load jointoken revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	tbl := JoinTokenTable{S: batch}
	rec, err := tbl.Get(hash)
	if err != nil {
		return nil, fmt.Errorf("cluster: read jointoken: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if rec == nil {
		f.cfg.Log.Info("cluster: ConsumeJoinToken target absent",
			"raft_index", raftIndex)
		return nil, nil
	}
	if rec.GetSingleUse() && rec.GetUsed() {
		f.cfg.Log.Info("cluster: ConsumeJoinToken already-used token",
			"raft_index", raftIndex)
		return nil, nil
	}
	if exp := rec.GetExpiryMs(); exp != 0 && uint64(nowMs) >= exp {
		f.cfg.Log.Info("cluster: ConsumeJoinToken expired",
			"raft_index", raftIndex, "expiry_ms", exp, "now_ms", nowMs)
		return nil, nil
	}
	if rec.GetSingleUse() {
		rec.Used = true
		if err := tbl.Put(batch, rec); err != nil {
			return nil, fmt.Errorf("cluster: mark jointoken used: %w", err)
		}
	}
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableJoinToken, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump jointoken revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.JoinTokenTable}}, nil
}

// applyDeleteJoinToken removes the named row (no-op if absent) and bumps
// the table revision. Same CAS semantics as Upsert.
func (f *FSM) applyDeleteJoinToken(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.DeleteJoinToken,
	raftIndex uint64,
) (*applyResult, error) {
	hash := cmd.GetTokenHash()
	if len(hash) == 0 {
		f.cfg.Log.Warn("cluster: DeleteJoinToken missing token_hash; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableJoinToken)
	if err != nil {
		return nil, fmt.Errorf("cluster: load jointoken revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (JoinTokenTable{S: batch}).Delete(batch, hash); err != nil {
		return nil, fmt.Errorf("cluster: delete jointoken: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableJoinToken, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump jointoken revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.JoinTokenTable}}, nil
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

// applyBulkUpsertLPOwners writes every record in one batch and bumps
// the table revision exactly once. One notifier fan-out for the whole
// batch (subscribers will re-Snapshot the entire table on wake — there
// is no benefit to fanning out per-row). Used by the metadata-leader
// bootstrap to seed the consistent-hash assignment for all 4096 LPs.
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

// applyInitiateLPTransfer writes a fresh LPTransferRecord at PHASE_INIT.
// Validates lp ∈ [0, LPCount), dest_shard ∈ PartitionTable.Shards, and
// rejects when a non-terminal transfer for the same lp already exists.
// Resolves source_shard from the LPOwnersTable; stamps
// expected_lpowners_revision from the current LPOwnersTable revision so
// the lpMover's later UpsertLpOwner CAS detects concurrent ownership
// drift.
//
// CAS via Envelope.precondition against RevisionTableLPTransfers
// (concurrent admin retries against a stale revision are rejected).
// Fires Notifiers.LPTransfersTable post-commit so the lpMover wakes
// immediately on the new row.
func (f *FSM) applyInitiateLPTransfer(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.InitiateLPTransfer,
	raftIndex uint64,
) (*applyResult, error) {
	if cmd.GetTransferId() == "" {
		f.cfg.Log.Warn("cluster: InitiateLPTransfer missing transfer_id; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	if cmd.GetLp() >= keys.LPCount {
		f.cfg.Log.Warn("cluster: InitiateLPTransfer lp out of range; ignoring",
			"raft_index", raftIndex, "lp", cmd.GetLp(), "lp_count", keys.LPCount)
		return &applyResult{}, nil
	}
	if cmd.GetDestShard() == 0 {
		f.cfg.Log.Warn("cluster: InitiateLPTransfer zero dest_shard; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	pt, err := (PartitionTableTable{S: batch}).Get()
	if err != nil {
		return nil, fmt.Errorf("cluster: load partition table: %w", err)
	}
	if pt == nil {
		f.cfg.Log.Warn("cluster: InitiateLPTransfer before partition table bootstrap; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	if _, ok := pt.GetShards()[cmd.GetDestShard()]; !ok {
		f.cfg.Log.Warn("cluster: InitiateLPTransfer unknown dest_shard; ignoring",
			"raft_index", raftIndex, "dest_shard", cmd.GetDestShard())
		return &applyResult{}, nil
	}
	owner, err := (LPOwnersTable{S: batch}).Get(cmd.GetLp())
	if err != nil {
		return nil, fmt.Errorf("cluster: load lpowner: %w", err)
	}
	if owner == nil {
		f.cfg.Log.Warn("cluster: InitiateLPTransfer for unseeded lp; ignoring",
			"raft_index", raftIndex, "lp", cmd.GetLp())
		return &applyResult{}, nil
	}
	if owner.GetShardId() == cmd.GetDestShard() {
		f.cfg.Log.Warn("cluster: InitiateLPTransfer lp already on dest_shard; ignoring",
			"raft_index", raftIndex, "lp", cmd.GetLp(), "dest_shard", cmd.GetDestShard())
		return &applyResult{}, nil
	}
	// Reject if any non-terminal transfer exists for this lp. Terminal
	// phases (CLEANED, ABORTED) are observability-only and may coexist
	// with a fresh initiation.
	existing, err := (LPTransferTable{S: batch}).List()
	if err != nil {
		return nil, fmt.Errorf("cluster: load lp transfers: %w", err)
	}
	for _, rec := range existing {
		if rec.GetLp() != cmd.GetLp() {
			continue
		}
		switch rec.GetPhase() {
		case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED,
			enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTED:
			// terminal — ignore
		default:
			f.cfg.Log.Warn("cluster: InitiateLPTransfer rejected, in-progress transfer for lp",
				"raft_index", raftIndex, "lp", cmd.GetLp(),
				"existing_transfer_id", rec.GetTransferId(), "phase", rec.GetPhase().String())
			return &applyResult{}, nil
		}
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableLPTransfers)
	if err != nil {
		return nil, fmt.Errorf("cluster: load lp transfers revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	lpownersRev, err := (RevisionTable{S: batch}).Get(RevisionTableLPOwners)
	if err != nil {
		return nil, fmt.Errorf("cluster: load lpowners revision: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	rec := &enginev1.LPTransferRecord{
		TransferId:               cmd.GetTransferId(),
		Lp:                       cmd.GetLp(),
		SourceShard:              owner.GetShardId(),
		DestShard:                cmd.GetDestShard(),
		Phase:                    enginev1.LPTransferPhase_LP_TRANSFER_PHASE_INIT,
		StartedAtMs:              nowMs,
		LastEventMs:              nowMs,
		ExpectedLpownersRevision: lpownersRev,
	}
	if err := (LPTransferTable{S: batch}).Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write lp transfer: %w", err)
	}
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableLPTransfers, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump lp transfers revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.LPTransfersTable}}, nil
}

// applyUpdateLPTransferPhase advances a transfer's phase. The lpMover
// proposes this after each side-effect (cross-shard send, CAS) lands.
// Updates last_event_ms (stall-detection clock) and optionally
// expected_lpowners_revision (re-stamped on FLIPPED so a resumed
// lpMover knows the revision to verify against).
func (f *FSM) applyUpdateLPTransferPhase(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.UpdateLPTransferPhase,
	raftIndex uint64,
) (*applyResult, error) {
	if cmd.GetTransferId() == "" {
		f.cfg.Log.Warn("cluster: UpdateLPTransferPhase missing transfer_id; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	tab := LPTransferTable{S: batch}
	rec, err := tab.Get(cmd.GetTransferId())
	if err != nil {
		return nil, fmt.Errorf("cluster: load lp transfer: %w", err)
	}
	if rec == nil {
		f.cfg.Log.Warn("cluster: UpdateLPTransferPhase for unknown transfer_id; ignoring",
			"raft_index", raftIndex, "transfer_id", cmd.GetTransferId())
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableLPTransfers)
	if err != nil {
		return nil, fmt.Errorf("cluster: load lp transfers revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	// Monotonic phase advance. The saga DAG is
	//   INIT → STAGED → FLIPPED → CLEANED
	//   (INIT|STAGED) → ABORTING → ABORTED
	// Same-phase is an idempotent no-op (absorbs duplicate acks from
	// retries). Invalid transitions log + drop, never error (would halt
	// the shard).
	if cmd.GetPhase() == rec.GetPhase() {
		return &applyResult{}, nil
	}
	if !isValidLPTransferAdvance(rec.GetPhase(), cmd.GetPhase()) {
		f.cfg.Log.Warn("cluster: UpdateLPTransferPhase invalid transition; dropping",
			"raft_index", raftIndex, "transfer_id", cmd.GetTransferId(),
			"from", rec.GetPhase().String(), "to", cmd.GetPhase().String())
		return &applyResult{}, nil
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	rec.Phase = cmd.GetPhase()
	rec.LastEventMs = nowMs
	if cmd.GetExpectedLpownersRevision() != 0 {
		rec.ExpectedLpownersRevision = cmd.GetExpectedLpownersRevision()
	}
	if err := tab.Put(batch, rec); err != nil {
		return nil, fmt.Errorf("cluster: write lp transfer: %w", err)
	}
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableLPTransfers, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump lp transfers revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.LPTransfersTable}}, nil
}

// isValidLPTransferAdvance reports whether (from → to) is a forward
// advance in the LP-transfer saga DAG. Used by applyUpdateLPTransferPhase
// to absorb duplicate / stale acks safely.
func isValidLPTransferAdvance(from, to enginev1.LPTransferPhase) bool {
	switch from {
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_UNSPECIFIED,
		enginev1.LPTransferPhase_LP_TRANSFER_PHASE_INIT,
		enginev1.LPTransferPhase_LP_TRANSFER_PHASE_SHIPPING:
		return to == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_SHIPPING ||
			to == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED ||
			to == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTING
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED:
		return to == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_FLIPPED ||
			to == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTING
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_FLIPPED:
		return to == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED
	case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTING:
		return to == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTED
	}
	return false
}

// applyRemoveLPTransfer drops a row after the operator-visibility grace
// window has elapsed. Delete-of-absent is a no-op (still bumps the
// revision so the operator's CAS-roundtrip CLI observes progress).
func (f *FSM) applyRemoveLPTransfer(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.RemoveLPTransfer,
	raftIndex uint64,
) (*applyResult, error) {
	if cmd.GetTransferId() == "" {
		f.cfg.Log.Warn("cluster: RemoveLPTransfer missing transfer_id; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableLPTransfers)
	if err != nil {
		return nil, fmt.Errorf("cluster: load lp transfers revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	if err := (LPTransferTable{S: batch}).Delete(batch, cmd.GetTransferId()); err != nil {
		return nil, fmt.Errorf("cluster: delete lp transfer: %w", err)
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableLPTransfers, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump lp transfers revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.LPTransfersTable}}, nil
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
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.PartitionTable}}, nil
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
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.PartitionTable}}, nil
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
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.PartitionTable}}, nil
}

// applySetRebalanceDrain writes (drain=true) or removes (drain=false) a
// RebalanceDrainRecord row for the given shard_id and bumps the table
// revision. CAS via Envelope.precondition against
// RevisionTableRebalanceDrain. Fires Notifiers.RebalanceDrainTable
// post-commit so the autonomous rebalancer wakes immediately on the
// change. shard_id == 0 is rejected (shard 0 is the metadata group and
// is never an LP owner).
//
// Both add and remove bump the revision even when the underlying row
// is already in the requested state — the bump is what makes a CAS
// roundtrip observable for the operator CLI.
func (f *FSM) applySetRebalanceDrain(
	batch storage.Batch,
	env *enginev1.Envelope,
	cmd *enginev1.SetRebalanceDrain,
	raftIndex uint64,
) (*applyResult, error) {
	shardID := cmd.GetShardId()
	if shardID == 0 {
		f.cfg.Log.Warn("cluster: SetRebalanceDrain zero shard_id; ignoring",
			"raft_index", raftIndex)
		return &applyResult{}, nil
	}
	ok, err := f.checkPrecondition(batch, env, RevisionTableRebalanceDrain)
	if err != nil {
		return nil, fmt.Errorf("cluster: load rebalance_drain revision: %w", err)
	}
	if !ok {
		return nil, nil
	}
	nowMs := env.GetHeader().GetCreatedAtMs()
	tab := RebalanceDrainTable{S: batch}
	if cmd.GetDrain() {
		rec := &enginev1.RebalanceDrainRecord{
			ShardId:   shardID,
			AddedAtMs: nowMs,
		}
		if err := tab.Put(batch, rec); err != nil {
			return nil, fmt.Errorf("cluster: write rebalance_drain: %w", err)
		}
	} else {
		if err := tab.Delete(batch, shardID); err != nil {
			return nil, fmt.Errorf("cluster: delete rebalance_drain: %w", err)
		}
	}
	if _, err := (RevisionTable{S: batch}).Bump(batch, RevisionTableRebalanceDrain, nowMs); err != nil {
		return nil, fmt.Errorf("cluster: bump rebalance_drain revision: %w", err)
	}
	return &applyResult{notify: []*TableNotifier{f.cfg.Notifiers.RebalanceDrainTable}}, nil
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

	// LookupDeploymentList returns *DeploymentList — every
	// DeploymentRecord on shard 0 plus RevisionTableDeployment, atomic
	// w.r.t. the read snapshot. Used by the Config ListDeployments RPC
	// so operator CAS-roundtrip flows pick up the revision in the same
	// SyncRead that produced the list.
	LookupDeploymentList struct{}

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

	// LookupSecrets returns *SecretList — every SecretRecord on shard 0
	// plus the table's CAS revision in one SyncRead. The SecretStore
	// Reconciler calls this on each TableNotifier wake.
	LookupSecrets struct{}

	// LookupModels returns *ModelList — every ModelRecord on shard 0 plus
	// the table's CAS revision in one SyncRead. The per-node processengine
	// TableResolver calls this on each ModelTable notifier wake.
	LookupModels struct{}

	// LookupPlatformConfig returns *PlatformConfigResult — the
	// PlatformConfigRecord singleton (nil Record when unset) plus the
	// platform-config table's CAS revision. The authz Reconciler calls this
	// on each PlatformConfigTable notifier wake to recompile the policy set.
	LookupPlatformConfig struct{}

	// LookupLPOwners returns *LPOwnersList — every LPOwnerRecord on
	// shard 0 plus the table's CAS revision in one SyncRead. The
	// per-node routing Reconciler calls this on each TableNotifier wake.
	LookupLPOwners struct{}

	// LookupLPTransfers returns *LPTransfersList — every
	// LPTransferRecord (in-progress + recently terminal) plus the
	// table's CAS revision. The lpMover calls this on each tick to
	// drive open transfers forward; the admin RPC uses it for
	// operator-facing list.
	LookupLPTransfers struct{}

	// LookupRebalanceDrains returns *RebalanceDrainList — every
	// RebalanceDrainRecord plus the table's CAS revision. The
	// autonomous rebalancer's advisor calls this on each tick to
	// subtract drained shards from the planner's input.
	LookupRebalanceDrains struct{}

	// LookupCARoots returns *CARootList — every CARootRecord on shard 0
	// plus the table's CAS revision in one SyncRead. The per-node
	// certmgr.ClusterIssuer calls this on each TableNotifier wake.
	LookupCARoots struct{}

	// LookupJoinTokens returns *JoinTokenList — every JoinTokenRecord on
	// shard 0 plus the table's CAS revision in one SyncRead. The
	// bootstrap server calls this to locate a redeemed token by hash on
	// each MeshSign call; admin RPC List paths read the same shape.
	LookupJoinTokens struct{}
)

// DeploymentList bundles every row in DeploymentTable with the table's
// CAS revision, atomic w.r.t. the read snapshot.
type DeploymentList struct {
	Records       []*enginev1.DeploymentRecord
	TableRevision uint64
}

// PlatformConfigResult bundles the PlatformConfigRecord singleton (nil Record
// when unset) with the platform-config table's CAS revision.
type PlatformConfigResult struct {
	Record        *enginev1.PlatformConfigRecord
	TableRevision uint64
}

// SecretList bundles every row in SecretTable with the table's CAS
// revision, atomic w.r.t. the read snapshot.
type SecretList struct {
	Records       []*enginev1.SecretRecord
	TableRevision uint64
}

// ModelList bundles every row in ModelTable with the table's CAS revision,
// atomic w.r.t. the read snapshot.
type ModelList struct {
	Records       []*enginev1.ModelRecord
	TableRevision uint64
}

// CARootList bundles every row in CARootTable with the table's CAS
// revision, atomic w.r.t. the read snapshot.
type CARootList struct {
	Records       []*enginev1.CARootRecord
	TableRevision uint64
}

// JoinTokenList bundles every row in JoinTokenTable with the table's
// CAS revision, atomic w.r.t. the read snapshot.
type JoinTokenList struct {
	Records       []*enginev1.JoinTokenRecord
	TableRevision uint64
}

// LPOwnersList bundles every row in LPOwnersTable with the table's CAS
// revision, atomic w.r.t. the read snapshot.
type LPOwnersList struct {
	Records       []*enginev1.LPOwnerRecord
	TableRevision uint64
}

// LPTransfersList bundles every row in LPTransferTable with the
// table's CAS revision.
type LPTransfersList struct {
	Records       []*enginev1.LPTransferRecord
	TableRevision uint64
}

// RebalanceDrainList bundles every row in RebalanceDrainTable with the
// table's CAS revision, atomic w.r.t. the read snapshot.
type RebalanceDrainList struct {
	Records       []*enginev1.RebalanceDrainRecord
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
	case LookupDeploymentList:
		records, err := (DeploymentTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableDeployment)
		if err != nil {
			return nil, err
		}
		return &DeploymentList{Records: records, TableRevision: rev}, nil
	case LookupPlatformConfig:
		rec, err := (PlatformConfigTable{S: store}).Get()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTablePlatformConfig)
		if err != nil {
			return nil, err
		}
		return &PlatformConfigResult{Record: rec, TableRevision: rev}, nil
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
	case LookupModels:
		records, err := (ModelTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableModel)
		if err != nil {
			return nil, err
		}
		return &ModelList{Records: records, TableRevision: rev}, nil
	case LookupCARoots:
		records, err := (CARootTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableCARoot)
		if err != nil {
			return nil, err
		}
		return &CARootList{Records: records, TableRevision: rev}, nil
	case LookupJoinTokens:
		records, err := (JoinTokenTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableJoinToken)
		if err != nil {
			return nil, err
		}
		return &JoinTokenList{Records: records, TableRevision: rev}, nil
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
	case LookupLPTransfers:
		records, err := (LPTransferTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableLPTransfers)
		if err != nil {
			return nil, err
		}
		return &LPTransfersList{Records: records, TableRevision: rev}, nil
	case LookupRebalanceDrains:
		records, err := (RebalanceDrainTable{S: store}).List()
		if err != nil {
			return nil, err
		}
		rev, err := (RevisionTable{S: store}).Get(RevisionTableRebalanceDrain)
		if err != nil {
			return nil, err
		}
		return &RebalanceDrainList{Records: records, TableRevision: rev}, nil
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
