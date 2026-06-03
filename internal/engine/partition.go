package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"time"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/limits"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/observability"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// LeadershipObserver is the subset of leadership behavior the FSM needs. It
// is intentionally narrow; the concrete *Leadership implements it and is
// wired by Host.StartPartition.
type LeadershipObserver interface {
	IsLeader() bool
	OnAnnounceLeader(cmd *enginev1.AnnounceLeader)
}

// PartitionConfig is the inert state needed by a Partition. The shardID and
// replicaID are supplied by dragonboat when the factory closure is called.
type PartitionConfig struct {
	Snapshotter *Snapshotter
	Leadership  LeadershipObserver
	Collector   *ActionCollector
	Log         *slog.Logger
	// OnActions, if non-nil, is invoked after each Update batch commits with
	// the actions accumulated on the leader. It runs inline on the
	// dragonboat apply goroutine — MUST NOT block on a Raft propose.
	OnActions func([]Action)
	// Partitioner maps a partition key to a destination shard id. Used to
	// stamp destination_shard_id on every outbox row the apply path
	// produces. Zero value (NumShards=0) yields same-shard for everything,
	// preserving single-partition behavior for single-node deployments.
	Partitioner routing.Partitioner

	// Metrics, when non-nil, is observed on every applied command:
	// ApplyTotal (kind, is_leader), ApplyDurationMs (kind), DedupHits,
	// JournalAppended (entry). Safe to leave nil — every observation is
	// guarded.
	Metrics *observability.Metrics

	// OnSnapshotPersisted, when non-nil, is invoked after a successful
	// SaveSnapshot. It runs inline on the dragonboat snapshot goroutine
	// and MUST NOT block — the intended pattern is a non-blocking send to
	// a buffered-1 trigger channel consumed by the snapshot producer,
	// allowing opportunistic archiving on real snapshot events.
	OnSnapshotPersisted func()
}

// Result.Value sentinels surfaced by Update. The default Value (when no
// apply arm explicitly stamps one) is uint64(len(ent.Cmd)), preserved
// from the original FSM contract — callers that don't read it stay
// happy. The LP-freeze gate stamps ResultValueLPFrozen when a transfer
// is in progress; the leader proposer translates it back to a typed
// rejection so callers can retry against the LP's new owner.
const (
	// ResultValueLPFrozen signals that an apply arm refused to mutate
	// state because the LP belonging to this command is frozen for an
	// in-progress cross-shard LP transfer (PR 3). The row is untouched;
	// applied_index still advances; no actions are emitted.
	ResultValueLPFrozen uint64 = 2
)

// errLPFrozen is the sentinel returned by LP-touching apply arms when
// the LP is frozen for an in-progress LP transfer. Update recognizes
// it, stamps ResultValueLPFrozen on the entry, and continues — does
// NOT return it to dragonboat (which would halt the shard).
var errLPFrozen = errors.New("partition: LP frozen for transfer")

// Partition is the dragonboat IOnDiskStateMachine for one reflow partition.
//
// Important contract notes (dragonboat v4 statemachine/disk.go):
//   - Update returning an error halts the shard. Logical/unknown command bugs
//     MUST be logged-and-continued, never returned.
//   - Update is serial; Lookup and SaveSnapshot may run concurrently.
//   - Open MUST return the highest applied Raft index from storage.
type Partition struct {
	shardID   uint64
	replicaID uint64
	cfg       PartitionConfig
}

// NewPartition constructs a Partition. In production the caller invokes
// Factory and dragonboat calls back with (shardID, replicaID); tests can use
// this directly with shardID=0/replicaID=0.
func NewPartition(shardID, replicaID uint64, cfg PartitionConfig) *Partition {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Collector == nil {
		cfg.Collector = &ActionCollector{}
	}
	return &Partition{shardID: shardID, replicaID: replicaID, cfg: cfg}
}

// Factory returns a dragonboat-compatible factory closure that constructs the
// Partition once shardID/replicaID are known.
func (cfg PartitionConfig) Factory() statemachine.CreateOnDiskStateMachineFunc {
	return func(shardID, replicaID uint64) statemachine.IOnDiskStateMachine {
		return NewPartition(shardID, replicaID, cfg)
	}
}

// Open returns the highest Raft index already applied to the on-disk store.
// dragonboat statemachine/disk.go:56-69.
func (p *Partition) Open(_ <-chan struct{}) (uint64, error) {
	store := p.cfg.Snapshotter.Store()
	if store == nil {
		return 0, errors.New("partition: snapshotter has no current store")
	}
	m, err := (tables.MetaTable{S: store}).Get()
	if err != nil {
		return 0, fmt.Errorf("partition: read applied_index: %w", err)
	}
	return m.GetAppliedIndex(), nil
}

// Update applies a batch of committed Raft entries.
//
// All mutations land in a single storage.Batch that is committed at the end.
// The applied-index is bumped atomically with the side-effects. Actions are
// pushed to the collector only when this node is the partition leader (mirrors
// the is_leader gate at restate state_machine/mod.rs:312-313).
func (p *Partition) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {
	store := p.cfg.Snapshotter.Store()
	if store == nil {
		return nil, errors.New("partition: snapshotter has no current store")
	}
	batch := store.NewBatch()
	defer batch.Close()

	// Bind tables to the BATCH (not the store) so reads within this
	// Update see the writes earlier entries in the same batch made.
	// Required because a single dragonboat Update may carry multiple
	// raft entries with read-after-write dependencies (e.g. under
	// partition-heal catch-up: Ingress → JournalAppend → Complete for
	// one invocation in a single batch). Without this, the JE/Complete
	// reads would see the row as Free and the FSM would reject the
	// transition, stranding the invocation in Scheduled/Invoked.
	// storage.Batch satisfies storage.Reader; indexed-batch semantics
	// give read-your-writes coherence.
	inv := tables.InvocationTable{S: batch}
	journal := tables.JournalTable{S: batch}
	timers := tables.TimerTable{S: batch}
	dedup := tables.DedupTable{S: batch}
	metaT := tables.MetaTable{S: batch}

	meta, err := metaT.Get()
	if err != nil {
		return nil, fmt.Errorf("partition: load meta: %w", err)
	}

	// isLeader is sampled once per batch by design — every entry in the
	// batch sees the same is_leader gate. AnnounceLeader handled inside
	// this batch updates Leadership state for the *next* batch; this
	// matches restate's apply pipeline (state_machine/mod.rs:312-313).
	// If batching ever shrinks to one entry per Update call the choice
	// becomes moot.
	isLeader := p.cfg.Leadership.IsLeader()
	if !isLeader {
		// Followers replay deterministically but emit no real side effects;
		// drop anything previously buffered.
		p.cfg.Collector.Clear()
	}

	for i, ent := range entries {
		entries[i].Result = statemachine.Result{Value: uint64(len(ent.Cmd))}

		var env enginev1.Envelope
		if err := proto.Unmarshal(ent.Cmd, &env); err != nil {
			p.cfg.Log.Warn("partition: malformed envelope; advancing applied index only",
				"raft_index", ent.Index, "err", err)
			meta.AppliedIndex = ent.Index
			continue
		}

		// Dedup. SelfProposal entries from older leader epochs are rejected
		// here (mirrors restate deduplication_table/mod.rs:90-137).
		// Arbitrary dedup is LP-prefixed so it rides the LP-transfer scan;
		// lpFromCommand derives the LP from the command kind so the row
		// follows the LP across shard moves.
		envLP := lpFromCommand(env.GetCommand())
		if d := env.GetHeader().GetDedup(); d != nil {
			dup, err := dedup.IsDuplicate(envLP, d)
			if err != nil {
				return nil, fmt.Errorf("partition: dedup check: %w", err)
			}
			if dup {
				if p.cfg.Metrics != nil {
					p.cfg.Metrics.DedupHits.Inc()
				}
				p.cfg.Log.Debug("partition: duplicate command skipped",
					"raft_index", ent.Index, "dedup", dedupString(d))
				meta.AppliedIndex = ent.Index
				continue
			}
		}

		kind := commandKindLabel(env.GetCommand())
		var applyStart time.Time
		if p.cfg.Metrics != nil {
			applyStart = time.Now()
		}
		applyErr := p.applyCommand(batch, store, &env, ent.Index, meta, inv, journal, timers, isLeader)
		if applyErr != nil {
			if errors.Is(applyErr, errLPFrozen) {
				// LP-freeze rejection: the apply arm short-circuited
				// at the freeze gate before any batch mutation or
				// action push, so no rollback is needed. Stamp the
				// sentinel, bump applied_index, continue.
				entries[i].Result = statemachine.Result{Value: ResultValueLPFrozen}
				meta.AppliedIndex = ent.Index
				if p.cfg.Metrics != nil {
					p.cfg.Metrics.ApplyTotal.WithLabelValues(kind, leaderLabel(isLeader)).Inc()
				}
				continue
			}
			return nil, applyErr
		}
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.ApplyTotal.WithLabelValues(kind, leaderLabel(isLeader)).Inc()
			p.cfg.Metrics.ApplyDurationMs.WithLabelValues(kind).Observe(
				float64(time.Since(applyStart).Microseconds()) / 1000.0,
			)
		}

		// Outbox-source bookkeeping: when a command was re-injected by an
		// outbox shuffler (Arbitrary dedup with "outbox/" producer), the
		// dispatch lifecycle depends on whether sender and receiver are the
		// same shard.
		//
		//   - Same-shard outbox: pop the local row in the same batch so
		//     apply + pop are atomic.
		//   - Cross-shard outbox: the producer's row lives on a different
		//     shard's OutboxTable, so we cannot pop it here. Instead we
		//     enqueue an OutboxAck on the local outbox addressed back to
		//     the producer shard; the ack flows over Raft (same Delivery
		//     gRPC pipeline) and pops the producer-side row on apply.
		//
		// OutboxAck commands themselves are excluded from the
		// ack-on-receive path: applying an ack already pops the producer
		// row in onOutboxAck, and we must not generate acks-for-acks
		// (would loop). The same applies if we ever see a cross-shard
		// envelope whose payload is an OutboxAck (defensive guard).
		if d := env.GetHeader().GetDedup(); d != nil {
			if arb := d.GetArbitrary(); arb != nil && isOutboxProducer(arb.GetProducerId()) {
				senderShard, senderOK := parseOutboxProducerShard(arb.GetProducerId())
				switch {
				case !senderOK:
					p.cfg.Log.Warn("partition: malformed outbox producer id; cannot route ack",
						"producer", arb.GetProducerId())
				case senderShard == p.shardID:
					outboxT := tables.OutboxTable{S: batch}
					if err := outboxT.Pop(batch, arb.GetSeq()); err != nil {
						p.cfg.Log.Warn("partition: outbox pop failed",
							"seq", arb.GetSeq(), "producer", arb.GetProducerId(), "err", err)
					}
				default:
					// Cross-shard: emit an OutboxAck back to the producer
					// unless the inbound command is itself an ack (no
					// ack-for-ack).
					if _, isAck := env.GetCommand().GetKind().(*enginev1.Command_OutboxAck); !isAck {
						ackEnv := &enginev1.OutboxEnvelope{
							DestinationShardId: senderShard,
							Kind: &enginev1.OutboxEnvelope_OutboxAck{
								OutboxAck: &enginev1.OutboxAck{
									ProducerShardId: senderShard,
									ProducerSeq:     arb.GetSeq(),
								},
							},
						}
						if seq, err := p.enqueueOutbox(batch, meta, ackEnv, isLeader); err != nil {
							p.cfg.Log.Warn("partition: outbox append (ack) failed",
								"seq", seq, "dest_shard", senderShard, "err", err)
						}
					}
				}
			}
			if err := dedup.Record(batch, envLP, d); err != nil {
				return nil, fmt.Errorf("partition: record dedup: %w", err)
			}
		}

		meta.AppliedIndex = ent.Index
	}

	if err := metaT.Put(batch, meta); err != nil {
		return nil, fmt.Errorf("partition: write meta: %w", err)
	}
	if err := batch.Commit(true); err != nil {
		return nil, fmt.Errorf("partition: commit batch: %w", err)
	}

	if !isLeader {
		p.cfg.Collector.Clear()
		return entries, nil
	}
	// Leader: drain collected actions and let the runner dispatch them.
	if p.cfg.OnActions != nil {
		if actions := p.cfg.Collector.Drain(); len(actions) > 0 {
			p.cfg.OnActions(actions)
		}
	}
	return entries, nil
}

// commandKindLabel returns the Prometheus label for a Command's oneof
// variant. Stable string set so dashboards don't churn when the proto
// evolves; unknown variants land in "unknown".
func commandKindLabel(cmd *enginev1.Command) string {
	switch cmd.GetKind().(type) {
	case *enginev1.Command_AnnounceLeader:
		return "AnnounceLeader"
	case *enginev1.Command_Invoke:
		return "Invoke"
	case *enginev1.Command_InvokerEffect:
		return "InvokerEffect"
	case *enginev1.Command_TimerFired:
		return "TimerFired"
	case *enginev1.Command_Purge:
		return "Purge"
	case *enginev1.Command_DeliverCallResult:
		return "DeliverCallResult"
	case *enginev1.Command_OutboxAck:
		return "OutboxAck"
	case *enginev1.Command_PromiseCompletionAck:
		return "PromiseCompletionAck"
	case *enginev1.Command_ReapInvocation:
		return "ReapInvocation"
	case *enginev1.Command_BeginLpTransfer:
		return "BeginLPTransfer"
	case *enginev1.Command_ApplyLpTransferSst:
		return "ApplyLPTransferSST"
	case *enginev1.Command_CommitLpTransfer:
		return "CommitLPTransfer"
	case *enginev1.Command_FinishLpTransfer:
		return "FinishLPTransfer"
	case *enginev1.Command_AbortLpTransfer:
		return "AbortLPTransfer"
	case *enginev1.Command_ProcessEvent:
		return "ProcessEvent"
	case *enginev1.Command_ProcessAdvanced:
		return "ProcessAdvanced"
	case nil:
		return "empty"
	default:
		return "unknown"
	}
}

// classifyCompletionOutcome maps an InvocationCompleted to a stable
// Prometheus label set: success, failure, cancelled, step_budget_exhausted.
// Reserved failure codes (wire.CancelledCode = 9002, handler
// .StepBudgetExhaustedCode = 9001) take precedence over the generic
// failure_message classification so a cancellation isn't double-counted.
func classifyCompletionOutcome(c *enginev1.InvocationCompleted) string {
	if c == nil {
		return "success"
	}
	switch c.GetFailureCode() {
	case wire.CancelledCode:
		return "cancelled"
	case 9001:
		return "step_budget_exhausted"
	}
	if c.GetFailureMessage() != "" {
		return "failure"
	}
	return "success"
}

// journalEntryKindLabel returns the Prometheus label for a JournalEntry
// oneof variant. Mirrors commandKindLabel: stable strings, unknown
// variants land in "unknown".
func journalEntryKindLabel(e *enginev1.JournalEntry) string {
	switch e.GetEntry().(type) {
	case *enginev1.JournalEntry_Input:
		return "Input"
	case *enginev1.JournalEntry_Run:
		return "Run"
	case *enginev1.JournalEntry_Sleep:
		return "Sleep"
	case *enginev1.JournalEntry_SleepResult:
		return "SleepResult"
	case *enginev1.JournalEntry_Call:
		return "Call"
	case *enginev1.JournalEntry_CallResult:
		return "CallResult"
	case *enginev1.JournalEntry_Awakeable:
		return "Awakeable"
	case *enginev1.JournalEntry_AwakeableResult:
		return "AwakeableResult"
	case *enginev1.JournalEntry_SetState:
		return "SetState"
	case *enginev1.JournalEntry_ClearState:
		return "ClearState"
	case *enginev1.JournalEntry_ClearAllState:
		return "ClearAllState"
	case *enginev1.JournalEntry_GetState:
		return "GetState"
	case *enginev1.JournalEntry_GetStateResult:
		return "GetStateResult"
	case *enginev1.JournalEntry_GetStateKeys:
		return "GetStateKeys"
	case *enginev1.JournalEntry_GetStateKeysResult:
		return "GetStateKeysResult"
	case *enginev1.JournalEntry_GetEagerStateKeys:
		return "GetEagerStateKeys"
	case *enginev1.JournalEntry_Signal:
		return "Signal"
	case *enginev1.JournalEntry_Output:
		return "Output"
	default:
		return "unknown"
	}
}

func leaderLabel(isLeader bool) string {
	if isLeader {
		return "true"
	}
	return "false"
}

// lpFromCommand returns the LP a command belongs to, for the purpose of
// keying its arbitrary dedup row. Arbitrary dedup is LP-prefixed (so it
// rides the LP transfer scan and follows the LP across shard moves), and
// each command kind that carries arbitrary dedup also carries enough state
// to derive an LP — either an InvocationId.partition_key or, for the
// LP-transfer command family, an explicit lp field.
//
// The few LP-agnostic command kinds (OutboxAck, which pops a shard-internal
// outbox row; AnnounceLeader, which uses SelfProposal dedup) key under
// LPNoLP, the sentinel that can never collide with a real LP (real LPs are
// < LPCount = 4096) and is therefore never range-deleted by FinishLPTransfer.
func lpFromCommand(cmd *enginev1.Command) uint32 {
	switch k := cmd.GetKind().(type) {
	case *enginev1.Command_Invoke:
		return keys.LPFromPartitionKey(k.Invoke.GetInvocationId().GetPartitionKey())
	case *enginev1.Command_InvokerEffect:
		return keys.LPFromPartitionKey(k.InvokerEffect.GetInvocationId().GetPartitionKey())
	case *enginev1.Command_TimerFired:
		return keys.LPFromPartitionKey(k.TimerFired.GetInvocationId().GetPartitionKey())
	case *enginev1.Command_DeliverCallResult:
		return keys.LPFromPartitionKey(k.DeliverCallResult.GetParentId().GetPartitionKey())
	case *enginev1.Command_Purge:
		return keys.LPFromPartitionKey(k.Purge.GetInvocationId().GetPartitionKey())
	case *enginev1.Command_PromiseCompletionAck:
		return keys.LPFromPartitionKey(k.PromiseCompletionAck.GetCallerId().GetPartitionKey())
	case *enginev1.Command_ReapInvocation:
		return keys.LPFromPartitionKey(k.ReapInvocation.GetInvocationId().GetPartitionKey())
	case *enginev1.Command_BeginLpTransfer:
		return k.BeginLpTransfer.GetLp()
	case *enginev1.Command_ApplyLpTransferSst:
		return k.ApplyLpTransferSst.GetLp()
	case *enginev1.Command_CommitLpTransfer:
		return k.CommitLpTransfer.GetLp()
	case *enginev1.Command_FinishLpTransfer:
		return k.FinishLpTransfer.GetLp()
	case *enginev1.Command_AbortLpTransfer:
		return k.AbortLpTransfer.GetLp()
	case *enginev1.Command_ProcessEvent:
		return keys.LPFromPartitionKey(k.ProcessEvent.GetPk())
	case *enginev1.Command_ProcessAdvanced:
		return keys.LPFromPartitionKey(k.ProcessAdvanced.GetPk())
	default:
		// OutboxAck (LP-agnostic shard-internal pop), AnnounceLeader
		// (SelfProposal dedup; lp ignored), unknown future kinds.
		return keys.LPNoLP
	}
}

// checkLPFreeze returns errLPFrozen when partitionKey's LP is frozen
// for an in-progress LP transfer. Callers MUST invoke this BEFORE any
// batch mutation or action push in an LP-touching apply arm, so that
// returning the sentinel cleanly rolls the entry back (the Update loop
// stamps ResultValueLPFrozen and skips). Returns nil on the hot path
// (no transfer in flight for this LP — a single point-Get against the
// batch, bloom-filter-friendly when absent).
func (p *Partition) checkLPFreeze(batch storage.Batch, partitionKey uint64) error {
	lp := keys.LPFromPartitionKey(partitionKey)
	row, err := (tables.LPFreezeTable{S: batch}).Get(lp)
	if err != nil {
		return fmt.Errorf("partition: lp freeze lookup: %w", err)
	}
	if row != nil {
		return errLPFrozen
	}
	return nil
}

// enqueueOutbox allocates the next outbox seq, writes env to the
// OutboxTable, bumps meta.next_outbox_seq, and (when leader) pushes
// an ActDispatchOutbox so the shuffler picks it up. Returns the seq
// and any storage error; meta is bumped in memory regardless, but
// since a non-nil error aborts the Update batch the increment is not
// persisted.
func (p *Partition) enqueueOutbox(batch storage.Batch, meta *enginev1.PartitionMeta, env *enginev1.OutboxEnvelope, isLeader bool) (uint64, error) {
	seq := meta.GetNextOutboxSeq()
	meta.NextOutboxSeq = seq + 1
	outboxT := tables.OutboxTable{S: batch}
	if err := outboxT.Append(batch, seq, env); err != nil {
		return seq, err
	}
	if isLeader {
		p.cfg.Collector.Push(ActDispatchOutbox{Seq: seq, Envelope: env})
	}
	return seq, nil
}

func (p *Partition) applyCommand(
	batch storage.Batch,
	store storage.Store,
	env *enginev1.Envelope,
	raftIndex uint64,
	meta *enginev1.PartitionMeta,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	timers tables.TimerTable,
	isLeader bool,
) error {
	// now is sourced from Header.created_at_ms — stamped once by the
	// leader-side proposer (see internal/engine/proposer.go) so every
	// replica reads the same value during Update. All production
	// envelopes flow through buildSelfProposalEnvelope or
	// buildIngressEnvelope and are guaranteed to carry the field; tests
	// that construct bare Envelopes are responsible for stamping it.
	now := env.GetHeader().GetCreatedAtMs()
	cmd := env.GetCommand()

	switch k := cmd.GetKind().(type) {
	case *enginev1.Command_AnnounceLeader:
		return p.onAnnounceLeader(batch, k.AnnounceLeader, meta, isLeader)
	case *enginev1.Command_Invoke:
		return p.onInvoke(batch, k.Invoke, now, inv, isLeader)
	case *enginev1.Command_InvokerEffect:
		return p.onInvokerEffect(batch, k.InvokerEffect, now, meta, inv, journal, isLeader)
	case *enginev1.Command_TimerFired:
		return p.onTimerFired(batch, k.TimerFired, now, inv, timers, isLeader)
	case *enginev1.Command_Purge:
		return p.onPurge(batch, k.Purge, inv, journal)
	case *enginev1.Command_DeliverCallResult:
		return p.onDeliverCallResult(batch, store, k.DeliverCallResult, now, inv, journal, isLeader)
	case *enginev1.Command_OutboxAck:
		return p.onOutboxAck(batch, k.OutboxAck)
	case *enginev1.Command_PromiseCompletionAck:
		return p.onPromiseCompletionAck(batch, k.PromiseCompletionAck, inv, journal, now, isLeader)
	case *enginev1.Command_ReapInvocation:
		return p.onReap(batch, k.ReapInvocation, inv, journal)
	case *enginev1.Command_BeginLpTransfer:
		return p.onBeginLPTransfer(batch, k.BeginLpTransfer, now, isLeader)
	case *enginev1.Command_ApplyLpTransferSst:
		return p.onApplyLPTransferSST(batch, store, k.ApplyLpTransferSst, isLeader)
	case *enginev1.Command_CommitLpTransfer:
		return p.onCommitLPTransfer(batch, k.CommitLpTransfer)
	case *enginev1.Command_FinishLpTransfer:
		return p.onFinishLPTransfer(batch, k.FinishLpTransfer, isLeader)
	case *enginev1.Command_AbortLpTransfer:
		return p.onAbortLPTransfer(batch, k.AbortLpTransfer, isLeader)
	case *enginev1.Command_ProcessEvent:
		return p.onProcessEvent(batch, k.ProcessEvent, now, isLeader)
	case *enginev1.Command_ProcessAdvanced:
		return p.onProcessAdvanced(batch, meta, k.ProcessAdvanced, now, isLeader)
	case *enginev1.Command_ProcessSubscribe:
		return p.onProcessSubscribe(batch, k.ProcessSubscribe)
	case *enginev1.Command_DeliverProcessMessage:
		return p.onDeliverProcessMessage(batch, meta, k.DeliverProcessMessage, now, isLeader)
	case nil:
		p.cfg.Log.Warn("partition: envelope has no command kind", "raft_index", raftIndex)
		return nil
	default:
		// Forward-compat: unknown command variants log + no-op. NEVER returns
		// error — that would halt the shard (dragonboat disk.go:113).
		p.cfg.Log.Warn("partition: unknown command kind; no-op",
			"raft_index", raftIndex, "kind", fmt.Sprintf("%T", k))
		return nil
	}
}

func (p *Partition) onAnnounceLeader(
	_ storage.Batch,
	cmd *enginev1.AnnounceLeader,
	meta *enginev1.PartitionMeta,
	_ bool,
) error {
	if cmd.GetLeaderEpoch() > meta.GetLatestAnnouncedEpoch() {
		meta.LatestAnnouncedEpoch = cmd.GetLeaderEpoch()
	}
	// Always notify the local leadership state; the implementation decides
	// whether to promote to Leader, step down, or ignore.
	p.cfg.Leadership.OnAnnounceLeader(cmd)
	return nil
}

// bandedEntityPK returns the partition_key for an entity row (idempotency,
// workflow_run, keylease, state, promise) addressed by (service, objectKey),
// banded under the owning invocation's tenant. The apply path and ingress
// Lookup must agree on this LP; recomputing from the routing tuple — rather
// than reading id's LP directly — keeps them aligned, bands an entity whose
// (service, objectKey) differs from the invocation's own target (cross-workflow
// promises), and stays robust to synthetic test ids that don't follow the mint
// invariant id.pk == PartitionKey(tenant, svc, objKey).
func bandedEntityPK(id *enginev1.InvocationId, service, objectKey string) uint64 {
	return routing.PartitionKey(keys.TenantFromPartitionKey(id.GetPartitionKey()), service, objectKey)
}

func (p *Partition) onInvoke(batch storage.Batch, cmd *enginev1.InvokeCommand, nowMs uint64, inv tables.InvocationTable, isLeader bool) error {
	id := cmd.GetInvocationId()
	target := cmd.GetTarget()
	// LP for the per-(service, object_key) namespaces (idempotency,
	// workflow_run, keylease) is banded under the invocation's tenant, so
	// apply and ingress's optimistic Lookup* (which band the same tuple from
	// the same principal) agree on the row.
	epk := bandedEntityPK(id, target.GetServiceName(), target.GetObjectKey())
	lp := keys.LPFromPartitionKey(epk)

	// Freeze gate. Must run before any state write so a frozen LP
	// rolls back cleanly via the Update loop's errLPFrozen handling.
	if err := p.checkLPFreeze(batch, epk); err != nil {
		return err
	}

	// Idempotency dedup. When idempotency_key is set, the first
	// InvokeCommand that lands wins; later submissions with the same
	// (service, handler, object_key, idempotency_key) tuple are dropped
	// silently. The new InvocationId is NOT registered — the caller that
	// minted it relied on ingress's optimistic LookupIdempotency to
	// surface the prior id before propose. Late losers polling on the
	// minted-but-dropped id will time out; cross-node races can be
	// hardened in a future improvement by writing a redirect status row.
	if ik := cmd.GetIdempotencyKey(); ik != "" {
		idemT := tables.IdempotencyTable{S: batch}
		prior, ierr := idemT.Get(lp, target.GetServiceName(), target.GetHandlerName(), target.GetObjectKey(), ik)
		if ierr != nil {
			return fmt.Errorf("onInvoke: idempotency lookup: %w", ierr)
		}
		if prior != nil {
			p.cfg.Log.Debug("partition: idempotency hit; dropping duplicate invocation",
				"prior_uuid", fmt.Sprintf("%x", prior.GetUuid()),
				"new_uuid", fmt.Sprintf("%x", id.GetUuid()),
				"service", target.GetServiceName(),
				"handler", target.GetHandlerName(),
				"object_key", target.GetObjectKey())
			return nil
		}
		if perr := idemT.Put(batch, lp, target.GetServiceName(), target.GetHandlerName(), target.GetObjectKey(), ik, id); perr != nil {
			return fmt.Errorf("onInvoke: idempotency record: %w", perr)
		}
	}

	// Workflow single-run-per-key dedup. For KIND_WORKFLOW Run handlers, a
	// successful submission claims (service, workflow_key) for the lifetime
	// of the run (and its retention window). Subsequent submissions to the
	// same (service, key) are dropped — ingress's optimistic LookupWorkflowRun
	// surfaces the existing InvocationId; a losing race lands here and the
	// new id is silently dropped (callers polling on a minted-but-dropped id
	// time out, same shape as idempotency dedup).
	if protocolv1.Kind(cmd.GetKind()) == protocolv1.Kind_KIND_WORKFLOW && target.GetObjectKey() != "" {
		runT := tables.WorkflowRunTable{S: batch}
		prior, rerr := runT.Get(lp, target.GetServiceName(), target.GetObjectKey())
		if rerr != nil {
			return fmt.Errorf("onInvoke: workflow_run lookup: %w", rerr)
		}
		if prior != nil {
			p.cfg.Log.Debug("partition: workflow_run hit; dropping duplicate workflow submission",
				"prior_uuid", fmt.Sprintf("%x", prior.GetUuid()),
				"new_uuid", fmt.Sprintf("%x", id.GetUuid()),
				"service", target.GetServiceName(),
				"workflow_key", target.GetObjectKey())
			return nil
		}
		if perr := runT.Put(batch, lp, target.GetServiceName(), target.GetObjectKey(), id); perr != nil {
			return fmt.Errorf("onInvoke: workflow_run record: %w", perr)
		}
	}

	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onInvoke: load status: %w", err)
	}
	// transitionOnInvoke records the new Scheduled status; for unkeyed
	// targets it also emits ActInvoke so the invoker session starts
	// immediately. For keyed targets we route through the per-key VO gate
	// (KeyLeaseTable + object_fsm) instead — only the lease holder may
	// activate; queued invocations sit in Scheduled until the gate releases.
	next, actions, err := transitionOnInvoke(id, cur, cmd, nowMs)
	if err != nil {
		p.cfg.Log.Warn("partition: invalid Invoke transition", "err", err)
		return nil
	}
	if err := inv.Put(batch, id, next); err != nil {
		return fmt.Errorf("onInvoke: write status: %w", err)
	}

	keyed := target.GetObjectKey() != ""
	// transitionOnInvoke is a no-op for Scheduled/Invoked re-entries; only
	// fresh Free → Scheduled produces actions. We only need to drive the gate
	// in that case.
	if keyed && len(actions) > 0 {
		klt := tables.KeyLeaseTable{S: batch}
		curLease, lerr := klt.Get(lp, target.GetServiceName(), target.GetObjectKey())
		if lerr != nil {
			return fmt.Errorf("onInvoke: load key lease: %w", lerr)
		}
		var leaseActs []Action
		sm, nextLease := buildObjectFSM(curLease, func(activated *enginev1.InvocationId) {
			leaseActs = append(leaseActs, ActInvoke{ID: activated, Target: target})
		})
		if ferr := sm.Fire(vobjEnqueue, id); ferr != nil {
			return fmt.Errorf("onInvoke: vobj fire: %w", ferr)
		}
		if perr := klt.Put(batch, lp, target.GetServiceName(), target.GetObjectKey(), nextLease); perr != nil {
			return fmt.Errorf("onInvoke: write key lease: %w", perr)
		}
		// Drop the original ActInvoke from transitionOnInvoke; the gate is
		// authoritative for keyed activations.
		actions = leaseActs
	}

	if isLeader {
		for _, a := range actions {
			p.cfg.Collector.Push(a)
		}
	}
	return nil
}

// processRootID derives the stable InvocationId handle for a process instance.
// Deterministic across replicas (apply runs on every replica) so timer/dedup
// rows keyed by the root id agree; partition_key routes to this shard and uuid
// is the truncated SHA-256 of (service, instance_key).
func processRootID(pk uint64, service, instanceKey string) *enginev1.InvocationId {
	h := sha256.Sum256([]byte(service + "\x00" + instanceKey))
	return &enginev1.InvocationId{PartitionKey: pk, Uuid: h[:16]}
}

// onProcessEvent applies a ProcessEvent landing on this shard (external ingress,
// a cross-shard outbox redelivery, or a timer fire): freeze-gate, then enqueue
// it on the addressed instance's inbox.
func (p *Partition) onProcessEvent(batch storage.Batch, ev *enginev1.ProcessEvent, nowMs uint64, isLeader bool) error {
	if err := p.checkLPFreeze(batch, ev.GetPk()); err != nil {
		return err
	}
	return p.enqueueInstanceEvent(batch, ev, nowMs, isLeader)
}

// enqueueInstanceEvent creates the instance on the start event, appends the
// event payload to the per-instance inbox, and — when no turn is in flight —
// activates it (emitting ActAdvanceProcess so the leader's procSession runs one
// iflow step). Turn serialization is the inbox cursor on ProcessInstanceRecord:
// concurrent events for one instance queue behind the active turn rather than
// racing the state blob. Logical/stale conditions log and return nil — never an
// error, which would halt the shard. The caller owns the freeze gate (the
// inline feedback-delivery path does not re-gate, matching applyCallResultToParent).
func (p *Partition) enqueueInstanceEvent(batch storage.Batch, ev *enginev1.ProcessEvent, nowMs uint64, isLeader bool) error {
	pk := ev.GetPk()
	lp := keys.LPFromPartitionKey(pk)
	service, instanceKey := ev.GetService(), ev.GetInstanceKey()
	procT := tables.ProcessInstanceTable{S: batch}
	inboxT := tables.ProcessInboxTable{S: batch}

	rec, ok, err := procT.Get(lp, service, instanceKey)
	if err != nil {
		return fmt.Errorf("enqueueInstanceEvent: load record: %w", err)
	}
	if ok && ev.GetModelRef() != nil {
		// A start event (model_ref set) for an instance that already exists: a
		// re-proposed StartProcess or a re-delivered child start. The instance is
		// already running (or finished); re-starting it would mis-feed the start
		// vars as a continuation event. Drop — starts are idempotent per
		// (service, instance_key).
		p.cfg.Log.Debug("partition: start ProcessEvent for existing instance; dropping",
			"service", service, "key", instanceKey)
		return nil
	}
	if !ok {
		// A start event (model_ref set) creates the instance; any other event
		// for an absent instance is a straggler (reaped / never existed / a
		// child result for an already-completed parent).
		if ev.GetModelRef() == nil {
			p.cfg.Log.Debug("partition: ProcessEvent for absent instance; dropping",
				"service", service, "key", instanceKey)
			return nil
		}
		rec = &enginev1.ProcessInstanceRecord{
			RootId:     processRootID(pk, service, instanceKey),
			ModelRef:   ev.GetModelRef(),
			Kind:       ev.GetKind(),
			ParentLink: ev.GetParentLink(),
			Status:     enginev1.ProcessStatus_PROCESS_STATUS_RUNNING,
			NextSeq:    1,
		}
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		p.cfg.Log.Debug("partition: ProcessEvent for terminal instance; dropping",
			"service", service, "key", instanceKey, "status", rec.GetStatus().String())
		return nil
	}

	seq := rec.GetNextSeq()
	if seq == 0 {
		seq = 1
	}
	rec.NextSeq = seq + 1
	lt := ev.GetLogicalTimeMs()
	if lt == 0 {
		lt = nowMs
	}
	entry := &enginev1.ProcessInboxEntry{Payload: ev.GetPayload(), LogicalTimeMs: lt}
	if err := inboxT.Append(batch, lp, service, instanceKey, seq, entry); err != nil {
		return fmt.Errorf("enqueueInstanceEvent: inbox append: %w", err)
	}

	if rec.GetActiveSeq() == 0 {
		rec.ActiveSeq = seq
		if isLeader {
			p.cfg.Collector.Push(ActAdvanceProcess{
				Pk: pk, Service: service, InstanceKey: instanceKey, Entry: entry,
			})
		}
	}
	if err := procT.Put(batch, lp, service, instanceKey, rec); err != nil {
		return fmt.Errorf("enqueueInstanceEvent: write record: %w", err)
	}
	return nil
}

// deliverProcessEvent routes a synthesized ProcessEvent to the addressed
// instance: inline onto the local inbox when this shard owns the instance's LP,
// else via the outbox to the owning shard (where onProcessEvent applies it).
// Mirrors deliverCallResultToParent's same-shard / cross-shard split.
func (p *Partition) deliverProcessEvent(batch storage.Batch, meta *enginev1.PartitionMeta, ev *enginev1.ProcessEvent, nowMs uint64, isLeader bool) error {
	shard := p.cfg.Partitioner.ShardForKey(ev.GetPk())
	if shard == 0 || shard == p.shardID {
		return p.enqueueInstanceEvent(batch, ev, nowMs, isLeader)
	}
	env := &enginev1.OutboxEnvelope{
		DestinationShardId: shard,
		Kind:               &enginev1.OutboxEnvelope_ProcessEvent{ProcessEvent: ev},
	}
	if _, err := p.enqueueOutbox(batch, meta, env, isLeader); err != nil {
		return fmt.Errorf("deliverProcessEvent: outbox append: %w", err)
	}
	return nil
}

// deliverProcessSubscribe routes a message subscription to the partition owning
// the message routing key (ps.pk): written directly when this shard owns that
// LP, else shipped via the outbox to the owning shard (where onProcessSubscribe
// applies it). A subscription must be co-located with where DeliverProcessMessage
// routes — which generally differs from the instance's own partition — so this
// mirrors deliverProcessEvent's same-shard / cross-shard split.
func (p *Partition) deliverProcessSubscribe(batch storage.Batch, meta *enginev1.PartitionMeta, ps *enginev1.ProcessSubscribe, isLeader bool) error {
	shard := p.cfg.Partitioner.ShardForKey(ps.GetPk())
	if shard == 0 || shard == p.shardID {
		return p.writeSubscription(batch, ps)
	}
	env := &enginev1.OutboxEnvelope{
		DestinationShardId: shard,
		Kind:               &enginev1.OutboxEnvelope_ProcessSubscribe{ProcessSubscribe: ps},
	}
	if _, err := p.enqueueOutbox(batch, meta, env, isLeader); err != nil {
		return fmt.Errorf("deliverProcessSubscribe: outbox append: %w", err)
	}
	return nil
}

// writeSubscription persists one MessageSubscription row on this shard, which
// owns ps.pk's LP. Used directly for a same-shard subscribe (the inline path,
// which like deliverProcessEvent does not re-gate the freeze) and by
// onProcessSubscribe for the cross-shard landing (which gates).
func (p *Partition) writeSubscription(batch storage.Batch, ps *enginev1.ProcessSubscribe) error {
	lp := keys.LPFromPartitionKey(ps.GetPk())
	subT := tables.MessageSubscriptionTable{S: batch}
	if err := subT.Put(batch, lp, ps.GetSub()); err != nil {
		return fmt.Errorf("writeSubscription: put: %w", err)
	}
	return nil
}

// onProcessAdvanced applies the result of one process turn: persist the new
// state blob and dequeue the completed turn, then either finish the instance
// (terminal) or actuate the turn's instructions and activate the next queued
// event. Instruction actuation and the terminal path run only for the turn that
// actually commits — a re-driven turn (whose first ProcessAdvanced never
// committed) sees the cursor unmoved and reproduces the same effects; a
// duplicate (first proposal already applied) is dropped at the active==0 guard,
// or at the absent-record guard once the instance has finished.
func (p *Partition) onProcessAdvanced(batch storage.Batch, meta *enginev1.PartitionMeta, adv *enginev1.ProcessAdvanced, nowMs uint64, isLeader bool) error {
	pk := adv.GetPk()
	if err := p.checkLPFreeze(batch, pk); err != nil {
		return err
	}
	lp := keys.LPFromPartitionKey(pk)
	service, instanceKey := adv.GetService(), adv.GetInstanceKey()
	procT := tables.ProcessInstanceTable{S: batch}
	inboxT := tables.ProcessInboxTable{S: batch}

	rec, ok, err := procT.Get(lp, service, instanceKey)
	if err != nil {
		return fmt.Errorf("onProcessAdvanced: load record: %w", err)
	}
	if !ok {
		p.cfg.Log.Debug("partition: ProcessAdvanced for absent instance; dropping",
			"service", service, "key", instanceKey)
		return nil
	}
	active := rec.GetActiveSeq()
	if active == 0 {
		// No turn was active: a duplicate ProcessAdvanced (e.g. a re-driven turn
		// whose first proposal already applied). Drop — re-persisting could
		// clobber a newer turn's state.
		p.cfg.Log.Debug("partition: ProcessAdvanced with no active turn; dropping",
			"service", service, "key", instanceKey)
		return nil
	}

	rec.StateBlob = adv.GetNewState()

	// Dequeue the completed turn.
	if err := inboxT.Delete(batch, lp, service, instanceKey, active); err != nil {
		return fmt.Errorf("onProcessAdvanced: inbox delete: %w", err)
	}

	if term := adv.GetTerminal(); term != nil {
		return p.finishProcessInstance(batch, meta, rec, adv, term, active, nowMs, isLeader)
	}

	// Non-terminal: actuate the turn's instructions, then advance the cursor.
	if err := p.actuateProcessInstructions(batch, meta, rec, adv, nowMs, isLeader); err != nil {
		return err
	}
	if next := active + 1; next < rec.GetNextSeq() {
		rec.ActiveSeq = next
		if isLeader {
			entry, eok, eerr := inboxT.Get(lp, service, instanceKey, next)
			if eerr != nil {
				return fmt.Errorf("onProcessAdvanced: load next inbox: %w", eerr)
			}
			if eok {
				p.cfg.Collector.Push(ActAdvanceProcess{
					Pk: pk, Service: service, InstanceKey: instanceKey, Entry: entry,
				})
			}
		}
	} else {
		rec.ActiveSeq = 0
	}

	if err := procT.Put(batch, lp, service, instanceKey, rec); err != nil {
		return fmt.Errorf("onProcessAdvanced: write record: %w", err)
	}
	return nil
}

// finishProcessInstance applies a terminal turn: deliver the result back to a
// process parent (call-activity / case-task node) if one is linked, then reap
// the instance — delete every remaining inbox row plus the record itself. There
// is no retention window yet: a completed instance is observable only via the
// result delivered to its parent (an external query surface is future work).
// The cross-shard delivery rides the durable outbox, so it survives the record
// delete that happens in the same batch.
func (p *Partition) finishProcessInstance(batch storage.Batch, meta *enginev1.PartitionMeta, rec *enginev1.ProcessInstanceRecord, adv *enginev1.ProcessAdvanced, term *enginev1.ProcessTerminal, active, nowMs uint64, isLeader bool) error {
	pk := adv.GetPk()
	lp := keys.LPFromPartitionKey(pk)
	service, instanceKey := adv.GetService(), adv.GetInstanceKey()
	inboxT := tables.ProcessInboxTable{S: batch}
	procT := tables.ProcessInstanceTable{S: batch}

	if pp := rec.GetParentLink().GetProcessParent(); pp != nil {
		ev := &enginev1.ProcessEvent{
			Pk:            pp.GetPk(),
			Service:       pp.GetService(),
			InstanceKey:   pp.GetInstanceKey(),
			LogicalTimeMs: nowMs,
			Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_ChildCompleted{
				ChildCompleted: &enginev1.ProcessChildCompleted{
					NodeId:         pp.GetNodeId(),
					InstanceIdx:    pp.GetInstanceIdx(),
					Output:         term.GetOutput(),
					Failed:         term.GetFailed(),
					FailureMessage: term.GetFailureMessage(),
				},
			}},
		}
		if err := p.deliverProcessEvent(batch, meta, ev, nowMs, isLeader); err != nil {
			return err
		}
	}

	// Reap: drop any still-queued inbox rows (active was already dequeued above)
	// and the record. Queued seqs are contiguous in (active, next_seq).
	for seq := active + 1; seq < rec.GetNextSeq(); seq++ {
		if err := inboxT.Delete(batch, lp, service, instanceKey, seq); err != nil {
			return fmt.Errorf("finishProcessInstance: inbox delete: %w", err)
		}
	}
	if err := procT.Delete(batch, lp, service, instanceKey); err != nil {
		return fmt.Errorf("finishProcessInstance: record delete: %w", err)
	}
	return nil
}

// actuateProcessInstructions turns a non-terminal turn's instruction lists into
// reflow-native side effects: a service task becomes an invocation carrying a
// process_parent link (its result feeds back via applyTerminalCompletion); a
// timer becomes a process timer (fires as a Command_ProcessEvent); a child start
// becomes a ProcessEvent(start) addressed to the child, itself process-parented
// back. Durable rows are written on every replica; leader-only actions drive the
// live timer/outbox services. Message subscriptions are not yet actuated — they
// need the inbound correlation read path, deferred.
func (p *Partition) actuateProcessInstructions(batch storage.Batch, meta *enginev1.PartitionMeta, rec *enginev1.ProcessInstanceRecord, adv *enginev1.ProcessAdvanced, nowMs uint64, isLeader bool) error {
	pk := adv.GetPk()
	service, instanceKey := adv.GetService(), adv.GetInstanceKey()
	root := rec.GetRootId()
	tenant := keys.TenantFromPartitionKey(pk)
	timersT := tables.TimerTable{S: batch}

	for i, ti := range adv.GetInvoke() {
		target := ti.GetTarget()
		calleeID := mintProcessTaskID(root, ti.GetNodeId(), ti.GetInstanceIdx(), rec.GetActiveSeq(), uint64(i), target)
		env := &enginev1.OutboxEnvelope{
			DestinationShardId: p.cfg.Partitioner.ShardForInvocation(calleeID),
			Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: calleeID,
				Target:       target,
				Input:        ti.GetInput(),
				ParentLink: &enginev1.ParentLink{ProcessParent: &enginev1.ProcessParent{
					Pk: pk, Service: service, InstanceKey: instanceKey,
					NodeId: ti.GetNodeId(), InstanceIdx: ti.GetInstanceIdx(),
				}},
			}},
		}
		if _, err := p.enqueueOutbox(batch, meta, env, isLeader); err != nil {
			return fmt.Errorf("actuateProcessInstructions: outbox append (task): %w", err)
		}
	}

	for _, ta := range adv.GetArmTimer() {
		tid := processTimerID(pk, service, instanceKey, ta.GetNodeId(), ta.GetSlot())
		pt := &enginev1.ProcessTimer{
			Service: service, InstanceKey: instanceKey,
			NodeId: ta.GetNodeId(), Slot: ta.GetSlot(),
		}
		if err := timersT.InsertProcess(batch, ta.GetFireAtMs(), tid, pt); err != nil {
			return fmt.Errorf("actuateProcessInstructions: timer insert: %w", err)
		}
		if isLeader {
			p.cfg.Collector.Push(ActRegisterTimer{FireAtMs: ta.GetFireAtMs(), ID: tid, Process: pt})
		}
	}

	for _, tc := range adv.GetCancelTimer() {
		tid := processTimerID(pk, service, instanceKey, tc.GetNodeId(), tc.GetSlot())
		var fires []uint64
		if err := timersT.ScanByInvocation(tid, func(fireAt uint64) error {
			fires = append(fires, fireAt)
			return nil
		}); err != nil {
			return fmt.Errorf("actuateProcessInstructions: timer scan: %w", err)
		}
		for _, fireAt := range fires {
			if err := timersT.Delete(batch, fireAt, tid); err != nil {
				return fmt.Errorf("actuateProcessInstructions: timer delete: %w", err)
			}
			if isLeader {
				p.cfg.Collector.Push(ActDeleteTimer{FireAtMs: fireAt, ID: tid})
			}
		}
	}

	for _, cs := range adv.GetStartChild() {
		childSvc := cs.GetModelRef().GetName()
		childKey := cs.GetInstanceKey()
		ev := &enginev1.ProcessEvent{
			Pk:            routing.PartitionKey(tenant, childSvc, childKey),
			Service:       childSvc,
			InstanceKey:   childKey,
			LogicalTimeMs: nowMs,
			Payload:       &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_External{External: cs.GetVars()}},
			ModelRef:      cs.GetModelRef(),
			Kind:          cs.GetKind(),
			ParentLink: &enginev1.ParentLink{ProcessParent: &enginev1.ProcessParent{
				Pk: pk, Service: service, InstanceKey: instanceKey, NodeId: cs.GetNodeId(),
			}},
		}
		if err := p.deliverProcessEvent(batch, meta, ev, nowMs, isLeader); err != nil {
			return fmt.Errorf("actuateProcessInstructions: child start: %w", err)
		}
	}

	// Message/signal subscriptions: a parked BPMN catch (WaitForSignal). The row
	// lives on the partition owning the message routing key (tenant, message_name,
	// correlation_key) — generally a different LP than this instance — so a future
	// DeliverProcessMessage can find it without knowing the instance's address.
	for _, sub := range adv.GetSubscribe() {
		ps := &enginev1.ProcessSubscribe{
			Pk: routing.PartitionKey(tenant, sub.GetMessageName(), sub.GetCorrelationKey()),
			Sub: &enginev1.MessageSubscription{
				InstancePk:     pk,
				Service:        service,
				InstanceKey:    instanceKey,
				NodeId:         sub.GetNodeId(),
				MessageName:    sub.GetMessageName(),
				CorrelationKey: sub.GetCorrelationKey(),
			},
		}
		if err := p.deliverProcessSubscribe(batch, meta, ps, isLeader); err != nil {
			return fmt.Errorf("actuateProcessInstructions: subscribe: %w", err)
		}
	}
	return nil
}

// onProcessSubscribe records one message subscription on this shard (it owns the
// message routing key's LP). This is the cross-shard landing of an
// OutboxEnvelope.process_subscribe; same-shard subscribes skip the Raft round
// trip by calling writeSubscription directly from actuateProcessInstructions.
func (p *Partition) onProcessSubscribe(batch storage.Batch, ps *enginev1.ProcessSubscribe) error {
	if err := p.checkLPFreeze(batch, ps.GetPk()); err != nil {
		return err
	}
	return p.writeSubscription(batch, ps)
}

// onDeliverProcessMessage applies an inbound correlated message: scan every
// MessageSubscription parked on (message_name, correlation_key) within this
// shard's owning LP, fan a ProcessMessageReceived ProcessEvent out to each
// subscribed instance (same-shard inline or cross-shard via the outbox), and
// one-shot-delete each consumed subscription row. A message with no current
// subscriber is a no-op (not buffered) — matching BPMN signal semantics, and
// making redelivery naturally idempotent (the rows are gone after the first
// apply). Scanning is separated from mutation so we never write to the batch
// while iterating it.
func (p *Partition) onDeliverProcessMessage(batch storage.Batch, meta *enginev1.PartitionMeta, dpm *enginev1.DeliverProcessMessage, nowMs uint64, isLeader bool) error {
	pk := dpm.GetPk()
	if err := p.checkLPFreeze(batch, pk); err != nil {
		return err
	}
	lp := keys.LPFromPartitionKey(pk)
	lt := dpm.GetLogicalTimeMs()
	if lt == 0 {
		lt = nowMs
	}
	subT := tables.MessageSubscriptionTable{S: batch}

	type delivery struct {
		key []byte
		ev  *enginev1.ProcessEvent
	}
	var deliveries []delivery
	if err := subT.ScanByCorrelation(lp, dpm.GetMessageName(), dpm.GetCorrelationKey(),
		func(key []byte, sub *enginev1.MessageSubscription) error {
			deliveries = append(deliveries, delivery{
				key: key,
				ev: &enginev1.ProcessEvent{
					Pk:            sub.GetInstancePk(),
					Service:       sub.GetService(),
					InstanceKey:   sub.GetInstanceKey(),
					LogicalTimeMs: lt,
					Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_MessageReceived{
						MessageReceived: &enginev1.ProcessMessageReceived{
							NodeId:         sub.GetNodeId(),
							MessageName:    dpm.GetMessageName(),
							CorrelationKey: dpm.GetCorrelationKey(),
							Payload:        dpm.GetPayload(),
						},
					}},
				},
			})
			return nil
		}); err != nil {
		return fmt.Errorf("onDeliverProcessMessage: scan: %w", err)
	}

	for _, d := range deliveries {
		if err := p.deliverProcessEvent(batch, meta, d.ev, nowMs, isLeader); err != nil {
			return fmt.Errorf("onDeliverProcessMessage: deliver: %w", err)
		}
		if err := subT.DeleteKey(batch, d.key); err != nil {
			return fmt.Errorf("onDeliverProcessMessage: delete subscription: %w", err)
		}
	}
	return nil
}

func (p *Partition) onInvokerEffect(batch storage.Batch, eff *enginev1.InvokerEffect, nowMs uint64, meta *enginev1.PartitionMeta, inv tables.InvocationTable, journal tables.JournalTable, isLeader bool) error {
	timersT := tables.TimerTable{S: batch}
	awakeT := tables.AwakeableTable{S: batch}

	id := eff.GetInvocationId()
	// Freeze gate (id-bearing variants). SignalDelivered carries a
	// Target instead of an id and is gated below once the active id
	// has been resolved via KeyLeaseTable; PromiseCompleted carries
	// neither and is routed by (service, workflow_key) — its gate
	// runs in the PromiseCompleted arm.
	if id != nil {
		if err := p.checkLPFreeze(batch, id.GetPartitionKey()); err != nil {
			return err
		}
	}
	// SignalDelivered is routed by Target (not InvocationId) so its
	// surrounding invocation_id is nil; the apply arm resolves the
	// active id via KeyLeaseTable. For every other variant id is set
	// and we load the current status up-front to share across cases.
	var (
		cur     *enginev1.InvocationStatus
		next    *enginev1.InvocationStatus
		actions []Action
	)
	if id != nil {
		var err error
		cur, err = inv.Get(id)
		if err != nil {
			return fmt.Errorf("onInvokerEffect: load status: %w", err)
		}
	}
	var err error
	switch k := eff.GetKind().(type) {
	case *enginev1.InvokerEffect_JournalAppended:
		entry := k.JournalAppended.GetEntry()
		// Persist the journal entry first.
		if err := journal.Append(batch, id, entry); err != nil {
			return fmt.Errorf("onInvokerEffect: journal append: %w", err)
		}
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.JournalAppended.WithLabelValues(journalEntryKindLabel(entry)).Inc()
		}
		// Per-entry-type side effects: timers, awakeable directory, outbox.
		switch e := entry.GetEntry().(type) {
		case *enginev1.JournalEntry_Sleep:
			if err := timersT.Insert(batch, e.Sleep.GetFireAtMs(), id, entry.GetIndex()); err != nil {
				return fmt.Errorf("onInvokerEffect: timer insert: %w", err)
			}
			if isLeader {
				p.cfg.Collector.Push(ActRegisterTimer{
					FireAtMs: e.Sleep.GetFireAtMs(),
					ID:       id,
					SleepIdx: entry.GetIndex(),
				})
			}
		case *enginev1.JournalEntry_Awakeable:
			akID := e.Awakeable.GetAwakeableId()
			if vErr := keys.ValidateAwakeableID(akID); vErr != nil {
				p.cfg.Log.Warn("partition: malformed awakeable id; skipping directory write",
					"err", vErr, "id", akID)
			} else {
				dir := &enginev1.AwakeableEntry{Owner: id, EntryIndex: entry.GetIndex()}
				if err := awakeT.Put(batch, akID, dir); err != nil {
					return fmt.Errorf("onInvokerEffect: awakeable put: %w", err)
				}
			}
		case *enginev1.JournalEntry_Call:
			calleeID := mintCalleeInvocationID(id, entry.GetIndex(), e.Call.GetTarget())
			env := &enginev1.OutboxEnvelope{
				DestinationShardId: p.cfg.Partitioner.ShardForInvocation(calleeID),
				Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
					InvocationId: calleeID,
					Target:       e.Call.GetTarget(),
					Input:        e.Call.GetInput(),
					// Forward the caller-supplied idempotency_key so the
					// callee's onInvoke runs dedup against (service, handler,
					// object_key, idempotency_key).
					IdempotencyKey: e.Call.GetIdempotencyKey(),
					// Stamp parent_link so the callee's Completed apply arm
					// can journal JECallResult back on the parent.
					ParentLink: &enginev1.ParentLink{
						ParentId:  id,
						CallIndex: entry.GetIndex(),
					},
				}},
			}
			if _, err := p.enqueueOutbox(batch, meta, env, isLeader); err != nil {
				return fmt.Errorf("onInvokerEffect: outbox append (call): %w", err)
			}
		case *enginev1.JournalEntry_OneWayCall:
			// Fire-and-forget. Identical to JECall but no parent_link, so the
			// callee's Completed apply arm has no JECallResult to journal
			// back on this invocation.
			calleeID := mintCalleeInvocationID(id, entry.GetIndex(), e.OneWayCall.GetTarget())
			env := &enginev1.OutboxEnvelope{
				DestinationShardId: p.cfg.Partitioner.ShardForInvocation(calleeID),
				Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
					InvocationId:   calleeID,
					Target:         e.OneWayCall.GetTarget(),
					Input:          e.OneWayCall.GetInput(),
					IdempotencyKey: e.OneWayCall.GetIdempotencyKey(),
				}},
			}
			if _, err := p.enqueueOutbox(batch, meta, env, isLeader); err != nil {
				return fmt.Errorf("onInvokerEffect: outbox append (one-way call): %w", err)
			}
		case *enginev1.JournalEntry_SetState:
			// Persist state rows so eager preload on the next session start
			// can serve GetState without a journal scan.
			if t := statusTarget(cur); t != nil {
				lpT := keys.LPFromPartitionKey(bandedEntityPK(id, t.GetServiceName(), t.GetObjectKey()))
				if err := (tables.StateTable{S: batch}).Set(batch, lpT, t, e.SetState.GetKey(), e.SetState.GetValue()); err != nil {
					return fmt.Errorf("onInvokerEffect: state set: %w", err)
				}
			} else {
				p.cfg.Log.Warn("partition: JESetState on status without target",
					"status", fmt.Sprintf("%T", cur.GetStatus()))
			}
		case *enginev1.JournalEntry_ClearState:
			if t := statusTarget(cur); t != nil {
				lpT := keys.LPFromPartitionKey(bandedEntityPK(id, t.GetServiceName(), t.GetObjectKey()))
				if err := (tables.StateTable{S: batch}).Clear(batch, lpT, t, e.ClearState.GetKey()); err != nil {
					return fmt.Errorf("onInvokerEffect: state clear: %w", err)
				}
			} else {
				p.cfg.Log.Warn("partition: JEClearState on status without target",
					"status", fmt.Sprintf("%T", cur.GetStatus()))
			}
		case *enginev1.JournalEntry_ClearAllState:
			// Bulk-wipe every state row scoped to the invocation's (service,
			// object_key). Target is extracted from the active status
			// (Invoked/Suspended); Completed/Free/Scheduled here would
			// indicate a divergent SDK and is dropped with a warning (we
			// still append the journal entry above for replay parity).
			if t := statusTarget(cur); t != nil {
				lpT := keys.LPFromPartitionKey(bandedEntityPK(id, t.GetServiceName(), t.GetObjectKey()))
				if err := (tables.StateTable{S: batch}).ClearObject(batch, lpT, t); err != nil {
					return fmt.Errorf("onInvokerEffect: state clear-all: %w", err)
				}
			} else {
				p.cfg.Log.Warn("partition: JEClearAllState on status without target",
					"status", fmt.Sprintf("%T", cur.GetStatus()))
			}
		case *enginev1.JournalEntry_GetState:
			// Lazy state fetch. Two-slot — the SDK pre-allocated cmdSlot
			// and resultSlot (= cmdSlot+1). Read the StateTable row and
			// append JEGetStateResult inline so the SDK sees the answer
			// on the next session start.
			t := statusTarget(cur)
			if t == nil {
				p.cfg.Log.Warn("partition: JEGetState on status without target",
					"status", fmt.Sprintf("%T", cur.GetStatus()))
				break
			}
			lpT := keys.LPFromPartitionKey(bandedEntityPK(id, t.GetServiceName(), t.GetObjectKey()))
			key := e.GetState.GetKey()
			resultIdx := e.GetState.GetResultCompletionId()
			val, present, gerr := (tables.StateTable{S: batch}).Get(lpT, t, key)
			if gerr != nil {
				return fmt.Errorf("onInvokerEffect: state get: %w", gerr)
			}
			resultEntry := &enginev1.JournalEntry{
				Index: resultIdx,
				Entry: &enginev1.JournalEntry_GetStateResult{
					GetStateResult: &enginev1.JEGetStateResult{
						Value:   val,
						Present: present,
					},
				},
			}
			if err := journal.Append(batch, id, resultEntry); err != nil {
				return fmt.Errorf("onInvokerEffect: journal append (get_state result): %w", err)
			}
			// Wake the suspended session. The current session is still
			// alive (mid-frame-pump) so StartInvocation queues a
			// pendingRespawn; once the SDK suspends and the session
			// exits, watchSession spawns a fresh one that replays the
			// just-stamped result.
			if isLeader {
				p.cfg.Collector.Push(ActInvoke{ID: id, Target: t})
			}
		case *enginev1.JournalEntry_GetStateKeys:
			t := statusTarget(cur)
			if t == nil {
				p.cfg.Log.Warn("partition: JEGetStateKeys on status without target",
					"status", fmt.Sprintf("%T", cur.GetStatus()))
				break
			}
			resultIdx := e.GetStateKeys.GetResultCompletionId()
			var keysOut []string
			lpT := keys.LPFromPartitionKey(bandedEntityPK(id, t.GetServiceName(), t.GetObjectKey()))
			if err := (tables.StateTable{S: batch}).ScanObject(lpT, t, func(k string, _ []byte) error {
				keysOut = append(keysOut, k)
				return nil
			}); err != nil {
				return fmt.Errorf("onInvokerEffect: state scan: %w", err)
			}
			resultEntry := &enginev1.JournalEntry{
				Index: resultIdx,
				Entry: &enginev1.JournalEntry_GetStateKeysResult{
					GetStateKeysResult: &enginev1.JEGetStateKeysResult{Keys: keysOut},
				},
			}
			if err := journal.Append(batch, id, resultEntry); err != nil {
				return fmt.Errorf("onInvokerEffect: journal append (get_state_keys result): %w", err)
			}
			if isLeader {
				p.cfg.Collector.Push(ActInvoke{ID: id, Target: t})
			}
		case *enginev1.JournalEntry_GetStateResult,
			*enginev1.JournalEntry_GetStateKeysResult:
			// Result entries are appended directly by the engine in the
			// inline branches above — they never arrive via the SDK's
			// JournalAppended effect. Defensive no-op for forward-compat.
		case *enginev1.JournalEntry_GetEagerStateKeys:
			// Single-slot. The SDK already shipped the sorted keys list
			// inline; journal.Append above persisted it. The session is
			// not suspended (the answer was local) so no ActInvoke wake
			// is needed.
		case *enginev1.JournalEntry_Signal:
			// A signal stays within the sender's tenant: route and key the
			// target under the sender's band (forward-carried on SignalSend).
			senderTenant := keys.TenantFromPartitionKey(id.GetPartitionKey())
			env := &enginev1.OutboxEnvelope{
				DestinationShardId: p.cfg.Partitioner.ShardForTarget(senderTenant, e.Signal.GetTarget()),
				Kind: &enginev1.OutboxEnvelope_Signal{Signal: &enginev1.SignalSend{
					Target:     e.Signal.GetTarget(),
					SignalName: e.Signal.GetSignalName(),
					Payload:    e.Signal.GetPayload(),
					Tenant:     senderTenant,
				}},
			}
			if _, err := p.enqueueOutbox(batch, meta, env, isLeader); err != nil {
				return fmt.Errorf("onInvokerEffect: outbox append (signal): %w", err)
			}
		case *enginev1.JournalEntry_AwaitSignal:
			// Probe the inbox: a signal delivered before this WaitSignal
			// call sits in signal_inbox/<id>/<name>. On hit, append the
			// JESignalResult inline at result_completion_id and delete
			// the inbox row. On miss, write a SignalAwaiter directory
			// row so a future SignalDelivered can stitch the result.
			name := e.AwaitSignal.GetSignalName()
			resultIdx := e.AwaitSignal.GetResultCompletionId()
			inboxT := tables.SignalInboxTable{S: batch}
			buffered, berr := inboxT.Get(id, name)
			if berr != nil {
				return fmt.Errorf("onInvokerEffect: signal inbox lookup: %w", berr)
			}
			if buffered != nil {
				resultEntry := &enginev1.JournalEntry{
					Index: resultIdx,
					Entry: &enginev1.JournalEntry_SignalResult{
						SignalResult: &enginev1.JESignalResult{
							SignalName: name,
							Payload:    buffered.GetPayload(),
						},
					},
				}
				if err := journal.Append(batch, id, resultEntry); err != nil {
					return fmt.Errorf("onInvokerEffect: journal append (signal result inline): %w", err)
				}
				if err := inboxT.Delete(batch, id, name); err != nil {
					return fmt.Errorf("onInvokerEffect: inbox delete (consumed): %w", err)
				}
				// The current session emitted JEAwaitSignal expecting a
				// later notification. Since we just inlined the result
				// into the journal, the session will still see no
				// replay entry at result_idx (its StartMessage was
				// frozen earlier) and emit Suspension. Push ActInvoke
				// here so the Invoker queues a pendingRespawn — the
				// fresh session picks up JESignalResult and resumes.
				if isLeader {
					if t := statusTarget(cur); t != nil {
						p.cfg.Collector.Push(ActInvoke{ID: id, Target: t})
					}
				}
			} else {
				awaiter := &enginev1.SignalAwaiter{
					Owner:      id,
					EntryIndex: entry.GetIndex(),
				}
				if err := (tables.SignalAwaiterTable{S: batch}).Put(batch, id, name, awaiter); err != nil {
					return fmt.Errorf("onInvokerEffect: awaiter put: %w", err)
				}
			}
		case *enginev1.JournalEntry_SignalResult:
			// Receiver-side result entry — written by the apply arm
			// itself (via the JournalEntry_AwaitSignal inbox-hit branch
			// or the InvokerEffect_SignalDelivered awaiter-stitch
			// branch). No additional side effect here; the entry's
			// presence in the journal drives the SDK future on next
			// session start.
		case *enginev1.JournalEntry_GetPromise:
			// Workflow-scoped Promise(name).Result(). Probe the promise
			// row at promise/<svc>/<key>/<name>. On Resolved/Rejected,
			// append JEPromiseResult inline at result_completion_id and
			// wake; on pending/absent, write a PromiseAwaiter row so a
			// future JECompletePromise / InvokerEffect.PromiseCompleted
			// can stitch the result.
			//
			// Scope is carried explicitly on JEGetPromise as (service,
			// workflow_key) — the SDK populates them from either
			// Context.Promise (caller's own (svc, key)) or
			// Context.WorkflowPromise(target, name) (foreign target).
			// Cross-partition: a GetPromise whose (svc, key) lives on
			// another shard returns absent on the local read, so the
			// awaiter row gets written on the wrong shard. WorkflowPromise
			// .Result() across partitions is not supported in this step
			// — callers should co-locate (which they always do today,
			// since the SDK uses the caller's own (svc, key) by default).
			svc := e.GetPromise.GetService()
			wfKey := e.GetPromise.GetWorkflowKey()
			if svc == "" || wfKey == "" {
				p.cfg.Log.Warn("partition: JEGetPromise with empty scope", "service", svc, "workflow_key", wfKey)
				break
			}
			name := e.GetPromise.GetName()
			resultIdx := e.GetPromise.GetResultCompletionId()
			// Promise LP is keyed on the workflow's (svc, wfKey), which may
			// differ from the calling invocation's LP (cross-workflow
			// WorkflowPromise.Result()).
			lpP := keys.LPFromPartitionKey(bandedEntityPK(id, svc, wfKey))
			pv, perr := (tables.PromiseTable{S: batch}).Get(lpP, svc, wfKey, name)
			if perr != nil {
				return fmt.Errorf("onInvokerEffect: promise lookup: %w", perr)
			}
			if pv != nil && pv.GetPending() == nil {
				// Already terminal — inline result.
				resultEntry := &enginev1.JournalEntry{
					Index: resultIdx,
					Entry: &enginev1.JournalEntry_PromiseResult{
						PromiseResult: promiseResultFromValue(name, pv),
					},
				}
				if err := journal.Append(batch, id, resultEntry); err != nil {
					return fmt.Errorf("onInvokerEffect: journal append (promise result inline): %w", err)
				}
				if isLeader {
					p.cfg.Collector.Push(ActInvoke{ID: id, Target: statusTarget(cur)})
				}
			} else {
				awaiter := &enginev1.PromiseAwaiter{
					Owner:      id,
					EntryIndex: entry.GetIndex(),
				}
				if err := (tables.PromiseAwaiterTable{S: batch}).PutForSlot(batch, lpP, svc, wfKey, name, awaiter); err != nil {
					return fmt.Errorf("onInvokerEffect: promise awaiter put: %w", err)
				}
			}
		case *enginev1.JournalEntry_PeekPromise:
			// Single-slot snapshot. The up-front journal.Append wrote the
			// entry with empty fields; we mutate it to stamp the
			// completed/value/failure snapshot and re-append (batch.Set
			// overwrites). The SDK sees the stamped entry on replay.
			svc := e.PeekPromise.GetService()
			wfKey := e.PeekPromise.GetWorkflowKey()
			if svc == "" || wfKey == "" {
				p.cfg.Log.Warn("partition: JEPeekPromise with empty scope", "service", svc, "workflow_key", wfKey)
				break
			}
			name := e.PeekPromise.GetName()
			lpP := keys.LPFromPartitionKey(bandedEntityPK(id, svc, wfKey))
			pv, perr := (tables.PromiseTable{S: batch}).Get(lpP, svc, wfKey, name)
			if perr != nil {
				return fmt.Errorf("onInvokerEffect: peek promise lookup: %w", perr)
			}
			e.PeekPromise.Completed = false
			e.PeekPromise.Value = nil
			e.PeekPromise.FailureMessage = ""
			if pv != nil {
				if r := pv.GetResolved(); r != nil {
					e.PeekPromise.Completed = true
					e.PeekPromise.Value = r.GetValue()
				} else if rj := pv.GetRejected(); rj != nil {
					e.PeekPromise.Completed = true
					e.PeekPromise.FailureMessage = rj.GetFailureMessage()
				}
			}
			if err := journal.Append(batch, id, entry); err != nil {
				return fmt.Errorf("onInvokerEffect: journal append (peek stamp): %w", err)
			}
		case *enginev1.JournalEntry_CompletePromise:
			// Two-slot. The scope (service, workflow_key) is carried
			// explicitly on the JE — see WorkflowPromise — so the apply
			// path can route cross-partition when the promise's owning
			// shard differs from the resolver's. Two paths:
			//
			//   destShard == p.shardID  — local apply: write
			//     PromiseValue (if not already terminal), wake any
			//     awaiters via applyPromiseAwaiterScan, journal the
			//     JEPromiseCompleteResult on the resolver's journal in
			//     the same batch.
			//
			//   destShard != p.shardID  — cross-partition: enqueue an
			//     OutboxEnvelope.PromiseCompletion with caller_id +
			//     result_completion_id; do NOT journal the
			//     JEPromiseCompleteResult locally. The owner shard's
			//     apply arm runs the local-style apply, then enqueues a
			//     PromiseCompletionAck envelope back to this shard whose
			//     apply arm appends the ack journal entry.
			svc := e.CompletePromise.GetService()
			wfKey := e.CompletePromise.GetWorkflowKey()
			if svc == "" || wfKey == "" {
				p.cfg.Log.Warn("partition: JECompletePromise with empty scope", "service", svc, "workflow_key", wfKey)
				break
			}
			name := e.CompletePromise.GetName()
			resultIdx := e.CompletePromise.GetResultCompletionId()
			destShard := p.cfg.Partitioner.ShardForTarget(keys.TenantFromPartitionKey(id.GetPartitionKey()), &enginev1.InvocationTarget{
				ServiceName: svc,
				ObjectKey:   wfKey,
			})

			if destShard != 0 && destShard != p.shardID {
				// Cross-partition: ship to owner shard. caller_id +
				// result_completion_id ride the envelope so the owner
				// shard can route the ack back.
				env := &enginev1.OutboxEnvelope{
					DestinationShardId: destShard,
					Kind: &enginev1.OutboxEnvelope_PromiseCompletion{
						PromiseCompletion: &enginev1.PromiseCompleted{
							Service:            svc,
							WorkflowKey:        wfKey,
							PromiseName:        name,
							Value:              e.CompletePromise.GetValue(),
							FailureMessage:     e.CompletePromise.GetFailureMessage(),
							CallerId:           id,
							ResultCompletionId: resultIdx,
							Tenant:             keys.TenantFromPartitionKey(id.GetPartitionKey()),
						},
					},
				}
				if _, err := p.enqueueOutbox(batch, meta, env, isLeader); err != nil {
					return fmt.Errorf("onInvokerEffect: outbox append (promise completion): %w", err)
				}
				// Resolver invocation will suspend on result_completion_id
				// per transitionOnJournalAppend; the ack path appends
				// JEPromiseCompleteResult and wakes.
				break
			}

			// Local apply path.
			promiseT := tables.PromiseTable{S: batch}
			lpP := keys.LPFromPartitionKey(bandedEntityPK(id, svc, wfKey))
			cur_pv, cerr := promiseT.Get(lpP, svc, wfKey, name)
			if cerr != nil {
				return fmt.Errorf("onInvokerEffect: promise lookup (complete): %w", cerr)
			}
			succeeded := false
			conflictMsg := "promise already completed"
			if cur_pv == nil || cur_pv.GetPending() != nil {
				newPV := buildPromiseValueFromJournal(e.CompletePromise, nowMs)
				if err := promiseT.Put(batch, lpP, svc, wfKey, name, newPV); err != nil {
					return fmt.Errorf("onInvokerEffect: promise put: %w", err)
				}
				succeeded = true
				conflictMsg = ""
				if err := p.applyPromiseAwaiterScan(batch, inv, journal, keys.TenantFromPartitionKey(id.GetPartitionKey()), svc, wfKey, name, newPV, false, isLeader, nowMs); err != nil {
					return err
				}
			}
			// Append the result entry on the resolver's journal.
			ack := &enginev1.JournalEntry{
				Index: resultIdx,
				Entry: &enginev1.JournalEntry_PromiseCompleteResult{
					PromiseCompleteResult: &enginev1.JEPromiseCompleteResult{
						Succeeded:      succeeded,
						FailureMessage: conflictMsg,
					},
				},
			}
			if err := journal.Append(batch, id, ack); err != nil {
				return fmt.Errorf("onInvokerEffect: journal append (promise complete ack): %w", err)
			}
			// Wake the resolver so it sees the ack on respawn.
			if isLeader {
				p.cfg.Collector.Push(ActInvoke{ID: id, Target: statusTarget(cur)})
			}
		case *enginev1.JournalEntry_PromiseResult, *enginev1.JournalEntry_PromiseCompleteResult:
			// Receiver-side result entries — written by the apply arm
			// itself (via JEGetPromise inline-hit, JECompletePromise
			// awaiter-stitch, JECompletePromise ack, or the ingress
			// PromiseCompleted effect). No additional side effect.
		}
		next, actions, err = transitionOnJournalAppend(id, cur, k.JournalAppended, nowMs)
	case *enginev1.InvokerEffect_RunProposal:
		// The SDK has produced the outcome of a ctx.Run body; persist it as
		// a JERun journal entry at the SDK-allocated index.
		//
		// When retryable=true the apply arm computes a backoff via
		// NextRetryDelay and schedules a retry timer (reusing TimerTable;
		// onTimerFired peeks the journal at sleep_index to skip the usual
		// JESleepResult write when the entry is a JERun). If the policy is
		// exhausted the proposal demotes to terminal — JERun{retryable=false}
		// — so the SDK's next fast-replay surfaces the failure to the
		// handler.
		rp := k.RunProposal
		idx := rp.GetEntryIndex()

		// Engine-authoritative attempt counting: the SDK ships only the
		// outcome of the current fn invocation; we determine which attempt
		// that was by reading the prior JERun (if any). attempt is the
		// 1-based count of fn invocations completed for this slot.
		priorAttempt := uint32(0)
		priorFailures := []string(nil)
		if prior, rerr := journal.Read(id, idx); rerr == nil {
			if pr := prior.GetRun(); pr != nil {
				priorAttempt = pr.GetAttempt()
				priorFailures = pr.GetAttemptFailures()
			}
		} else if !errors.Is(rerr, storage.ErrNotFound) {
			return fmt.Errorf("onInvokerEffect: journal read (prior run): %w", rerr)
		}
		attempt := priorAttempt + 1

		// Append the latest retryable failure to the running history so the
		// journal explains a multi-attempt invocation. Terminal failures
		// also append (the final message is preserved alongside the prior
		// retries); successes leave the history as-is.
		failures := priorFailures
		if rp.GetRetryable() || rp.GetFailureMessage() != "" {
			failures = append(append([]string(nil), priorFailures...), rp.GetFailureMessage())
		}

		writeRun := func(retryable bool) error {
			return journal.Append(batch, id, &enginev1.JournalEntry{
				Index: idx,
				Entry: &enginev1.JournalEntry_Run{Run: &enginev1.JERun{
					Value:           rp.GetValue(),
					FailureMessage:  rp.GetFailureMessage(),
					Attempt:         attempt,
					Retryable:       retryable,
					AttemptFailures: failures,
				}},
			})
		}

		if !rp.GetRetryable() {
			if err := writeRun(false); err != nil {
				return fmt.Errorf("onInvokerEffect: journal append (run): %w", err)
			}
			next, actions = cur, nil
			break
		}

		// retryable=true — try to schedule a retry.
		delay, okPolicy := NextRetryDelay(rp.GetRetryPolicy(), attempt)
		if !okPolicy {
			// Policy exhausted — demote to terminal AND schedule an
			// immediate wake so the SDK observes the terminal JERun
			// on its next session. Without the wake the invocation
			// stays Suspended (the SDK emitted SuspensionMessage right
			// after the retryable proposal expecting a retry timer to
			// fire).
			if err := writeRun(false); err != nil {
				return fmt.Errorf("onInvokerEffect: journal append (run exhausted): %w", err)
			}
			fireAtMs := nowMs + 1
			if err := timersT.Insert(batch, fireAtMs, id, idx); err != nil {
				return fmt.Errorf("onInvokerEffect: timer insert (run exhausted): %w", err)
			}
			if isLeader {
				p.cfg.Collector.Push(ActRegisterTimer{
					FireAtMs: fireAtMs,
					ID:       id,
					SleepIdx: idx,
				})
			}
			next, actions = cur, nil
			break
		}

		// nowMs is the leader-stamped envelope wall clock — deterministic
		// across replicas (see applyCommand and proposer.go).
		fireAtMs := nowMs + uint64(delay/time.Millisecond)
		if fireAtMs <= nowMs {
			fireAtMs = nowMs + 1
		}
		if err := writeRun(true); err != nil {
			return fmt.Errorf("onInvokerEffect: journal append (run retry): %w", err)
		}
		if err := timersT.Insert(batch, fireAtMs, id, idx); err != nil {
			return fmt.Errorf("onInvokerEffect: timer insert (run retry): %w", err)
		}
		if isLeader {
			p.cfg.Collector.Push(ActRegisterTimer{
				FireAtMs: fireAtMs,
				ID:       id,
				SleepIdx: idx,
			})
		}
		// State stays as-is — the upcoming Suspended effect (proposed by
		// the SDK after this RunProposal) handles the FSM transition.
		next, actions = cur, nil
	case *enginev1.InvokerEffect_AwakeableResolved:
		akID := k.AwakeableResolved.GetAwakeableId()
		dir, dirErr := awakeT.Get(akID)
		if dirErr != nil {
			if errors.Is(dirErr, storage.ErrNotFound) {
				p.cfg.Log.Warn("partition: AwakeableResolved for unknown id", "id", akID)
				return nil
			}
			return fmt.Errorf("onInvokerEffect: awakeable lookup: %w", dirErr)
		}
		// Place the result entry one index past the originating JEAwakeable
		// (mirrors the SleepResult-at-sleep_index+1 convention).
		resultIdx := dir.GetEntryIndex() + 1
		resultEntry := &enginev1.JournalEntry{
			Index: resultIdx,
			Entry: &enginev1.JournalEntry_AwakeableResult{
				AwakeableResult: &enginev1.JEAwakeableResult{
					AwakeableId:    akID,
					Value:          k.AwakeableResolved.GetValue(),
					FailureMessage: k.AwakeableResolved.GetFailureMessage(),
				},
			},
		}
		if err := journal.Append(batch, id, resultEntry); err != nil {
			return fmt.Errorf("onInvokerEffect: journal append (awakeable result): %w", err)
		}
		if err := awakeT.Delete(batch, akID); err != nil {
			return fmt.Errorf("onInvokerEffect: awakeable delete: %w", err)
		}
		next, actions, err = transitionOnAwakeableResolved(
			id, cur, resultIdx,
			k.AwakeableResolved.GetValue(),
			k.AwakeableResolved.GetFailureMessage(),
			nowMs,
		)
	case *enginev1.InvokerEffect_SignalDelivered:
		// Receiver-side: resolve Target → active InvocationId via
		// KeyLeaseTable.current_invocation, then route. The well-known
		// __cancel__ name short-circuits to a terminal Completed; other
		// signal names will land in the per-(inv, name) inbox once
		// Step 2 lands the inbox + awaiter tables. For Step 1 we drop
		// non-cancel signals with a warning — there's no reader yet.
		sigTarget := k.SignalDelivered.GetTarget()
		sigName := k.SignalDelivered.GetSignalName()
		if sigTarget.GetObjectKey() == "" {
			p.cfg.Log.Warn("partition: signal delivered for unkeyed target", "service", sigTarget.GetServiceName(), "handler", sigTarget.GetHandlerName())
			return nil
		}
		// Freeze gate (Target-routed variant). Band by the sender's tenant,
		// forward-carried on SignalDelivered, so the key-lease lookup hits
		// the same LP the target invocation was minted under.
		sigTenant := k.SignalDelivered.GetTenant()
		if err := p.checkLPFreeze(batch, routing.PartitionKey(sigTenant, sigTarget.GetServiceName(), sigTarget.GetObjectKey())); err != nil {
			return err
		}
		klt := tables.KeyLeaseTable{S: batch}
		sigLP := keys.LPFromPartitionKey(routing.PartitionKey(sigTenant, sigTarget.GetServiceName(), sigTarget.GetObjectKey()))
		lease, lerr := klt.Get(sigLP, sigTarget.GetServiceName(), sigTarget.GetObjectKey())
		if lerr != nil {
			return fmt.Errorf("onInvokerEffect: signal key-lease lookup: %w", lerr)
		}
		if lease == nil || lease.GetState() != enginev1.KeyLeaseStatus_ACTIVE {
			p.cfg.Log.Warn("partition: signal to inactive key dropped",
				"service", sigTarget.GetServiceName(),
				"object_key", sigTarget.GetObjectKey(),
				"signal", sigName)
			return nil
		}
		activeID := lease.GetCurrentInvocation()
		activeCur, gerr := inv.Get(activeID)
		if gerr != nil {
			return fmt.Errorf("onInvokerEffect: signal load active status: %w", gerr)
		}
		if sigName == wire.WellKnownCancelSignal {
			completed := &enginev1.InvocationCompleted{
				FailureMessage: "invocation cancelled",
				FailureCode:    wire.CancelledCode,
			}
			cnext, cacts, cerr := p.applyTerminalCompletion(batch, activeID, activeCur, completed, inv, journal, timersT, meta, isLeader, nowMs)
			if cerr != nil {
				return cerr
			}
			if cnext != nil {
				if perr := inv.Put(batch, activeID, cnext); perr != nil {
					return fmt.Errorf("onInvokerEffect: write status (cancel): %w", perr)
				}
			}
			if isLeader {
				for _, a := range cacts {
					p.cfg.Collector.Push(a)
				}
			}
			return nil
		}
		// Non-cancel signal: stitch into a pending JEAwaitSignal if
		// the handler is already waiting, otherwise buffer in the
		// inbox so a future WaitSignal(name) call can consume it.
		// Either way the FSM wakes the invocation so the session can
		// observe the result.
		payload := k.SignalDelivered.GetPayload()
		awaiterT := tables.SignalAwaiterTable{S: batch}
		inboxT := tables.SignalInboxTable{S: batch}
		awaiter, aerr := awaiterT.Get(activeID, sigName)
		if aerr != nil {
			return fmt.Errorf("onInvokerEffect: signal awaiter lookup: %w", aerr)
		}
		if awaiter != nil {
			// Stitch result at awaiter.entry_index + 1 (= the
			// result_completion_id the SDK allocated when emitting
			// JEAwaitSignal).
			resultIdx := awaiter.GetEntryIndex() + 1
			resultEntry := &enginev1.JournalEntry{
				Index: resultIdx,
				Entry: &enginev1.JournalEntry_SignalResult{
					SignalResult: &enginev1.JESignalResult{
						SignalName: sigName,
						Payload:    payload,
					},
				},
			}
			if err := journal.Append(batch, activeID, resultEntry); err != nil {
				return fmt.Errorf("onInvokerEffect: journal append (signal result stitch): %w", err)
			}
			if err := awaiterT.Delete(batch, activeID, sigName); err != nil {
				return fmt.Errorf("onInvokerEffect: awaiter delete: %w", err)
			}
		} else {
			// No pending awaiter — buffer in the inbox. The next
			// WaitSignal(name) consumes it synchronously.
			entry := &enginev1.SignalInboxEntry{
				SignalName:    sigName,
				Payload:       payload,
				DeliveredAtMs: nowMs,
			}
			if err := inboxT.Put(batch, activeID, sigName, entry); err != nil {
				return fmt.Errorf("onInvokerEffect: inbox put: %w", err)
			}
		}
		id, cur = activeID, activeCur
		next, actions, err = transitionOnSignalDelivered(
			activeID, activeCur, 0,
			sigName,
			payload,
			nowMs,
		)
	case *enginev1.InvokerEffect_Completed:
		cnext, cacts, cerr := p.applyTerminalCompletion(batch, id, cur, k.Completed, inv, journal, timersT, meta, isLeader, nowMs)
		next, actions, err = cnext, cacts, cerr
	case *enginev1.InvokerEffect_PromiseCompleted:
		// Two entry points land here:
		//   - Ingress.ResolveWorkflowPromise: caller_id unset; conflict
		//     is silent (the RPC reply already carries the result).
		//   - Cross-partition OutboxEnvelope.PromiseCompletion: caller_id
		//     is set; on apply we additionally enqueue a
		//     PromiseCompletionAck back to the producer shard so the
		//     resolver's JEPromiseCompleteResult lands with the right
		//     succeeded/conflict bit.
		// Apply path: write PromiseValue (if not already terminal); wake
		// every pending awaiter via transitionOnPromiseResolved +
		// ActInvoke. The helper writes each awaiter's inv.Put and pushes
		// its actions directly.
		pc := k.PromiseCompleted
		svc := pc.GetService()
		wk := pc.GetWorkflowKey()
		name := pc.GetPromiseName()
		if svc == "" || wk == "" || name == "" {
			p.cfg.Log.Warn("partition: PromiseCompleted with empty addressing",
				"service", svc, "key", wk, "name", name)
			return nil
		}
		// Freeze gate ((service, workflow_key)-routed variant). Band by the
		// forward-carried tenant so the promise/awaiter rows land under the
		// workflow's LP band on this owner shard.
		pcTenant := pc.GetTenant()
		if err := p.checkLPFreeze(batch, routing.PartitionKey(pcTenant, svc, wk)); err != nil {
			return err
		}
		promiseT := tables.PromiseTable{S: batch}
		lpP := keys.LPFromPartitionKey(routing.PartitionKey(pcTenant, svc, wk))
		cur_pv, perr := promiseT.Get(lpP, svc, wk, name)
		if perr != nil {
			return fmt.Errorf("onInvokerEffect: promise lookup (ingress): %w", perr)
		}
		succeeded := false
		conflictMsg := "promise already completed"
		if cur_pv == nil || cur_pv.GetPending() != nil {
			newPV := buildPromiseValueFromEffect(pc, nowMs)
			if err := promiseT.Put(batch, lpP, svc, wk, name, newPV); err != nil {
				return fmt.Errorf("onInvokerEffect: promise put (ingress): %w", err)
			}
			succeeded = true
			conflictMsg = ""
			if err := p.applyPromiseAwaiterScan(batch, inv, journal, pcTenant, svc, wk, name, newPV, true, isLeader, nowMs); err != nil {
				return err
			}
		}
		// Cross-partition: route the succeeded/conflict ack back to the
		// resolver shard. caller_id zero indicates ingress (no ack
		// needed; the RPC reply carries the signal).
		if cid := pc.GetCallerId(); cid != nil && cid.GetUuid() != nil {
			callerShard := p.cfg.Partitioner.ShardForInvocation(cid)
			ackEnv := &enginev1.OutboxEnvelope{
				DestinationShardId: callerShard,
				Kind: &enginev1.OutboxEnvelope_PromiseCompletionAck{
					PromiseCompletionAck: &enginev1.PromiseCompletionAck{
						CallerId:           cid,
						ResultCompletionId: pc.GetResultCompletionId(),
						Succeeded:          succeeded,
						FailureMessage:     conflictMsg,
					},
				},
			}
			if _, err := p.enqueueOutbox(batch, meta, ackEnv, isLeader); err != nil {
				return fmt.Errorf("onInvokerEffect: outbox append (promise completion ack): %w", err)
			}
		}
		// All awaiter state changes applied inside the helper; the
		// post-switch inv.Put/actions loop is a no-op here.
		return nil
	case *enginev1.InvokerEffect_Suspended:
		next, actions, err = transitionOnSuspend(id, cur, k.Suspended, nowMs)
	case nil:
		p.cfg.Log.Warn("partition: InvokerEffect with no kind")
		return nil
	default:
		p.cfg.Log.Warn("partition: unknown InvokerEffect kind", "kind", fmt.Sprintf("%T", k))
		return nil
	}
	if err != nil {
		p.cfg.Log.Warn("partition: invalid InvokerEffect transition", "err", err)
		return nil
	}
	if next != nil {
		if err := inv.Put(batch, id, next); err != nil {
			return fmt.Errorf("onInvokerEffect: write status: %w", err)
		}
	}
	if isLeader {
		for _, a := range actions {
			p.cfg.Collector.Push(a)
		}
	}
	return nil
}

// applyTerminalCompletion folds the terminal-effect post-processing
// (FSM transition + parent JECallResult delivery + key-lease release +
// timer reap) into a reusable helper. The InvokerEffect_Completed apply
// arm uses it directly; the InvokerEffect_SignalDelivered cancel branch
// uses it after synthesizing an InvocationCompleted with FailureCode=
// CancelledCode. The caller is responsible for persisting the returned
// next status via inv.Put and pushing actions onto the collector.
func (p *Partition) applyTerminalCompletion(
	batch storage.Batch,
	id *enginev1.InvocationId,
	cur *enginev1.InvocationStatus,
	completed *enginev1.InvocationCompleted,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	timersT tables.TimerTable,
	meta *enginev1.PartitionMeta,
	isLeader bool,
	nowMs uint64,
) (*enginev1.InvocationStatus, []Action, error) {
	next, actions, err := transitionOnComplete(id, cur, completed, nowMs)
	if err != nil {
		return next, actions, err
	}
	// Extract parent_link + terminating target from the pre-transition
	// status. Completed → Completed (idempotent replay) falls through
	// with both nil, so the parent delivery / lease release / timer
	// reap are naturally skipped.
	var pl *enginev1.ParentLink
	var completedTarget *enginev1.InvocationTarget
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Scheduled:
		pl = s.Scheduled.GetParentLink()
		completedTarget = s.Scheduled.GetTarget()
	case *enginev1.InvocationStatus_Invoked:
		pl = s.Invoked.GetParentLink()
		completedTarget = s.Invoked.GetTarget()
	case *enginev1.InvocationStatus_Suspended:
		pl = s.Suspended.GetParentLink()
		completedTarget = s.Suspended.GetTarget()
	}
	// Outcome metric: classify and emit on the leader only so cluster-
	// wide aggregation isn't multiplied by replica count. completedTarget
	// nil means this was an idempotent replay — already counted.
	if isLeader && completedTarget != nil && p.cfg.Metrics != nil {
		p.cfg.Metrics.InvocationsCompleted.WithLabelValues(
			completedTarget.GetServiceName(),
			classifyCompletionOutcome(completed),
		).Inc()
	}
	if pl.GetParentId() != nil {
		parentActs, perr := p.deliverCallResultToParent(batch, inv, journal, meta, pl, completed.GetOutput(), completed.GetFailureMessage(), nowMs, isLeader)
		if perr != nil {
			return next, actions, perr
		}
		actions = append(actions, parentActs...)
	} else if pp := pl.GetProcessParent(); pp != nil {
		// This invocation was an iflow service task. Feed its result back to the
		// awaiting node as a ProcessEvent{task_completed} instead of a
		// JECallResult. Delivery pushes its own actions (same-shard activation /
		// outbox dispatch) onto the collector, so nothing is appended here.
		ev := &enginev1.ProcessEvent{
			Pk:            pp.GetPk(),
			Service:       pp.GetService(),
			InstanceKey:   pp.GetInstanceKey(),
			LogicalTimeMs: nowMs,
			Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
				TaskCompleted: &enginev1.ProcessTaskCompleted{
					NodeId:         pp.GetNodeId(),
					InstanceIdx:    pp.GetInstanceIdx(),
					Output:         completed.GetOutput(),
					Failed:         completed.GetFailureMessage() != "",
					FailureMessage: completed.GetFailureMessage(),
				},
			}},
		}
		if derr := p.deliverProcessEvent(batch, meta, ev, nowMs, isLeader); derr != nil {
			return next, actions, derr
		}
	}
	if completedTarget.GetObjectKey() != "" {
		leaseActs, rerr := p.releaseKeyLease(batch, keys.TenantFromPartitionKey(id.GetPartitionKey()), completedTarget)
		if rerr != nil {
			return next, actions, rerr
		}
		actions = append(actions, leaseActs...)
	}
	if completedTarget != nil {
		var pending []uint64
		if serr := timersT.ScanByInvocation(id, func(fireAt uint64) error {
			pending = append(pending, fireAt)
			return nil
		}); serr != nil {
			return next, actions, fmt.Errorf("applyTerminalCompletion: scan timers: %w", serr)
		}
		for _, fireAt := range pending {
			if derr := timersT.Delete(batch, fireAt, id); derr != nil {
				return next, actions, fmt.Errorf("applyTerminalCompletion: delete timer: %w", derr)
			}
			if isLeader {
				actions = append(actions, ActDeleteTimer{FireAtMs: fireAt, ID: id})
			}
		}
	}
	// Retention: every Completed invocation schedules exactly one reap
	// keyed by its invocation id. The window is the longer workflow
	// retention when this invocation is still the workflow run for its
	// (service, object_key) — WorkflowRunTable only carries a row for
	// KIND_WORKFLOW Run handlers, so a hit + id match is the safe Kind
	// discriminator without persisting Kind on InvocationStatus — and the
	// shorter invocation retention otherwise. completedTarget==nil marks
	// an idempotent replay (already Completed); skip so we don't double-
	// schedule.
	if completedTarget != nil {
		// Window: the invoker stamps the completing deployment's resolved
		// invocation/workflow windows onto InvocationCompleted (this apply
		// path can't read shard-0's DeploymentRecord). Zero → fall back to
		// the engine limits default (the cancel-synthesis path has no
		// invoker). Workflow window applies only when this invocation is
		// still the workflow run for its (service, object_key) — the
		// workflow_run match is the Kind discriminator without persisting
		// Kind on InvocationStatus.
		retention := completed.GetInvocationRetentionMs()
		if retention == 0 {
			retention = limits.DefaultInvocationRetentionMs
		}
		if completedTarget.GetObjectKey() != "" {
			runT := tables.WorkflowRunTable{S: batch}
			runLP := keys.LPFromPartitionKey(bandedEntityPK(id, completedTarget.GetServiceName(), completedTarget.GetObjectKey()))
			runRow, rerr := runT.Get(runLP, completedTarget.GetServiceName(), completedTarget.GetObjectKey())
			if rerr != nil {
				return next, actions, fmt.Errorf("applyTerminalCompletion: workflow_run lookup: %w", rerr)
			}
			if runRow != nil && runRow.GetPartitionKey() == id.GetPartitionKey() && bytes.Equal(runRow.GetUuid(), id.GetUuid()) {
				retention = completed.GetWorkflowRetentionMs()
				if retention == 0 {
					retention = limits.DefaultWorkflowRetentionMs
				}
			}
		}
		fireAt := nowMs + retention
		if werr := (tables.ReapTable{S: batch}).Put(batch, fireAt, id); werr != nil {
			return next, actions, fmt.Errorf("applyTerminalCompletion: reap put: %w", werr)
		}
		if isLeader {
			actions = append(actions, ActScheduleReap{FireAtMs: fireAt, ID: id})
		}
	}
	return next, actions, nil
}

// applyPromiseAwaiterScan iterates every PromiseAwaiter row at
// (svc, workflowKey, name), stitches JEPromiseResult on each owner's
// journal at awaiter.entry_index+1, deletes each row, and wakes each
// owner.
//
// Two wake modes:
//   - runFSM=false (JECompletePromise resolver path): the awaiter wakes
//     via an ActInvoke pushed directly onto the collector; the next
//     replay picks up the new journal entry. No FSM transition — the
//     post-switch flow in onInvokerEffect runs the resolver's own
//     transitionOnJournalAppend.
//   - runFSM=true (InvokerEffect.PromiseCompleted ingress path): the
//     helper runs transitionOnPromiseResolved per owner and persists
//     the new status via inv.Put inline, pushing per-owner actions onto
//     the collector. The caller must skip the post-switch
//     inv.Put/actions loop (the apply arm returns nil after calling
//     this helper in runFSM mode).
//
// Awaiters scope to (svc, workflow_key) so they share the workflow's
// shard; the scan is local. The target carried in ActInvoke is the
// workflow's (service, object_key) — owners are co-located by design.
func (p *Partition) applyPromiseAwaiterScan(
	batch storage.Batch,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	tenant uint32,
	svc, workflowKey, name string,
	newPV *enginev1.PromiseValue,
	runFSM bool,
	isLeader bool,
	nowMs uint64,
) error {
	awaiterT := tables.PromiseAwaiterTable{S: batch}
	lpP := keys.LPFromPartitionKey(routing.PartitionKey(tenant, svc, workflowKey))
	var awaiters []*enginev1.PromiseAwaiter
	if err := awaiterT.ScanForName(lpP, svc, workflowKey, name, func(a *enginev1.PromiseAwaiter) error {
		awaiters = append(awaiters, proto.Clone(a).(*enginev1.PromiseAwaiter))
		return nil
	}); err != nil {
		return fmt.Errorf("onInvokerEffect: promise awaiter scan: %w", err)
	}
	wakeTarget := &enginev1.InvocationTarget{ServiceName: svc, ObjectKey: workflowKey}
	for _, a := range awaiters {
		resultIdx := a.GetEntryIndex() + 1
		resultEntry := &enginev1.JournalEntry{
			Index: resultIdx,
			Entry: &enginev1.JournalEntry_PromiseResult{
				PromiseResult: promiseResultFromValue(name, newPV),
			},
		}
		if err := journal.Append(batch, a.GetOwner(), resultEntry); err != nil {
			return fmt.Errorf("onInvokerEffect: journal append (promise result stitch): %w", err)
		}
		if err := awaiterT.DeleteForSlot(batch, lpP, svc, workflowKey, name, a.GetEntryIndex()); err != nil {
			return fmt.Errorf("onInvokerEffect: promise awaiter delete: %w", err)
		}
		if !runFSM {
			if isLeader {
				p.cfg.Collector.Push(ActInvoke{ID: a.GetOwner(), Target: wakeTarget})
			}
			continue
		}
		ownerCur, gerr := inv.Get(a.GetOwner())
		if gerr != nil {
			return fmt.Errorf("onInvokerEffect: load awaiter status: %w", gerr)
		}
		ownerNext, ownerActs, terr := transitionOnPromiseResolved(a.GetOwner(), ownerCur, resultIdx, nowMs)
		if terr != nil {
			p.cfg.Log.Warn("partition: invalid promise-resolved transition",
				"owner", a.GetOwner(), "err", terr)
			continue
		}
		if ownerNext != nil {
			if err := inv.Put(batch, a.GetOwner(), ownerNext); err != nil {
				return fmt.Errorf("onInvokerEffect: write awaiter status: %w", err)
			}
		}
		if isLeader {
			for _, act := range ownerActs {
				p.cfg.Collector.Push(act)
			}
		}
	}
	return nil
}

// deliverCallResultToParent dispatches the callee's terminal result back
// to the parent invocation. When the parent lives on the local partition
// (parentShard == localShard) the call applies inline via
// applyCallResultToParent (loads parent status, appends JECallResult,
// runs transitionOnCallResultDelivered, persists). When the parent lives
// on a different partition the call enqueues an
// OutboxEnvelope_DeliverCallResult on the local outbox; the destination
// partition's apply path will run the same logic via onDeliverCallResult.
//
// Returns the parent-side actions to push onto the collector. Cross-shard
// dispatches return no actions on this side; the destination shard
// generates the wake actions when DeliverCallResult applies there.
func (p *Partition) deliverCallResultToParent(batch storage.Batch, inv tables.InvocationTable, journal tables.JournalTable, meta *enginev1.PartitionMeta, pl *enginev1.ParentLink, output []byte, failureMessage string, nowMs uint64, isLeader bool) ([]Action, error) {
	parentID := pl.GetParentId()
	parentShard := p.cfg.Partitioner.ShardForInvocation(parentID)

	// Same-shard (or single-partition fallback): apply inline.
	if parentShard == 0 || parentShard == p.shardID {
		return p.applyCallResultToParent(batch, inv, journal, parentID, pl.GetCallIndex(), output, failureMessage, nowMs)
	}

	// Cross-shard: enqueue an outbox row addressed to the parent's shard.
	env := &enginev1.OutboxEnvelope{
		DestinationShardId: parentShard,
		Kind: &enginev1.OutboxEnvelope_DeliverCallResult{
			DeliverCallResult: &enginev1.DeliverCallResult{
				ParentId:       parentID,
				CallIndex:      pl.GetCallIndex(),
				Result:         output,
				FailureMessage: failureMessage,
			},
		},
	}
	if _, err := p.enqueueOutbox(batch, meta, env, isLeader); err != nil {
		return nil, fmt.Errorf("deliverCallResultToParent: outbox append: %w", err)
	}
	return nil, nil
}

// applyCallResultToParent runs the local-side journal + FSM transition
// for a parent-bound JECallResult. Used by both the same-partition fast
// path (deliverCallResultToParent inline) and the cross-partition
// receiver-side apply arm (onDeliverCallResult). Idempotent on replay:
// JournalTable.Append is a no-op at an existing index, and
// transitionOnCallResultDelivered is a no-op when the parent is already
// Completed.
func (p *Partition) applyCallResultToParent(
	batch storage.Batch,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	parentID *enginev1.InvocationId,
	callIndex uint32,
	output []byte,
	failureMessage string,
	nowMs uint64,
) ([]Action, error) {
	parentStatus, err := inv.Get(parentID)
	if err != nil {
		return nil, fmt.Errorf("applyCallResultToParent: load parent status: %w", err)
	}
	completionIdx := callIndex + 1
	resultEntry := &enginev1.JournalEntry{
		Index: completionIdx,
		Entry: &enginev1.JournalEntry_CallResult{
			CallResult: &enginev1.JECallResult{
				CallIndex:      callIndex,
				Result:         output,
				FailureMessage: failureMessage,
			},
		},
	}
	if err := journal.Append(batch, parentID, resultEntry); err != nil {
		return nil, fmt.Errorf("applyCallResultToParent: journal append: %w", err)
	}
	parentNext, parentActions, terr := transitionOnCallResultDelivered(
		parentID, parentStatus, completionIdx, output, failureMessage, nowMs,
	)
	if terr != nil {
		p.cfg.Log.Warn("partition: invalid CallResultDelivered transition on parent",
			"err", terr, "parent_uuid", fmt.Sprintf("%x", parentID.GetUuid()))
		return nil, nil
	}
	if parentNext != nil {
		if err := inv.Put(batch, parentID, parentNext); err != nil {
			return nil, fmt.Errorf("applyCallResultToParent: persist parent status: %w", err)
		}
	}
	return parentActions, nil
}

// onDeliverCallResult is the cross-partition apply arm. The DeliverCallResult
// command landed on the parent's shard via the outbox → Delivery gRPC →
// Raft pipeline; from here on the logic is identical to the same-shard path.
func (p *Partition) onDeliverCallResult(
	batch storage.Batch,
	_ storage.Store,
	cmd *enginev1.DeliverCallResult,
	nowMs uint64,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	isLeader bool,
) error {
	if err := p.checkLPFreeze(batch, cmd.GetParentId().GetPartitionKey()); err != nil {
		return err
	}
	actions, err := p.applyCallResultToParent(
		batch, inv, journal,
		cmd.GetParentId(), cmd.GetCallIndex(),
		cmd.GetResult(), cmd.GetFailureMessage(),
		nowMs,
	)
	if err != nil {
		return err
	}
	if isLeader {
		for _, a := range actions {
			p.cfg.Collector.Push(a)
		}
	}
	return nil
}

// onOutboxAck pops the producer-side outbox row referenced by the ack.
// Same-shard producers receive their ack via the standard
// arbitrary-dedup pop in the Update loop (because the ack-on-receive
// path emits an outbox-shaped command); cross-shard producers receive
// their ack here. Misrouted acks (producer_shard != local) are dropped
// silently so replays / fan-out cannot corrupt unrelated outboxes.
func (p *Partition) onOutboxAck(batch storage.Batch, ack *enginev1.OutboxAck) error {
	if ack.GetProducerShardId() != p.shardID {
		p.cfg.Log.Warn("partition: OutboxAck for foreign shard; dropping",
			"ack_producer_shard", ack.GetProducerShardId(),
			"local_shard", p.shardID,
			"seq", ack.GetProducerSeq())
		return nil
	}
	outboxT := tables.OutboxTable{S: batch}
	if err := outboxT.Pop(batch, ack.GetProducerSeq()); err != nil {
		p.cfg.Log.Warn("partition: outbox pop (via ack) failed",
			"seq", ack.GetProducerSeq(), "err", err)
	}
	return nil
}

// onPromiseCompletionAck applies the cross-partition acknowledgement that
// a JECompletePromise has landed on the workflow's owning shard. The
// resolver invocation lives on this shard; we append
// JEPromiseCompleteResult at the recorded result_completion_id and
// transition (Suspended → Invoked) so it wakes.
//
// Idempotent on replay: if the journal slot is already populated
// (another ack already landed), the append is a no-op overwrite; the
// FSM transition is a no-op when the invocation is already Invoked.
func (p *Partition) onPromiseCompletionAck(
	batch storage.Batch,
	ack *enginev1.PromiseCompletionAck,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	nowMs uint64,
	isLeader bool,
) error {
	callerID := ack.GetCallerId()
	if callerID == nil {
		p.cfg.Log.Warn("partition: PromiseCompletionAck with nil caller_id; dropping")
		return nil
	}
	if err := p.checkLPFreeze(batch, callerID.GetPartitionKey()); err != nil {
		return err
	}
	cur, err := inv.Get(callerID)
	if err != nil {
		return fmt.Errorf("onPromiseCompletionAck: load status: %w", err)
	}
	resultIdx := ack.GetResultCompletionId()
	entry := &enginev1.JournalEntry{
		Index: resultIdx,
		Entry: &enginev1.JournalEntry_PromiseCompleteResult{
			PromiseCompleteResult: &enginev1.JEPromiseCompleteResult{
				Succeeded:      ack.GetSucceeded(),
				FailureMessage: ack.GetFailureMessage(),
			},
		},
	}
	if err := journal.Append(batch, callerID, entry); err != nil {
		return fmt.Errorf("onPromiseCompletionAck: journal append: %w", err)
	}
	next, actions, terr := transitionOnJournalAppend(callerID, cur, &enginev1.JournalEntryAppended{Entry: entry}, nowMs)
	if terr != nil {
		p.cfg.Log.Warn("partition: invalid PromiseCompletionAck transition", "err", terr)
		return nil
	}
	if next != nil {
		if err := inv.Put(batch, callerID, next); err != nil {
			return fmt.Errorf("onPromiseCompletionAck: write status: %w", err)
		}
	}
	if isLeader {
		for _, a := range actions {
			p.cfg.Collector.Push(a)
		}
	}
	return nil
}

// releaseKeyLease handles VO gate cleanup. When a keyed invocation
// transitions to Completed, this fires vobjComplete on the per-key FSM
// and writes the resulting KeyLeaseStatus back into the same Pebble
// batch. If a queued invocation was waiting, the FSM's onActivate hook
// captures an ActInvoke for it; the caller appends those actions to the
// apply path's collector.
//
// Idempotent on replay: the caller guards entry via cur.GetStatus() so a
// second Completed apply pass (which finds cur already Completed) never
// enters this function.

// onReap deletes the durable footprint of a Completed invocation whose
// retention window has elapsed. Always: the originating reap row + the
// per-invocation rows (purgeInvocationRows). If this id is still the
// workflow run for its (service, key) — workflow_run points at it — it
// additionally clears the entity-scoped state / promise /
// promise_awaiter / workflow_run rows for that key.
//
// Idempotent: the reap row is removed in the same batch even on a no-op
// apply, so a duplicate proposal from a stale leader can't re-add it.
// The entity-cleanup arm is guarded by the workflow_run pointer so a
// re-claimed key's fresh run keeps its state.
func (p *Partition) onReap(
	batch storage.Batch,
	cmd *enginev1.ReapInvocation,
	inv tables.InvocationTable,
	journal tables.JournalTable,
) error {
	id := cmd.GetInvocationId()
	if id == nil {
		p.cfg.Log.Warn("partition: ReapInvocation with nil id")
		return nil
	}
	if err := p.checkLPFreeze(batch, id.GetPartitionKey()); err != nil {
		return err
	}
	// Always delete the originating reap row — even on a no-op apply.
	if err := (tables.ReapTable{S: batch}).Delete(batch, cmd.GetFireAtMs(), id); err != nil {
		return fmt.Errorf("onReap: reap row delete: %w", err)
	}
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onReap: load status: %w", err)
	}
	if _, _, terr := transitionOnPurge(id, cur); terr != nil {
		// Not in a purgeable state (Completed/Free). A row can't normally
		// leave Completed, so this is a stale/duplicate fire; leave rows.
		p.cfg.Log.Warn("partition: Reap on non-purgeable status; skipping",
			"status", fmt.Sprintf("%T", cur.GetStatus()))
		return nil
	}

	// Entity-scoped cleanup: only when this id is still the workflow run
	// for its (service, key). Plain invocations and virtual-object calls
	// own no workflow_run row; a re-claimed key points workflow_run at a
	// newer run, so deleting here would clobber the new run's state.
	if c := cur.GetCompleted(); c != nil {
		if target := c.GetTarget(); target != nil && target.GetObjectKey() != "" {
			svc, wfKey := target.GetServiceName(), target.GetObjectKey()
			lpW := keys.LPFromPartitionKey(bandedEntityPK(id, svc, wfKey))
			runT := tables.WorkflowRunTable{S: batch}
			runRow, rerr := runT.Get(lpW, svc, wfKey)
			if rerr != nil {
				return fmt.Errorf("onReap: workflow_run lookup: %w", rerr)
			}
			if runRow != nil && runRow.GetPartitionKey() == id.GetPartitionKey() && bytes.Equal(runRow.GetUuid(), id.GetUuid()) {
				if err := (tables.StateTable{S: batch}).ClearObject(batch, lpW, &enginev1.InvocationTarget{ServiceName: svc, ObjectKey: wfKey}); err != nil {
					return fmt.Errorf("onReap: state clear-object: %w", err)
				}
				if err := (tables.PromiseTable{S: batch}).DeleteAllForWorkflow(batch, lpW, svc, wfKey); err != nil {
					return fmt.Errorf("onReap: promise delete-all: %w", err)
				}
				if err := (tables.PromiseAwaiterTable{S: batch}).DeleteAllForWorkflow(batch, lpW, svc, wfKey); err != nil {
					return fmt.Errorf("onReap: promise_awaiter delete-all: %w", err)
				}
				if err := runT.Delete(batch, lpW, svc, wfKey); err != nil {
					return fmt.Errorf("onReap: workflow_run delete: %w", err)
				}
			}
		}
	}

	return p.purgeInvocationRows(batch, id, inv, journal)
}

func (p *Partition) releaseKeyLease(batch storage.Batch, tenant uint32, target *enginev1.InvocationTarget) ([]Action, error) {
	klt := tables.KeyLeaseTable{S: batch}
	lp := keys.LPFromPartitionKey(routing.PartitionKey(tenant, target.GetServiceName(), target.GetObjectKey()))
	cur, err := klt.Get(lp, target.GetServiceName(), target.GetObjectKey())
	if err != nil {
		return nil, fmt.Errorf("releaseKeyLease: load: %w", err)
	}
	if cur == nil || cur.GetState() == enginev1.KeyLeaseStatus_IDLE {
		// Nothing to release. Shouldn't happen for keyed completions, but
		// tolerate (e.g. crash-recovery replay where the lease was already
		// rewritten by a prior apply pass).
		return nil, nil
	}
	var leaseActs []Action
	sm, next := buildObjectFSM(cur, func(activated *enginev1.InvocationId) {
		leaseActs = append(leaseActs, ActInvoke{ID: activated, Target: target})
	})
	if ferr := sm.Fire(vobjComplete); ferr != nil {
		return nil, fmt.Errorf("releaseKeyLease: vobj fire: %w", ferr)
	}
	if perr := klt.Put(batch, lp, target.GetServiceName(), target.GetObjectKey(), next); perr != nil {
		return nil, fmt.Errorf("releaseKeyLease: write: %w", perr)
	}
	return leaseActs, nil
}

// promiseResultFromValue builds a JEPromiseResult from a terminal
// PromiseValue. Pending state is a programming error here — callers
// must check pv.GetPending() == nil before invoking.
func promiseResultFromValue(name string, pv *enginev1.PromiseValue) *enginev1.JEPromiseResult {
	out := &enginev1.JEPromiseResult{Name: name}
	if r := pv.GetResolved(); r != nil {
		out.Value = r.GetValue()
	} else if rj := pv.GetRejected(); rj != nil {
		out.FailureMessage = rj.GetFailureMessage()
	}
	return out
}

// buildPromiseValueFromJournal materialises a Resolved or Rejected
// PromiseValue from a JECompletePromise journal entry. FailureMessage
// non-empty = Rejected; otherwise Resolved.
func buildPromiseValueFromJournal(cp *enginev1.JECompletePromise, nowMs uint64) *enginev1.PromiseValue {
	if fm := cp.GetFailureMessage(); fm != "" {
		return &enginev1.PromiseValue{
			State: &enginev1.PromiseValue_Rejected{
				Rejected: &enginev1.Rejected{FailureMessage: fm, CompletedAtMs: nowMs},
			},
			CreatedAtMs: nowMs,
		}
	}
	return &enginev1.PromiseValue{
		State: &enginev1.PromiseValue_Resolved{
			Resolved: &enginev1.Resolved{Value: cp.GetValue(), CompletedAtMs: nowMs},
		},
		CreatedAtMs: nowMs,
	}
}

// buildPromiseValueFromEffect materialises a Resolved or Rejected
// PromiseValue from an InvokerEffect.PromiseCompleted (ingress path).
// Same value/failure semantics as buildPromiseValueFromJournal.
func buildPromiseValueFromEffect(eff *enginev1.PromiseCompleted, nowMs uint64) *enginev1.PromiseValue {
	if fm := eff.GetFailureMessage(); fm != "" {
		return &enginev1.PromiseValue{
			State: &enginev1.PromiseValue_Rejected{
				Rejected: &enginev1.Rejected{FailureMessage: fm, CompletedAtMs: nowMs},
			},
			CreatedAtMs: nowMs,
		}
	}
	return &enginev1.PromiseValue{
		State: &enginev1.PromiseValue_Resolved{
			Resolved: &enginev1.Resolved{Value: eff.GetValue(), CompletedAtMs: nowMs},
		},
		CreatedAtMs: nowMs,
	}
}

// statusTarget extracts the InvocationTarget from a status. Returns nil
// for Free/Completed (no active target) and for nil/zero statuses. Used
// by apply arms that need the (service, object_key) tuple of the running
// invocation, e.g. JEClearAllState.
func statusTarget(cur *enginev1.InvocationStatus) *enginev1.InvocationTarget {
	switch s := cur.GetStatus().(type) {
	case *enginev1.InvocationStatus_Scheduled:
		return s.Scheduled.GetTarget()
	case *enginev1.InvocationStatus_Invoked:
		return s.Invoked.GetTarget()
	case *enginev1.InvocationStatus_Suspended:
		return s.Suspended.GetTarget()
	default:
		return nil
	}
}

// mintCalleeInvocationID derives a deterministic InvocationId for the
// callee of a JECall, hashing the parent uuid with the JECall journal
// index AND the target's (service, handler, object_key). Determinism keeps
// the result identical across replay on every replica. Mixing the target
// into the hash prevents UUID collisions in the edge case where the parent
// gets Purged and Re-Invoked: without it, ChildCall(idx=N, target=X) then
// Purge+Re-Invoke then ChildCall(idx=N, target=Y) would mint two children
// with the same UUID and different PartitionKeys — independently legal in
// storage (each shard keys by full (PK, uuid)) but a footgun for any code
// reasoning about uuids as a process-wide identifier.
//
// The partition_key is derived from the target tuple (service, object_key)
// so cross-partition Call dispatch routes the callee to its owning
// partition (single-partition deployments degenerate to the local shard
// via the Partitioner fallback).
func mintCalleeInvocationID(parent *enginev1.InvocationId, idx uint32, target *enginev1.InvocationTarget) *enginev1.InvocationId {
	h := sha256.New()
	h.Write(parent.GetUuid())
	var idxBuf [4]byte
	binary.BigEndian.PutUint32(idxBuf[:], idx)
	h.Write(idxBuf[:])
	hashLP(h, target.GetServiceName())
	hashLP(h, target.GetHandlerName())
	hashLP(h, target.GetObjectKey())
	sum := h.Sum(nil)
	return &enginev1.InvocationId{
		// A call stays in the parent's tenant: band the callee's pk by the
		// parent's band so the callee routes/keys within the same tenant.
		PartitionKey: routing.PartitionKey(keys.TenantFromPartitionKey(parent.GetPartitionKey()), target.GetServiceName(), target.GetObjectKey()),
		Uuid:         append([]byte(nil), sum[:16]...),
	}
}

// hashLP length-prefixes s into h so adjacent fields cannot collide (("ab","c")
// vs ("a","bc")) regardless of contents. Safer than a NUL separator: the SDK
// rejects NUL inside identifiers, but length-prefixing stays robust if that
// invariant is ever relaxed. Shared by the deterministic id minters.
func hashLP(h hash.Hash, s string) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s)))
	h.Write(lenBuf[:])
	h.Write([]byte(s))
}

// mintProcessTaskID derives the deterministic InvocationId for a process
// service-task invocation: the instance root uuid hashed with the node id,
// instance index, turn seq, fan-out index, and target tuple.
//
// turnSeq (the instance's active inbox seq for the turn that emitted this
// dispatch) is what keeps a node that dispatches more than once over an
// instance's lifetime — a rework / exclusive-gateway loop, an error-boundary
// retry — from colliding on a single id. Without it every dispatch of node N
// reduces to hash(root, N, target), so the second dispatch re-mints the first's
// id and the receiving shard's onInvoke dedups it against the still-Completed
// prior row (transitionOnInvoke: Completed → ErrInvalidTransition → dropped);
// the task never re-runs and the instance hangs. turnSeq is durable and
// replay-stable — it lives in the record and only advances when a
// ProcessAdvanced commits — so a re-driven turn (whose ProcessAdvanced never
// committed) re-mints the same id and DedupTable absorbs the retry, while a
// genuine re-dispatch on a later turn gets a distinct id.
//
// fanoutIdx separates dispatches that turnSeq cannot: a single turn can emit
// several Invoke instructions for the SAME (nodeID, instanceIdx) when a node has
// completionQuantity > 1 (BPMN §10.2 — N tokens leave the node, each entering
// the next activity, so the next activity dispatches N times in one turn). With
// only turnSeq those N share an id and the receiver dedups them down to one
// dispatch, dropping the extra tokens (a startQuantity barrier downstream then
// never fills and the instance parks). fanoutIdx is the invoke's ordinal in the
// turn's deterministic instruction list, so it is replay-stable; MI instances
// already differ by instanceIdx, so adding it there is harmless. partition_key
// routes the callee to its target's shard, banded under the instance's tenant.
func mintProcessTaskID(root *enginev1.InvocationId, nodeID, instanceIdx string, turnSeq, fanoutIdx uint64, target *enginev1.InvocationTarget) *enginev1.InvocationId {
	h := sha256.New()
	h.Write(root.GetUuid())
	hashLP(h, nodeID)
	hashLP(h, instanceIdx)
	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], turnSeq)
	h.Write(seqBuf[:])
	binary.BigEndian.PutUint64(seqBuf[:], fanoutIdx)
	h.Write(seqBuf[:])
	hashLP(h, target.GetServiceName())
	hashLP(h, target.GetHandlerName())
	hashLP(h, target.GetObjectKey())
	sum := h.Sum(nil)
	return &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(keys.TenantFromPartitionKey(root.GetPartitionKey()), target.GetServiceName(), target.GetObjectKey()),
		Uuid:         append([]byte(nil), sum[:16]...),
	}
}

// processTimerID derives the synthetic InvocationId for a process timer. pk
// routes the timer to the instance's shard; the uuid is the truncated SHA-256
// of (service, instance_key, node_id, slot), so each (node, slot) gets a
// distinct timer row and the per-invocation index cancels exactly it.
func processTimerID(pk uint64, service, instanceKey, nodeID string, slot uint32) *enginev1.InvocationId {
	h := sha256.New()
	hashLP(h, service)
	hashLP(h, instanceKey)
	hashLP(h, nodeID)
	var slotBuf [4]byte
	binary.BigEndian.PutUint32(slotBuf[:], slot)
	h.Write(slotBuf[:])
	sum := h.Sum(nil)
	return &enginev1.InvocationId{PartitionKey: pk, Uuid: append([]byte(nil), sum[:16]...)}
}

func (p *Partition) onTimerFired(batch storage.Batch, cmd *enginev1.TimerFired, nowMs uint64, inv tables.InvocationTable, timers tables.TimerTable, isLeader bool) error {
	id := cmd.GetInvocationId()
	if err := p.checkLPFreeze(batch, id.GetPartitionKey()); err != nil {
		return err
	}
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onTimerFired: load status: %w", err)
	}
	next, actions, err := transitionOnTimerFired(id, cur, cmd, nowMs)
	if err != nil {
		p.cfg.Log.Warn("partition: invalid TimerFired transition", "err", err)
	}

	// Delete the timer row regardless of FSM outcome — even an invalid
	// transition must clear the row so we don't re-fire forever.
	if delErr := timers.Delete(batch, cmd.GetFireAtMs(), id); delErr != nil {
		return fmt.Errorf("onTimerFired: delete timer: %w", delErr)
	}

	// On invalid transition: stop here. Appending a SleepResult to a
	// journal whose status doesn't expect one (e.g. Free/Scheduled)
	// pollutes the entry stream — the SDK never reaches the wake site
	// because the invocation is terminal/unstarted, but the journal
	// tail still shows a wake that didn't happen. The timer is
	// cleared, no action queued, status unchanged.
	if err != nil {
		return nil
	}

	// Distinguish a Sleep timer from a Run-retry timer. Sleep timers
	// anchor on a JESleep at sleep_index and require a JESleepResult at
	// sleep_index+1; retry timers anchor on a JERun at sleep_index
	// (written by the JERunProposal apply arm) and write no follow-up
	// journal entry — the SDK's fast-replay sees the JERun{retryable=true}
	// directly and re-invokes fn.
	//
	// Reads the anchor directly from the store, NOT from the pending
	// batch. Safe today because TimerFired never coexists with the
	// anchor's append in the same Update batch — the anchor was written
	// by an earlier batch (JESleep on Sleep dispatch; JERun on
	// JERunProposal apply) which committed before the timer can fire.
	// If batching ever fuses appends with TimerFired in one batch this
	// read needs to consult the in-flight batch first.
	journal := tables.JournalTable{S: batch}
	anchor, anchorErr := journal.Read(id, cmd.GetSleepIndex())
	if anchorErr == nil {
		if _, isRun := anchor.GetEntry().(*enginev1.JournalEntry_Run); isRun {
			if next != nil {
				if err := inv.Put(batch, id, next); err != nil {
					return fmt.Errorf("onTimerFired: write status: %w", err)
				}
			}
			if isLeader {
				for _, a := range actions {
					p.cfg.Collector.Push(a)
				}
			}
			return nil
		}
	}

	// Append a SleepResult journal entry so the SDK sees the wake-up.
	je := &enginev1.JournalEntry{
		Index: cmd.GetSleepIndex() + 1,
		Entry: &enginev1.JournalEntry_SleepResult{
			SleepResult: &enginev1.JESleepResult{SleepIndex: cmd.GetSleepIndex()},
		},
	}
	if err := journal.Append(batch, id, je); err != nil {
		return fmt.Errorf("onTimerFired: append SleepResult: %w", err)
	}

	if next != nil {
		if err := inv.Put(batch, id, next); err != nil {
			return fmt.Errorf("onTimerFired: write status: %w", err)
		}
	}
	if isLeader {
		for _, a := range actions {
			p.cfg.Collector.Push(a)
		}
	}
	return nil
}

// purgeInvocationRows deletes the per-invocation rows for id: the
// InvocationStatus, the journal, and the signal inbox/awaiter rows.
// Shared by the operator-driven onPurge and the retention-driven onReap.
//
// State rows (state/<svc>/<key>/...) are intentionally NOT deleted here:
// they are addressed by (service, object_key), not invocation_id, and
// outlive any single invocation (a virtual object's state persists
// across invocations on the same key). Entity-scoped cleanup for a
// workflow run is onReap's separate, conditional concern. Signal
// inbox/awaiter rows ARE deleted because they are keyed by inv_id and
// aren't cleared by the Completed transition (the inbox can carry
// signals that arrived between Suspend and Complete). Pending timers are
// already cleared on the Invoked/Suspended → Completed transition, and
// both callers gate on a Completed/Free status, so none survive here.
func (p *Partition) purgeInvocationRows(
	batch storage.Batch,
	id *enginev1.InvocationId,
	inv tables.InvocationTable,
	journal tables.JournalTable,
) error {
	if err := inv.Delete(batch, id); err != nil {
		return fmt.Errorf("purge inv: %w", err)
	}
	if err := journal.DeletePrefix(batch, id); err != nil {
		return fmt.Errorf("purge journal: %w", err)
	}
	if err := (tables.SignalInboxTable{S: batch}).DeleteAllForInvocation(batch, id); err != nil {
		return fmt.Errorf("purge signal inbox: %w", err)
	}
	if err := (tables.SignalAwaiterTable{S: batch}).DeleteAllForInvocation(batch, id); err != nil {
		return fmt.Errorf("purge signal awaiter: %w", err)
	}
	return nil
}

// onPurge is the operator-driven immediate cleanup of a Completed
// invocation's per-invocation rows (the ingress PurgeInvocation RPC).
// No-op when the invocation isn't in a purgeable (Completed/Free) state.
// The timed counterpart is onReap.
func (p *Partition) onPurge(
	batch storage.Batch,
	cmd *enginev1.PurgeInvocation,
	inv tables.InvocationTable,
	journal tables.JournalTable,
) error {
	id := cmd.GetInvocationId()
	if err := p.checkLPFreeze(batch, id.GetPartitionKey()); err != nil {
		return err
	}
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onPurge: load status: %w", err)
	}
	if _, _, err := transitionOnPurge(id, cur); err != nil {
		p.cfg.Log.Warn("partition: invalid Purge transition", "err", err)
		return nil
	}
	return p.purgeInvocationRows(batch, id, inv, journal)
}

// Lookup is the sealed marker interface for linearizable-read query types
// accepted by (*Partition).Lookup. Implement isLookup() to add a new
// query variant.
type Lookup interface{ isLookup() }

// LookupInvocation returns the InvocationStatus for the given id.
type LookupInvocation struct{ ID *enginev1.InvocationId }

func (LookupInvocation) isLookup() {}

// LookupAppliedIndex returns the partition's current applied Raft index.
type LookupAppliedIndex struct{}

func (LookupAppliedIndex) isLookup() {}

// LookupAwakeable returns the AwakeableEntry for an id, or
// storage.ErrNotFound. Used by ingress to find the partition that owns
// an outstanding awakeable.
type LookupAwakeable struct{ ID string }

func (LookupAwakeable) isLookup() {}

// LookupIdempotency returns the InvocationId previously bound to a
// (service, handler, object_key, idempotency_key) tuple. Result is
// *enginev1.InvocationId or nil if not bound. Used by ingress to convert
// a duplicate SubmitInvocation into a no-op + return-prior-id.
type LookupIdempotency struct {
	Service        string
	Handler        string
	ObjectKey      string
	IdempotencyKey string
	// Tenant is the band of the requesting principal; the row LP must match
	// what onInvoke wrote (banded by the invocation's pk).
	Tenant uint32
}

func (LookupIdempotency) isLookup() {}

// LookupWorkflowRun returns the InvocationId of the active or completed
// run for (service, workflow_key). Result is *enginev1.InvocationId or nil
// when no run claims this key. Used by ingress to dedup KIND_WORKFLOW
// SubmitInvocation: optimistic miss → propose; hit → return the existing
// id without minting a new one.
type LookupWorkflowRun struct {
	Service     string
	WorkflowKey string
	// Tenant is the band of the requesting principal (see LookupIdempotency).
	Tenant uint32
}

func (LookupWorkflowRun) isLookup() {}

// LookupState resolves a single state value. Result is StateLookupResult
// so callers can distinguish "absent" (Present=false) from "present-but-
// empty" (Present=true, len(Value)==0).
type LookupState struct {
	Target *enginev1.InvocationTarget
	Key    string
	// Tenant is the band of the requesting principal (see LookupIdempotency).
	Tenant uint32
}

func (LookupState) isLookup() {}

// StateLookupResult is the value returned by Lookup(LookupState).
type StateLookupResult struct {
	Value   []byte
	Present bool
}

// LookupProcessInstance returns the ProcessInstanceRecord for (service,
// instance_key) banded under Tenant, or Present=false when absent — the instance
// never started, or already reached a terminal and was reaped. Used by ingress to
// observe a started/parked/completed instance without a dedicated await RPC.
type LookupProcessInstance struct {
	Service     string
	InstanceKey string
	// Tenant is the band of the requesting principal (see LookupIdempotency).
	Tenant uint32
}

func (LookupProcessInstance) isLookup() {}

// ProcessInstanceLookupResult is the value returned by Lookup(LookupProcessInstance).
type ProcessInstanceLookupResult struct {
	Record  *enginev1.ProcessInstanceRecord
	Present bool
}

// Lookup performs a linearizable read against the partition's on-disk store.
// query must be one of the Lookup marker types defined in this package;
// an unrecognised type returns an error. Implements statemachine.IOnDiskStateMachine.
func (p *Partition) Lookup(query any) (any, error) {
	store := p.cfg.Snapshotter.Store()
	if store == nil {
		return nil, errors.New("partition: snapshotter has no current store")
	}
	switch q := query.(type) {
	case LookupInvocation:
		return (tables.InvocationTable{S: store}).Get(q.ID)
	case LookupAppliedIndex:
		m, err := (tables.MetaTable{S: store}).Get()
		if err != nil {
			return nil, err
		}
		return m.GetAppliedIndex(), nil
	case LookupAwakeable:
		return (tables.AwakeableTable{S: store}).Get(q.ID)
	case LookupState:
		lp := keys.LPFromPartitionKey(routing.PartitionKey(q.Tenant, q.Target.GetServiceName(), q.Target.GetObjectKey()))
		v, present, err := (tables.StateTable{S: store}).Get(lp, q.Target, q.Key)
		if err != nil {
			return nil, err
		}
		return StateLookupResult{Value: v, Present: present}, nil
	case LookupIdempotency:
		lp := keys.LPFromPartitionKey(routing.PartitionKey(q.Tenant, q.Service, q.ObjectKey))
		return (tables.IdempotencyTable{S: store}).Get(lp, q.Service, q.Handler, q.ObjectKey, q.IdempotencyKey)
	case LookupWorkflowRun:
		lp := keys.LPFromPartitionKey(routing.PartitionKey(q.Tenant, q.Service, q.WorkflowKey))
		return (tables.WorkflowRunTable{S: store}).Get(lp, q.Service, q.WorkflowKey)
	case LookupProcessInstance:
		lp := keys.LPFromPartitionKey(routing.PartitionKey(q.Tenant, q.Service, q.InstanceKey))
		rec, ok, err := (tables.ProcessInstanceTable{S: store}).Get(lp, q.Service, q.InstanceKey)
		if err != nil {
			return nil, err
		}
		return ProcessInstanceLookupResult{Record: rec, Present: ok}, nil
	default:
		return nil, fmt.Errorf("partition: unknown lookup type %T", query)
	}
}

// Sync flushes pending writes (no-op when batches are committed with sync=true,
// which is the default in our Update path).
func (p *Partition) Sync() error {
	store := p.cfg.Snapshotter.Store()
	if store == nil {
		return nil
	}
	return store.Flush()
}

// PrepareSnapshot returns nil. Pebble Checkpoint (used by SaveSnapshot) is
// itself an atomic point-in-time snapshot, so we don't need an explicit cookie.
func (p *Partition) PrepareSnapshot() (any, error) { return nil, nil }

// SaveSnapshot writes a tar of a fresh Pebble checkpoint to w. May run
// concurrent with Update per dragonboat disk.go:37-43 — Pebble Checkpoint is
// online so this is safe. On success, fires OnSnapshotPersisted (if
// configured) so the archive producer can run opportunistically.
func (p *Partition) SaveSnapshot(_ any, w io.Writer, _ <-chan struct{}) error {
	if err := p.cfg.Snapshotter.SaveSnapshot(w); err != nil {
		return err
	}
	if p.cfg.OnSnapshotPersisted != nil {
		p.cfg.OnSnapshotPersisted()
	}
	return nil
}

// RecoverFromSnapshot replaces the on-disk state with the snapshot stream and
// clears any buffered actions (which were derived from the discarded state).
func (p *Partition) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	if err := p.cfg.Snapshotter.RecoverFromSnapshot(r); err != nil {
		return err
	}
	p.cfg.Collector.Clear()
	return nil
}

// Close releases the underlying store.
func (p *Partition) Close() error {
	if p.cfg.Snapshotter != nil {
		return p.cfg.Snapshotter.Close()
	}
	return nil
}

func dedupString(d *enginev1.Dedup) string {
	switch k := d.GetKind().(type) {
	case *enginev1.Dedup_SelfProposal:
		return fmt.Sprintf("self(epoch=%d,seq=%d)", k.SelfProposal.GetLeaderEpoch(), k.SelfProposal.GetSeq())
	case *enginev1.Dedup_Arbitrary:
		return fmt.Sprintf("arb(%s,seq=%d)", k.Arbitrary.GetProducerId(), k.Arbitrary.GetSeq())
	default:
		return "?"
	}
}
