package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/observability"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// LeadershipObserver is the subset of leadership behavior the FSM needs. It
// is intentionally narrow; Step 11 supplies a concrete *Leadership that
// implements it.
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
	// preserving Phase 1-3.5 single-partition behavior. Phase 4.1.
	Partitioner routing.Partitioner

	// Metrics, when non-nil, is observed on every applied command:
	// ApplyTotal (kind, is_leader), ApplyDurationMs (kind), DedupHits,
	// JournalAppended (entry). Safe to leave nil — every observation is
	// guarded.
	Metrics *observability.Metrics
}

// Partition is the dragonboat IOnDiskStateMachine for one reflow partition.
//
// Mirrors restate crates/worker/src/partition/state_machine/mod.rs:305-343
// (the apply path) and partition/mod.rs:1049-1063 (the dedup check).
//
// Important contract notes (dragonboat v4 statemachine/disk.go):
//   - Update returning an error halts the shard (line 113). Logical/unknown
//     command bugs MUST be logged-and-continued.
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

	inv := tables.InvocationTable{S: store}
	journal := tables.JournalTable{S: store}
	timers := tables.TimerTable{S: store}
	dedup := tables.DedupTable{S: store}
	metaT := tables.MetaTable{S: store}

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
		if d := env.GetHeader().GetDedup(); d != nil {
			dup, err := dedup.IsDuplicate(d)
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
		if err := p.applyCommand(batch, store, &env, ent.Index, meta, inv, journal, timers, isLeader); err != nil {
			return nil, err
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
		//     apply + pop are atomic (Phase 1-3 behavior).
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
					outboxT := tables.OutboxTable{S: store}
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
						if seq, err := p.enqueueOutbox(batch, store, meta, ackEnv, isLeader); err != nil {
							p.cfg.Log.Warn("partition: outbox append (ack) failed",
								"seq", seq, "dest_shard", senderShard, "err", err)
						}
					}
				}
			}
			if err := dedup.Record(batch, d); err != nil {
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
	case nil:
		return "empty"
	default:
		return "unknown"
	}
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
	case *enginev1.JournalEntry_GetEagerState:
		return "GetEagerState"
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

// enqueueOutbox allocates the next outbox seq, writes env to the
// OutboxTable, bumps meta.next_outbox_seq, and (when leader) pushes
// an ActDispatchOutbox so the shuffler picks it up. Returns the seq
// and any storage error; meta is bumped in memory regardless, but
// since a non-nil error aborts the Update batch the increment is not
// persisted.
func (p *Partition) enqueueOutbox(
	batch storage.Batch,
	store storage.Store,
	meta *enginev1.PartitionMeta,
	env *enginev1.OutboxEnvelope,
	isLeader bool,
) (uint64, error) {
	seq := meta.GetNextOutboxSeq()
	meta.NextOutboxSeq = seq + 1
	outboxT := tables.OutboxTable{S: store}
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
		return p.onInvoke(batch, store, k.Invoke, now, inv, isLeader)
	case *enginev1.Command_InvokerEffect:
		return p.onInvokerEffect(batch, store, k.InvokerEffect, now, meta, inv, journal, isLeader)
	case *enginev1.Command_TimerFired:
		return p.onTimerFired(batch, store, k.TimerFired, now, inv, timers, isLeader)
	case *enginev1.Command_Purge:
		return p.onPurge(batch, k.Purge, inv, journal, timers, isLeader)
	case *enginev1.Command_DeliverCallResult:
		return p.onDeliverCallResult(batch, store, k.DeliverCallResult, now, inv, journal, isLeader)
	case *enginev1.Command_OutboxAck:
		return p.onOutboxAck(batch, store, k.OutboxAck)
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

func (p *Partition) onInvoke(
	batch storage.Batch,
	store storage.Store,
	cmd *enginev1.InvokeCommand,
	nowMs uint64,
	inv tables.InvocationTable,
	isLeader bool,
) error {
	id := cmd.GetInvocationId()
	target := cmd.GetTarget()

	// Phase 3 idempotency dedup. When idempotency_key is set, the first
	// InvokeCommand that lands wins; later submissions with the same
	// (service, handler, object_key, idempotency_key) tuple are dropped
	// silently. The new InvocationId is NOT registered — the caller that
	// minted it relied on ingress's optimistic LookupIdempotency to
	// surface the prior id before propose. Late losers polling on the
	// minted-but-dropped id will time out; cross-node races can be
	// hardened in a future phase by writing a redirect status row.
	if ik := cmd.GetIdempotencyKey(); ik != "" {
		idemT := tables.IdempotencyTable{S: store}
		prior, ierr := idemT.Get(target.GetServiceName(), target.GetHandlerName(), target.GetObjectKey(), ik)
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
		if perr := idemT.Put(batch, target.GetServiceName(), target.GetHandlerName(), target.GetObjectKey(), ik, id); perr != nil {
			return fmt.Errorf("onInvoke: idempotency record: %w", perr)
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
		klt := tables.KeyLeaseTable{S: store}
		curLease, lerr := klt.Get(target.GetServiceName(), target.GetObjectKey())
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
		if perr := klt.Put(batch, target.GetServiceName(), target.GetObjectKey(), nextLease); perr != nil {
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

func (p *Partition) onInvokerEffect(
	batch storage.Batch,
	store storage.Store,
	eff *enginev1.InvokerEffect,
	nowMs uint64,
	meta *enginev1.PartitionMeta,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	isLeader bool,
) error {
	id := eff.GetInvocationId()
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onInvokerEffect: load status: %w", err)
	}

	timersT := tables.TimerTable{S: store}
	awakeT := tables.AwakeableTable{S: store}

	var (
		next    *enginev1.InvocationStatus
		actions []Action
	)
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
					// Phase 3: forward the caller-supplied idempotency_key so
					// the callee's onInvoke runs the dedup against
					// (service, handler, object_key, idempotency_key).
					IdempotencyKey: e.Call.GetIdempotencyKey(),
					// Phase 2.5: stamp parent_link so the callee's Completed
					// apply arm can journal JECallResult back on the parent.
					ParentLink: &enginev1.ParentLink{
						ParentId:  id,
						CallIndex: entry.GetIndex(),
					},
				}},
			}
			if _, err := p.enqueueOutbox(batch, store, meta, env, isLeader); err != nil {
				return fmt.Errorf("onInvokerEffect: outbox append (call): %w", err)
			}
		case *enginev1.JournalEntry_SetState:
			// Phase 3 — persist state rows so eager preload on the next
			// session start can serve GetState without a journal scan.
			if t := statusTarget(cur); t != nil {
				if err := (tables.StateTable{S: store}).Set(batch, t, e.SetState.GetKey(), e.SetState.GetValue()); err != nil {
					return fmt.Errorf("onInvokerEffect: state set: %w", err)
				}
			} else {
				p.cfg.Log.Warn("partition: JESetState on status without target",
					"status", fmt.Sprintf("%T", cur.GetStatus()))
			}
		case *enginev1.JournalEntry_ClearState:
			if t := statusTarget(cur); t != nil {
				if err := (tables.StateTable{S: store}).Clear(batch, t, e.ClearState.GetKey()); err != nil {
					return fmt.Errorf("onInvokerEffect: state clear: %w", err)
				}
			} else {
				p.cfg.Log.Warn("partition: JEClearState on status without target",
					"status", fmt.Sprintf("%T", cur.GetStatus()))
			}
		case *enginev1.JournalEntry_ClearAllState:
			// Phase 3 — bulk-wipe every state row scoped to the invocation's
			// (service, object_key). Target is extracted from the active
			// status (Invoked/Suspended); Completed/Free/Scheduled here
			// would indicate a divergent SDK and is dropped with a warning
			// (we still append the journal entry above for replay parity).
			if t := statusTarget(cur); t != nil {
				if err := (tables.StateTable{S: store}).ClearObject(batch, t); err != nil {
					return fmt.Errorf("onInvokerEffect: state clear-all: %w", err)
				}
			} else {
				p.cfg.Log.Warn("partition: JEClearAllState on status without target",
					"status", fmt.Sprintf("%T", cur.GetStatus()))
			}
		case *enginev1.JournalEntry_Signal:
			env := &enginev1.OutboxEnvelope{
				DestinationShardId: p.cfg.Partitioner.ShardForInvocation(e.Signal.GetTargetInvocationId()),
				Kind: &enginev1.OutboxEnvelope_Signal{Signal: &enginev1.SignalSend{
					TargetInvocationId: e.Signal.GetTargetInvocationId(),
					SignalName:         e.Signal.GetSignalName(),
					Payload:            e.Signal.GetPayload(),
				}},
			}
			if _, err := p.enqueueOutbox(batch, store, meta, env, isLeader); err != nil {
				return fmt.Errorf("onInvokerEffect: outbox append (signal): %w", err)
			}
		}
		next, actions, err = transitionOnJournalAppend(id, cur, k.JournalAppended, nowMs)
	case *enginev1.InvokerEffect_RunProposal:
		// The SDK has produced the outcome of a ctx.Run body; persist it as
		// a JERun journal entry at the SDK-allocated index.
		//
		// Phase 3: when retryable=true the apply arm computes a backoff via
		// NextRetryDelay and schedules a retry timer (reusing TimerTable;
		// onTimerFired peeks the journal at sleep_index to skip the usual
		// JESleepResult write when the entry is a JERun). If the policy is
		// exhausted the proposal demotes to terminal — JERun{retryable=false}
		// — so the SDK's next fast-replay surfaces the failure to the
		// handler.
		rp := k.RunProposal
		idx := rp.GetEntryIndex()
		writeRun := func(retryable bool) error {
			return journal.Append(batch, id, &enginev1.JournalEntry{
				Index: idx,
				Entry: &enginev1.JournalEntry_Run{Run: &enginev1.JERun{
					Value:          rp.GetValue(),
					FailureMessage: rp.GetFailureMessage(),
					Attempt:        rp.GetAttempt(),
					Retryable:      retryable,
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
		delay, okPolicy := NextRetryDelay(rp.GetRetryPolicy(), rp.GetAttempt())
		if !okPolicy {
			// Policy exhausted — demote to terminal.
			if err := writeRun(false); err != nil {
				return fmt.Errorf("onInvokerEffect: journal append (run exhausted): %w", err)
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
		// Phase 2: receive-side journal entry is deferred; the FSM still
		// transitions state so a suspended invocation wakes up. CompletionID
		// is left at 0 — the Invoker session inspects its waker queue on
		// resume rather than relying on the notification carrying a real
		// index. Step 11 may revisit.
		next, actions, err = transitionOnSignalDelivered(
			id, cur, 0,
			k.SignalDelivered.GetSignalName(),
			k.SignalDelivered.GetPayload(),
			nowMs,
		)
	case *enginev1.InvokerEffect_Completed:
		next, actions, err = transitionOnComplete(id, cur, k.Completed, nowMs)
		// Phase 2.5 — deliver JECallResult to the parent invocation if this
		// callee was spawned via ctx.Call. Extract parent_link from either
		// Invoked or Suspended (both are valid pre-Completed states; see
		// transitionOnComplete's race-safety note). Completed → Completed
		// is idempotent and must NOT re-deliver — that's why we read from
		// cur (the prior status) rather than the new Completed status.
		if err == nil {
			var pl *enginev1.ParentLink
			var completedTarget *enginev1.InvocationTarget
			switch s := cur.GetStatus().(type) {
			case *enginev1.InvocationStatus_Invoked:
				pl = s.Invoked.GetParentLink()
				completedTarget = s.Invoked.GetTarget()
			case *enginev1.InvocationStatus_Suspended:
				pl = s.Suspended.GetParentLink()
				completedTarget = s.Suspended.GetTarget()
			}
			if pl.GetParentId() != nil {
				parentActs, perr := p.deliverCallResultToParent(
					batch, store, inv, journal, meta, pl,
					k.Completed.GetOutput(),
					k.Completed.GetFailureMessage(),
					nowMs,
					isLeader,
				)
				if perr != nil {
					return perr
				}
				actions = append(actions, parentActs...)
			}
			// Phase 3 — release the per-key VO lease and activate the next
			// queued invocation, if any. Guarded by completedTarget != nil
			// so replay (cur already Completed) is a no-op: the prior
			// Completed status carries no target on this code path.
			if completedTarget.GetObjectKey() != "" {
				leaseActs, rerr := p.releaseKeyLease(batch, store, completedTarget)
				if rerr != nil {
					return rerr
				}
				actions = append(actions, leaseActs...)
			}
		}
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

// deliverCallResultToParent dispatches the callee's terminal result back
// to the parent invocation. When the parent lives on the local partition
// (parentShard == localShard) the call applies inline via
// applyCallResultToParent (loads parent status, appends JECallResult,
// runs transitionOnCallResultDelivered, persists). When the parent lives
// on a different partition (Phase 4.1) the call enqueues an
// OutboxEnvelope_DeliverCallResult on the local outbox; the destination
// partition's apply path will run the same logic via onDeliverCallResult.
//
// Returns the parent-side actions to push onto the collector. Cross-shard
// dispatches return no actions on this side; the destination shard
// generates the wake actions when DeliverCallResult applies there.
func (p *Partition) deliverCallResultToParent(
	batch storage.Batch,
	store storage.Store,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	meta *enginev1.PartitionMeta,
	pl *enginev1.ParentLink,
	output []byte,
	failureMessage string,
	nowMs uint64,
	isLeader bool,
) ([]Action, error) {
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
	if _, err := p.enqueueOutbox(batch, store, meta, env, isLeader); err != nil {
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
// Raft pipeline; from here on the logic is identical to the same-shard
// path. Phase 4.1.
func (p *Partition) onDeliverCallResult(
	batch storage.Batch,
	_ storage.Store,
	cmd *enginev1.DeliverCallResult,
	nowMs uint64,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	isLeader bool,
) error {
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
func (p *Partition) onOutboxAck(batch storage.Batch, store storage.Store, ack *enginev1.OutboxAck) error {
	if ack.GetProducerShardId() != p.shardID {
		p.cfg.Log.Warn("partition: OutboxAck for foreign shard; dropping",
			"ack_producer_shard", ack.GetProducerShardId(),
			"local_shard", p.shardID,
			"seq", ack.GetProducerSeq())
		return nil
	}
	outboxT := tables.OutboxTable{S: store}
	if err := outboxT.Pop(batch, ack.GetProducerSeq()); err != nil {
		p.cfg.Log.Warn("partition: outbox pop (via ack) failed",
			"seq", ack.GetProducerSeq(), "err", err)
	}
	return nil
}

// releaseKeyLease is the Phase 3 companion to the VO gate. When a keyed
// invocation transitions to Completed, this fires vobjComplete on the
// per-key FSM and writes the resulting KeyLeaseStatus back into the same
// Pebble batch. If a queued invocation was waiting, the FSM's onActivate
// hook captures an ActInvoke for it; the caller appends those actions to
// the apply path's collector.
//
// Idempotent on replay: the caller guards entry via cur.GetStatus() so a
// second Completed apply pass (which finds cur already Completed) never
// enters this function.
func (p *Partition) releaseKeyLease(
	batch storage.Batch,
	store storage.Store,
	target *enginev1.InvocationTarget,
) ([]Action, error) {
	klt := tables.KeyLeaseTable{S: store}
	cur, err := klt.Get(target.GetServiceName(), target.GetObjectKey())
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
	if perr := klt.Put(batch, target.GetServiceName(), target.GetObjectKey(), next); perr != nil {
		return nil, fmt.Errorf("releaseKeyLease: write: %w", perr)
	}
	return leaseActs, nil
}

// statusTarget extracts the InvocationTarget from a status. Returns nil
// for Free/Completed (no active target) and for nil/zero statuses. Used
// by apply arms that need the (service, object_key) tuple of the running
// invocation, e.g. JEClearAllState. Phase 3.
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
// index. Determinism keeps the result identical across replay on every
// replica. Phase 4.1: partition_key derives from the target tuple
// (service, object_key) so cross-partition Call dispatch routes the
// callee to its owning partition (single-partition deployments still
// degenerate to the local shard via the Partitioner fallback).
func mintCalleeInvocationID(parent *enginev1.InvocationId, idx uint32, target *enginev1.InvocationTarget) *enginev1.InvocationId {
	h := sha256.New()
	h.Write(parent.GetUuid())
	var idxBuf [4]byte
	binary.BigEndian.PutUint32(idxBuf[:], idx)
	h.Write(idxBuf[:])
	sum := h.Sum(nil)
	return &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()),
		Uuid:         append([]byte(nil), sum[:16]...),
	}
}

func (p *Partition) onTimerFired(
	batch storage.Batch,
	store storage.Store,
	cmd *enginev1.TimerFired,
	nowMs uint64,
	inv tables.InvocationTable,
	timers tables.TimerTable,
	isLeader bool,
) error {
	id := cmd.GetInvocationId()
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onTimerFired: load status: %w", err)
	}
	next, actions, err := transitionOnTimerFired(id, cur, cmd, nowMs)
	if err != nil {
		p.cfg.Log.Warn("partition: invalid TimerFired transition", "err", err)
		// Still need to clear the timer row so we don't re-fire forever.
	}

	// Delete the timer row regardless of FSM outcome.
	if delErr := timers.Delete(batch, cmd.GetFireAtMs(), id); delErr != nil {
		return fmt.Errorf("onTimerFired: delete timer: %w", delErr)
	}

	// Phase 3: distinguish a Sleep timer from a Run-retry timer. Sleep
	// timers anchor on a JESleep at sleep_index and require a JESleepResult
	// at sleep_index+1; retry timers anchor on a JERun at sleep_index
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
	journal := tables.JournalTable{S: store}
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

func (p *Partition) onPurge(
	batch storage.Batch,
	cmd *enginev1.PurgeInvocation,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	timers tables.TimerTable,
	isLeader bool,
) error {
	id := cmd.GetInvocationId()
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onPurge: load status: %w", err)
	}
	if _, _, err := transitionOnPurge(id, cur); err != nil {
		p.cfg.Log.Warn("partition: invalid Purge transition", "err", err)
		return nil
	}
	if err := inv.Delete(batch, id); err != nil {
		return fmt.Errorf("onPurge: delete inv: %w", err)
	}
	if err := journal.DeletePrefix(batch, id); err != nil {
		return fmt.Errorf("onPurge: delete journal: %w", err)
	}

	// Reap any pending timer rows for this invocation. The timer keyspace
	// is sorted by fire_at_ms first, so finding rows for one id requires
	// scanning the whole table. Acceptable here (purge is a cleanup path,
	// not the hot path), but a timer_idx/<id>/<fire_at> companion table
	// would be the right scale fix when the timer set grows large.
	var pending []tables.TimerEntry
	if err := timers.ScanAll(func(e tables.TimerEntry) error {
		if bytes.Equal(e.ID.GetUuid(), id.GetUuid()) && e.ID.GetPartitionKey() == id.GetPartitionKey() {
			pending = append(pending, e)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("onPurge: scan timers: %w", err)
	}
	for _, e := range pending {
		if err := timers.Delete(batch, e.FireAtMs, e.ID); err != nil {
			return fmt.Errorf("onPurge: delete timer: %w", err)
		}
		if isLeader {
			p.cfg.Collector.Push(ActDeleteTimer{FireAtMs: e.FireAtMs, ID: e.ID})
		}
	}
	return nil
}

// Lookup is invoked by dragonboat for linearizable reads. Phase 1 supports a
// small fixed set of typed queries.
type Lookup interface{ isLookup() }

// LookupInvocation returns the InvocationStatus for the given id.
type LookupInvocation struct{ ID *enginev1.InvocationId }

func (LookupInvocation) isLookup() {}

// LookupAppliedIndex returns the partition's current applied Raft index.
type LookupAppliedIndex struct{}

func (LookupAppliedIndex) isLookup() {}

// LookupAwakeable returns the AwakeableEntry for an id, or
// storage.ErrNotFound. Used by ingress to find the partition that owns an
// outstanding awakeable. Phase 2.
type LookupAwakeable struct{ ID string }

func (LookupAwakeable) isLookup() {}

// LookupIdempotency returns the InvocationId previously bound to a
// (service, handler, object_key, idempotency_key) tuple. Result is
// *enginev1.InvocationId or nil if not bound. Used by ingress to convert
// a duplicate SubmitInvocation into a no-op + return-prior-id. Phase 3.
type LookupIdempotency struct {
	Service        string
	Handler        string
	ObjectKey      string
	IdempotencyKey string
}

func (LookupIdempotency) isLookup() {}

// LookupState resolves a single state value. Result is StateLookupResult
// so callers can distinguish "absent" (Present=false) from "present-but-
// empty" (Present=true, len(Value)==0). Phase 2.
type LookupState struct {
	Target *enginev1.InvocationTarget
	Key    string
}

func (LookupState) isLookup() {}

// StateLookupResult is the value returned by Lookup(LookupState).
type StateLookupResult struct {
	Value   []byte
	Present bool
}

// Lookup implements statemachine.IOnDiskStateMachine.
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
		v, present, err := (tables.StateTable{S: store}).Get(q.Target, q.Key)
		if err != nil {
			return nil, err
		}
		return StateLookupResult{Value: v, Present: present}, nil
	case LookupIdempotency:
		return (tables.IdempotencyTable{S: store}).Get(q.Service, q.Handler, q.ObjectKey, q.IdempotencyKey)
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
// online so this is safe.
func (p *Partition) SaveSnapshot(_ any, w io.Writer, _ <-chan struct{}) error {
	return p.cfg.Snapshotter.SaveSnapshot(w)
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
