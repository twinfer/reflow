package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

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
	NowFn       func() uint64
	Log         *slog.Logger
	// OnActions, if non-nil, is invoked after each Update batch commits with
	// the actions accumulated on the leader. It runs inline on the
	// dragonboat apply goroutine — MUST NOT block on a Raft propose.
	OnActions func([]Action)
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
	if cfg.NowFn == nil {
		cfg.NowFn = func() uint64 { return 0 }
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
				p.cfg.Log.Debug("partition: duplicate command skipped",
					"raft_index", ent.Index, "dedup", dedupString(d))
				meta.AppliedIndex = ent.Index
				continue
			}
		}

		if err := p.applyCommand(batch, &env, ent.Index, meta, inv, journal, timers, isLeader); err != nil {
			return nil, err
		}

		// Outbox pop: when a command was re-injected by a local outbox
		// shuffler (Arbitrary dedup with "outbox/" producer), the original
		// row is no longer needed once we've applied. Pop in the same batch
		// so apply + pop are atomic.
		if d := env.GetHeader().GetDedup(); d != nil {
			if arb := d.GetArbitrary(); arb != nil && isOutboxProducer(arb.GetProducerId()) {
				outboxT := tables.OutboxTable{S: store}
				if err := outboxT.Pop(batch, arb.GetSeq()); err != nil {
					p.cfg.Log.Warn("partition: outbox pop failed",
						"seq", arb.GetSeq(), "producer", arb.GetProducerId(), "err", err)
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

func (p *Partition) applyCommand(
	batch storage.Batch,
	env *enginev1.Envelope,
	raftIndex uint64,
	meta *enginev1.PartitionMeta,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	timers tables.TimerTable,
	isLeader bool,
) error {
	now := p.cfg.NowFn()
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
		return p.onPurge(batch, k.Purge, inv, journal, isLeader)
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
	cmd *enginev1.InvokeCommand,
	nowMs uint64,
	inv tables.InvocationTable,
	isLeader bool,
) error {
	id := cmd.GetInvocationId()
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onInvoke: load status: %w", err)
	}
	next, actions, err := transitionOnInvoke(id, cur, cmd, nowMs)
	if err != nil {
		p.cfg.Log.Warn("partition: invalid Invoke transition", "err", err)
		return nil
	}
	if err := inv.Put(batch, id, next); err != nil {
		return fmt.Errorf("onInvoke: write status: %w", err)
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

	store := p.cfg.Snapshotter.Store()
	timersT := tables.TimerTable{S: store}
	outboxT := tables.OutboxTable{S: store}
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
			env := &enginev1.OutboxEnvelope{
				Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
					InvocationId: mintCalleeInvocationID(id, entry.GetIndex()),
					Target:       e.Call.GetTarget(),
					Input:        e.Call.GetInput(),
					// Phase 2.5: stamp parent_link so the callee's Completed
					// apply arm can journal JECallResult back on the parent.
					ParentLink: &enginev1.ParentLink{
						ParentId:  id,
						CallIndex: entry.GetIndex(),
					},
				}},
			}
			seq := meta.GetNextOutboxSeq()
			meta.NextOutboxSeq = seq + 1
			if err := outboxT.Append(batch, seq, env); err != nil {
				return fmt.Errorf("onInvokerEffect: outbox append (call): %w", err)
			}
			if isLeader {
				p.cfg.Collector.Push(ActDispatchOutbox{Seq: seq, Envelope: env})
			}
		case *enginev1.JournalEntry_Signal:
			env := &enginev1.OutboxEnvelope{
				Kind: &enginev1.OutboxEnvelope_Signal{Signal: &enginev1.SignalSend{
					TargetInvocationId: e.Signal.GetTargetInvocationId(),
					SignalName:         e.Signal.GetSignalName(),
					Payload:            e.Signal.GetPayload(),
				}},
			}
			seq := meta.GetNextOutboxSeq()
			meta.NextOutboxSeq = seq + 1
			if err := outboxT.Append(batch, seq, env); err != nil {
				return fmt.Errorf("onInvokerEffect: outbox append (signal): %w", err)
			}
			if isLeader {
				p.cfg.Collector.Push(ActDispatchOutbox{Seq: seq, Envelope: env})
			}
		}
		next, actions, err = transitionOnJournalAppend(id, cur, k.JournalAppended, nowMs)
	case *enginev1.InvokerEffect_RunProposal:
		// The SDK has produced the outcome of a ctx.Run body; persist it as
		// a JERun journal entry at the SDK-allocated index. Replay sees the
		// stored value; the body is never re-executed.
		runEntry := &enginev1.JournalEntry{
			Index: k.RunProposal.GetEntryIndex(),
			Entry: &enginev1.JournalEntry_Run{Run: &enginev1.JERun{
				Value:          k.RunProposal.GetValue(),
				FailureMessage: k.RunProposal.GetFailureMessage(),
			}},
		}
		if err := journal.Append(batch, id, runEntry); err != nil {
			return fmt.Errorf("onInvokerEffect: journal append (run): %w", err)
		}
		// State stays Invoked; no FSM transition needed. The Invoker session
		// observes the persisted entry on its next poll.
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
			switch s := cur.GetStatus().(type) {
			case *enginev1.InvocationStatus_Invoked:
				pl = s.Invoked.GetParentLink()
			case *enginev1.InvocationStatus_Suspended:
				pl = s.Suspended.GetParentLink()
			}
			if pl.GetParentId() != nil {
				parentActs, perr := p.deliverCallResultToParent(
					batch, inv, journal, pl,
					k.Completed.GetOutput(),
					k.Completed.GetFailureMessage(),
					nowMs,
				)
				if perr != nil {
					return perr
				}
				actions = append(actions, parentActs...)
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

// deliverCallResultToParent handles the same-partition Phase 2.5 path for
// returning a callee's result to its parent. Called from onInvokerEffect's
// Completed arm when the callee's prior Invoked status carried a ParentLink.
//
// Steps, all inside the existing Pebble batch:
//
//  1. Load the parent's current InvocationStatus.
//  2. Append JECallResult to the parent's journal at call_index + 1.
//     Journal append is idempotent at an existing index (see
//     JournalTable.Append) so replay-on-startup re-applying this effect is
//     a no-op.
//  3. Run transitionOnCallResultDelivered on the parent — Suspended wakes
//     to Invoked + emits ActInvoke + ActDeliverNotification; Invoked emits
//     only the notification; Completed/late-arrival is a no-op.
//  4. Persist the parent's new status.
//
// Returns the parent-side actions so the caller can push them onto the
// collector alongside any callee-side actions.
//
// Phase 4 (cross-partition) will replace step 1+2+4 with an outbox-style
// "deliver to parent's shard" effect; the FSM transition (step 3) is
// unchanged.
func (p *Partition) deliverCallResultToParent(
	batch storage.Batch,
	inv tables.InvocationTable,
	journal tables.JournalTable,
	pl *enginev1.ParentLink,
	output []byte,
	failureMessage string,
	nowMs uint64,
) ([]Action, error) {
	parentID := pl.GetParentId()
	parentStatus, err := inv.Get(parentID)
	if err != nil {
		return nil, fmt.Errorf("deliverCallResultToParent: load parent status: %w", err)
	}
	completionIdx := pl.GetCallIndex() + 1
	resultEntry := &enginev1.JournalEntry{
		Index: completionIdx,
		Entry: &enginev1.JournalEntry_CallResult{
			CallResult: &enginev1.JECallResult{
				CallIndex:      pl.GetCallIndex(),
				Result:         output,
				FailureMessage: failureMessage,
			},
		},
	}
	if err := journal.Append(batch, parentID, resultEntry); err != nil {
		return nil, fmt.Errorf("deliverCallResultToParent: journal parent JECallResult: %w", err)
	}
	parentNext, parentActions, terr := transitionOnCallResultDelivered(
		parentID, parentStatus, completionIdx, output, failureMessage, nowMs,
	)
	if terr != nil {
		// Mirror onInvokerEffect's policy: log and continue, but don't write
		// a stale status.
		p.cfg.Log.Warn("partition: invalid CallResultDelivered transition on parent",
			"err", terr, "parent_uuid", fmt.Sprintf("%x", parentID.GetUuid()))
		return nil, nil
	}
	if parentNext != nil {
		if err := inv.Put(batch, parentID, parentNext); err != nil {
			return nil, fmt.Errorf("deliverCallResultToParent: persist parent status: %w", err)
		}
	}
	return parentActions, nil
}

// mintCalleeInvocationID derives a deterministic InvocationId for the
// callee of a JECall, hashing the parent uuid with the JECall journal
// index. Determinism keeps the result identical across replay on every
// replica. partition_key is set to the parent's so Phase 2 single-partition
// deployments route the call to the same shard.
func mintCalleeInvocationID(parent *enginev1.InvocationId, idx uint32) *enginev1.InvocationId {
	h := sha256.New()
	h.Write(parent.GetUuid())
	var idxBuf [4]byte
	binary.BigEndian.PutUint32(idxBuf[:], idx)
	h.Write(idxBuf[:])
	sum := h.Sum(nil)
	return &enginev1.InvocationId{
		PartitionKey: parent.GetPartitionKey(),
		Uuid:         append([]byte(nil), sum[:16]...),
	}
}

func (p *Partition) onTimerFired(
	batch storage.Batch,
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
	// Append a SleepResult journal entry so the SDK sees the wake-up.
	je := &enginev1.JournalEntry{
		Index: cmd.GetSleepIndex() + 1,
		Entry: &enginev1.JournalEntry_SleepResult{
			SleepResult: &enginev1.JESleepResult{SleepIndex: cmd.GetSleepIndex()},
		},
	}
	if err := (tables.JournalTable{S: p.cfg.Snapshotter.Store()}).Append(batch, id, je); err != nil {
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
	_ bool,
) error {
	id := cmd.GetInvocationId()
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onPurge: load status: %w", err)
	}
	if _, _, err := transitionOnPurge(id, cur, p.cfg.NowFn()); err != nil {
		p.cfg.Log.Warn("partition: invalid Purge transition", "err", err)
		return nil
	}
	if err := inv.Delete(batch, id); err != nil {
		return fmt.Errorf("onPurge: delete inv: %w", err)
	}
	if err := journal.DeletePrefix(batch, id); err != nil {
		return fmt.Errorf("onPurge: delete journal: %w", err)
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
