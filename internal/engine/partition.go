package engine

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
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

		if d := env.GetHeader().GetDedup(); d != nil {
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
		return p.onInvokerEffect(batch, k.InvokerEffect, now, inv, journal, isLeader)
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
	inv tables.InvocationTable,
	journal tables.JournalTable,
	isLeader bool,
) error {
	id := eff.GetInvocationId()
	cur, err := inv.Get(id)
	if err != nil {
		return fmt.Errorf("onInvokerEffect: load status: %w", err)
	}

	var (
		next    *enginev1.InvocationStatus
		actions []Action
	)
	switch k := eff.GetKind().(type) {
	case *enginev1.InvokerEffect_JournalAppended:
		// Persist the journal entry first.
		if err := journal.Append(batch, id, k.JournalAppended.GetEntry()); err != nil {
			return fmt.Errorf("onInvokerEffect: journal append: %w", err)
		}
		// Also persist a timer entry when the journal entry is Sleep.
		if sleep, ok := k.JournalAppended.GetEntry().GetEntry().(*enginev1.JournalEntry_Sleep); ok {
			t := tables.TimerTable{S: p.cfg.Snapshotter.Store()}
			if err := t.Insert(batch, sleep.Sleep.GetFireAtMs(), id, k.JournalAppended.GetEntry().GetIndex()); err != nil {
				return fmt.Errorf("onInvokerEffect: timer insert: %w", err)
			}
			if isLeader {
				p.cfg.Collector.Push(ActRegisterTimer{
					FireAtMs: sleep.Sleep.GetFireAtMs(),
					ID:       id,
					SleepIdx: k.JournalAppended.GetEntry().GetIndex(),
				})
			}
		}
		next, actions, err = transitionOnJournalAppend(id, cur, k.JournalAppended, nowMs)
	case *enginev1.InvokerEffect_Completed:
		next, actions, err = transitionOnComplete(id, cur, k.Completed, nowMs)
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
	if err := inv.Put(batch, id, next); err != nil {
		return fmt.Errorf("onInvokerEffect: write status: %w", err)
	}
	if isLeader {
		for _, a := range actions {
			p.cfg.Collector.Push(a)
		}
	}
	return nil
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
