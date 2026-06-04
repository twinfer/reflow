package engine

import (
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Action is a post-commit side-effect intent. The partition's state machine
// pushes Actions into an ActionCollector during Update; on the leader, the
// runner drains and dispatches them after the storage batch is committed.
// Followers replay the same commands but discard their actions.
//
// Mirrors restate crates/worker/src/partition/state_machine/actions.rs:31-114.
// Reflow's surface is narrower than Restate's because the wake path uses
// "respawn the session via ActInvoke" instead of a bidi notification
// channel — see invocation_fsm.transitionOnAwakeableResolved. The
// notification/awakeable/abort/ingress-response actions Restate has
// were never wired here; they are intentionally absent and should not
// be reintroduced without a concurrent reader plan.
type Action interface{ isAction() }

// ActInvoke asks the invoker to begin (or resume) execution of an invocation.
type ActInvoke struct {
	ID     *enginev1.InvocationId
	Target *enginev1.InvocationTarget
}

func (ActInvoke) isAction() {}

// ActRegisterTimer hands a newly persisted timer to the leader-side TimerService.
// Process is non-nil for a process timer (fires as Command_ProcessEvent instead
// of Command_TimerFired); SleepIdx applies to a plain sleep / run-retry timer.
type ActRegisterTimer struct {
	FireAtMs uint64
	ID       *enginev1.InvocationId
	SleepIdx uint32
	Process  *enginev1.ProcessTimer
}

func (ActRegisterTimer) isAction() {}

// ActDeleteTimer cancels an outstanding timer (e.g. after early completion).
type ActDeleteTimer struct {
	FireAtMs uint64
	ID       *enginev1.InvocationId
}

func (ActDeleteTimer) isAction() {}

// ActDispatchOutbox hands a freshly-appended OutboxEnvelope to the leader's
// outbox shuffler. The shuffler proposes the envelope's command via
// ProposeIngress and pops the row once the proposal commits. On crash
// before pop, the next leader rescans the OutboxTable and re-proposes —
// DedupTable absorbs the duplicate.
type ActDispatchOutbox struct {
	Seq      uint64
	Envelope *enginev1.OutboxEnvelope
}

func (ActDispatchOutbox) isAction() {}

// ActScheduleReap hands a freshly-written reap row to the leader's
// ReapService. The reaper fires at FireAtMs by proposing
// Command.ReapInvocation against the apply path, which deletes the
// invocation's per-invocation rows and, for a workflow run, its
// entity-scoped state / promise / workflow_run rows.
type ActScheduleReap struct {
	FireAtMs uint64
	ID       *enginev1.InvocationId
}

func (ActScheduleReap) isAction() {}

// ActScheduleProcessReap hands a freshly-written process-instance reap row to
// the leader's process ReapService. The reaper fires at FireAtMs by proposing
// Command.ReapProcessInstance, which deletes the terminal instance's retained
// record. The process-plane analog of ActScheduleReap.
type ActScheduleProcessReap struct {
	FireAtMs    uint64
	Pk          uint64
	Service     string
	InstanceKey string
}

func (ActScheduleProcessReap) isAction() {}

// ActStartLPTransferScan is emitted by onBeginLPTransfer (source side)
// after the freeze row is durable. The runner hands it to the leader-
// side LPTransferService, which opens a read snapshot, builds one
// SST per LP-prefixed namespace, uploads them to the destination's
// replicas, and proposes a single ApplyLPTransferSST.
type ActStartLPTransferScan struct {
	TransferID string
	LP         uint32
	DestShard  uint64
}

func (ActStartLPTransferScan) isAction() {}

// ActSignalLPTransferStaged is emitted by onApplyLPTransferSST (dest
// side) when the is_final SST applies. The runner enqueues an outbox
// envelope back to shard 0 carrying UpdateLPTransferPhase{phase=STAGED}
// so the lpMover advances the saga.
type ActSignalLPTransferStaged struct {
	TransferID  string
	LP          uint32
	SourceShard uint64
}

func (ActSignalLPTransferStaged) isAction() {}

// ActSignalLPTransferCleaned is emitted by onFinishLPTransfer (source
// side) after the LP keyspace range-delete commits. Routed via outbox
// to shard 0 carrying UpdateLPTransferPhase{phase=CLEANED}.
type ActSignalLPTransferCleaned struct {
	TransferID string
}

func (ActSignalLPTransferCleaned) isAction() {}

// ActSignalLPTransferAbortAck is emitted by onAbortLPTransfer on both
// partition sides after rollback completes. Routed via outbox to
// shard 0 so the lpMover knows both sides have cleaned up before
// advancing to ABORTED.
type ActSignalLPTransferAbortAck struct {
	TransferID string
}

func (ActSignalLPTransferAbortAck) isAction() {}

// ActAdvanceProcess asks the invoker to run one process-instance turn: load
// the ProcessInstanceRecord, run the injected iflow engine on the carried
// inbox entry, and propose ProcessAdvanced. Emitted by the ProcessEvent /
// ProcessAdvanced apply arms when an inbox seq becomes the active turn.
type ActAdvanceProcess struct {
	Pk          uint64
	Service     string
	InstanceKey string
	Entry       *enginev1.ProcessInboxEntry
}

func (ActAdvanceProcess) isAction() {}

// ActionCollector is a single-goroutine append-only buffer of Actions
// produced during one Update call. It is owned by the partition's apply path
// and is not safe for concurrent use.
type ActionCollector struct {
	actions []Action
}

// Push appends a to the collector's buffer.
func (c *ActionCollector) Push(a Action) {
	c.actions = append(c.actions, a)
}

// Drain returns the accumulated actions and resets the collector.
func (c *ActionCollector) Drain() []Action {
	out := c.actions
	c.actions = nil
	return out
}

// Clear discards accumulated actions. Used on followers to mirror the
// leader-only semantics of restate state_machine/mod.rs:312-313 (`is_leader`
// gates whether actions are dispatched).
func (c *ActionCollector) Clear() {
	c.actions = nil
}

// Len returns the number of buffered actions.
func (c *ActionCollector) Len() int { return len(c.actions) }
