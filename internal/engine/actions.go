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
type ActRegisterTimer struct {
	FireAtMs uint64
	ID       *enginev1.InvocationId
	SleepIdx uint32
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
