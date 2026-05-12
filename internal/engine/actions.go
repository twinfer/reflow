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
// Phase 1 carried only ActInvoke, the timer pair, ActAbortInvocation, and
// ActIngressResponse. Phase 2 adds notification + outbox + awakeable
// delivery actions, and starts emitting ActAbortInvocation on leadership
// changes.
type Action interface{ isAction() }

// ActInvoke asks the invoker to begin (or resume) execution of an invocation.
// In Phase 1 the invoker is a stub; the test harness observes ActInvoke and
// synthesises InvokerEffect commands directly.
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

// ActAbortInvocation tells the invoker to stop driving an in-flight invocation
// (e.g. after Cancel or after the leader changes).
type ActAbortInvocation struct {
	ID *enginev1.InvocationId
}

func (ActAbortInvocation) isAction() {}

// ActIngressResponse delivers a completed invocation's output to the caller
// that initiated it. The RequestID matches the ingress request token.
type ActIngressResponse struct {
	RequestID string
	ID        *enginev1.InvocationId
	Output    []byte
	Failure   string
}

func (ActIngressResponse) isAction() {}

// ActDeliverNotification asks the Invoker to deliver a Completion message
// to a running session. CompletionID is the journal entry index of the
// originating command (Sleep, Call, Awakeable, lazy GetState). Exactly one
// of Value / Failure / Void describes the result:
//
//	Failure non-empty             → failure completion
//	Void true (and Failure empty) → void completion
//	otherwise                     → value completion carrying Value
//
// Phase 2.
type ActDeliverNotification struct {
	ID           *enginev1.InvocationId
	CompletionID uint32
	Value        []byte
	Failure      string
	Void         bool
}

func (ActDeliverNotification) isAction() {}

// ActDispatchOutbox hands a freshly-appended OutboxEnvelope to the leader's
// outbox shuffler. The shuffler proposes the envelope's command via
// ProposeIngress and pops the row once the proposal commits. On crash
// before pop, the next leader rescans the OutboxTable and re-proposes —
// DedupTable absorbs the duplicate. Phase 2.
type ActDispatchOutbox struct {
	Seq      uint64
	Envelope *enginev1.OutboxEnvelope
}

func (ActDispatchOutbox) isAction() {}

// ActDeliverAwakeable surfaces an external awakeable resolution to the
// Invoker side-band, distinct from the JEAwakeableResult-driven
// ActDeliverNotification path. The Invoker uses it to update in-memory
// awakeable bookkeeping for sessions that are currently running (not
// suspended) so a subsequent ctx.Awakeable poll observes the result
// without a journal re-read. Phase 2.
type ActDeliverAwakeable struct {
	ID          *enginev1.InvocationId
	AwakeableID string
	Value       []byte
	Failure     string
}

func (ActDeliverAwakeable) isAction() {}

// ActionCollector is a single-goroutine append-only buffer of Actions
// produced during one Update call. It is owned by the partition's apply path
// and is not safe for concurrent use.
type ActionCollector struct {
	actions []Action
}

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
