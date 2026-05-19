package handler

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// replayEntry is one decoded protocolv1 frame the handler received
// during the replay phase. wireContext keeps a map of slot → entry so
// durable primitive calls (Sleep, SetState, …) can short-circuit when
// the journal already records the operation.
//
// For result-bearing notifications (SleepCompletionNotificationMessage,
// CallCompletionNotificationMessage, …) the payload field carries the
// marshaled notification — the primitive's wireContext method decodes
// it lazily into the return value.
//
// For write-only commands (SetState, ClearState, ClearAllState) the
// presence of the entry is itself the signal: replay-hit means "engine
// already journaled this; skip the emit."
type replayEntry struct {
	typeCode uint16
	payload  []byte
}

// wireContext implements Context for handlers running on the
// protocolv1 wire. Sleep / Run / Call / OneWayCall / Awakeable /
// SetState / ClearState / ClearAllState / GetState / All / Any /
// SendSignal / CancelInvocation / WaitSignal / Promise are all wired.
type wireContext struct {
	ctx          context.Context
	input        []byte
	invocationID *enginev1.InvocationId
	partitionKey uint64
	// service, handler, key are the addressing tuple from StartMessage.
	// Used by Promise to derive the workflow scope when binding a
	// DurablePromise. kind is the protocolv1.Kind from StartMessage;
	// Promise methods reject when this is not KIND_WORKFLOW /
	// KIND_WORKFLOW_SHARED.
	service string
	handler string
	key     string
	kind    protocolv1.Kind

	sink  frameSink
	codec wire.Codec

	// stateCache is the eager-preloaded K/V snapshot for this
	// invocation's (service, object_key), populated from
	// StartMessage.state_map. GetState reads from this directly; writes
	// (SetState, ClearState, ClearAllState) update it inline to keep
	// reads in the same session coherent with their preceding writes.
	// Lazy fetches populate it on present-hits so repeat reads of the
	// same key short-circuit without re-journaling.
	stateCache map[string][]byte
	// absentKeys tracks keys we know are absent in this session — from
	// ClearState writes or lazy fetches that returned void. Lets repeat
	// reads short-circuit without re-fetching. Cleared by SetState (the
	// key is back) and by ClearAllState (replaced by the cache-empty +
	// partialState=false invariant).
	absentKeys map[string]bool
	// partialState mirrors StartMessage.partial_state: when true, the
	// preload was incomplete (state exceeded the eager cap) so cache
	// misses must lazy-fetch from the engine. ClearAllState flips this
	// back to false because the wipe is journaled atomically — after the
	// session-local stateCache is emptied, the durable state matches.
	partialState bool

	// replay holds the protocolv1 frames the handler received between
	// StartMessage and the user-code phase, keyed by journal slot. On
	// each durable-primitive call, wireContext checks replay[slot]
	// first: if present, the engine already journaled the operation
	// (and possibly its result) so the call short-circuits without
	// re-emitting. If absent, the call emits its command frame and
	// suspends pending the next respawn cycle.
	replay map[uint32]*replayEntry

	// maxJournalEntries is the per-invocation step budget (engine
	// default + DeploymentRecord override, both clamped at the
	// engine's hard ceiling). 0 disables the SDK pre-flight check —
	// the wire-session backstop still applies.
	maxJournalEntries uint32

	mu sync.Mutex
	// nextSlot is the index of the next journal slot the handler will
	// claim. Slot 0 is JEInput, user-allocated slots start at 1.
	nextSlot uint32
	// suspended flips to true the first time a durable primitive returns
	// a not-yet-resolved future. All subsequent ctx calls short-circuit
	// to ErrSuspended; the session loop catches it on handler return,
	// emits SuspensionMessage, and the engine respawns the session
	// with extended replay once the awaited event lands.
	suspended      bool
	awaitingTokens []string
}

var _ Context = (*wireContext)(nil)

// newWireContext constructs a wireContext for one session. sink is the
// write-only frame channel back to the engine (HTTP/2 response writer
// today, Connect ServerStream tomorrow); codec must match the engine's.
// nextSlot starts at 1 because slot 0 is reserved for JEInput (the
// engine writes it before the handler runs).
func newWireContext(
	ctx context.Context,
	id *enginev1.InvocationId,
	input []byte,
	sink frameSink,
	codec wire.Codec,
	stateCache map[string][]byte,
	replay map[uint32]*replayEntry,
	partitionKey uint64,
	maxJournalEntries uint32,
	service, handler, key string,
	kind protocolv1.Kind,
) *wireContext {
	if replay == nil {
		replay = make(map[uint32]*replayEntry)
	}
	return &wireContext{
		ctx:               ctx,
		input:             input,
		invocationID:      id,
		partitionKey:      partitionKey,
		service:           service,
		handler:           handler,
		key:               key,
		kind:              kind,
		sink:              sink,
		codec:             codec,
		stateCache:        stateCache,
		replay:            replay,
		nextSlot:          1,
		maxJournalEntries: maxJournalEntries,
	}
}

func (c *wireContext) Context() context.Context             { return c.ctx }
func (c *wireContext) Input() []byte                        { return c.input }
func (c *wireContext) InvocationID() *enginev1.InvocationId { return c.invocationID }

// allocSlot reserves span consecutive journal indices and returns the
// first. err is non-nil in two cases:
//
//   - ErrSuspended — the context is already suspended; caller
//     should propagate it up the handler stack so the session loop
//     emits SuspensionMessage.
//   - *Failure (StepBudgetExhaustedCode) — the per-invocation
//     journal-entry cap would be exceeded; caller propagates the
//     failure so the handler terminates cleanly. The same check runs
//     defensively on the engine side as a backstop.
func (c *wireContext) allocSlot(span uint32) (start uint32, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.suspended {
		return 0, ErrSuspended
	}
	if c.maxJournalEntries > 0 && c.nextSlot+span > c.maxJournalEntries {
		return 0, NewStepBudgetExhaustedFailure(c.nextSlot, c.maxJournalEntries)
	}
	start = c.nextSlot
	c.nextSlot += span
	return start, nil
}

// suspend flips the suspended bit and accumulates a waker token. All
// subsequent ctx calls short-circuit so the handler unwinds to the
// session loop promptly.
func (c *wireContext) suspend(tokens ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.suspended = true
	c.awaitingTokens = append(c.awaitingTokens, tokens...)
}

// snapshotAwaiting returns a copy of the awaitingTokens slice so the
// session loop can serialize them into SuspensionMessage without
// holding the mutex.
func (c *wireContext) snapshotAwaiting() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.awaitingTokens))
	copy(out, c.awaitingTokens)
	return out
}

// lookupReplay returns the replay entry at slot, or nil if no entry was
// shipped at that index. Read-only — does not advance any cursor.
func (c *wireContext) lookupReplay(slot uint32) *replayEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replay[slot]
}

// GetState reads durable state for key.
//
// Resolution order:
//  1. stateCache hit (eager preload + session SetState + prior lazy
//     fetch) → return inline.
//  2. absentKeys hit (session ClearState, or prior lazy fetch returned
//     void) → return absent inline.
//  3. partialState=false → snapshot was complete; absent.
//  4. partialState=true → emit JEGetState command, suspend pending
//     JEGetStateResult on the result slot. On result-hit, populate
//     stateCache or absentKeys so repeat reads short-circuit.
//
// Slot cost: 0 on cache-hit / known-absent / complete-snapshot paths;
// 2 (JEGetState + JEGetStateResult) on lazy fetch.
func (c *wireContext) GetState(key string) ([]byte, bool, error) {
	c.mu.Lock()
	if c.stateCache != nil {
		if v, ok := c.stateCache[key]; ok {
			out := append([]byte(nil), v...)
			c.mu.Unlock()
			return out, true, nil
		}
	}
	if c.absentKeys[key] {
		c.mu.Unlock()
		return nil, false, nil
	}
	if !c.partialState {
		c.mu.Unlock()
		return nil, false, nil
	}
	c.mu.Unlock()

	cmdSlot, allocErr := c.allocSlot(2)
	if allocErr != nil {
		return nil, false, allocErr
	}
	resultSlot := cmdSlot + 1

	if c.lookupReplay(cmdSlot) == nil {
		msg := &protocolv1.GetLazyStateCommandMessage{
			Key:                []byte(key),
			ResultCompletionId: resultSlot,
		}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return nil, false, fmt.Errorf("marshal GetLazyStateCommandMessage: %w", err)
		}
		if err := c.sink.Send(wire.FrameFor(wire.TypeCmdGetLazyState, payload)); err != nil {
			return nil, false, err
		}
	}

	entry := c.lookupReplay(resultSlot)
	if entry == nil {
		c.suspend(fmt.Sprintf("completion:%d", resultSlot))
		return nil, false, ErrSuspended
	}
	if entry.typeCode != wire.TypeNoteGetLazyState {
		return nil, false, fmt.Errorf("getstate slot %d carries unexpected frame type 0x%04x", resultSlot, entry.typeCode)
	}
	var note protocolv1.GetLazyStateCompletionNotificationMessage
	if err := c.codec.Unmarshal(entry.payload, &note); err != nil {
		return nil, false, fmt.Errorf("decode replayed GetLazyStateCompletionNotificationMessage: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	switch r := note.GetResult().(type) {
	case *protocolv1.GetLazyStateCompletionNotificationMessage_Value:
		v := r.Value.GetContent()
		if c.stateCache == nil {
			c.stateCache = make(map[string][]byte)
		}
		c.stateCache[key] = append([]byte(nil), v...)
		delete(c.absentKeys, key)
		return append([]byte(nil), v...), true, nil
	case *protocolv1.GetLazyStateCompletionNotificationMessage_Void:
		if c.absentKeys == nil {
			c.absentKeys = make(map[string]bool)
		}
		c.absentKeys[key] = true
		return nil, false, nil
	default:
		return nil, false, nil
	}
}

// GetStateKeys returns the lexicographically-sorted set of state keys
// for the invocation's owning virtual object.
//
// Two paths:
//
//   - Eager (1 slot, partialState=false): the SDK derives the keys from
//     its local stateCache (StartMessage.state_map plus session writes)
//     and ships them inline as JEGetEagerStateKeys. The apply arm trusts
//     the payload and stamps it as-is; no completion needed.
//   - Lazy (2 slots, partialState=true): JEGetStateKeys + JEGetStateKeysResult.
//     The apply arm scans StateTable for the (service, object_key) prefix
//     because the SDK's local view is known-incomplete.
//
// On replay the slot-frame type code decides the path so a partialState
// flip between sessions doesn't misalign slot counts.
func (c *wireContext) GetStateKeys() ([]string, error) {
	c.mu.Lock()
	nextSlot := c.nextSlot
	partialState := c.partialState
	var span uint32 = 1
	if entry := c.replay[nextSlot]; entry != nil {
		switch entry.typeCode {
		case wire.TypeCmdGetEagerStateKeys:
			span = 1
		case wire.TypeCmdGetLazyStateKeys:
			span = 2
		default:
			c.mu.Unlock()
			return nil, fmt.Errorf("getstatekeys slot %d carries unexpected frame type 0x%04x", nextSlot, entry.typeCode)
		}
	} else if partialState {
		span = 2
	}
	c.mu.Unlock()

	cmdSlot, allocErr := c.allocSlot(span)
	if allocErr != nil {
		return nil, allocErr
	}
	if span == 1 {
		return c.getStateKeysEager(cmdSlot)
	}
	return c.getStateKeysLazy(cmdSlot)
}

// getStateKeysEager handles the single-slot path. On fresh emit it
// derives the sorted key list from stateCache and ships it inline. On
// replay-hit it decodes the original payload so the answer is durable
// even if stateCache changed shape between sessions.
func (c *wireContext) getStateKeysEager(cmdSlot uint32) ([]string, error) {
	if entry := c.lookupReplay(cmdSlot); entry != nil {
		var msg protocolv1.GetEagerStateKeysCommandMessage
		if err := c.codec.Unmarshal(entry.payload, &msg); err != nil {
			return nil, fmt.Errorf("decode replayed GetEagerStateKeysCommandMessage: %w", err)
		}
		src := msg.GetValue().GetKeys()
		out := make([]string, len(src))
		for i, k := range src {
			out[i] = string(k)
		}
		return out, nil
	}
	keys := c.snapshotEagerKeys()
	keysBytes := make([][]byte, len(keys))
	for i, k := range keys {
		keysBytes[i] = []byte(k)
	}
	msg := &protocolv1.GetEagerStateKeysCommandMessage{
		Value: &protocolv1.StateKeys{Keys: keysBytes},
	}
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal GetEagerStateKeysCommandMessage: %w", err)
	}
	if err := c.sink.Send(wire.FrameFor(wire.TypeCmdGetEagerStateKeys, payload)); err != nil {
		return nil, err
	}
	return keys, nil
}

// getStateKeysLazy handles the two-slot path. cmdSlot was already
// allocated by the caller; the result lands at cmdSlot+1.
func (c *wireContext) getStateKeysLazy(cmdSlot uint32) ([]string, error) {
	resultSlot := cmdSlot + 1

	if c.lookupReplay(cmdSlot) == nil {
		msg := &protocolv1.GetLazyStateKeysCommandMessage{
			ResultCompletionId: resultSlot,
		}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshal GetLazyStateKeysCommandMessage: %w", err)
		}
		if err := c.sink.Send(wire.FrameFor(wire.TypeCmdGetLazyStateKeys, payload)); err != nil {
			return nil, err
		}
	}

	entry := c.lookupReplay(resultSlot)
	if entry == nil {
		c.suspend(fmt.Sprintf("completion:%d", resultSlot))
		return nil, ErrSuspended
	}
	if entry.typeCode != wire.TypeNoteGetLazyStateKeys {
		return nil, fmt.Errorf("getstatekeys slot %d carries unexpected frame type 0x%04x", resultSlot, entry.typeCode)
	}
	var note protocolv1.GetLazyStateKeysCompletionNotificationMessage
	if err := c.codec.Unmarshal(entry.payload, &note); err != nil {
		return nil, fmt.Errorf("decode replayed GetLazyStateKeysCompletionNotificationMessage: %w", err)
	}
	src := note.GetStateKeys().GetKeys()
	out := make([]string, len(src))
	for i, k := range src {
		out[i] = string(k)
	}
	return out, nil
}

// snapshotEagerKeys derives the sorted state-key list from the local
// session view. stateCache always reflects present-only keys (ClearState
// removes them and adds to absentKeys; ClearAllState wipes both), so we
// just sort its keys.
func (c *wireContext) snapshotEagerKeys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.stateCache))
	for k := range c.stateCache {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// SetState journals a state write by emitting SetStateCommandMessage.
// The engine decodes the frame, proposes JESetState, and the apply path
// commits the row to StateTable. The eager cache is updated inline so
// subsequent GetState calls in this session observe the write.
//
// On replay (engine already journaled this write in a prior run), the
// emit is skipped — the replay buffer at this slot proves the entry
// is durable already.
func (c *wireContext) SetState(key string, value []byte) error {
	slot, err := c.allocSlot(1)
	if err != nil {
		return err
	}
	if c.lookupReplay(slot) == nil {
		msg := &protocolv1.SetStateCommandMessage{
			Key:   []byte(key),
			Value: &protocolv1.Value{Content: value},
		}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal SetStateCommandMessage: %w", err)
		}
		if err := c.sink.Send(wire.FrameFor(wire.TypeCmdSetState, payload)); err != nil {
			return err
		}
	}
	c.mu.Lock()
	if c.stateCache == nil {
		c.stateCache = make(map[string][]byte)
	}
	c.stateCache[key] = append([]byte(nil), value...)
	delete(c.absentKeys, key)
	c.mu.Unlock()
	return nil
}

// ClearState removes durable state for key. Write-only — no completion.
// Eager cache is updated inline so subsequent GetState in this session
// returns (nil, false, nil).
func (c *wireContext) ClearState(key string) error {
	slot, err := c.allocSlot(1)
	if err != nil {
		return err
	}
	if c.lookupReplay(slot) == nil {
		msg := &protocolv1.ClearStateCommandMessage{Key: []byte(key)}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal ClearStateCommandMessage: %w", err)
		}
		if err := c.sink.Send(wire.FrameFor(wire.TypeCmdClearState, payload)); err != nil {
			return err
		}
	}
	c.mu.Lock()
	delete(c.stateCache, key)
	if c.absentKeys == nil {
		c.absentKeys = make(map[string]bool)
	}
	c.absentKeys[key] = true
	c.mu.Unlock()
	return nil
}

// ClearAllState wipes every state row scoped to the invocation's
// (service, object_key). Journaled as a single JEClearAllState entry;
// the eager cache is reset inline.
func (c *wireContext) ClearAllState() error {
	slot, err := c.allocSlot(1)
	if err != nil {
		return err
	}
	if c.lookupReplay(slot) == nil {
		msg := &protocolv1.ClearAllStateCommandMessage{}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal ClearAllStateCommandMessage: %w", err)
		}
		if err := c.sink.Send(wire.FrameFor(wire.TypeCmdClearAllState, payload)); err != nil {
			return err
		}
	}
	c.mu.Lock()
	for k := range c.stateCache {
		delete(c.stateCache, k)
	}
	for k := range c.absentKeys {
		delete(c.absentKeys, k)
	}
	// The durable wipe materialises in the same batch as the JEClearAllState
	// append, so the session-local stateCache (now empty) is authoritative
	// for any subsequent GetState — no more lazy fetches required.
	c.partialState = false
	c.mu.Unlock()
	return nil
}

// Sleep schedules a durable wake-up d into the future. Returns a Future
// whose Result blocks (via suspend-and-respawn) until the wake-up fires
// — the payload is always nil, the resolution itself is the signal.
//
// Suspension is deferred to Future.Result so composition under
// All/Any works: each call only allocates a slot + emits a frame, and
// the combinator decides when (if at all) to suspend.
func (c *wireContext) Sleep(d time.Duration) Future {
	cmdSlot, allocErr := c.allocSlot(2)
	if allocErr != nil {
		return futureFromAllocErr(allocErr)
	}
	resultSlot := cmdSlot + 1

	if c.lookupReplay(resultSlot) != nil {
		return readyFuture{value: nil}
	}
	if c.lookupReplay(cmdSlot) == nil {
		// Fresh sleep — emit the command. wake_up_time is absolute ms
		// since the UNIX epoch; the engine apply path stores it on
		// JESleep.FireAtMs and schedules a timer for that instant.
		wakeAt := uint64(time.Now().UnixMilli()) + uint64(d.Milliseconds())
		msg := &protocolv1.SleepCommandMessage{
			WakeUpTime:         wakeAt,
			ResultCompletionId: resultSlot,
		}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return errFuture{err: fmt.Errorf("marshal SleepCommandMessage: %w", err)}
		}
		if err := c.sink.Send(wire.FrameFor(wire.TypeCmdSleep, payload)); err != nil {
			return errFuture{err: err}
		}
	}
	return sleepFuture{ctx: c, resultSlot: resultSlot}
}

// Run executes fn at most once and journals the outcome via the
// RunCommandMessage / ProposeRunCompletionMessage frame pair.
//
//   - Replay hit with non-retryable JERun: return cached value/failure
//     without re-invoking fn.
//   - Replay hit with retryable JERun: re-invoke fn with attempt+1 and
//     a fresh idempotency key (the engine scheduled a backoff timer
//     that fired; this respawn is the retry).
//   - No replay: invoke fn locally with attempt=1, emit
//     RunCommandMessage + ProposeRunCompletionMessage with the
//     outcome. On retryable error, suspend pending the engine's
//     backoff timer.
func (c *wireContext) Run(name string, fn RunFunc, opts ...RunOption) ([]byte, error) {
	if fn == nil {
		return nil, fmt.Errorf("reflow: ctx.Run fn must not be nil")
	}
	resolved := ApplyRunOptions(opts)
	slot, allocErr := c.allocSlot(1)
	if allocErr != nil {
		return nil, allocErr
	}

	// Determine the current attempt + idempotency key:
	//   - Replay carries a TypeNoteRunDone → terminal outcome cached.
	//   - Replay carries a TypeCmdRun marker → engine stamped the
	//     next-attempt counter + idempotency key onto it; prefer those
	//     wire-stamped values so the engine stays authoritative.
	//   - No replay → this is the first call, attempt=1, key derived
	//     locally (the engine will recompute the same value on replay).
	attempt := uint32(1)
	idempotencyKey := ""
	if entry := c.lookupReplay(slot); entry != nil {
		switch entry.typeCode {
		case wire.TypeNoteRunDone:
			var note protocolv1.RunCompletionNotificationMessage
			if err := c.codec.Unmarshal(entry.payload, &note); err != nil {
				return nil, fmt.Errorf("decode replayed RunCompletionNotificationMessage: %w", err)
			}
			switch r := note.GetResult().(type) {
			case *protocolv1.RunCompletionNotificationMessage_Value:
				out := make([]byte, len(r.Value.GetContent()))
				copy(out, r.Value.GetContent())
				return out, nil
			case *protocolv1.RunCompletionNotificationMessage_Failure:
				return nil, NewFailure(r.Failure.GetCode(), r.Failure.GetMessage())
			}
			return nil, nil
		case wire.TypeCmdRun:
			var marker protocolv1.RunCommandMessage
			if err := c.codec.Unmarshal(entry.payload, &marker); err == nil {
				if a := marker.GetAttempt(); a != 0 {
					attempt = a
				}
				idempotencyKey = marker.GetIdempotencyKey()
			}
		}
	}
	if idempotencyKey == "" {
		idempotencyKey = deriveIdempotencyKey(c.invocationID, slot, attempt)
	}
	rctx := NewRunContext(c.ctx, attempt, idempotencyKey)
	value, fnErr := fn(rctx)
	var (
		failureMessage string
		retryable      bool
	)
	if fnErr != nil {
		if f, ok := AsFailure(fnErr); ok {
			failureMessage = f.Message
		} else {
			failureMessage = fnErr.Error()
			retryable = true
		}
		value = nil
	}

	runCmd := &protocolv1.RunCommandMessage{
		ResultCompletionId: slot,
		Name:               name,
		Attempt:            attempt,
		IdempotencyKey:     idempotencyKey,
	}
	cmdPayload, err := c.codec.Marshal(runCmd)
	if err != nil {
		return nil, fmt.Errorf("marshal RunCommandMessage: %w", err)
	}
	if err := c.sink.Send(wire.FrameFor(wire.TypeCmdRun, cmdPayload)); err != nil {
		return nil, err
	}
	prop := &protocolv1.ProposeRunCompletionMessage{
		ResultCompletionId: slot,
		Retryable:          retryable,
		RetryPolicy:        runOptionsToWirePolicy(resolved),
	}
	if failureMessage != "" {
		prop.Result = &protocolv1.ProposeRunCompletionMessage_Failure{
			Failure: &protocolv1.Failure{Message: failureMessage},
		}
	} else {
		prop.Result = &protocolv1.ProposeRunCompletionMessage_Value{Value: value}
	}
	propPayload, err := c.codec.Marshal(prop)
	if err != nil {
		return nil, fmt.Errorf("marshal ProposeRunCompletionMessage: %w", err)
	}
	if err := c.sink.Send(wire.FrameFor(wire.TypeProposeRunDone, propPayload)); err != nil {
		return nil, err
	}

	if retryable {
		// Engine reads the embedded retry_policy on the proposal; if
		// the attempt budget permits, it journals JERun{retryable=true}
		// and schedules a backoff timer. The respawn carries an
		// updated marker with attempt+1 so fn sees a fresh
		// RunContext.Attempt() / IdempotencyKey().
		c.suspend(fmt.Sprintf("run-retry:%d", slot))
		return nil, ErrSuspended
	}
	if failureMessage != "" {
		return nil, NewFailure(0, failureMessage)
	}
	return value, nil
}

// runOptionsToWirePolicy lifts the user-facing RunOptions into the
// protocolv1 RunRetryPolicy carried on every ProposeRunCompletion.
// Returns nil when every field is zero so the engine uses defaults.
func runOptionsToWirePolicy(o RunOptions) *protocolv1.RunRetryPolicy {
	if o.MaxAttempts == 0 && o.InitialInterval == 0 && o.Factor == 0 && o.MaxInterval == 0 {
		return nil
	}
	return &protocolv1.RunRetryPolicy{
		InitialIntervalMs: uint64(o.InitialInterval / time.Millisecond),
		Factor:            o.Factor,
		MaxIntervalMs:     uint64(o.MaxInterval / time.Millisecond),
		MaxAttempts:       o.MaxAttempts,
	}
}

// deriveIdempotencyKey mirrors invoker.DeriveIdempotencyKey so the SDK
// can stamp the first-attempt frame with the same value the engine
// would derive on a replay-driven retry.
func deriveIdempotencyKey(invID *enginev1.InvocationId, slot, attempt uint32) string {
	var buf [16 + 8 + 4 + 4]byte
	uuid := invID.GetUuid()
	if len(uuid) >= 16 {
		copy(buf[:16], uuid[:16])
	}
	binary.BigEndian.PutUint64(buf[16:24], invID.GetPartitionKey())
	binary.BigEndian.PutUint32(buf[24:28], slot)
	binary.BigEndian.PutUint32(buf[28:32], attempt)
	h := sha256.Sum256(buf[:])
	return hex.EncodeToString(h[:8])
}

// Call invokes target with input and returns a Future resolving to the
// callee's response. Suspension is deferred to Future.Result so calls
// compose under All/Any.
func (c *wireContext) Call(target Target, input []byte, opts ...CallOption) Future {
	resolved := ApplyCallOptions(opts)
	cmdSlot, allocErr := c.allocSlot(2)
	if allocErr != nil {
		return futureFromAllocErr(allocErr)
	}
	resultSlot := cmdSlot + 1

	if c.lookupReplay(cmdSlot) == nil && c.lookupReplay(resultSlot) == nil {
		// Fresh call — emit the command.
		msg := &protocolv1.CallCommandMessage{
			ServiceName:        target.Service,
			HandlerName:        target.Handler,
			Parameter:          input,
			Key:                target.Key,
			ResultCompletionId: resultSlot,
		}
		if resolved.IdempotencyKey != "" {
			tok := resolved.IdempotencyKey
			msg.IdempotencyToken = &tok
		}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return errFuture{err: fmt.Errorf("marshal CallCommandMessage: %w", err)}
		}
		if err := c.sink.Send(wire.FrameFor(wire.TypeCmdCall, payload)); err != nil {
			return errFuture{err: err}
		}
	}
	return callFuture{ctx: c, resultSlot: resultSlot}
}

// OneWayCall invokes target with input fire-and-forget. Single-slot;
// no future returned because the wire never plumbs a response back to
// this invocation.
func (c *wireContext) OneWayCall(target Target, input []byte) error {
	slot, err := c.allocSlot(1)
	if err != nil {
		return err
	}
	if c.lookupReplay(slot) != nil {
		return nil // already journaled in a prior run
	}
	msg := &protocolv1.OneWayCallCommandMessage{
		ServiceName: target.Service,
		HandlerName: target.Handler,
		Parameter:   input,
		Key:         target.Key,
	}
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal OneWayCallCommandMessage: %w", err)
	}
	return c.sink.Send(wire.FrameFor(wire.TypeCmdOneWayCall, payload))
}

// Awakeable mints a fresh awakeable id bound to this invocation's
// partition_key and returns a Future that resolves when external
// callers invoke ingress.ResolveAwakeable with the matching id.
//
// Suspension is deferred to Future.Result so Awakeable composes under
// All/Any.
func (c *wireContext) Awakeable() (string, Future) {
	cmdSlot, allocErr := c.allocSlot(2)
	if allocErr != nil {
		return "", futureFromAllocErr(allocErr)
	}
	resultSlot := cmdSlot + 1

	if id := c.replayAwakeableID(cmdSlot); id != "" {
		// Already journaled — surface the same id and bind the future
		// to the result slot. awakeableFuture.Result decides whether
		// the result is in (readyFuture-equivalent) or suspends.
		return id, awakeableFuture{ctx: c, resultSlot: resultSlot, id: id}
	}

	// Fresh awakeable. Mint id locally using the embedded partition_key
	// so ingress.ResolveAwakeable can route to the owning shard.
	id, err := mintAwakeableID(c.partitionKey)
	if err != nil {
		return "", errFuture{err: err}
	}
	msg := &protocolv1.AwakeableCommandMessage{
		ResultCompletionId: resultSlot,
		AwakeableId:        id,
	}
	payload, err := c.codec.Marshal(msg)
	if err != nil {
		return "", errFuture{err: fmt.Errorf("marshal AwakeableCommandMessage: %w", err)}
	}
	if err := c.sink.Send(wire.FrameFor(wire.TypeCmdAwakeable, payload)); err != nil {
		return "", errFuture{err: err}
	}
	return id, awakeableFuture{ctx: c, resultSlot: resultSlot, id: id}
}

// replayAwakeableID decodes the AwakeableCommandMessage at slot and
// returns the id, or "" if decoding fails. Used to surface the id on
// replay-hits where the SDK called Awakeable in a prior run.
func (c *wireContext) replayAwakeableID(slot uint32) string {
	entry := c.lookupReplay(slot)
	if entry == nil || entry.typeCode != wire.TypeCmdAwakeable {
		return ""
	}
	var cmd protocolv1.AwakeableCommandMessage
	if err := c.codec.Unmarshal(entry.payload, &cmd); err != nil {
		return ""
	}
	return cmd.GetAwakeableId()
}

// mintAwakeableID generates a fresh "awk_<22 base64url>" identifier
// whose first 8 bytes encode ownerPartitionKey big-endian and the
// remaining 8 are random. ingress.ResolveAwakeable uses the embedded
// partition_key to route resolution to the owning shard with a single
// read.
func mintAwakeableID(ownerPartitionKey uint64) (string, error) {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], ownerPartitionKey)
	if _, err := rand.Read(buf[8:]); err != nil {
		return "", fmt.Errorf("reflow: awakeable id rng: %w", err)
	}
	return "awk_" + base64.RawURLEncoding.EncodeToString(buf[:]), nil
}

// All composes child futures: Results blocks until every child has
// resolved (or any child surfaces a *Failure). Pure SDK composition —
// no journal slot is allocated. Children must emit their command
// frames before this is called; the combinator only orchestrates
// suspension and result collection.
func (c *wireContext) All(futures ...Future) AllResult {
	return &allResult{ctx: c, children: append([]Future(nil), futures...)}
}

// Any composes child futures: Result resolves to the lowest-indexed
// child whose Poll reports resolved at suspend-time. "First" is by
// argument order, not wall clock, so replay is deterministic.
func (c *wireContext) Any(futures ...Future) Future {
	return &anyFuture{ctx: c, children: append([]Future(nil), futures...)}
}

// SendSignal delivers signalName + payload to the active invocation for
// target's (service, key). Single-slot; mirrors OneWayCall — the receiver
// shard resolves Target → active InvocationId via KeyLeaseTable when the
// outbox-shuffled SignalDelivered effect applies.
//
// target.Key must be non-empty. Empty Key returns a *Failure with
// SendSignalUnkeyedCode immediately, without journaling — there's no
// useful receiver for an unkeyed signal.
func (c *wireContext) SendSignal(target Target, signalName string, payload []byte) error {
	if target.Key == "" {
		return NewSendSignalUnkeyedFailure(target.Service, target.Handler)
	}
	slot, err := c.allocSlot(1)
	if err != nil {
		return err
	}
	if c.lookupReplay(slot) != nil {
		return nil // already journaled in a prior run
	}
	msg := &protocolv1.SendSignalCommandMessage{
		ServiceName: target.Service,
		HandlerName: target.Handler,
		Key:         target.Key,
		SignalName:  signalName,
		Payload:     payload,
	}
	encoded, err := c.codec.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal SendSignalCommandMessage: %w", err)
	}
	return c.sink.Send(wire.FrameFor(wire.TypeCmdSendSignal, encoded))
}

// CancelInvocation forces the active invocation for target's (service,
// key) to terminate with FailureCode=CancelledCode. Sugar over
// SendSignal; the receiver shard special-cases the WellKnownCancelSignal
// name to synthesize a terminal Completed.
func (c *wireContext) CancelInvocation(target Target) error {
	return c.SendSignal(target, WellKnownCancelSignal, nil)
}

// Promise returns a durable-promise handle bound to this workflow's
// (service, key) scope. Method calls on the returned handle validate
// that the surrounding invocation is a workflow (Kind == KIND_WORKFLOW
// or KIND_WORKFLOW_SHARED) — non-workflow callers get a *Failure on
// first method use.
func (c *wireContext) Promise(name string) DurablePromise {
	return &durablePromise{ctx: c, name: name, service: c.service, key: c.key}
}

// WorkflowPromise returns a durable-promise handle bound to the foreign
// workflow at (target.Service, target.Key). Use when the caller is not
// on the workflow's own (service, key); the apply path routes the
// completion cross-partition when target hashes off this shard.
// target.Handler is ignored — promises are workflow-run-scoped.
func (c *wireContext) WorkflowPromise(target Target, name string) DurablePromise {
	return &durablePromise{ctx: c, name: name, service: target.Service, key: target.Key}
}

// durablePromise binds a wireContext + promise scope (service, key) +
// name; each method allocates its own journal slots when invoked.
// Concrete result types are surfaced lazily so unused promise handles
// cost nothing in the journal.
type durablePromise struct {
	ctx     *wireContext
	name    string
	service string
	key     string
}

// requireWorkflow returns *Failure when the surrounding invocation is
// not a workflow. Called by every method before any slot allocation
// so an invalid handle never pollutes the journal.
func (p *durablePromise) requireWorkflow() *Failure {
	switch p.ctx.kind {
	case protocolv1.Kind_KIND_WORKFLOW, protocolv1.Kind_KIND_WORKFLOW_SHARED:
		return nil
	default:
		return NewPromiseNotWorkflowFailure(p.ctx.service, p.ctx.handler)
	}
}

func (p *durablePromise) Result() Future {
	if f := p.requireWorkflow(); f != nil {
		return errFuture{err: f}
	}
	cmdSlot, allocErr := p.ctx.allocSlot(2)
	if allocErr != nil {
		return futureFromAllocErr(allocErr)
	}
	resultSlot := cmdSlot + 1

	if p.ctx.lookupReplay(cmdSlot) == nil {
		msg := &protocolv1.GetPromiseCommandMessage{
			Service:            p.service,
			Key:                p.key,
			Name:               p.name,
			ResultCompletionId: resultSlot,
		}
		payload, err := p.ctx.codec.Marshal(msg)
		if err != nil {
			return errFuture{err: fmt.Errorf("marshal GetPromiseCommandMessage: %w", err)}
		}
		if err := p.ctx.sink.Send(wire.FrameFor(wire.TypeCmdGetPromise, payload)); err != nil {
			return errFuture{err: err}
		}
	}
	return promiseResultFuture{ctx: p.ctx, resultSlot: resultSlot, name: p.name}
}

func (p *durablePromise) Peek() (value []byte, completed bool, failure *Failure, err error) {
	if f := p.requireWorkflow(); f != nil {
		return nil, false, f, nil
	}
	slot, allocErr := p.ctx.allocSlot(1)
	if allocErr != nil {
		return nil, false, nil, allocErr
	}
	// Single-slot, store-only on replay: when the slot is already in
	// the replay buffer, decode the snapshot directly. On fresh runs,
	// emit the command and suspend pending the apply arm's stamped
	// reply (which materialises as a replay entry on the next
	// respawn).
	if entry := p.ctx.lookupReplay(slot); entry != nil {
		if entry.typeCode != wire.TypeCmdPeekPromise {
			return nil, false, nil, fmt.Errorf("peek slot %d carries unexpected frame type 0x%04x", slot, entry.typeCode)
		}
		var msg protocolv1.PeekPromiseCommandMessage
		if err := p.ctx.codec.Unmarshal(entry.payload, &msg); err != nil {
			return nil, false, nil, fmt.Errorf("decode replayed PeekPromiseCommandMessage: %w", err)
		}
		switch r := msg.GetResult().(type) {
		case *protocolv1.PeekPromiseCommandMessage_Value:
			return r.Value.GetContent(), msg.GetCompleted(), nil, nil
		case *protocolv1.PeekPromiseCommandMessage_Failure:
			return nil, msg.GetCompleted(), NewFailure(r.Failure.GetCode(), r.Failure.GetMessage()), nil
		default:
			return nil, msg.GetCompleted(), nil, nil
		}
	}
	msg := &protocolv1.PeekPromiseCommandMessage{
		Service: p.service,
		Key:     p.key,
		Name:    p.name,
	}
	payload, err := p.ctx.codec.Marshal(msg)
	if err != nil {
		return nil, false, nil, fmt.Errorf("marshal PeekPromiseCommandMessage: %w", err)
	}
	if err := p.ctx.sink.Send(wire.FrameFor(wire.TypeCmdPeekPromise, payload)); err != nil {
		return nil, false, nil, err
	}
	p.ctx.suspend(fmt.Sprintf("promise_peek:%s:%d", p.name, slot))
	return nil, false, nil, ErrSuspended
}

func (p *durablePromise) Resolve(value []byte) error {
	return p.complete(value, nil)
}

func (p *durablePromise) Reject(failure *Failure) error {
	if failure == nil {
		return fmt.Errorf("reflow: DurablePromise.Reject called with nil failure")
	}
	return p.complete(nil, failure)
}

// complete emits a JECompletePromise + awaits the JEPromiseCompleteResult
// in slot+1. Two slots; mirrors Sleep's await-on-result-slot shape.
func (p *durablePromise) complete(value []byte, failure *Failure) error {
	if f := p.requireWorkflow(); f != nil {
		return f
	}
	cmdSlot, allocErr := p.ctx.allocSlot(2)
	if allocErr != nil {
		return allocErr
	}
	resultSlot := cmdSlot + 1

	if p.ctx.lookupReplay(cmdSlot) == nil {
		msg := &protocolv1.CompletePromiseCommandMessage{
			Service:            p.service,
			Key:                p.key,
			Name:               p.name,
			ResultCompletionId: resultSlot,
		}
		if failure != nil {
			msg.Completion = &protocolv1.CompletePromiseCommandMessage_CompletionFailure{
				CompletionFailure: &protocolv1.Failure{Code: failure.Code, Message: failure.Message},
			}
		} else {
			msg.Completion = &protocolv1.CompletePromiseCommandMessage_CompletionValue{
				CompletionValue: &protocolv1.Value{Content: value},
			}
		}
		payload, err := p.ctx.codec.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal CompletePromiseCommandMessage: %w", err)
		}
		if err := p.ctx.sink.Send(wire.FrameFor(wire.TypeCmdCompletePromise, payload)); err != nil {
			return err
		}
	}

	// Wait for the apply arm's JEPromiseCompleteResult — either
	// already present in replay (succeeded or already-completed) or
	// pending suspension.
	entry := p.ctx.lookupReplay(resultSlot)
	if entry == nil {
		p.ctx.suspend(fmt.Sprintf("promise_complete:%s:%d", p.name, resultSlot))
		return ErrSuspended
	}
	if entry.typeCode != wire.TypeNoteCompletePromise {
		return fmt.Errorf("promise-complete slot %d carries unexpected frame type 0x%04x", resultSlot, entry.typeCode)
	}
	var note protocolv1.CompletePromiseCompletionNotificationMessage
	if err := p.ctx.codec.Unmarshal(entry.payload, &note); err != nil {
		return fmt.Errorf("decode replayed CompletePromiseCompletionNotificationMessage: %w", err)
	}
	if f := note.GetFailure(); f != nil {
		return NewFailure(f.GetCode(), f.GetMessage())
	}
	return nil
}

// WaitSignal registers interest in a named signal. Two-slot allocation
// mirrors Awakeable: cmdSlot for the JEAwaitSignal command, resultSlot
// (cmdSlot+1) for the JESignalResult notification.
//
// On a fresh run: emits a TypeCmdAwaitSignal frame. On replay: surfaces
// the cached payload from the resultSlot replay entry (if delivered) or
// returns a future that suspends waiting for the engine to stitch one.
func (c *wireContext) WaitSignal(signalName string) Future {
	cmdSlot, allocErr := c.allocSlot(2)
	if allocErr != nil {
		return futureFromAllocErr(allocErr)
	}
	resultSlot := cmdSlot + 1

	if c.lookupReplay(cmdSlot) == nil {
		msg := &protocolv1.AwaitSignalCommandMessage{
			SignalName:         signalName,
			ResultCompletionId: resultSlot,
		}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return errFuture{err: fmt.Errorf("marshal AwaitSignalCommandMessage: %w", err)}
		}
		if err := c.sink.Send(wire.FrameFor(wire.TypeCmdAwaitSignal, payload)); err != nil {
			return errFuture{err: err}
		}
	}
	return signalFuture{ctx: c, resultSlot: resultSlot, name: signalName}
}
