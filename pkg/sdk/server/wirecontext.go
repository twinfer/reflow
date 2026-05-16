package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/sdk"
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

// wireContext implements sdk.Context for handlers running on the
// protocolv1 wire. Sleep / Run / Call / OneWayCall / Awakeable /
// SetState / ClearState / ClearAllState / GetState / All / Any are all
// wired; SendSignal still returns ErrWireNotImplemented pending the
// receiver-side Target→InvocationId routing.
type wireContext struct {
	ctx          context.Context
	input        []byte
	invocationID *enginev1.InvocationId
	partitionKey uint64

	stream frameStream
	codec  handlerclient.Codec

	// stateCache is the eager-preloaded K/V snapshot for this
	// invocation's (service, object_key), populated from
	// StartMessage.state_map. GetState reads from this directly; writes
	// (SetState, ClearState, ClearAllState) update it inline to keep
	// reads in the same session coherent with their preceding writes.
	stateCache map[string][]byte
	// partialState mirrors StartMessage.partial_state: when true, the
	// preload was incomplete (e.g. state exceeded the cap) and a cache
	// miss must surface as an error rather than "key absent" — lazy
	// fetch isn't wired yet.
	partialState bool

	// replay holds the protocolv1 frames the handler received between
	// StartMessage and the user-code phase, keyed by journal slot. On
	// each durable-primitive call, wireContext checks replay[slot]
	// first: if present, the engine already journaled the operation
	// (and possibly its result) so the call short-circuits without
	// re-emitting. If absent, the call emits its command frame and
	// suspends pending the next respawn cycle.
	replay map[uint32]*replayEntry

	mu sync.Mutex
	// nextSlot is the index of the next journal slot the handler will
	// claim. Mirrors inproc.go's allocSlot contract: slot 0 is JEInput,
	// user-allocated slots start at 1.
	nextSlot uint32
	// suspended flips to true the first time a durable primitive returns
	// a not-yet-resolved future. All subsequent ctx calls short-circuit
	// to ErrSuspended; the session loop catches it on handler return,
	// emits SuspensionMessage, and the engine respawns the session
	// with extended replay once the awaited event lands.
	suspended      bool
	awaitingTokens []string
}

var _ sdk.Context = (*wireContext)(nil)

// newWireContext constructs a wireContext for one session. stream is the
// transport-neutral frame view (HTTP/2 server adapter on the handler
// side); codec must match the engine's. nextSlot starts at 1 because
// slot 0 is reserved for JEInput (the engine writes it before the
// handler runs).
func newWireContext(
	ctx context.Context,
	id *enginev1.InvocationId,
	input []byte,
	stream frameStream,
	codec handlerclient.Codec,
	stateCache map[string][]byte,
	replay map[uint32]*replayEntry,
	partitionKey uint64,
) *wireContext {
	if replay == nil {
		replay = make(map[uint32]*replayEntry)
	}
	return &wireContext{
		ctx:          ctx,
		input:        input,
		invocationID: id,
		partitionKey: partitionKey,
		stream:       stream,
		codec:        codec,
		stateCache:   stateCache,
		replay:       replay,
		nextSlot:     1,
	}
}

func (c *wireContext) Context() context.Context             { return c.ctx }
func (c *wireContext) Input() []byte                        { return c.input }
func (c *wireContext) InvocationID() *enginev1.InvocationId { return c.invocationID }

// allocSlot reserves span consecutive journal indices and returns the
// first. Mirrors inproc.go's allocSlot contract so replay-by-slot lines
// up across the two impls. Returns ok=false when the context is
// already suspended — callers should propagate sdk.ErrSuspended up the
// handler stack so the session loop emits SuspensionMessage.
func (c *wireContext) allocSlot(span uint32) (start uint32, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.suspended {
		return 0, false
	}
	start = c.nextSlot
	c.nextSlot += span
	return start, true
}

// suspend flips the suspended bit and accumulates a waker token. All
// subsequent ctx calls short-circuit so the handler unwinds to the
// session loop promptly. Mirrors inprocContext.suspend.
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

// GetState serves the eager-preloaded value for key. Reads after
// SetState / ClearState within the same session see the updated cache
// so handlers don't double-bounce through the wire to observe their
// own writes.
//
// Three result shapes mirroring inproc.go's GetState contract:
//   - (val, true, nil)   — key present in the eager snapshot.
//   - (nil, false, nil)  — preload was complete and key is absent.
//   - (nil, false, err)  — partialState: preload was incomplete (overflow);
//     lazy state fetch isn't wired so a miss is unavailable, not absent.
func (c *wireContext) GetState(key string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stateCache == nil && !c.partialState {
		return nil, false, nil
	}
	if c.stateCache != nil {
		if v, ok := c.stateCache[key]; ok {
			// Copy out so handler mutations don't poison the cache.
			out := make([]byte, len(v))
			copy(out, v)
			return out, true, nil
		}
	}
	if c.partialState {
		return nil, false, ErrLazyStateUnavailable
	}
	return nil, false, nil
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
	slot, ok := c.allocSlot(1)
	if !ok {
		return sdk.ErrSuspended
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
		if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdSetState, payload)); err != nil {
			return err
		}
	}
	c.mu.Lock()
	if c.stateCache == nil {
		c.stateCache = make(map[string][]byte)
	}
	c.stateCache[key] = append([]byte(nil), value...)
	c.mu.Unlock()
	return nil
}

// ClearState removes durable state for key. Write-only — no completion.
// Eager cache is updated inline so subsequent GetState in this session
// returns (nil, false, nil).
func (c *wireContext) ClearState(key string) error {
	slot, ok := c.allocSlot(1)
	if !ok {
		return sdk.ErrSuspended
	}
	if c.lookupReplay(slot) == nil {
		msg := &protocolv1.ClearStateCommandMessage{Key: []byte(key)}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal ClearStateCommandMessage: %w", err)
		}
		if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdClearState, payload)); err != nil {
			return err
		}
	}
	c.mu.Lock()
	delete(c.stateCache, key)
	c.mu.Unlock()
	return nil
}

// ClearAllState wipes every state row scoped to the invocation's
// (service, object_key). Journaled as a single JEClearAllState entry;
// the eager cache is reset inline.
func (c *wireContext) ClearAllState() error {
	slot, ok := c.allocSlot(1)
	if !ok {
		return sdk.ErrSuspended
	}
	if c.lookupReplay(slot) == nil {
		msg := &protocolv1.ClearAllStateCommandMessage{}
		payload, err := c.codec.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal ClearAllStateCommandMessage: %w", err)
		}
		if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdClearAllState, payload)); err != nil {
			return err
		}
	}
	c.mu.Lock()
	for k := range c.stateCache {
		delete(c.stateCache, k)
	}
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
func (c *wireContext) Sleep(d time.Duration) sdk.Future {
	cmdSlot, ok := c.allocSlot(2)
	if !ok {
		return suspendedFuture{}
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
		if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdSleep, payload)); err != nil {
			return errFuture{err: err}
		}
	}
	return sleepFuture{ctx: c, resultSlot: resultSlot}
}

// Run executes fn at most once and journals the outcome via the
// RunCommandMessage / ProposeRunCompletionMessage frame pair. Mirrors
// inproc.go's Run semantics:
//
//   - Replay hit with non-retryable JERun: return cached value/failure
//     without re-invoking fn.
//   - Replay hit with retryable JERun: re-invoke fn (the engine
//     scheduled a backoff timer that fired; this respawn is the retry).
//   - No replay: invoke fn locally, emit RunCommandMessage +
//     ProposeRunCompletionMessage with the outcome. On retryable error,
//     suspend pending the engine's backoff timer.
func (c *wireContext) Run(name string, fn func() ([]byte, error)) ([]byte, error) {
	if fn == nil {
		return nil, fmt.Errorf("reflow: ctx.Run fn must not be nil")
	}
	slot, ok := c.allocSlot(1)
	if !ok {
		return nil, sdk.ErrSuspended
	}

	if entry := c.lookupReplay(slot); entry != nil && entry.typeCode == handlerclient.TypeNoteRunDone {
		// Replay hit with the cached outcome. Decode and surface it.
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
			return nil, sdk.NewFailure(r.Failure.GetCode(), r.Failure.GetMessage())
		}
		// Empty result: treat as nil value.
		return nil, nil
	}

	// Fresh execution (first attempt or a retryable retry — both run fn).
	value, fnErr := fn()
	var (
		failureMessage string
		retryable      bool
	)
	if fnErr != nil {
		if f, ok := sdk.AsFailure(fnErr); ok {
			failureMessage = f.Message
		} else {
			failureMessage = fnErr.Error()
			retryable = true
		}
		value = nil
	}

	// Emit RunCommandMessage (marker — engine advances its slot counter)
	// followed by ProposeRunCompletionMessage carrying the outcome.
	runCmd := &protocolv1.RunCommandMessage{ResultCompletionId: slot, Name: name}
	cmdPayload, err := c.codec.Marshal(runCmd)
	if err != nil {
		return nil, fmt.Errorf("marshal RunCommandMessage: %w", err)
	}
	if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdRun, cmdPayload)); err != nil {
		return nil, err
	}
	prop := &protocolv1.ProposeRunCompletionMessage{
		ResultCompletionId: slot,
		Retryable:          retryable,
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
	if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeProposeRunDone, propPayload)); err != nil {
		return nil, err
	}

	if retryable {
		// Engine writes JERun{retryable=true}, schedules backoff. SDK
		// suspends; on respawn the replay sees JERun + retryable=true
		// (via the failure variant) and re-invokes fn with the next
		// attempt.
		c.suspend(fmt.Sprintf("run-retry:%d", slot))
		return nil, sdk.ErrSuspended
	}
	if failureMessage != "" {
		return nil, sdk.NewFailure(0, failureMessage)
	}
	return value, nil
}

// Call invokes target with input and returns a Future resolving to the
// callee's response. Suspension is deferred to Future.Result so calls
// compose under All/Any.
func (c *wireContext) Call(target sdk.Target, input []byte, opts ...sdk.CallOption) sdk.Future {
	resolved := sdk.ApplyCallOptions(opts)
	cmdSlot, ok := c.allocSlot(2)
	if !ok {
		return suspendedFuture{}
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
		if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdCall, payload)); err != nil {
			return errFuture{err: err}
		}
	}
	return callFuture{ctx: c, resultSlot: resultSlot}
}

// OneWayCall invokes target with input fire-and-forget. Single-slot;
// no future returned because the wire never plumbs a response back to
// this invocation.
func (c *wireContext) OneWayCall(target sdk.Target, input []byte) error {
	slot, ok := c.allocSlot(1)
	if !ok {
		return sdk.ErrSuspended
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
	return c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdOneWayCall, payload))
}

// Awakeable mints a fresh awakeable id bound to this invocation's
// partition_key and returns a Future that resolves when external
// callers invoke ingress.ResolveAwakeable with the matching id.
//
// Suspension is deferred to Future.Result so Awakeable composes under
// All/Any.
func (c *wireContext) Awakeable() (string, sdk.Future) {
	cmdSlot, ok := c.allocSlot(2)
	if !ok {
		return "", suspendedFuture{}
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
	if err := c.stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdAwakeable, payload)); err != nil {
		return "", errFuture{err: err}
	}
	return id, awakeableFuture{ctx: c, resultSlot: resultSlot, id: id}
}

// replayAwakeableID decodes the AwakeableCommandMessage at slot and
// returns the id, or "" if decoding fails. Used to surface the id on
// replay-hits where the SDK called Awakeable in a prior run.
func (c *wireContext) replayAwakeableID(slot uint32) string {
	entry := c.lookupReplay(slot)
	if entry == nil || entry.typeCode != handlerclient.TypeCmdAwakeable {
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
// remaining 8 are random. Mirrors invoker.newAwakeableID so the wire
// and inproc paths share the same id shape; ingress.ResolveAwakeable
// uses the embedded partition_key to route resolution to the owning
// shard with a single read.
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
func (c *wireContext) All(futures ...sdk.Future) sdk.AllResult {
	return &allResult{ctx: c, children: append([]sdk.Future(nil), futures...)}
}

// Any composes child futures: Result resolves to the lowest-indexed
// child whose Poll reports resolved at suspend-time. "First" is by
// argument order, not wall clock, so replay is deterministic.
func (c *wireContext) Any(futures ...sdk.Future) sdk.Future {
	return &anyFuture{ctx: c, children: append([]sdk.Future(nil), futures...)}
}

func (c *wireContext) SendSignal(sdk.Target, string, []byte) error {
	return ErrWireNotImplemented
}
