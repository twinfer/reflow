package invoker

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// errNotImplementedPhase2 marks ctx methods whose engine-side wiring is
// scheduled for a later step in Phase 2. The handler sees this as a
// regular error and may report it back to the caller.
var errNotImplementedPhase2 = errors.New("reflow: ctx operation not yet implemented in Phase 2")

// invocationContext is the in-process implementation of sdk.Context used
// by the Go SDK. One instance lives per session run (every replay
// produces a fresh struct so journalIndex and the per-handler counter
// stay snapshot-pure).
//
// All blocking ctx methods follow the same shape:
//
//  1. Allocate an entry-index span via allocSlot.
//  2. Look up the command index in the journal snapshot. If present →
//     replay (no engine round-trip).
//  3. If absent → live: build the entry, propose it via the session's
//     proposer, and either return immediately (write-only ops) or
//     suspend (completable ops).
//
// The journal snapshot is taken once at session-start; live writes
// progress through the proposer but do not back-update the snapshot.
// That is fine because each handler-allocated index is used at most
// once: subsequent ctx calls always advance past it.
type invocationContext struct {
	s            *session
	input        []byte
	journalIndex map[uint32]*enginev1.JournalEntry

	mu          sync.Mutex
	nextIdx     uint32   // next index to allocate (index 0 holds JEInput)
	suspended   bool     // once true, every ctx call short-circuits to ErrSuspended
	suspendedOn []string // tokens to surface in InvocationSuspended.awaiting_on

	// stateCache is the eager-preloaded snapshot of this invocation's
	// (service, object_key) state rows. Populated at session start from
	// StateTable.ScanObject; updated as the handler journals
	// SetState/ClearState/ClearAllState. Nil when preload overflowed the
	// 64 KiB cap — GetState falls back to ErrNotImplementedPhase2 in that
	// case, matching the existing lazy-path stub. Phase 3.
	stateCache map[string][]byte
}

// newInvocationContext constructs an invocationContext for one session
// run. nextIdx starts at 1 — index 0 is reserved for JEInput, which is
// not user-allocated.
func newInvocationContext(s *session, input []byte, journalIndex map[uint32]*enginev1.JournalEntry, stateCache map[string][]byte) *invocationContext {
	return &invocationContext{
		s:            s,
		input:        input,
		journalIndex: journalIndex,
		nextIdx:      1,
		stateCache:   stateCache,
	}
}

// Context returns the session-scoped go context. Cancelled when the
// session is torn down (abort, leader-loss, host shutdown).
func (c *invocationContext) Context() context.Context { return c.s.ctx }

// Input returns the original input bytes. Same value on every replay.
func (c *invocationContext) Input() []byte { return c.input }

// InvocationID returns the durable identifier. Stable across replays.
func (c *invocationContext) InvocationID() *enginev1.InvocationId { return c.s.id }

// allocSlot reserves a contiguous range of entry indices [start,
// start+span). For completable ops span=2 (command + result); for
// write-only ops span=1. Returns ok=false when the context is already
// suspended — the caller should treat that as sdk.ErrSuspended.
func (c *invocationContext) allocSlot(span uint32) (start uint32, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.suspended {
		return 0, false
	}
	start = c.nextIdx
	c.nextIdx += span
	return start, true
}

// suspend records a waker token and flips the suspended bit. Subsequent
// ctx calls short-circuit to ErrSuspended immediately. Always returns
// sdk.ErrSuspended for callers to propagate.
func (c *invocationContext) suspend(token string) error {
	c.mu.Lock()
	c.suspended = true
	c.suspendedOn = append(c.suspendedOn, token)
	c.mu.Unlock()
	return sdk.ErrSuspended
}

// lookupEntry returns the journal entry at idx, or nil. Read-only on the
// snapshot — safe without the mutex.
func (c *invocationContext) lookupEntry(idx uint32) *enginev1.JournalEntry {
	return c.journalIndex[idx]
}

// divergenceErr is returned when replay finds the wrong entry type at a
// previously-allocated index. Surfaces as a terminal Failure to the
// handler — the body has changed shape between runs and replay can no
// longer be trusted.
func divergenceErr(idx uint32, want string, got *enginev1.JournalEntry) error {
	return fmt.Errorf("reflow: journal divergence at index %d: expected %s, got %T",
		idx, want, got.GetEntry())
}

// --- sdk.Context method implementations ---

// Sleep journals a JESleep and suspends. On wake-up the timer service
// has appended JESleepResult at idx+1, which the next session run sees
// in its journal snapshot and fast-replays.
func (c *invocationContext) Sleep(d time.Duration) error {
	start, ok := c.allocSlot(2)
	if !ok {
		return sdk.ErrSuspended
	}
	if existing := c.lookupEntry(start); existing != nil {
		// Replay path: JESleep must be present at start. JESleepResult
		// is checked at start+1 — if absent, we re-issue Suspended for
		// the same waker since the timer hasn't fired yet.
		if _, isSleep := existing.GetEntry().(*enginev1.JournalEntry_Sleep); !isSleep {
			return divergenceErr(start, "JESleep", existing)
		}
		if result := c.lookupEntry(start + 1); result != nil {
			if _, isResult := result.GetEntry().(*enginev1.JournalEntry_SleepResult); !isResult {
				return divergenceErr(start+1, "JESleepResult", result)
			}
			return nil
		}
		return c.suspend(fmt.Sprintf("sleep:%d", start))
	}

	// Live first execution.
	now := uint64(time.Now().UnixMilli())
	fireAtMs := now + uint64(d/time.Millisecond)
	if d > 0 && fireAtMs <= now {
		fireAtMs = now + 1
	}
	entry := &enginev1.JournalEntry{
		Index: start,
		Entry: &enginev1.JournalEntry_Sleep{
			Sleep: &enginev1.JESleep{FireAtMs: fireAtMs},
		},
	}
	if err := c.s.proposeJournal(entry); err != nil {
		return err
	}
	return c.suspend(fmt.Sprintf("sleep:%d", start))
}

// Run wraps a deterministic side-effect block. Replay returns the
// journaled outcome; live execution invokes fn once, journals the
// outcome via JERunProposal, and returns the same value.
//
// Phase 3 — fn errors are classified:
//   - *sdk.Failure (via sdk.NewFailure / errors.As):   terminal; the
//     failure is journaled as JERun{retryable=false} and surfaced to
//     the handler on the same call.
//   - Any other error:                                  retryable; the
//     engine schedules a backoff timer and re-invokes fn on wake-up.
//     The handler call suspends and is replayed when the retry fires.
//
// Retryable replays: on the next session run, fast-replay finds a
// JERun{retryable=true, attempt=N} at this slot; rather than returning
// the stored failure, ctx.Run falls through to live execution with
// attempt = N+1, journals the new proposal, and either succeeds or
// keeps retrying.
func (c *invocationContext) Run(_ string, fn func() ([]byte, error)) ([]byte, error) {
	if fn == nil {
		return nil, errors.New("reflow: ctx.Run fn must not be nil")
	}
	start, ok := c.allocSlot(1)
	if !ok {
		return nil, sdk.ErrSuspended
	}

	var priorAttempt uint32
	if existing := c.lookupEntry(start); existing != nil {
		run, isRun := existing.GetEntry().(*enginev1.JournalEntry_Run)
		if !isRun {
			return nil, divergenceErr(start, "JERun", existing)
		}
		if !run.Run.GetRetryable() {
			// Terminal — replay value or failure.
			if msg := run.Run.GetFailureMessage(); msg != "" {
				return nil, sdk.NewFailure(0, msg)
			}
			return cloneBytes(run.Run.GetValue()), nil
		}
		// Retryable JERun left by a prior failed attempt — re-invoke fn
		// with the next attempt number.
		priorAttempt = run.Run.GetAttempt() + 1
	}

	// Live execution (first attempt or retry).
	value, fnErr := fn()
	var failureMessage string
	retryable := false
	if fnErr != nil {
		if f, ok := sdk.AsFailure(fnErr); ok {
			failureMessage = f.Message
		} else {
			failureMessage = fnErr.Error()
			retryable = true
		}
		value = nil
	}

	eff := &enginev1.InvokerEffect{
		InvocationId: c.s.id,
		Kind: &enginev1.InvokerEffect_RunProposal{
			RunProposal: &enginev1.JERunProposal{
				EntryIndex:     start,
				Value:          value,
				FailureMessage: failureMessage,
				Retryable:      retryable,
				Attempt:        priorAttempt,
			},
		},
	}
	if err := c.s.proposeEffect(eff); err != nil {
		return nil, err
	}
	if retryable {
		// Engine schedules the backoff timer; the SDK waits for the
		// resulting Invoked re-entry, where fast-replay sees the
		// retryable JERun and falls through to a fresh fn call.
		return nil, c.suspend(fmt.Sprintf("run-retry:%d", start))
	}
	if failureMessage != "" {
		return nil, sdk.NewFailure(0, failureMessage)
	}
	return value, nil
}

// Call journals a JECall and suspends; the callee's eventual result
// arrives as JECallResult at start+1 and is fast-replayed on the next
// session run. Phase 2 wires the outbox/ingress return path in Steps 8
// and 13.
func (c *invocationContext) Call(target sdk.Target, input []byte, opts ...sdk.CallOption) ([]byte, error) {
	resolved := sdk.ApplyCallOptions(opts)
	start, ok := c.allocSlot(2)
	if !ok {
		return nil, sdk.ErrSuspended
	}
	if existing := c.lookupEntry(start); existing != nil {
		if _, isCall := existing.GetEntry().(*enginev1.JournalEntry_Call); !isCall {
			return nil, divergenceErr(start, "JECall", existing)
		}
		if result := c.lookupEntry(start + 1); result != nil {
			cr, isResult := result.GetEntry().(*enginev1.JournalEntry_CallResult)
			if !isResult {
				return nil, divergenceErr(start+1, "JECallResult", result)
			}
			return cloneBytes(cr.CallResult.GetResult()), nil
		}
		return nil, c.suspend(fmt.Sprintf("call:%d", start))
	}

	entry := &enginev1.JournalEntry{
		Index: start,
		Entry: &enginev1.JournalEntry_Call{
			Call: &enginev1.JECall{
				Target:         targetToProto(target),
				Input:          input,
				IdempotencyKey: resolved.IdempotencyKey,
			},
		},
	}
	if err := c.s.proposeJournal(entry); err != nil {
		return nil, err
	}
	return nil, c.suspend(fmt.Sprintf("call:%d", start))
}

// OneWayCall is the fire-and-forget variant. The proto does not yet
// model JEOneWayCall — Phase 2 Step 11 ships the engine-side scaffolding
// for the two-way Call path; the one-way wire-up follows once the
// proto and outbox shuffler grow a JEOneWayCall slot.
func (c *invocationContext) OneWayCall(_ sdk.Target, _ []byte) error {
	return errNotImplementedPhase2
}

// GetState is served from the eager-preloaded stateCache populated at
// session start (Phase 3). When the cache is nil — set when the eager
// preload exceeded the 64 KiB cap — GetState reports an unavailable
// state cache via errNotImplementedPhase2 so the handler can surface a
// distinct failure. Lazy command+notification fallback is a future
// extension.
func (c *invocationContext) GetState(key string) ([]byte, bool, error) {
	if c.stateCache == nil {
		return nil, false, errNotImplementedPhase2
	}
	v, present := c.stateCache[key]
	if !present {
		return nil, false, nil
	}
	return cloneBytes(v), true, nil
}

// SetState writes durable state for key. Single-entry op: the FSM stores
// the value in StateTable when it applies the JESetState.
func (c *invocationContext) SetState(key string, value []byte) error {
	start, ok := c.allocSlot(1)
	if !ok {
		return sdk.ErrSuspended
	}
	if existing := c.lookupEntry(start); existing != nil {
		if _, isSet := existing.GetEntry().(*enginev1.JournalEntry_SetState); !isSet {
			return divergenceErr(start, "JESetState", existing)
		}
		return nil
	}
	entry := &enginev1.JournalEntry{
		Index: start,
		Entry: &enginev1.JournalEntry_SetState{
			SetState: &enginev1.JESetState{Key: key, Value: value},
		},
	}
	if err := c.s.proposeJournal(entry); err != nil {
		return err
	}
	// Keep the eager-preloaded cache coherent with the just-journaled
	// write so subsequent GetState calls in this same run see it.
	if c.stateCache != nil {
		c.mu.Lock()
		c.stateCache[key] = append([]byte(nil), value...)
		c.mu.Unlock()
	}
	return nil
}

// ClearState removes durable state for key. Same shape as SetState.
func (c *invocationContext) ClearState(key string) error {
	start, ok := c.allocSlot(1)
	if !ok {
		return sdk.ErrSuspended
	}
	if existing := c.lookupEntry(start); existing != nil {
		if _, isClear := existing.GetEntry().(*enginev1.JournalEntry_ClearState); !isClear {
			return divergenceErr(start, "JEClearState", existing)
		}
		return nil
	}
	entry := &enginev1.JournalEntry{
		Index: start,
		Entry: &enginev1.JournalEntry_ClearState{
			ClearState: &enginev1.JEClearState{Key: key},
		},
	}
	if err := c.s.proposeJournal(entry); err != nil {
		return err
	}
	if c.stateCache != nil {
		c.mu.Lock()
		delete(c.stateCache, key)
		c.mu.Unlock()
	}
	return nil
}

// ClearAllState wipes every state row scoped to the invocation's
// (service, object_key). Journaled as a single JEClearAllState entry; the
// apply arm executes a Pebble DeleteRange over the object's state prefix.
// Phase 3.
func (c *invocationContext) ClearAllState() error {
	start, ok := c.allocSlot(1)
	if !ok {
		return sdk.ErrSuspended
	}
	if existing := c.lookupEntry(start); existing != nil {
		if _, isCAS := existing.GetEntry().(*enginev1.JournalEntry_ClearAllState); !isCAS {
			return divergenceErr(start, "JEClearAllState", existing)
		}
		return nil
	}
	entry := &enginev1.JournalEntry{
		Index: start,
		Entry: &enginev1.JournalEntry_ClearAllState{
			ClearAllState: &enginev1.JEClearAllState{},
		},
	}
	if err := c.s.proposeJournal(entry); err != nil {
		return err
	}
	if c.stateCache != nil {
		c.mu.Lock()
		for k := range c.stateCache {
			delete(c.stateCache, k)
		}
		c.mu.Unlock()
	}
	return nil
}

// Awakeable mints a new awakeable id, journals JEAwakeable at the
// allocated command index, and returns a future bound to start+1
// where the eventual JEAwakeableResult will land. The returned id is
// stable across replays — the journaled entry preserves it.
//
// On the first execution the id is freshly minted; on replay the
// existing JEAwakeable carries the id used originally.
func (c *invocationContext) Awakeable() (string, sdk.AwakeableFuture) {
	start, ok := c.allocSlot(2)
	if !ok {
		return "", &suspendedAwakeable{ctx: c}
	}
	if existing := c.lookupEntry(start); existing != nil {
		ak, isAk := existing.GetEntry().(*enginev1.JournalEntry_Awakeable)
		if !isAk {
			return "", &erroredAwakeable{err: divergenceErr(start, "JEAwakeable", existing)}
		}
		return ak.Awakeable.GetAwakeableId(), &awakeableFuture{
			ctx:       c,
			originIdx: start,
			id:        ak.Awakeable.GetAwakeableId(),
		}
	}

	id, err := newAwakeableID()
	if err != nil {
		return "", &erroredAwakeable{err: err}
	}
	entry := &enginev1.JournalEntry{
		Index: start,
		Entry: &enginev1.JournalEntry_Awakeable{
			Awakeable: &enginev1.JEAwakeable{AwakeableId: id},
		},
	}
	if err := c.s.proposeJournal(entry); err != nil {
		return "", &erroredAwakeable{err: err}
	}
	return id, &awakeableFuture{ctx: c, originIdx: start, id: id}
}

// SendSignal is deferred. JESignal targets an InvocationId, but the
// public sdk.Target carries (Service, Handler, Key) — Phase 2 lacks the
// resolver from target+key to a live invocation. Returns
// errNotImplementedPhase2 until the receiver-side routing is wired.
func (c *invocationContext) SendSignal(_ sdk.Target, _ string, _ []byte) error {
	return errNotImplementedPhase2
}

// awakeableFuture is the live path: the result will arrive in a future
// journal entry at originIdx+1.
type awakeableFuture struct {
	ctx       *invocationContext
	originIdx uint32
	id        string
}

func (f *awakeableFuture) Result() ([]byte, error) {
	if existing := f.ctx.lookupEntry(f.originIdx + 1); existing != nil {
		ar, ok := existing.GetEntry().(*enginev1.JournalEntry_AwakeableResult)
		if !ok {
			return nil, divergenceErr(f.originIdx+1, "JEAwakeableResult", existing)
		}
		if msg := ar.AwakeableResult.GetFailureMessage(); msg != "" {
			return nil, sdk.NewFailure(0, msg)
		}
		return cloneBytes(ar.AwakeableResult.GetValue()), nil
	}
	return nil, f.ctx.suspend("awakeable:" + f.id)
}

// erroredAwakeable surfaces a setup-time error from Awakeable() via the
// returned future's Result. Lets the caller decide whether to early-fail
// or work around it.
type erroredAwakeable struct{ err error }

func (e *erroredAwakeable) Result() ([]byte, error) { return nil, e.err }

// suspendedAwakeable is returned when Awakeable is called after the
// context has already been suspended (allocSlot refused). Result
// short-circuits to ErrSuspended.
type suspendedAwakeable struct{ ctx *invocationContext }

func (s *suspendedAwakeable) Result() ([]byte, error) { return nil, sdk.ErrSuspended }

// targetToProto converts the public Target into the proto shape.
func targetToProto(t sdk.Target) *enginev1.InvocationTarget {
	return &enginev1.InvocationTarget{
		ServiceName: t.Service,
		HandlerName: t.Handler,
		ObjectKey:   t.Key,
	}
}

// newAwakeableID mints a fresh "awk_<22-char base64url>" identifier. 16
// random bytes encode to 22 base64url characters with no padding.
func newAwakeableID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("reflow: awakeable id rng: %w", err)
	}
	return "awk_" + base64.RawURLEncoding.EncodeToString(buf[:]), nil
}

// cloneBytes returns a fresh copy of b. Defensive copies on journal
// reads keep handler-mutation of the returned slice from corrupting the
// in-memory journalIndex (which may be re-read on subsequent ctx
// methods, e.g. divergence checks against the entry type).
func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
