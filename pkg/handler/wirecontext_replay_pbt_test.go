package handler

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// TestWireContext_ReplayDeterminism is a property-based test for the
// SDK's session-replay invariant. The "exactly-once side-effect replay"
// rule is the most load-bearing guarantee in a durable execution
// engine: when a session crashes mid-execution, the next session
// receives the journal as a replay stream, walks the same handler code,
// and MUST observe the same values without re-emitting any frame the
// engine has already journaled.
//
// Existing coverage for this lives in hand-rolled integration tests
// (TestWireDispatch_HTTP2_LazyState_Fetch*) that exercise one specific
// scenario each. This property generates random scripts and asserts
// the invariant generatively.
//
// Scope (Phase 1): the state surface — SetState, ClearState,
// ClearAllState, GetState (cache-hit / known-absent / complete-snapshot
// paths), and GetStateKeys (eager path). Lazy-fetch state, Sleep, Call,
// Run, Awakeable, Signal, Promise are deferred: each needs a synthetic
// engine to resolve 2-slot operations inline, which is much more
// machinery than the state surface needs.
//
// The property:
//
//  1. Draw an initial eager-state snapshot and a script of N ctx calls.
//     partialState is held false so every read serves from the local
//     cache (no synthetic engine needed for lazy fetch).
//  2. Fresh run: build a wireContext with the initial snapshot and an
//     empty replay map. Execute every step, capturing emitted frames
//     and per-step results.
//  3. Build a replay map directly from the captured frames. For the
//     ops in scope (all single-slot) the engine apply path + the
//     replay translation are identity: the frame the SDK emits is
//     exactly the frame replay re-feeds. Slot N := emitted-frame
//     index N (none of the in-scope ops short-circuits while still
//     consuming a slot, and the ops that short-circuit — GetState
//     cache-hit, GetState known-absent — never call allocSlot).
//  4. Replay run: fresh wireContext with the same initial snapshot
//     seeded with the replay map. Execute the same script.
//  5. Assert:
//     (a) the replay run emits zero new frames, and
//     (b) every step's result equals the fresh run's.
//
// Failure modes this catches that the integration tests don't:
//   - any code change that makes a replay path emit a frame the
//     journal doesn't already have (silent journal mismatch);
//   - any code change that makes a replay path return a different
//     value than the fresh path (e.g. forgetting to update absentKeys
//     in one branch but not the other);
//   - any allocSlot drift between fresh / replay (cursor desync).
func TestWireContext_ReplayDeterminism(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		initState := initStateGen.Draw(rt, "init_state")
		script := eagerScriptGen.Draw(rt, "script")

		freshCtx, freshSink := newReplayPBTWireContext(t, initState, nil)
		freshResults := runScript(rt, freshCtx, script, "fresh")

		replay := buildReplayMap(rt, freshSink.sent)

		replayCtx, replaySink := newReplayPBTWireContext(t, initState, replay)
		replayResults := runScript(rt, replayCtx, script, "replay")

		if len(replaySink.sent) != 0 {
			types := make([]string, len(replaySink.sent))
			for i, f := range replaySink.sent {
				code, _, _ := wire.UnpackHeader(f.GetHeader())
				types[i] = fmt.Sprintf("0x%04x", code)
			}
			rt.Fatalf("replay emitted %d new frames %v; want 0\nscript: %s",
				len(replaySink.sent), types, scriptString(script))
		}

		if len(freshResults) != len(replayResults) {
			rt.Fatalf("result count mismatch: fresh=%d replay=%d\nscript: %s",
				len(freshResults), len(replayResults), scriptString(script))
		}
		for i := range freshResults {
			if !equalStepResult(freshResults[i], replayResults[i]) {
				rt.Fatalf("step %d (%s) diverges: fresh=%s replay=%s\nscript: %s",
					i, script[i], freshResults[i], replayResults[i], scriptString(script))
			}
		}
	})
}

// ----------------------------------------------------------------------
// Generators
//
// Generators are package-level values so the multi-slot PBT can reuse
// the state-surface step gens (setStepGen / clearStepGen / ... ) inside
// its own OneOf without redefining them. Labels passed to Draw inside
// a Custom generator are namespaced to that generator, so the
// fmt.Sprintf("foo_%d", i) per-step labels go away — rapid handles
// disambiguation in its draw tree.
// ----------------------------------------------------------------------

// keyPool is intentionally small so rapid hits write / clear / read
// interleavings on the same key. Three keys is enough to drive
// non-trivial state shapes.
var replayPBTKeyPool = []string{"alpha", "beta", "gamma"}

// valuePool covers the empty-bytes edge case (present + empty is
// distinct from absent) alongside short representative payloads.
var replayPBTValuePool = [][]byte{
	[]byte(""),
	[]byte("v0"),
	[]byte("v1"),
}

var (
	keyGen       = rapid.SampledFrom(replayPBTKeyPool)
	valueGen     = rapid.SampledFrom(replayPBTValuePool)
	initStateGen = rapid.MapOfN(keyGen, valueGen, 0, len(replayPBTKeyPool))

	setStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepSet{key: keyGen.Draw(t, "key"), value: valueGen.Draw(t, "value")}
	})
	clearStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepClear{key: keyGen.Draw(t, "key")}
	})
	clearAllStepGen = rapid.Just[scriptStep](stepClearAll{})
	getStepGen      = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepGet{key: keyGen.Draw(t, "key")}
	})
	getKeysStepGen = rapid.Just[scriptStep](stepGetKeys{})

	eagerStepGen = rapid.OneOf(
		setStepGen, clearStepGen, clearAllStepGen, getStepGen, getKeysStepGen,
	)
	eagerScriptGen = rapid.SliceOfN(eagerStepGen, 0, 16)
)

// ----------------------------------------------------------------------
// Script step types
// ----------------------------------------------------------------------

type scriptStep interface {
	apply(rt *rapid.T, ctx *wireContext, phase string) stepResult
	fmt.Stringer
}

// stepResult is the union of every shape a step can return. Compared
// element-wise by equalStepResult; printed by String for failure
// messages.
type stepResult struct {
	kind    string // "void" | "get" | "keys"
	val     []byte
	present bool
	keys    []string
	err     error
}

func (r stepResult) String() string {
	switch r.kind {
	case "void":
		return fmt.Sprintf("void{err=%v}", r.err)
	case "get":
		return fmt.Sprintf("get{val=%q,present=%v,err=%v}", r.val, r.present, r.err)
	case "keys":
		return fmt.Sprintf("keys{%v,err=%v}", r.keys, r.err)
	}
	return fmt.Sprintf("?{kind=%q}", r.kind)
}

func equalStepResult(a, b stepResult) bool {
	if a.kind != b.kind {
		return false
	}
	if (a.err == nil) != (b.err == nil) {
		return false
	}
	if a.err != nil && a.err.Error() != b.err.Error() {
		return false
	}
	switch a.kind {
	case "void":
		return true
	case "get":
		return a.present == b.present && bytes.Equal(a.val, b.val)
	case "keys":
		if len(a.keys) != len(b.keys) {
			return false
		}
		for i := range a.keys {
			if a.keys[i] != b.keys[i] {
				return false
			}
		}
		return true
	}
	return false
}

type stepSet struct {
	key   string
	value []byte
}

func (s stepSet) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	return stepResult{kind: "void", err: ctx.SetState(s.key, s.value)}
}
func (s stepSet) String() string { return fmt.Sprintf("Set(%q,%q)", s.key, s.value) }

type stepClear struct{ key string }

func (s stepClear) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	return stepResult{kind: "void", err: ctx.ClearState(s.key)}
}
func (s stepClear) String() string { return fmt.Sprintf("Clear(%q)", s.key) }

type stepClearAll struct{}

func (s stepClearAll) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	return stepResult{kind: "void", err: ctx.ClearAllState()}
}
func (s stepClearAll) String() string { return "ClearAll()" }

type stepGet struct{ key string }

func (s stepGet) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	v, p, err := ctx.GetState(s.key)
	return stepResult{kind: "get", val: v, present: p, err: err}
}
func (s stepGet) String() string { return fmt.Sprintf("Get(%q)", s.key) }

type stepGetKeys struct{}

func (s stepGetKeys) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	keys, err := ctx.GetStateKeys()
	return stepResult{kind: "keys", keys: keys, err: err}
}
func (s stepGetKeys) String() string { return "GetKeys()" }

func runScript(rt *rapid.T, ctx *wireContext, script []scriptStep, phase string) []stepResult {
	results := make([]stepResult, len(script))
	for i, step := range script {
		results[i] = step.apply(rt, ctx, phase)
		// Errors are part of the per-step result and compared by
		// equalStepResult — Run with a terminal *Failure outcome is
		// the canonical case. Only ErrSuspended is fatal: that means
		// the synthetic engine failed to stamp a result and the SDK
		// is now in a bad state for subsequent steps.
		if errors.Is(results[i].err, ErrSuspended) {
			rt.Fatalf("%s step %d (%s) suspended; synthetic engine missed a result slot", phase, i, step)
		}
	}
	return results
}

func scriptString(script []scriptStep) string {
	parts := make([]string, len(script))
	for i, s := range script {
		parts[i] = s.String()
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// ----------------------------------------------------------------------
// Replay-map builder
// ----------------------------------------------------------------------

// buildReplayMap synthesises the replay map the engine would feed back
// after journaling the SDK-emitted frames. For Phase 1's single-slot
// ops, the engine apply-path stamps the frame's payload as the journal
// entry, and wire_replay.translateEntry renders the same payload at the
// same slot. So the synthesis is identity: replay[slot=k+1] = frame k.
//
// Slot accounting: in the fresh run every script step that calls
// allocSlot consumes one slot (none of the in-scope ops are 2-slot).
// The ops that don't allocSlot (GetState cache-hit / known-absent /
// complete-snapshot-absent) also don't emit. So emitted-frame index N
// (zero-based) corresponds to slot N+1 (wireContext starts at slot 1
// since slot 0 is reserved for JEInput).
func buildReplayMap(rt *rapid.T, frames []*protocolv1.Frame) map[uint32]*replayEntry {
	out := make(map[uint32]*replayEntry, len(frames))
	for i, f := range frames {
		typeCode, _, _ := wire.UnpackHeader(f.GetHeader())
		switch typeCode {
		case wire.TypeCmdSetState,
			wire.TypeCmdClearState,
			wire.TypeCmdClearAllState,
			wire.TypeCmdGetEagerStateKeys:
			// In-scope single-slot ops. Replay frame == emit frame.
		default:
			rt.Fatalf("buildReplayMap: out-of-scope frame type 0x%04x at index %d; script should only generate single-slot ops",
				typeCode, i)
		}
		out[uint32(i+1)] = &replayEntry{
			typeCode: typeCode,
			payload:  f.GetPayload(),
		}
	}
	return out
}

// newReplayPBTWireContext builds a wireContext seeded with the given
// initial eager-state snapshot and (optionally) replay map. partialState
// is false — Phase 1 keeps every read on the in-cache fast path so no
// synthetic engine is needed to resolve lazy fetches.
func newReplayPBTWireContext(t *testing.T, init map[string][]byte, replay map[uint32]*replayEntry) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	// Copy init so the fresh / replay runs don't share state via the
	// stateCache pointer (wireContext mutates the map directly).
	var cache map[string][]byte
	if init != nil {
		cache = make(map[string][]byte, len(init))
		for k, v := range init {
			cache[k] = append([]byte(nil), v...)
		}
	}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(),
		cache, replay, 7, 0, "Svc", "Hdr", "obj", protocolv1.Kind_KIND_OBJECT)
	return wctx, stream
}
