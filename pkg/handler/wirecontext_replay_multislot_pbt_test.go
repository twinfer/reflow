package handler

import (
	"fmt"
	"maps"
	"sort"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// TestWireContext_ReplayDeterminism_MultiSlot extends the Phase 1 PBT
// (wirecontext_replay_pbt_test.go) to multi-slot operations: Sleep and
// the lazy paths of GetState / GetStateKeys. Phase 1 ran with
// partialState=false so every read served from the local cache; this
// test holds partialState=true so cache misses actually trigger
// 2-slot lazy fetches, and adds Sleep as a representative 2-slot op
// whose result is always void.
//
// The fresh run can't use a passive sink because a 2-slot op suspends
// without its result slot populated. A syntheticEngine acts as the sink:
// when the SDK emits a 2-slot command frame, the engine immediately
// stamps the corresponding result frame in the replay map being built,
// so the SDK's next lookupReplay finds it and resolves the future
// without suspending. The engine mirrors model state so lazy GetState
// results reflect every prior SetState/ClearState/ClearAllState the
// SDK shipped in this session — same read-your-own-writes semantic the
// real engine apply path provides.
//
// Property (unchanged in shape from Phase 1):
//
//  1. Fresh run with syntheticEngine sink → captures emit frames AND
//     the replay map built up alongside them.
//  2. Replay run with a passive sink + the replay map → executes the
//     same script.
//  3. Replay run must emit zero frames; every step result must equal
//     the fresh run's.
//
// Scope: state + Sleep + the easy multi-slot ops that fit the same
// "engine stamps result inline" template — Call, OneWayCall, Awakeable,
// SendSignal. Out of scope:
//
//   - AwaitSignal: needs an inbox model (signals delivered before the
//     await are inlined; otherwise the result is stitched in by a later
//     SignalDelivered effect).
//   - Promise (Get/Peek/Complete): workflow-scoped, multi-handler
//     coordination, scope-name model.
//   - Run: emits a marker (RunCommandMessage) plus a separate
//     ProposeRunCompletionMessage carrying the SDK-computed outcome —
//     the propose-completion shape doesn't fit the "engine stamps
//     result" template.
//
// Each deserves a focused PBT rather than being bundled here.
func TestWireContext_ReplayDeterminism_MultiSlot(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		initState := initStateGen.Draw(rt, "init_state")
		script := multiSlotScriptGen.Draw(rt, "script")

		engine := newSyntheticEngine(initState, wire.DefaultCodec())
		// The engine and the SDK MUST share the same replay map by
		// reference — the engine's Send writes result frames into it,
		// the SDK's lookupReplay reads from it. The synchronous Send
		// → lookupReplay pattern inside wireContext relies on that
		// shared view.
		freshCtx := newMultiSlotWireContext(t, initState, true, engine, engine.replay)
		freshResults := runScript(rt, freshCtx, script, "fresh")

		// Snapshot the replay map by value — the replay run mutates it
		// indirectly via wireContext's SetState / ClearState / etc.
		// touching stateCache (the replay map itself stays stable, but
		// safer to hand over a copy).
		replay := maps.Clone(engine.replay)
		replayCtx, replaySink := newMultiSlotReplayWireContext(t, initState, true, replay)
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
// Generators (multi-slot extension)
//
// Reuses the state-surface step gens from the Phase 1 file
// (setStepGen / clearStepGen / clearAllStepGen / getStepGen /
// getKeysStepGen) and adds the multi-slot ops on top via rapid.OneOf.
// ----------------------------------------------------------------------

var (
	sleepStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepSleep{durMs: uint64(rapid.IntRange(1, 1000).Draw(t, "dur_ms"))}
	})
	callInputGen = rapid.Map(rapid.SampledFrom([]string{"a", "bb", "ccc"}),
		func(s string) []byte { return []byte(s) })
	callStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepCall{input: callInputGen.Draw(t, "input")}
	})
	oneWayCallInputGen = rapid.Map(rapid.SampledFrom([]string{"ow0", "ow1"}),
		func(s string) []byte { return []byte(s) })
	oneWayCallStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepOneWayCall{input: oneWayCallInputGen.Draw(t, "input")}
	})
	awakeableStepGen = rapid.Just[scriptStep](stepAwakeable{})
	signalNameGen    = rapid.SampledFrom([]string{"sig_a", "sig_b"})
	signalPayloadGen = rapid.Map(rapid.SampledFrom([]string{"", "p0", "p1"}),
		func(s string) []byte { return []byte(s) })
	sendSignalStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepSendSignal{
			name:    signalNameGen.Draw(t, "name"),
			payload: signalPayloadGen.Draw(t, "payload"),
		}
	})

	multiSlotStepGen = rapid.OneOf(
		// State-surface (reused from Phase 1; lazy paths exercised
		// because the multi-slot test runs with partialState=true).
		setStepGen, clearStepGen, clearAllStepGen, getStepGen, getKeysStepGen,
		// Multi-slot + fire-and-forget added in Phase 2B and the
		// Call/OneWayCall/Awakeable/SendSignal extension.
		sleepStepGen, callStepGen, oneWayCallStepGen, awakeableStepGen, sendSignalStepGen,
	)
	multiSlotScriptGen = rapid.SliceOfN(multiSlotStepGen, 0, 16)
)

type stepSleep struct{ durMs uint64 }

func (s stepSleep) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	_, err := ctx.Sleep(time.Duration(s.durMs) * time.Millisecond).Result()
	return stepResult{kind: "void", err: err}
}
func (s stepSleep) String() string { return fmt.Sprintf("Sleep(%dms)", s.durMs) }

// Call target is fixed — these PBTs care about the replay invariant,
// not routing. The synthetic engine echoes input as the result so
// callFuture.Result has something deterministic to compare.
var pbtCallTarget = Target{Service: "Svc", Handler: "Hdr", Key: "ckey"}

type stepCall struct{ input []byte }

func (s stepCall) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	v, err := ctx.Call(pbtCallTarget, s.input).Result()
	return stepResult{kind: "get", val: v, present: err == nil, err: err}
}
func (s stepCall) String() string { return fmt.Sprintf("Call(%q)", s.input) }

type stepOneWayCall struct{ input []byte }

func (s stepOneWayCall) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	return stepResult{kind: "void", err: ctx.OneWayCall(pbtCallTarget, s.input)}
}
func (s stepOneWayCall) String() string { return fmt.Sprintf("OneWayCall(%q)", s.input) }

type stepAwakeable struct{}

func (s stepAwakeable) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	// Awakeable returns (id, future). The id is non-deterministic on the
	// fresh run (mintAwakeableID uses crypto/rand) but the same id is
	// surfaced on replay (from the journaled cmd frame). Discard id from
	// the result comparison since we're testing the result-resolution
	// path; the awakeableFuture's id-matching is exercised inline by
	// the SDK reading the cmd payload.
	_, future := ctx.Awakeable()
	v, err := future.Result()
	return stepResult{kind: "get", val: v, present: err == nil, err: err}
}
func (s stepAwakeable) String() string { return "Awakeable()" }

type stepSendSignal struct {
	name    string
	payload []byte
}

func (s stepSendSignal) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	return stepResult{kind: "void", err: ctx.SendSignal(pbtCallTarget, s.name, s.payload)}
}
func (s stepSendSignal) String() string { return fmt.Sprintf("SendSignal(%q,%q)", s.name, s.payload) }

// ----------------------------------------------------------------------
// syntheticEngine — the active sink that stands in for the real engine
// apply path during the fresh PBT run.
// ----------------------------------------------------------------------

// syntheticEngine intercepts every frame the SDK emits, advances a slot
// cursor identical to what the real engine would use, and stamps both
// the command entry AND its result (for 2-slot ops) into a replay map
// that grows alongside the run. The replay map is the artefact the
// replay phase consumes.
//
// State model: tracks (service, object_key) state by mirroring every
// observed SetState / ClearState / ClearAllState frame. Lazy GetState
// results reflect this model — same read-your-own-writes contract the
// real apply path provides via in-batch StateTable + journal.Append
// coherence.
//
// Slot accounting mirrors wireContext.nextSlot: starts at 1 (slot 0
// is JEInput), advances by 1 for single-slot ops, by 2 for two-slot.
type syntheticEngine struct {
	codec    wire.Codec
	model    map[string][]byte
	replay   map[uint32]*replayEntry
	sent     []*protocolv1.Frame
	nextSlot uint32
}

func newSyntheticEngine(init map[string][]byte, codec wire.Codec) *syntheticEngine {
	model := make(map[string][]byte, len(init))
	for k, v := range init {
		model[k] = append([]byte(nil), v...)
	}
	return &syntheticEngine{
		codec:    codec,
		model:    model,
		replay:   make(map[uint32]*replayEntry),
		nextSlot: 1,
	}
}

func (e *syntheticEngine) Send(f *protocolv1.Frame) error {
	e.sent = append(e.sent, f)
	typeCode, _, _ := wire.UnpackHeader(f.GetHeader())
	slot := e.nextSlot

	switch typeCode {
	// --- single-slot writes: stamp cmd, mutate model, advance 1 ---
	case wire.TypeCmdSetState:
		var cmd protocolv1.SetStateCommandMessage
		if err := e.codec.Unmarshal(f.GetPayload(), &cmd); err != nil {
			return fmt.Errorf("syntheticEngine: decode SetState: %w", err)
		}
		e.model[string(cmd.GetKey())] = append([]byte(nil), cmd.GetValue().GetContent()...)
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		e.nextSlot++

	case wire.TypeCmdClearState:
		var cmd protocolv1.ClearStateCommandMessage
		if err := e.codec.Unmarshal(f.GetPayload(), &cmd); err != nil {
			return fmt.Errorf("syntheticEngine: decode ClearState: %w", err)
		}
		delete(e.model, string(cmd.GetKey()))
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		e.nextSlot++

	case wire.TypeCmdClearAllState:
		for k := range e.model {
			delete(e.model, k)
		}
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		e.nextSlot++

	// --- single-slot read with inline payload: stamp cmd, advance 1 ---
	case wire.TypeCmdGetEagerStateKeys:
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		e.nextSlot++

	// --- two-slot: stamp cmd AND synthesised result, advance 2 ---
	case wire.TypeCmdGetLazyState:
		var cmd protocolv1.GetLazyStateCommandMessage
		if err := e.codec.Unmarshal(f.GetPayload(), &cmd); err != nil {
			return fmt.Errorf("syntheticEngine: decode GetLazyState: %w", err)
		}
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		note := &protocolv1.GetLazyStateCompletionNotificationMessage{
			CompletionId: slot + 1,
		}
		if v, ok := e.model[string(cmd.GetKey())]; ok {
			note.Result = &protocolv1.GetLazyStateCompletionNotificationMessage_Value{
				Value: &protocolv1.Value{Content: append([]byte(nil), v...)},
			}
		} else {
			note.Result = &protocolv1.GetLazyStateCompletionNotificationMessage_Void{
				Void: &protocolv1.Void{},
			}
		}
		payload, err := e.codec.Marshal(note)
		if err != nil {
			return fmt.Errorf("syntheticEngine: marshal GetLazyStateNote: %w", err)
		}
		e.replay[slot+1] = &replayEntry{typeCode: wire.TypeNoteGetLazyState, payload: payload}
		e.nextSlot += 2

	case wire.TypeCmdGetLazyStateKeys:
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		keys := make([]string, 0, len(e.model))
		for k := range e.model {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		keysBytes := make([][]byte, len(keys))
		for i, k := range keys {
			keysBytes[i] = []byte(k)
		}
		note := &protocolv1.GetLazyStateKeysCompletionNotificationMessage{
			CompletionId: slot + 1,
			StateKeys:    &protocolv1.StateKeys{Keys: keysBytes},
		}
		payload, err := e.codec.Marshal(note)
		if err != nil {
			return fmt.Errorf("syntheticEngine: marshal GetLazyStateKeysNote: %w", err)
		}
		e.replay[slot+1] = &replayEntry{typeCode: wire.TypeNoteGetLazyStateKeys, payload: payload}
		e.nextSlot += 2

	case wire.TypeCmdSleep:
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		note := &protocolv1.SleepCompletionNotificationMessage{
			CompletionId: slot + 1,
			Void:         &protocolv1.Void{},
		}
		payload, err := e.codec.Marshal(note)
		if err != nil {
			return fmt.Errorf("syntheticEngine: marshal SleepNote: %w", err)
		}
		e.replay[slot+1] = &replayEntry{typeCode: wire.TypeNoteSleepDone, payload: payload}
		e.nextSlot += 2

	case wire.TypeCmdCall:
		var cmd protocolv1.CallCommandMessage
		if err := e.codec.Unmarshal(f.GetPayload(), &cmd); err != nil {
			return fmt.Errorf("syntheticEngine: decode Call: %w", err)
		}
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		// Echo input as the callee's result — deterministic, lets the
		// SDK's callFuture.Result do its decode and return something
		// the PBT can compare across fresh / replay.
		note := &protocolv1.CallCompletionNotificationMessage{
			CompletionId: slot + 1,
			Result: &protocolv1.CallCompletionNotificationMessage_Value{
				Value: &protocolv1.Value{Content: append([]byte(nil), cmd.GetParameter()...)},
			},
		}
		payload, err := e.codec.Marshal(note)
		if err != nil {
			return fmt.Errorf("syntheticEngine: marshal CallNote: %w", err)
		}
		e.replay[slot+1] = &replayEntry{typeCode: wire.TypeNoteCallDone, payload: payload}
		e.nextSlot += 2

	case wire.TypeCmdOneWayCall:
		// Fire-and-forget. Single-slot, no result; the SDK never reads
		// back so stamping the cmd is enough for replay.
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		e.nextSlot++

	case wire.TypeCmdAwakeable:
		var cmd protocolv1.AwakeableCommandMessage
		if err := e.codec.Unmarshal(f.GetPayload(), &cmd); err != nil {
			return fmt.Errorf("syntheticEngine: decode Awakeable: %w", err)
		}
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		// Resolve immediately with a deterministic value. awakeableFuture
		// expects a SignalNotificationMessage with a Name-shaped signal_id
		// (the awakeable id from the cmd) — id-matching happens inside
		// the SDK, not in the wire frame's slot stamp.
		note := &protocolv1.SignalNotificationMessage{
			SignalId: &protocolv1.SignalNotificationMessage_Name{Name: cmd.GetAwakeableId()},
			Result: &protocolv1.SignalNotificationMessage_Value{
				Value: &protocolv1.Value{Content: []byte("awk-result")},
			},
		}
		payload, err := e.codec.Marshal(note)
		if err != nil {
			return fmt.Errorf("syntheticEngine: marshal AwakeableNote: %w", err)
		}
		e.replay[slot+1] = &replayEntry{typeCode: wire.TypeNoteSignal, payload: payload}
		e.nextSlot += 2

	case wire.TypeCmdSendSignal:
		// Fire-and-forget signal. Single-slot, no result.
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		e.nextSlot++

	default:
		return fmt.Errorf("syntheticEngine: unsupported frame type 0x%04x (extend the engine to add new ops)", typeCode)
	}
	return nil
}

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

func newMultiSlotWireContext(t *testing.T, init map[string][]byte, partial bool, sink *syntheticEngine, replay map[uint32]*replayEntry) *wireContext {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	// Copy init so the fresh / replay runs don't share state via the
	// stateCache pointer.
	var cache map[string][]byte
	if init != nil {
		cache = make(map[string][]byte, len(init))
		for k, v := range init {
			cache[k] = append([]byte(nil), v...)
		}
	}
	wctx := newWireContext(t.Context(), id, []byte("hello"), sink, wire.DefaultCodec(),
		cache, replay, 7, 0, "Svc", "Hdr", "obj", protocolv1.Kind_KIND_OBJECT)
	wctx.partialState = partial
	return wctx
}

func newMultiSlotReplayWireContext(t *testing.T, init map[string][]byte, partial bool, replay map[uint32]*replayEntry) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	var cache map[string][]byte
	if init != nil {
		cache = make(map[string][]byte, len(init))
		for k, v := range init {
			cache[k] = append([]byte(nil), v...)
		}
	}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(),
		cache, replay, 7, 0, "Svc", "Hdr", "obj", protocolv1.Kind_KIND_OBJECT)
	wctx.partialState = partial
	return wctx, stream
}
