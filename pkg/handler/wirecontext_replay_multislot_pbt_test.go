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
// Scope: state + Sleep. Call / Run / Awakeable / Signal / Promise have
// different state models (callee outcomes, signal inboxes, promise
// stores) and each merits its own focused PBT — bundling them into
// one synthetic engine would duplicate logic that already lives in
// integration tests for each.
func TestWireContext_ReplayDeterminism_MultiSlot(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		initState := drawInitState(rt)
		script := drawMultiSlotScript(rt)

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
// Script generator (multi-slot variant)
// ----------------------------------------------------------------------

func drawMultiSlotScript(rt *rapid.T) []scriptStep {
	n := rapid.IntRange(0, 16).Draw(rt, "ms_script_len")
	if n == 0 {
		return nil
	}
	out := make([]scriptStep, n)
	for i := range out {
		out[i] = drawMultiSlotStep(rt, i)
	}
	return out
}

func drawMultiSlotStep(rt *rapid.T, i int) scriptStep {
	// Same step kinds as Phase 1 plus Sleep. We reuse the existing
	// step types (their apply method on *wireContext works identically;
	// the difference is only in which path inside wireContext fires).
	kind := rapid.IntRange(0, 5).Draw(rt, fmt.Sprintf("ms_step_kind_%d", i))
	switch kind {
	case 0:
		return stepSet{
			key:   rapid.SampledFrom(replayPBTKeyPool).Draw(rt, fmt.Sprintf("ms_set_key_%d", i)),
			value: rapid.SampledFrom(replayPBTValuePool).Draw(rt, fmt.Sprintf("ms_set_val_%d", i)),
		}
	case 1:
		return stepClear{
			key: rapid.SampledFrom(replayPBTKeyPool).Draw(rt, fmt.Sprintf("ms_clear_key_%d", i)),
		}
	case 2:
		return stepClearAll{}
	case 3:
		return stepGet{
			key: rapid.SampledFrom(replayPBTKeyPool).Draw(rt, fmt.Sprintf("ms_get_key_%d", i)),
		}
	case 4:
		return stepGetKeys{}
	case 5:
		return stepSleep{
			durMs: uint64(rapid.IntRange(1, 1000).Draw(rt, fmt.Sprintf("ms_sleep_dur_%d", i))),
		}
	}
	panic("unreachable")
}

type stepSleep struct{ durMs uint64 }

func (s stepSleep) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	_, err := ctx.Sleep(time.Duration(s.durMs) * time.Millisecond).Result()
	return stepResult{kind: "void", err: err}
}
func (s stepSleep) String() string { return fmt.Sprintf("Sleep(%dms)", s.durMs) }

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
