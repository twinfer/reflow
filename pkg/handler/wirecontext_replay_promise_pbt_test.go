package handler

import (
	"fmt"
	"maps"
	"testing"

	"pgregory.net/rapid"

	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// TestWireContext_ReplayDeterminism_Promise extends the replay-PBT
// family to the DurablePromise surface. Promise requires KIND_WORKFLOW
// (or KIND_WORKFLOW_SHARED) — the wireContext rejects Promise method
// calls on KIND_SERVICE / KIND_OBJECT — so this test sets up its own
// wireContext separate from the multi-slot PBT.
//
// Covered: Get (Result) + Complete (Resolve + Reject). Peek is
// deliberately skipped: its fresh path unconditionally suspends after
// emitting the cmd, expecting the engine apply arm to stamp the
// snapshot directly onto the journal entry on a later session. The
// fresh→replay shape of this PBT can't drive that.
//
// Synthetic engine state:
//   - promiseStore tracks the (name → outcome) decisions made by prior
//     Complete steps. A Get for a name not yet completed resolves with
//     the default (nil) outcome — semantically that's "not yet
//     completed" but the engine never suspends in this PBT, so the
//     SDK sees an empty result and surfaces (nil, nil). The fresh and
//     replay runs see the same default, so the property still holds.
func TestWireContext_ReplayDeterminism_Promise(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		script := promiseScriptGen.Draw(rt, "script")

		engine := newPromiseSyntheticEngine(wire.DefaultCodec())
		freshCtx := newPromisePBTWireContext(t, engine, engine.replay)
		freshResults := runScript(rt, freshCtx, script, "fresh")

		replay := maps.Clone(engine.replay)
		replayCtx, replaySink := newPromisePBTReplayWireContext(t, replay)
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
// Step types
// ----------------------------------------------------------------------

type stepGetPromise struct{ name string }

func (s stepGetPromise) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	v, err := ctx.Promise(s.name).Result().Result()
	return stepResult{kind: "get", val: v, present: err == nil, err: err}
}
func (s stepGetPromise) String() string { return fmt.Sprintf("Get(%q)", s.name) }

type stepResolvePromise struct {
	name  string
	value []byte
}

func (s stepResolvePromise) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	return stepResult{kind: "void", err: ctx.Promise(s.name).Resolve(s.value)}
}
func (s stepResolvePromise) String() string { return fmt.Sprintf("Resolve(%q,%q)", s.name, s.value) }

type stepRejectPromise struct {
	name       string
	failureMsg string
}

func (s stepRejectPromise) apply(_ *rapid.T, ctx *wireContext, _ string) stepResult {
	return stepResult{kind: "void", err: ctx.Promise(s.name).Reject(NewFailure(0, s.failureMsg))}
}
func (s stepRejectPromise) String() string {
	return fmt.Sprintf("Reject(%q,%q)", s.name, s.failureMsg)
}

// ----------------------------------------------------------------------
// Generators
// ----------------------------------------------------------------------

var (
	promiseNameGen  = rapid.SampledFrom([]string{"p_alpha", "p_beta"})
	promiseValueGen = rapid.Map(rapid.SampledFrom([]string{"v0", "v1"}), func(s string) []byte { return []byte(s) })
	promiseFailGen  = rapid.SampledFrom([]string{"boom", "denied"})

	getPromiseStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepGetPromise{name: promiseNameGen.Draw(t, "name")}
	})
	resolvePromiseStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepResolvePromise{
			name:  promiseNameGen.Draw(t, "name"),
			value: promiseValueGen.Draw(t, "value"),
		}
	})
	rejectPromiseStepGen = rapid.Custom(func(t *rapid.T) scriptStep {
		return stepRejectPromise{
			name:       promiseNameGen.Draw(t, "name"),
			failureMsg: promiseFailGen.Draw(t, "fail"),
		}
	})

	promiseStepGen   = rapid.OneOf(getPromiseStepGen, resolvePromiseStepGen, rejectPromiseStepGen)
	promiseScriptGen = rapid.SliceOfN(promiseStepGen, 0, 12)
)

// ----------------------------------------------------------------------
// Synthetic engine — Promise variant
// ----------------------------------------------------------------------

// promiseOutcome captures what a prior Complete (Resolve/Reject) recorded
// for a given promise name. failure non-empty marks a Reject; otherwise
// it's a Resolve with value bytes (which may be empty).
type promiseOutcome struct {
	resolved bool
	value    []byte
	failure  string
}

type promiseSyntheticEngine struct {
	codec    wire.Codec
	store    map[string]promiseOutcome
	replay   map[uint32]*replayEntry
	sent     []*protocolv1.Frame
	nextSlot uint32
}

func newPromiseSyntheticEngine(codec wire.Codec) *promiseSyntheticEngine {
	return &promiseSyntheticEngine{
		codec:    codec,
		store:    make(map[string]promiseOutcome),
		replay:   make(map[uint32]*replayEntry),
		nextSlot: 1,
	}
}

func (e *promiseSyntheticEngine) Send(f *protocolv1.Frame) error {
	e.sent = append(e.sent, f)
	typeCode, _, _ := wire.UnpackHeader(f.GetHeader())
	slot := e.nextSlot

	switch typeCode {
	case wire.TypeCmdGetPromise:
		var cmd protocolv1.GetPromiseCommandMessage
		if err := e.codec.Unmarshal(f.GetPayload(), &cmd); err != nil {
			return fmt.Errorf("promiseSyntheticEngine: decode GetPromise: %w", err)
		}
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		note := &protocolv1.GetPromiseCompletionNotificationMessage{CompletionId: slot + 1}
		if outcome, ok := e.store[cmd.GetName()]; ok && outcome.resolved {
			if outcome.failure != "" {
				note.Result = &protocolv1.GetPromiseCompletionNotificationMessage_Failure{
					Failure: &protocolv1.Failure{Message: outcome.failure},
				}
			} else {
				note.Result = &protocolv1.GetPromiseCompletionNotificationMessage_Value{
					Value: &protocolv1.Value{Content: append([]byte(nil), outcome.value...)},
				}
			}
		}
		// If the promise isn't in the store, leave note.Result nil
		// (default branch in promiseResultFuture.Result returns
		// (nil, nil)). Real engines would have left the SDK suspended;
		// the PBT trades that for a deterministic null outcome so the
		// fresh / replay comparison still holds.
		payload, err := e.codec.Marshal(note)
		if err != nil {
			return fmt.Errorf("promiseSyntheticEngine: marshal GetPromiseNote: %w", err)
		}
		e.replay[slot+1] = &replayEntry{typeCode: wire.TypeNoteGetPromise, payload: payload}
		e.nextSlot += 2

	case wire.TypeCmdCompletePromise:
		var cmd protocolv1.CompletePromiseCommandMessage
		if err := e.codec.Unmarshal(f.GetPayload(), &cmd); err != nil {
			return fmt.Errorf("promiseSyntheticEngine: decode CompletePromise: %w", err)
		}
		e.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		// Record the first Complete; subsequent completes for the same
		// name would surface as already-completed failures in real life,
		// but the PBT keeps the model simple — first writer wins, later
		// Completes still get Void back. Replay sees the same.
		if _, already := e.store[cmd.GetName()]; !already {
			outcome := promiseOutcome{resolved: true}
			switch c := cmd.GetCompletion().(type) {
			case *protocolv1.CompletePromiseCommandMessage_CompletionValue:
				outcome.value = append([]byte(nil), c.CompletionValue.GetContent()...)
			case *protocolv1.CompletePromiseCommandMessage_CompletionFailure:
				outcome.failure = c.CompletionFailure.GetMessage()
			}
			e.store[cmd.GetName()] = outcome
		}
		note := &protocolv1.CompletePromiseCompletionNotificationMessage{
			CompletionId: slot + 1,
			Result:       &protocolv1.CompletePromiseCompletionNotificationMessage_Void{Void: &protocolv1.Void{}},
		}
		payload, err := e.codec.Marshal(note)
		if err != nil {
			return fmt.Errorf("promiseSyntheticEngine: marshal CompletePromiseNote: %w", err)
		}
		e.replay[slot+1] = &replayEntry{typeCode: wire.TypeNoteCompletePromise, payload: payload}
		e.nextSlot += 2

	default:
		return fmt.Errorf("promiseSyntheticEngine: unsupported frame type 0x%04x", typeCode)
	}
	return nil
}

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

func newPromisePBTWireContext(t *testing.T, sink *promiseSyntheticEngine, replay map[uint32]*replayEntry) *wireContext {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	wctx := newWireContext(t.Context(), id, []byte("hello"), sink, wire.DefaultCodec(),
		nil, replay, 7, 0, "Workflow", "tick", "wkey", protocolv1.Kind_KIND_WORKFLOW)
	return wctx
}

func newPromisePBTReplayWireContext(t *testing.T, replay map[uint32]*replayEntry) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(),
		nil, replay, 7, 0, "Workflow", "tick", "wkey", protocolv1.Kind_KIND_WORKFLOW)
	return wctx, stream
}
