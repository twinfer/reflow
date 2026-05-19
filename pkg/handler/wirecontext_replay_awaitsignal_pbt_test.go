package handler

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// TestWireContext_AwaitSignal_PendingThenDelivered is a multi-session
// PBT for the "signal arrives AFTER the await" path. The single-session
// multi-slot PBT covers the buffered-delivery case (engine stamps the
// result inline at the cmd slot); this one covers what the
// single-session shape can't reach: the SDK suspends pending an
// external delivery, the engine later stitches the result into the
// journal, the next session reads it back.
//
// Scenario shape: one signal name + one delivery outcome (Value /
// Failure / Void — every variant of SignalNotificationMessage.Result).
//
// Simulation:
//
//   - Session 1: SDK calls ctx.WaitSignal(name).Result(). The sink
//     stamps only the cmd at cmdSlot (it does NOT pre-resolve, since
//     this PBT is the pending-delivery case). signalFuture.Result
//     finds no entry at resultSlot, suspends with ErrSuspended.
//   - Between sessions: the test "stitches" the engine's would-be
//     SignalDelivered effect by writing a TypeNoteSignal frame at
//     resultSlot — the same shape wire_replay.translateEntry produces
//     for JESignalResult on the next replay.
//   - Session 2: SDK re-enters WaitSignal. The cmd is in replay → no
//     re-emit. signalFuture.Result reads the stitched note and
//     decodes the payload.
//
// Asserted invariants per scenario:
//
//   - Session 1 returns (nil, ErrSuspended) — never (payload, nil).
//   - Session 2 emits zero frames (the cmd is replay-suppressed).
//   - Session 2's decoded value/error matches the stitched delivery:
//     Value → (payload, nil), Failure → (nil, *Failure with message),
//     Void → (nil, nil).
//
// Out of scope: multiple AwaitSignals in one script, or AwaitSignal
// interleaved with other ops. The single-session PBTs cover composition
// of the buffered case; this PBT focuses on the pending-then-delivered
// path that nothing else exercises.
func TestWireContext_AwaitSignal_PendingThenDelivered(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		scenario := awaitSignalScenarioGen.Draw(rt, "scenario")

		// --- Session 1: SDK suspends pending the delivery. ---
		replay := make(map[uint32]*replayEntry)
		sink := newAwaitSignalSink(wire.DefaultCodec(), replay)
		ctx := newAwaitSignalWireContext(t, sink, replay)

		v1, err1 := ctx.WaitSignal(scenario.name).Result()
		if !errors.Is(err1, ErrSuspended) {
			rt.Fatalf("session 1 = (%q, %v); want ErrSuspended (scenario: %s)",
				v1, err1, awaitSignalScenarioString(scenario))
		}
		if v1 != nil {
			rt.Fatalf("session 1 value = %q; want nil on suspend", v1)
		}
		// Sink must have stamped the cmd at slot 1 (WaitSignal allocates
		// 2 slots from nextSlot=1). The result slot must be empty —
		// stitching the delivery is the test's job, not the sink's.
		if _, ok := replay[1]; !ok {
			rt.Fatalf("session 1: replay missing cmd at slot 1")
		}
		if _, ok := replay[2]; ok {
			rt.Fatalf("session 1: replay has unexpected entry at slot 2; sink shouldn't pre-resolve in the pending PBT")
		}

		// --- Stitch the engine's SignalDelivered effect into the
		// journal-shaped replay map. Mirrors what
		// wire_replay.translateEntry produces for JESignalResult: a
		// TypeNoteSignal carrying the delivered payload at the result
		// slot the cmd reserved.
		stitchSignalDelivery(t, replay, 2, scenario)

		// --- Session 2: replay-carried. SDK must not emit. ---
		stream2 := &fakeStream{}
		ctx2 := newWireContext(t.Context(), runRetryTestInvID, []byte("hello"), stream2,
			wire.DefaultCodec(), nil, replay, 7, 0, "Svc", "Hdr", "skey",
			protocolv1.Kind_KIND_OBJECT)

		v2, err2 := ctx2.WaitSignal(scenario.name).Result()
		if len(stream2.sent) != 0 {
			types := make([]string, len(stream2.sent))
			for i, f := range stream2.sent {
				code, _, _ := wire.UnpackHeader(f.GetHeader())
				types[i] = fmt.Sprintf("0x%04x", code)
			}
			rt.Fatalf("session 2 emitted %d frames %v; want 0 (cmd is in replay)",
				len(stream2.sent), types)
		}

		switch scenario.delivery.kind {
		case "void":
			if v2 != nil || err2 != nil {
				rt.Fatalf("void delivery → (%q, %v); want (nil, nil)", v2, err2)
			}
		case "failure":
			var f *Failure
			if !errors.As(err2, &f) {
				rt.Fatalf("failure delivery → err = %v; want *Failure", err2)
			}
			if f.Message != scenario.delivery.failureMsg {
				rt.Fatalf("failure delivery → message = %q; want %q",
					f.Message, scenario.delivery.failureMsg)
			}
		case "value":
			if err2 != nil {
				rt.Fatalf("value delivery → err = %v; want nil", err2)
			}
			if !bytes.Equal(v2, scenario.delivery.value) {
				rt.Fatalf("value delivery → (%q); want (%q)", v2, scenario.delivery.value)
			}
		default:
			rt.Fatalf("scenario has unknown delivery kind %q", scenario.delivery.kind)
		}
	})
}

// ----------------------------------------------------------------------
// Scenario type + generator
// ----------------------------------------------------------------------

type signalDelivery struct {
	kind       string // "value" | "failure" | "void"
	value      []byte
	failureMsg string
}

type awaitSignalScenario struct {
	name     string
	delivery signalDelivery
}

var (
	awaitSignalNameGen     = rapid.SampledFrom([]string{"sig_a", "sig_b"})
	awaitSignalPayloadGen  = rapid.Map(rapid.SampledFrom([]string{"", "p0", "p1"}), func(s string) []byte { return []byte(s) })
	awaitSignalFailMsgGen  = rapid.SampledFrom([]string{"denied", "cancelled"})
	awaitSignalDeliveryGen = rapid.Custom(func(t *rapid.T) signalDelivery {
		kind := rapid.SampledFrom([]string{"value", "failure", "void"}).Draw(t, "kind")
		switch kind {
		case "value":
			return signalDelivery{kind: kind, value: awaitSignalPayloadGen.Draw(t, "payload")}
		case "failure":
			return signalDelivery{kind: kind, failureMsg: awaitSignalFailMsgGen.Draw(t, "fail")}
		default:
			return signalDelivery{kind: "void"}
		}
	})
	awaitSignalScenarioGen = rapid.Custom(func(t *rapid.T) awaitSignalScenario {
		return awaitSignalScenario{
			name:     awaitSignalNameGen.Draw(t, "name"),
			delivery: awaitSignalDeliveryGen.Draw(t, "delivery"),
		}
	})
)

func awaitSignalScenarioString(s awaitSignalScenario) string {
	switch s.delivery.kind {
	case "value":
		return fmt.Sprintf("WaitSignal(%q)→Value(%q)", s.name, s.delivery.value)
	case "failure":
		return fmt.Sprintf("WaitSignal(%q)→Failure(%q)", s.name, s.delivery.failureMsg)
	default:
		return fmt.Sprintf("WaitSignal(%q)→Void", s.name)
	}
}

// ----------------------------------------------------------------------
// awaitSignalSink — captures the cmd frame for slot accounting. Does NOT
// pre-resolve the result slot: this PBT is specifically the
// pending-then-delivered case where stitching is deferred to the test
// body.
// ----------------------------------------------------------------------

type awaitSignalSink struct {
	codec  wire.Codec
	replay map[uint32]*replayEntry
}

func newAwaitSignalSink(codec wire.Codec, replay map[uint32]*replayEntry) *awaitSignalSink {
	return &awaitSignalSink{codec: codec, replay: replay}
}

func (s *awaitSignalSink) Send(f *protocolv1.Frame) error {
	typeCode, _, _ := wire.UnpackHeader(f.GetHeader())
	if typeCode != wire.TypeCmdAwaitSignal {
		return fmt.Errorf("awaitSignalSink: unexpected frame type 0x%04x", typeCode)
	}
	var cmd protocolv1.AwaitSignalCommandMessage
	if err := s.codec.Unmarshal(f.GetPayload(), &cmd); err != nil {
		return fmt.Errorf("awaitSignalSink: decode AwaitSignalCmd: %w", err)
	}
	// cmd slot is one before the encoded result_completion_id.
	cmdSlot := cmd.GetResultCompletionId() - 1
	s.replay[cmdSlot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
	return nil
}

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

// stitchSignalDelivery writes the TypeNoteSignal frame the engine's
// SignalDelivered effect would produce. Slot is the result slot the
// cmd reserved. The signal id is encoded by name (matches the SDK's
// signalFuture.Result discriminator).
func stitchSignalDelivery(t *testing.T, replay map[uint32]*replayEntry, slot uint32, scenario awaitSignalScenario) {
	t.Helper()
	note := &protocolv1.SignalNotificationMessage{
		SignalId: &protocolv1.SignalNotificationMessage_Name{Name: scenario.name},
	}
	switch scenario.delivery.kind {
	case "value":
		note.Result = &protocolv1.SignalNotificationMessage_Value{
			Value: &protocolv1.Value{Content: append([]byte(nil), scenario.delivery.value...)},
		}
	case "failure":
		note.Result = &protocolv1.SignalNotificationMessage_Failure{
			Failure: &protocolv1.Failure{Message: scenario.delivery.failureMsg},
		}
	case "void":
		note.Result = &protocolv1.SignalNotificationMessage_Void{Void: &protocolv1.Void{}}
	}
	payload, err := wire.DefaultCodec().Marshal(note)
	if err != nil {
		t.Fatalf("stitchSignalDelivery: marshal: %v", err)
	}
	replay[slot] = &replayEntry{typeCode: wire.TypeNoteSignal, payload: payload}
}

func newAwaitSignalWireContext(t *testing.T, sink *awaitSignalSink, replay map[uint32]*replayEntry) *wireContext {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	wctx := newWireContext(t.Context(), id, []byte("hello"), sink, wire.DefaultCodec(),
		nil, replay, 7, 0, "Svc", "Hdr", "skey", protocolv1.Kind_KIND_OBJECT)
	return wctx
}
