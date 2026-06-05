package handler

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/twinfer/reflw/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
)

// TestWireContext_RunRetry_MultiSession is the load-bearing PBT for
// the SDK's retryable-Run path. The single-session replay PBTs can't
// drive this code at all — a retryable outcome causes the SDK to
// suspend with ErrSuspended, and the fresh→replay shape ends right
// there. Retry semantics only emerge when the journal carries the
// marker forward across a session respawn.
//
// Scenario shape: a list of N transient outcomes followed by exactly
// one terminal outcome (success or *Failure). Each scenario is replayed
// against the SDK in a loop:
//
//   - session k: SDK enters Run, fn returns scenario.attempts[k-1].
//     Transient (plain error from fn) → SDK emits marker +
//     ProposeRunCompletion(retryable=true), suspends with
//     ErrSuspended. The runRetrySink builds the next session's
//     replay map: a marker at the same slot, attempt bumped to k+1,
//     idempotency key re-derived for the new attempt (mirroring what
//     the engine apply path stamps on JERun.retryable=true).
//   - terminal session: fn returns success or *Failure. SDK emits
//     marker + ProposeRunCompletion(retryable=false) and returns the
//     value/failure directly (no suspend).
//
// Invariants asserted per scenario:
//
//   - fn was invoked exactly len(scenario.attempts) times.
//   - On the kth invocation (1-indexed) the RunContext reported
//     Attempt()==k.
//   - Idempotency key changed between every consecutive pair of
//     attempts (the SDK derives it from (id, slot, attempt) on the
//     first attempt and reads marker.IdempotencyKey on subsequent
//     attempts; the engine recomputes for each new attempt so both
//     sides agree).
//   - The final ctx.Run return value/error matches the terminal
//     outcome's value/failure.
//
// Out of scope: scripts mixing Run with other ops. A single Run per
// scenario keeps the multi-session loop linear; the existing
// single-session PBTs already cover Run composition with other ops in
// the terminal case.
func TestWireContext_RunRetry_MultiSession(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		scenario := runScenarioGen.Draw(rt, "scenario")

		var invocations []runInvocation
		replay := make(map[uint32]*replayEntry)
		var (
			finalVal []byte
			finalErr error
		)
		// Bound the loop generously — len(scenario.attempts) sessions
		// is the natural cap, but bail with a clear error if the SDK
		// ever fails to converge.
		const maxSessions = 32
		converged := false
		for session := range maxSessions {
			if session >= len(scenario.attempts)+2 {
				rt.Fatalf("simulation overran: %d sessions, scenario has %d attempts", session, len(scenario.attempts))
			}
			sink := newRunRetrySink(wire.DefaultCodec(), replay)
			ctx := newRunRetryWireContext(t, sink, replay)

			v, err := ctx.Run(scenario.runName, func(rc *RunContext) ([]byte, error) {
				invIdx := len(invocations)
				if invIdx >= len(scenario.attempts) {
					return nil, fmt.Errorf("fn called more times than scenario allows: %d > %d", invIdx+1, len(scenario.attempts))
				}
				invocations = append(invocations, runInvocation{
					attempt: rc.Attempt(),
					key:     rc.IdempotencyKey(),
				})
				out := scenario.attempts[invIdx]
				switch {
				case out.transient:
					return nil, errors.New(out.transientMsg)
				case out.success:
					return append([]byte(nil), out.value...), nil
				default:
					return nil, NewFailure(0, out.failureMsg)
				}
			})

			if errors.Is(err, ErrSuspended) {
				// Retryable suspend — sink already stamped the
				// next-session marker into the replay map by reference.
				continue
			}
			finalVal, finalErr = v, err
			converged = true
			break
		}
		if !converged {
			rt.Fatalf("simulation did not converge in %d sessions; invocations=%d, scenario=%d",
				maxSessions, len(invocations), len(scenario.attempts))
		}

		// --- invariant checks ---
		if got, want := len(invocations), len(scenario.attempts); got != want {
			rt.Fatalf("invocation count = %d; want %d (scenario: %s)", got, want, scenarioString(scenario))
		}
		for i, inv := range invocations {
			if want := uint32(i + 1); inv.attempt != want {
				rt.Fatalf("invocation %d attempt = %d; want %d (scenario: %s)", i, inv.attempt, want, scenarioString(scenario))
			}
			if inv.key == "" {
				rt.Fatalf("invocation %d idempotency key empty (scenario: %s)", i, scenarioString(scenario))
			}
		}
		for i := 1; i < len(invocations); i++ {
			if invocations[i].key == invocations[i-1].key {
				rt.Fatalf("idempotency key did not change between attempt %d and %d (key=%q, scenario: %s)",
					i, i+1, invocations[i].key, scenarioString(scenario))
			}
		}

		// Final outcome must match the scenario's terminal step.
		terminal := scenario.attempts[len(scenario.attempts)-1]
		if terminal.transient {
			rt.Fatalf("scenario's terminal entry was marked transient — generator bug")
		}
		if terminal.success {
			if finalErr != nil {
				rt.Fatalf("final err = %v; want nil (success outcome %q)", finalErr, terminal.value)
			}
			if !bytes.Equal(finalVal, terminal.value) {
				rt.Fatalf("final value = %q; want %q", finalVal, terminal.value)
			}
		} else {
			var failure *Failure
			if !errors.As(finalErr, &failure) {
				rt.Fatalf("final err = %v; want *Failure (terminal failure outcome)", finalErr)
			}
			if failure.Message != terminal.failureMsg {
				rt.Fatalf("final failure message = %q; want %q", failure.Message, terminal.failureMsg)
			}
		}
	})
}

// ----------------------------------------------------------------------
// Scenario type + generator
// ----------------------------------------------------------------------

type runAttemptOutcome struct {
	transient    bool   // SDK classifies plain errors as retryable.
	success      bool   // (terminal only) true → fn returns value; false → returns *Failure.
	value        []byte // terminal success payload
	failureMsg   string // terminal failure payload
	transientMsg string // text of the plain error returned for retryables
}

type runScenario struct {
	runName  string
	attempts []runAttemptOutcome
}

type runInvocation struct {
	attempt uint32
	key     string
}

var (
	runScenarioNameGen      = rapid.SampledFrom([]string{"r_alpha", "r_beta"})
	runScenarioValueGen     = rapid.Map(rapid.SampledFrom([]string{"ok", "ok2", ""}), func(s string) []byte { return []byte(s) })
	runScenarioFailureGen   = rapid.SampledFrom([]string{"boom", "denied"})
	runScenarioTransientGen = rapid.SampledFrom([]string{"transient", "io-blip", "timeout"})

	// transient outcome generator.
	runTransientOutcomeGen = rapid.Custom(func(t *rapid.T) runAttemptOutcome {
		return runAttemptOutcome{transient: true, transientMsg: runScenarioTransientGen.Draw(t, "msg")}
	})

	// terminal outcome generator (success | failure).
	runTerminalOutcomeGen = rapid.Custom(func(t *rapid.T) runAttemptOutcome {
		if rapid.Bool().Draw(t, "success") {
			return runAttemptOutcome{success: true, value: runScenarioValueGen.Draw(t, "value")}
		}
		return runAttemptOutcome{failureMsg: runScenarioFailureGen.Draw(t, "fail")}
	})

	// 0–4 transients followed by exactly one terminal.
	runScenarioGen = rapid.Custom(func(t *rapid.T) runScenario {
		n := rapid.IntRange(0, 4).Draw(t, "transients_n")
		out := runScenario{runName: runScenarioNameGen.Draw(t, "name")}
		for range n {
			out.attempts = append(out.attempts, runTransientOutcomeGen.Draw(t, "transient"))
		}
		out.attempts = append(out.attempts, runTerminalOutcomeGen.Draw(t, "terminal"))
		return out
	})
)

func scenarioString(s runScenario) string {
	parts := make([]string, len(s.attempts))
	for i, a := range s.attempts {
		switch {
		case a.transient:
			parts[i] = fmt.Sprintf("T(%q)", a.transientMsg)
		case a.success:
			parts[i] = fmt.Sprintf("OK(%q)", a.value)
		default:
			parts[i] = fmt.Sprintf("FAIL(%q)", a.failureMsg)
		}
	}
	return fmt.Sprintf("Run(%q)[%s]", s.runName, strings.Join(parts, ", "))
}

// ----------------------------------------------------------------------
// runRetrySink — synthetic engine that survives across sessions.
//
// On a retryable propose, it stamps a fresh marker at the same slot
// with attempt+1 (mirroring what wire_replay.translateEntry would build
// when the engine apply path advances JERun.attempt). On a terminal
// propose, it overwrites the marker with a TypeNoteRunDone — the SDK
// returns directly so the next session is unused, but the stamp keeps
// the synthetic engine internally consistent.
// ----------------------------------------------------------------------

type runRetrySink struct {
	codec       wire.Codec
	replay      map[uint32]*replayEntry // shared by reference with the wireContext
	pendingCmd  *protocolv1.Frame       // most-recent RunCommandMessage (waiting for the propose)
	pendingSlot uint32                  // slot the pending cmd was stamped at
}

func newRunRetrySink(codec wire.Codec, replay map[uint32]*replayEntry) *runRetrySink {
	return &runRetrySink{codec: codec, replay: replay}
}

func (s *runRetrySink) Send(f *protocolv1.Frame) error {
	typeCode, _, _ := wire.UnpackHeader(f.GetHeader())
	switch typeCode {
	case wire.TypeCmdRun:
		// Stamp the marker so subsequent sessions can reproduce it.
		// Slot is encoded on the frame itself (Connect path stamps).
		// For our sink the slot is always 1 (single-Run scripts), but
		// we read it from the frame to stay honest if scenarios grow.
		var marker protocolv1.RunCommandMessage
		if err := s.codec.Unmarshal(f.GetPayload(), &marker); err != nil {
			return fmt.Errorf("runRetrySink: decode RunCmd: %w", err)
		}
		slot := marker.GetResultCompletionId()
		s.replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		s.pendingCmd = f
		s.pendingSlot = slot
		return nil

	case wire.TypeProposeRunDone:
		var prop protocolv1.ProposeRunCompletionMessage
		if err := s.codec.Unmarshal(f.GetPayload(), &prop); err != nil {
			return fmt.Errorf("runRetrySink: decode Propose: %w", err)
		}
		slot := prop.GetResultCompletionId()
		if s.pendingCmd == nil || s.pendingSlot != slot {
			return fmt.Errorf("runRetrySink: propose for slot %d without matching cmd", slot)
		}
		if prop.GetRetryable() {
			// Stamp the next-session marker: bump attempt, re-derive
			// idempotency key. Mirrors invoker/wire_replay.go's
			// translateEntry for JERun.retryable=true.
			var prev protocolv1.RunCommandMessage
			if err := s.codec.Unmarshal(s.pendingCmd.GetPayload(), &prev); err != nil {
				return fmt.Errorf("runRetrySink: decode prev marker: %w", err)
			}
			nextAttempt := prev.GetAttempt() + 1
			nextMarker := &protocolv1.RunCommandMessage{
				ResultCompletionId: slot,
				Attempt:            nextAttempt,
				IdempotencyKey:     deriveIdempotencyKey(runRetryTestInvID, slot, nextAttempt),
				Name:               prev.GetName(),
			}
			payload, err := s.codec.Marshal(nextMarker)
			if err != nil {
				return fmt.Errorf("runRetrySink: marshal next marker: %w", err)
			}
			s.replay[slot] = &replayEntry{typeCode: wire.TypeCmdRun, payload: payload}
			s.pendingCmd, s.pendingSlot = nil, 0
			return nil
		}
		// Terminal — replace the marker with a completion note.
		note := &protocolv1.RunCompletionNotificationMessage{CompletionId: slot}
		switch r := prop.GetResult().(type) {
		case *protocolv1.ProposeRunCompletionMessage_Value:
			note.Result = &protocolv1.RunCompletionNotificationMessage_Value{
				Value: &protocolv1.Value{Content: append([]byte(nil), r.Value...)},
			}
		case *protocolv1.ProposeRunCompletionMessage_Failure:
			note.Result = &protocolv1.RunCompletionNotificationMessage_Failure{Failure: r.Failure}
		}
		payload, err := s.codec.Marshal(note)
		if err != nil {
			return fmt.Errorf("runRetrySink: marshal RunCompletionNote: %w", err)
		}
		s.replay[slot] = &replayEntry{typeCode: wire.TypeNoteRunDone, payload: payload}
		s.pendingCmd, s.pendingSlot = nil, 0
		return nil

	default:
		return fmt.Errorf("runRetrySink: unexpected frame type 0x%04x (this PBT only drives Run)", typeCode)
	}
}

// runRetryTestInvID is the invocation id used by every multi-session
// simulation. Kept package-level so the sink can derive idempotency
// keys without threading it through every Send call.
var runRetryTestInvID = &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}

func newRunRetryWireContext(t *testing.T, sink *runRetrySink, replay map[uint32]*replayEntry) *wireContext {
	t.Helper()
	wctx := newWireContext(t.Context(), runRetryTestInvID, []byte("hello"), sink, wire.DefaultCodec(),
		nil, replay, 7, 0, "Svc", "Hdr", "rkey", protocolv1.Kind_KIND_OBJECT)
	return wctx
}
