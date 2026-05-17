package invoker

import (
	"crypto/sha256"
	"encoding/binary"
	hexenc "encoding/hex"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// replayFrame is the wire-shaped output of a JournalEntry translation —
// a (typeCode, marshaled-payload) pair ready to hand to stream.Send via
// handlerclient.FrameFor. Keeping it pre-marshaled avoids re-encoding
// per session.
type replayFrame struct {
	typeCode uint16
	payload  []byte
}

// translateJournal expands a slice of JournalEntry into the sequence
// of protocolv1 frames the handler should receive during replay. The
// returned slice is in journal order; the caller writes each frame
// in turn between StartMessage and the handler's user-code phase.
//
// JE variants the wire path doesn't yet model are logged and skipped
// rather than failing — additive extension turns each one into a real
// translation as the protocol grows.
func translateJournal(invID *enginev1.InvocationId, entries []*enginev1.JournalEntry, codec handlerclient.Codec, log *slog.Logger) ([]replayFrame, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make([]replayFrame, 0, len(entries))
	for _, e := range entries {
		frames, err := translateEntry(invID, e, codec, log)
		if err != nil {
			return nil, err
		}
		out = append(out, frames...)
	}
	return out, nil
}

// DeriveIdempotencyKey returns a stable token for (invocation_id, slot,
// attempt) as the first 16 hex chars of sha256. The engine stamps it on
// replayed RunCommandMessage frames; the SDK derives the same value
// independently when running fn on the first attempt so both sides
// agree on the per-attempt key without an extra round trip.
func DeriveIdempotencyKey(invID *enginev1.InvocationId, slot, attempt uint32) string {
	var buf [16 + 8 + 4 + 4]byte
	uuid := invID.GetUuid()
	if len(uuid) >= 16 {
		copy(buf[:16], uuid[:16])
	}
	binary.BigEndian.PutUint64(buf[16:24], invID.GetPartitionKey())
	binary.BigEndian.PutUint32(buf[24:28], slot)
	binary.BigEndian.PutUint32(buf[28:32], attempt)
	h := sha256.Sum256(buf[:])
	return hexenc.EncodeToString(h[:8])
}

// translateEntry produces zero, one, or two replay frames for a single
// JournalEntry. JECall is the only entry that fans out today (one for
// the call command, then the result notification). Run fans out into
// a marker + a completion notification when terminal; retryable runs
// emit only the marker so the SDK re-invokes fn on respawn.
func translateEntry(invID *enginev1.InvocationId, e *enginev1.JournalEntry, codec handlerclient.Codec, log *slog.Logger) ([]replayFrame, error) {
	switch entry := e.GetEntry().(type) {
	case *enginev1.JournalEntry_Input:
		msg := &protocolv1.InputCommandMessage{
			Value: &protocolv1.Value{Content: entry.Input.GetValue()},
		}
		return marshalFrame(codec, handlerclient.TypeCmdInput, msg)

	case *enginev1.JournalEntry_Sleep:
		// result_completion_id is the slot the result will land at.
		// JESleep allocates 2 slots (cmd + result); cmd index is on
		// the JournalEntry, result is index+1.
		msg := &protocolv1.SleepCommandMessage{
			WakeUpTime:         entry.Sleep.GetFireAtMs(),
			ResultCompletionId: e.GetIndex() + 1,
		}
		return marshalFrame(codec, handlerclient.TypeCmdSleep, msg)

	case *enginev1.JournalEntry_SleepResult:
		msg := &protocolv1.SleepCompletionNotificationMessage{
			// completion_id matches the SleepCommandMessage's
			// result_completion_id (i.e., this entry's own index).
			CompletionId: e.GetIndex(),
			Void:         &protocolv1.Void{},
		}
		return marshalFrame(codec, handlerclient.TypeNoteSleepDone, msg)

	case *enginev1.JournalEntry_SetState:
		msg := &protocolv1.SetStateCommandMessage{
			Key:   []byte(entry.SetState.GetKey()),
			Value: &protocolv1.Value{Content: entry.SetState.GetValue()},
		}
		return marshalFrame(codec, handlerclient.TypeCmdSetState, msg)

	case *enginev1.JournalEntry_ClearState:
		msg := &protocolv1.ClearStateCommandMessage{
			Key: []byte(entry.ClearState.GetKey()),
		}
		return marshalFrame(codec, handlerclient.TypeCmdClearState, msg)

	case *enginev1.JournalEntry_ClearAllState:
		msg := &protocolv1.ClearAllStateCommandMessage{}
		return marshalFrame(codec, handlerclient.TypeCmdClearAllState, msg)

	case *enginev1.JournalEntry_Call:
		// Call allocates 2 slots: cmd at e.Index, result at e.Index+1.
		// The wire ships result_completion_id pointing at the matching
		// CallCompletionNotificationMessage.
		t := entry.Call.GetTarget()
		msg := &protocolv1.CallCommandMessage{
			ServiceName:        t.GetServiceName(),
			HandlerName:        t.GetHandlerName(),
			Parameter:          entry.Call.GetInput(),
			Key:                t.GetObjectKey(),
			ResultCompletionId: e.GetIndex() + 1,
		}
		if tok := entry.Call.GetIdempotencyKey(); tok != "" {
			msg.IdempotencyToken = &tok
		}
		return marshalFrame(codec, handlerclient.TypeCmdCall, msg)

	case *enginev1.JournalEntry_CallResult:
		// Translate to a CallCompletionNotificationMessage. completion_id
		// matches the original CallCommandMessage's
		// result_completion_id, which is this entry's own index.
		msg := &protocolv1.CallCompletionNotificationMessage{
			CompletionId: e.GetIndex(),
		}
		if fm := entry.CallResult.GetFailureMessage(); fm != "" {
			msg.Result = &protocolv1.CallCompletionNotificationMessage_Failure{
				Failure: &protocolv1.Failure{Message: fm},
			}
		} else {
			msg.Result = &protocolv1.CallCompletionNotificationMessage_Value{
				Value: &protocolv1.Value{Content: entry.CallResult.GetResult()},
			}
		}
		return marshalFrame(codec, handlerclient.TypeNoteCallDone, msg)

	case *enginev1.JournalEntry_OneWayCall:
		t := entry.OneWayCall.GetTarget()
		msg := &protocolv1.OneWayCallCommandMessage{
			ServiceName: t.GetServiceName(),
			HandlerName: t.GetHandlerName(),
			Parameter:   entry.OneWayCall.GetInput(),
			Key:         t.GetObjectKey(),
		}
		if tok := entry.OneWayCall.GetIdempotencyKey(); tok != "" {
			msg.IdempotencyToken = &tok
		}
		return marshalFrame(codec, handlerclient.TypeCmdOneWayCall, msg)

	case *enginev1.JournalEntry_Run:
		// JERun translates to a RunCommandMessage marker. When the run
		// is terminal (retryable=false), a follow-up
		// RunCompletionNotificationMessage carries the cached outcome
		// so wireContext.Run returns it without re-invoking fn. When
		// retryable=true the notification is omitted: the SDK sees the
		// marker (slot consumed) but no result and re-invokes fn on
		// the next attempt. The marker carries the next-attempt count
		// and a derived idempotency key so the SDK can surface them to
		// fn via RunContext on the retry call.
		nextAttempt := entry.Run.GetAttempt() + 1
		if !entry.Run.GetRetryable() {
			// Replay-hit (terminal): the marker exposes the attempt
			// that produced the final outcome so the SDK can observe
			// it for debugging even though fn won't re-run.
			nextAttempt = entry.Run.GetAttempt()
		}
		cmd := &protocolv1.RunCommandMessage{
			ResultCompletionId: e.GetIndex(),
			Attempt:            nextAttempt,
			IdempotencyKey:     DeriveIdempotencyKey(invID, e.GetIndex(), nextAttempt),
		}
		cmdPayload, err := codec.Marshal(cmd)
		if err != nil {
			return nil, fmt.Errorf("marshal RunCommandMessage: %w", err)
		}
		if entry.Run.GetRetryable() {
			return []replayFrame{
				{typeCode: handlerclient.TypeCmdRun, payload: cmdPayload},
			}, nil
		}
		note := &protocolv1.RunCompletionNotificationMessage{
			CompletionId: e.GetIndex(),
		}
		if fm := entry.Run.GetFailureMessage(); fm != "" {
			note.Result = &protocolv1.RunCompletionNotificationMessage_Failure{
				Failure: &protocolv1.Failure{Message: fm},
			}
		} else {
			note.Result = &protocolv1.RunCompletionNotificationMessage_Value{
				Value: &protocolv1.Value{Content: entry.Run.GetValue()},
			}
		}
		notePayload, err := codec.Marshal(note)
		if err != nil {
			return nil, fmt.Errorf("marshal RunCompletionNotificationMessage: %w", err)
		}
		return []replayFrame{
			{typeCode: handlerclient.TypeCmdRun, payload: cmdPayload},
			{typeCode: handlerclient.TypeNoteRunDone, payload: notePayload},
		}, nil

	case *enginev1.JournalEntry_Awakeable:
		// Awakeable allocates 2 slots: cmd at e.Index, result at e.Index+1.
		// The cmd frame carries the minted awakeable id so the SDK can
		// return it from the replayed Awakeable call without re-minting.
		msg := &protocolv1.AwakeableCommandMessage{
			ResultCompletionId: e.GetIndex() + 1,
			AwakeableId:        entry.Awakeable.GetAwakeableId(),
		}
		return marshalFrame(codec, handlerclient.TypeCmdAwakeable, msg)

	case *enginev1.JournalEntry_AwakeableResult:
		// Translate to a SignalNotificationMessage with the name variant
		// matching the awakeable id. Result slot = e.Index (the second
		// of the 2-slot Awakeable pair).
		msg := &protocolv1.SignalNotificationMessage{
			SignalId: &protocolv1.SignalNotificationMessage_Name{
				Name: entry.AwakeableResult.GetAwakeableId(),
			},
		}
		if fm := entry.AwakeableResult.GetFailureMessage(); fm != "" {
			msg.Result = &protocolv1.SignalNotificationMessage_Failure{
				Failure: &protocolv1.Failure{Message: fm},
			}
		} else if v := entry.AwakeableResult.GetValue(); v != nil {
			msg.Result = &protocolv1.SignalNotificationMessage_Value{
				Value: &protocolv1.Value{Content: v},
			}
		} else {
			msg.Result = &protocolv1.SignalNotificationMessage_Void{Void: &protocolv1.Void{}}
		}
		return marshalFrame(codec, handlerclient.TypeNoteSignal, msg)

	default:
		log.Debug("invoker.wire: skipping JE variant in replay (not yet wired)",
			"index", e.GetIndex(),
			"variant", fmt.Sprintf("%T", entry))
		return nil, nil
	}
}

// marshalFrame wraps codec.Marshal so the per-variant branches above
// stay concise.
func marshalFrame(codec handlerclient.Codec, typeCode uint16, msg proto.Message) ([]replayFrame, error) {
	payload, err := codec.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal frame 0x%04x: %w", typeCode, err)
	}
	return []replayFrame{{typeCode: typeCode, payload: payload}}, nil
}
