package invoker

import (
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
// rather than failing — additive extension across 5f.4-5f.6 turns
// each one into a real translation.
func translateJournal(entries []*enginev1.JournalEntry, codec handlerclient.Codec, log *slog.Logger) ([]replayFrame, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make([]replayFrame, 0, len(entries))
	for _, e := range entries {
		frames, err := translateEntry(e, codec, log)
		if err != nil {
			return nil, err
		}
		out = append(out, frames...)
	}
	return out, nil
}

// translateEntry produces zero, one, or two replay frames for a single
// JournalEntry. JECall is the only entry that fans out today (one for
// the call command, then both the invocation-id and result
// notifications — fully wired in 5f.4). 5f.3 emits Input, Sleep +
// SleepResult, SetState, ClearState, ClearAllState.
func translateEntry(e *enginev1.JournalEntry, codec handlerclient.Codec, log *slog.Logger) ([]replayFrame, error) {
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
		// Mirrors inproc.go's Call slot accounting; the wire ships
		// result_completion_id pointing at the matching
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
