package invoker

import (
	"crypto/sha256"
	"encoding/binary"
	hexenc "encoding/hex"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// replayFrame is the wire-shaped output of a JournalEntry translation
// — a (typeCode, slot, marshaled-payload) tuple. slot is the journal
// index the SDK should place this entry at; stamping it here lets the
// SDK build its replay map without decoding payloads to extract
// completion_id / matching awakeable id.
type replayFrame struct {
	typeCode uint16
	slot     uint32
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
func translateJournal(invID *enginev1.InvocationId, entries []*enginev1.JournalEntry, codec wire.Codec, log *slog.Logger) ([]replayFrame, error) {
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
func translateEntry(invID *enginev1.InvocationId, e *enginev1.JournalEntry, codec wire.Codec, log *slog.Logger) ([]replayFrame, error) {
	switch entry := e.GetEntry().(type) {
	case *enginev1.JournalEntry_Input:
		msg := &protocolv1.InputCommandMessage{
			Value: &protocolv1.Value{Content: entry.Input.GetValue()},
		}
		return marshalFrame(codec, wire.TypeCmdInput, e.GetIndex(), msg)

	case *enginev1.JournalEntry_Sleep:
		// result_completion_id is the slot the result will land at.
		// JESleep allocates 2 slots (cmd + result); cmd index is on
		// the JournalEntry, result is index+1.
		msg := &protocolv1.SleepCommandMessage{
			WakeUpTime:         entry.Sleep.GetFireAtMs(),
			ResultCompletionId: e.GetIndex() + 1,
		}
		return marshalFrame(codec, wire.TypeCmdSleep, e.GetIndex(), msg)

	case *enginev1.JournalEntry_SleepResult:
		msg := &protocolv1.SleepCompletionNotificationMessage{
			// completion_id matches the SleepCommandMessage's
			// result_completion_id (i.e., this entry's own index).
			CompletionId: e.GetIndex(),
			Void:         &protocolv1.Void{},
		}
		return marshalFrame(codec, wire.TypeNoteSleepDone, e.GetIndex(), msg)

	case *enginev1.JournalEntry_SetState:
		msg := &protocolv1.SetStateCommandMessage{
			Key:   []byte(entry.SetState.GetKey()),
			Value: &protocolv1.Value{Content: entry.SetState.GetValue()},
		}
		return marshalFrame(codec, wire.TypeCmdSetState, e.GetIndex(), msg)

	case *enginev1.JournalEntry_ClearState:
		msg := &protocolv1.ClearStateCommandMessage{
			Key: []byte(entry.ClearState.GetKey()),
		}
		return marshalFrame(codec, wire.TypeCmdClearState, e.GetIndex(), msg)

	case *enginev1.JournalEntry_ClearAllState:
		msg := &protocolv1.ClearAllStateCommandMessage{}
		return marshalFrame(codec, wire.TypeCmdClearAllState, e.GetIndex(), msg)

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
		return marshalFrame(codec, wire.TypeCmdCall, e.GetIndex(), msg)

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
		return marshalFrame(codec, wire.TypeNoteCallDone, e.GetIndex(), msg)

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
		return marshalFrame(codec, wire.TypeCmdOneWayCall, e.GetIndex(), msg)

	case *enginev1.JournalEntry_Signal:
		// JESignal replays as the SendSignalCommandMessage the SDK
		// originally emitted. Single-slot (no result-pair); on replay
		// wireContext.SendSignal sees the slot occupied and returns
		// nil without re-emitting.
		t := entry.Signal.GetTarget()
		msg := &protocolv1.SendSignalCommandMessage{
			ServiceName: t.GetServiceName(),
			HandlerName: t.GetHandlerName(),
			Key:         t.GetObjectKey(),
			SignalName:  entry.Signal.GetSignalName(),
			Payload:     entry.Signal.GetPayload(),
		}
		return marshalFrame(codec, wire.TypeCmdSendSignal, e.GetIndex(), msg)

	case *enginev1.JournalEntry_AwaitSignal:
		// JEAwaitSignal replays as the original AwaitSignalCommandMessage
		// at cmdSlot. The matching JESignalResult (if delivered) replays
		// separately at resultSlot=cmdSlot+1.
		msg := &protocolv1.AwaitSignalCommandMessage{
			SignalName:         entry.AwaitSignal.GetSignalName(),
			ResultCompletionId: entry.AwaitSignal.GetResultCompletionId(),
		}
		return marshalFrame(codec, wire.TypeCmdAwaitSignal, e.GetIndex(), msg)

	case *enginev1.JournalEntry_SignalResult:
		// JESignalResult replays as a SignalNotificationMessage at the
		// result slot. The same TypeNoteSignal frame code carries
		// awakeable resolutions; the SDK's signalFuture and
		// awakeableFuture distinguish by the slot they're polling.
		msg := &protocolv1.SignalNotificationMessage{
			SignalId: &protocolv1.SignalNotificationMessage_Name{Name: entry.SignalResult.GetSignalName()},
			Result: &protocolv1.SignalNotificationMessage_Value{
				Value: &protocolv1.Value{Content: entry.SignalResult.GetPayload()},
			},
		}
		return marshalFrame(codec, wire.TypeNoteSignal, e.GetIndex(), msg)

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
				{typeCode: wire.TypeCmdRun, slot: e.GetIndex(), payload: cmdPayload},
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
			{typeCode: wire.TypeCmdRun, slot: e.GetIndex(), payload: cmdPayload},
			{typeCode: wire.TypeNoteRunDone, slot: e.GetIndex(), payload: notePayload},
		}, nil

	case *enginev1.JournalEntry_Awakeable:
		// Awakeable allocates 2 slots: cmd at e.Index, result at e.Index+1.
		// The cmd frame carries the minted awakeable id so the SDK can
		// return it from the replayed Awakeable call without re-minting.
		msg := &protocolv1.AwakeableCommandMessage{
			ResultCompletionId: e.GetIndex() + 1,
			AwakeableId:        entry.Awakeable.GetAwakeableId(),
		}
		return marshalFrame(codec, wire.TypeCmdAwakeable, e.GetIndex(), msg)

	case *enginev1.JournalEntry_GetPromise:
		msg := &protocolv1.GetPromiseCommandMessage{
			Name:               entry.GetPromise.GetName(),
			ResultCompletionId: entry.GetPromise.GetResultCompletionId(),
			Service:            entry.GetPromise.GetService(),
			Key:                entry.GetPromise.GetWorkflowKey(),
		}
		return marshalFrame(codec, wire.TypeCmdGetPromise, e.GetIndex(), msg)

	case *enginev1.JournalEntry_PromiseResult:
		msg := &protocolv1.GetPromiseCompletionNotificationMessage{
			CompletionId: e.GetIndex(),
		}
		if fm := entry.PromiseResult.GetFailureMessage(); fm != "" {
			msg.Result = &protocolv1.GetPromiseCompletionNotificationMessage_Failure{
				Failure: &protocolv1.Failure{Message: fm},
			}
		} else {
			msg.Result = &protocolv1.GetPromiseCompletionNotificationMessage_Value{
				Value: &protocolv1.Value{Content: entry.PromiseResult.GetValue()},
			}
		}
		return marshalFrame(codec, wire.TypeNoteGetPromise, e.GetIndex(), msg)

	case *enginev1.JournalEntry_PeekPromise:
		// JEPeekPromise replays as the snapshot stamped by the apply
		// arm: completed flag + (value | failure_message). Single slot;
		// the SDK's Peek decodes the frame at the slot it allocated and
		// returns the cached value without re-emitting.
		msg := &protocolv1.PeekPromiseCommandMessage{
			Name:      entry.PeekPromise.GetName(),
			Completed: entry.PeekPromise.GetCompleted(),
			Service:   entry.PeekPromise.GetService(),
			Key:       entry.PeekPromise.GetWorkflowKey(),
		}
		if fm := entry.PeekPromise.GetFailureMessage(); fm != "" {
			msg.Result = &protocolv1.PeekPromiseCommandMessage_Failure{
				Failure: &protocolv1.Failure{Message: fm},
			}
		} else if entry.PeekPromise.GetCompleted() {
			msg.Result = &protocolv1.PeekPromiseCommandMessage_Value{
				Value: &protocolv1.Value{Content: entry.PeekPromise.GetValue()},
			}
		}
		return marshalFrame(codec, wire.TypeCmdPeekPromise, e.GetIndex(), msg)

	case *enginev1.JournalEntry_CompletePromise:
		msg := &protocolv1.CompletePromiseCommandMessage{
			Name:               entry.CompletePromise.GetName(),
			ResultCompletionId: entry.CompletePromise.GetResultCompletionId(),
			Service:            entry.CompletePromise.GetService(),
			Key:                entry.CompletePromise.GetWorkflowKey(),
		}
		if fm := entry.CompletePromise.GetFailureMessage(); fm != "" {
			msg.Completion = &protocolv1.CompletePromiseCommandMessage_CompletionFailure{
				CompletionFailure: &protocolv1.Failure{Message: fm},
			}
		} else {
			msg.Completion = &protocolv1.CompletePromiseCommandMessage_CompletionValue{
				CompletionValue: &protocolv1.Value{Content: entry.CompletePromise.GetValue()},
			}
		}
		return marshalFrame(codec, wire.TypeCmdCompletePromise, e.GetIndex(), msg)

	case *enginev1.JournalEntry_PromiseCompleteResult:
		msg := &protocolv1.CompletePromiseCompletionNotificationMessage{
			CompletionId: e.GetIndex(),
		}
		if !entry.PromiseCompleteResult.GetSucceeded() {
			msg.Result = &protocolv1.CompletePromiseCompletionNotificationMessage_Failure{
				Failure: &protocolv1.Failure{
					Code:    9005, // PromiseAlreadyCompletedCode, mirrored from pkg/handler/errors.go
					Message: entry.PromiseCompleteResult.GetFailureMessage(),
				},
			}
		} else {
			msg.Result = &protocolv1.CompletePromiseCompletionNotificationMessage_Void{Void: &protocolv1.Void{}}
		}
		return marshalFrame(codec, wire.TypeNoteCompletePromise, e.GetIndex(), msg)

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
		return marshalFrame(codec, wire.TypeNoteSignal, e.GetIndex(), msg)

	default:
		log.Debug("invoker.wire: skipping JE variant in replay (not yet wired)",
			"index", e.GetIndex(),
			"variant", fmt.Sprintf("%T", entry))
		return nil, nil
	}
}

// marshalFrame wraps codec.Marshal so the per-variant branches above
// stay concise. slot is the journal index the SDK should place this
// frame at — typically the JournalEntry's own index, except for
// result-pair entries where it's the result-slot (already encoded in
// the message's completion_id).
func marshalFrame(codec wire.Codec, typeCode uint16, slot uint32, msg proto.Message) ([]replayFrame, error) {
	payload, err := codec.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal frame 0x%04x: %w", typeCode, err)
	}
	return []replayFrame{{typeCode: typeCode, slot: slot, payload: payload}}, nil
}
