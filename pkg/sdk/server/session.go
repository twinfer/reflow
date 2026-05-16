package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// frameStream is the transport-neutral view of one session. The HTTP/2
// server adapts its request body / response writer onto this contract;
// the session driver below is shared so future transports can plug in
// with the same shape.
type frameStream interface {
	Send(*protocolv1.Frame) error
	Recv() (*protocolv1.Frame, error)
}

// runSession drives one handler invocation: read StartMessage, replay
// any prior journal entries the engine ships, look up the handler, run
// it, then emit OutputCommandMessage + EndMessage (or
// SuspensionMessage / ErrorMessage). route is the transport-supplied
// (service, handler) hint — HTTP/2 fills it from the URL path
// /invoke/<service>/<handler>; StartMessage echoes the same tuple. When
// both are populated they MUST agree.
//
// The returned error is logged by the transport; it is NOT mirrored as
// an ErrorMessage on the wire (that frame is reserved for protocol-level
// failures the engine should treat as terminal). Transport-level
// cleanup (closing the response body) is the caller's responsibility.
func runSession(
	ctx context.Context,
	stream frameStream,
	registry *sdk.Registry,
	codec handlerclient.Codec,
	route handlerclient.Route,
) error {
	start, err := readStart(stream, codec)
	if err != nil {
		return fmt.Errorf("read StartMessage: %w", err)
	}

	// Resolve the route. StartMessage is authoritative when populated;
	// HTTP/2's URL path acts as a cross-check.
	service := start.GetServiceName()
	handler := start.GetHandlerName()
	if service == "" {
		service = route.Service
	}
	if handler == "" {
		handler = route.Handler
	}
	if route.Service != "" && service != route.Service {
		return sendError(stream, codec, 571,
			fmt.Sprintf("StartMessage.service_name=%q disagrees with URL path service=%q",
				start.GetServiceName(), route.Service))
	}
	if route.Handler != "" && handler != route.Handler {
		return sendError(stream, codec, 571,
			fmt.Sprintf("StartMessage.handler_name=%q disagrees with URL path handler=%q",
				start.GetHandlerName(), route.Handler))
	}
	if service == "" || handler == "" {
		return sendError(stream, codec, 571,
			"session missing (service, handler) routing: provide either "+
				"StartMessage.service_name/handler_name or an HTTP/2 URL path")
	}
	fn, _, ok := registry.Lookup(&sdk.Target{Service: service, Handler: handler})
	if !ok {
		return sendError(stream, codec, 404,
			fmt.Sprintf("no handler registered for %s/%s", service, handler))
	}

	// Consume the replay phase: read known_entries frames, build the
	// replay buffer + capture the JEInput payload as the handler input.
	input, replay, err := readReplay(stream, codec, start.GetKnownEntries())
	if err != nil {
		return fmt.Errorf("read replay: %w", err)
	}

	// Build the wire context and run the handler. A panic is recovered
	// and translated to an ErrorMessage so the engine doesn't hang on
	// the stream.
	invID := &enginev1.InvocationId{Uuid: start.GetId()}
	stateCache := stateMapToCache(start.GetStateMap())
	wctx := newWireContext(ctx, invID, input, stream, codec, stateCache, replay)

	output, runErr := runHandler(wctx, fn, input)

	// Suspension path: handler called a primitive whose result wasn't
	// in the replay buffer. Emit SuspensionMessage and exit; the engine
	// proposes InvokerEffect_Suspended and respawns the session once
	// the awaited completion lands in the journal.
	if errors.Is(runErr, sdk.ErrSuspended) {
		return sendSuspension(stream, codec, wctx.snapshotAwaiting())
	}

	out := &protocolv1.OutputCommandMessage{}
	if runErr != nil {
		if f, ok := sdk.AsFailure(runErr); ok {
			out.Result = &protocolv1.OutputCommandMessage_Failure{
				Failure: &protocolv1.Failure{Code: f.Code, Message: f.Message},
			}
		} else {
			// Non-Failure errors are transient on the engine side, but
			// the minimum-viable wire path treats them as terminal
			// failure carrying the error message. The engine surfaces
			// the message verbatim into InvocationStatus.Completed.
			// Full retryable-vs-terminal classification on the wire is
			// part of the wire-protocol expansion.
			out.Result = &protocolv1.OutputCommandMessage_Failure{
				Failure: &protocolv1.Failure{Code: 0, Message: runErr.Error()},
			}
		}
	} else {
		out.Result = &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: output},
		}
	}
	outBytes, err := codec.Marshal(out)
	if err != nil {
		return fmt.Errorf("marshal OutputCommandMessage: %w", err)
	}
	if err := stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdOutput, outBytes)); err != nil {
		return fmt.Errorf("send OutputCommandMessage: %w", err)
	}
	endBytes, err := codec.Marshal(&protocolv1.EndMessage{})
	if err != nil {
		return fmt.Errorf("marshal EndMessage: %w", err)
	}
	if err := stream.Send(handlerclient.FrameFor(handlerclient.TypeEnd, endBytes)); err != nil {
		return fmt.Errorf("send EndMessage: %w", err)
	}
	return nil
}

// runHandler invokes h(wctx, input) under a panic recover so a buggy
// handler cannot tear down the session goroutine.
func runHandler(wctx *wireContext, h sdk.Handler, input []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(wctx, input)
}

// readStart consumes the StartMessage frame. Errors if the first frame
// is not TypeStart or fails to decode.
func readStart(stream frameStream, codec handlerclient.Codec) (*protocolv1.StartMessage, error) {
	f, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if err := handlerclient.ValidatePayload(f); err != nil {
		return nil, err
	}
	typeCode, _, _ := handlerclient.UnpackHeader(f.GetHeader())
	if typeCode != handlerclient.TypeStart {
		return nil, fmt.Errorf("first frame type 0x%04x; expected StartMessage (0x%04x)",
			typeCode, handlerclient.TypeStart)
	}
	var start protocolv1.StartMessage
	if err := codec.Unmarshal(f.GetPayload(), &start); err != nil {
		return nil, fmt.Errorf("decode StartMessage: %w", err)
	}
	return &start, nil
}

// readReplay consumes the count replay frames the engine ships after
// StartMessage. Returns the input payload (from JEInput, which is
// always slot 0 if present) plus the replay buffer indexed by the
// slot each command/notification corresponds to.
//
// Slot accounting:
//   - InputCommandMessage → slot 0 (also captured as the input bytes).
//   - SleepCommandMessage at index N → slot N (the cmd entry).
//   - SleepCompletionNotificationMessage at index N → slot N (the
//     result entry; SDK looks it up by completion_id).
//   - SetState / ClearState / ClearAllState at index N → slot N
//     (write-only; replay-hit means "skip re-emit").
//
// 5f.3 supports the frame types translateEntry knows about. Unknown
// types are added to the buffer at a synthetic slot (count-based) so
// future SDK versions can ignore them safely; if a real translation
// lands the engine + handler stay in sync via the JE → frame table.
func readReplay(stream frameStream, codec handlerclient.Codec, count uint32) ([]byte, map[uint32]*replayEntry, error) {
	replay := make(map[uint32]*replayEntry, count)
	if count == 0 {
		return nil, replay, nil
	}
	var (
		input    []byte
		nextSlot uint32 // running cursor for slot assignment
	)
	for i := range count {
		f, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil, fmt.Errorf("replay truncated at frame %d/%d", i, count)
			}
			return nil, nil, err
		}
		if err := handlerclient.ValidatePayload(f); err != nil {
			return nil, nil, err
		}
		typeCode, _, _ := handlerclient.UnpackHeader(f.GetHeader())
		switch typeCode {
		case handlerclient.TypeCmdInput:
			var in protocolv1.InputCommandMessage
			if err := codec.Unmarshal(f.GetPayload(), &in); err != nil {
				return nil, nil, fmt.Errorf("decode replay InputCommandMessage: %w", err)
			}
			input = in.GetValue().GetContent()
			replay[0] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
			if nextSlot == 0 {
				nextSlot = 1
			}
		case handlerclient.TypeCmdSleep:
			replay[nextSlot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
			nextSlot += 2 // Sleep allocates 2 slots: cmd + result
		case handlerclient.TypeNoteSleepDone:
			// Result slot = previously-allocated nextSlot - 1 (the
			// SleepCommandMessage advanced by 2; the result lands at
			// cmd+1).
			var note protocolv1.SleepCompletionNotificationMessage
			if err := codec.Unmarshal(f.GetPayload(), &note); err != nil {
				return nil, nil, fmt.Errorf("decode replay SleepCompletionNotificationMessage: %w", err)
			}
			replay[note.GetCompletionId()] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
		case handlerclient.TypeCmdSetState, handlerclient.TypeCmdClearState, handlerclient.TypeCmdClearAllState:
			replay[nextSlot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
			nextSlot++
		default:
			// Forward-compat: stash the frame at a per-count synthetic
			// slot to keep the cursor advancing in lockstep with the
			// engine; an SDK that doesn't recognize the type can still
			// finish replay cleanly.
			replay[nextSlot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}
			nextSlot++
		}
	}
	return input, replay, nil
}

// sendSuspension emits a SuspensionMessage frame and closes the
// session cleanly. The engine maps awaitingTokens into
// InvocationSuspended.awaiting_on for observability; the wake path
// itself is respawn-driven by Suspended→Invoked transitions on the
// next completion event.
func sendSuspension(stream frameStream, codec handlerclient.Codec, awaitingTokens []string) error {
	sm := &protocolv1.SuspensionMessage{}
	// Translate token strings back to typed waiting_* fields. Tokens
	// shaped "completion:<N>" land in waiting_completions; "signal:<N>"
	// in waiting_signals; everything else falls through to
	// waiting_named_signals. The strings are descriptive (the engine
	// stuffs them straight into awaiting_on for observability) so this
	// translation is lossless even if a token doesn't match any prefix.
	for _, t := range awaitingTokens {
		var (
			id  uint32
			tag string
		)
		if _, err := fmt.Sscanf(t, "completion:%d", &id); err == nil {
			sm.WaitingCompletions = append(sm.WaitingCompletions, id)
			continue
		}
		if _, err := fmt.Sscanf(t, "signal:%d", &id); err == nil {
			sm.WaitingSignals = append(sm.WaitingSignals, id)
			continue
		}
		_ = tag
		sm.WaitingNamedSignals = append(sm.WaitingNamedSignals, t)
	}
	body, err := codec.Marshal(sm)
	if err != nil {
		return fmt.Errorf("marshal SuspensionMessage: %w", err)
	}
	return stream.Send(handlerclient.FrameFor(handlerclient.TypeSuspension, body))
}

// sendError emits an ErrorMessage frame, terminating the session. The
// engine treats ErrorMessage as a terminal failure with the supplied
// code + message round-tripped into InvocationStatus.Completed.
func sendError(stream frameStream, codec handlerclient.Codec, code uint32, message string) error {
	em := &protocolv1.ErrorMessage{Code: code, Message: message}
	body, err := codec.Marshal(em)
	if err != nil {
		return fmt.Errorf("marshal ErrorMessage: %w", err)
	}
	return stream.Send(handlerclient.FrameFor(handlerclient.TypeError, body))
}

// stateMapToCache materializes StartMessage.state_map into the in-memory
// map wireContext serves GetState from. Returns nil when entries is
// empty so callers can distinguish "no preload" (cache=nil) from "empty
// preload" (cache={}) — both are semantically the same to GetState but
// keeping nil avoids an allocation for unkeyed services.
func stateMapToCache(entries []*protocolv1.StartMessage_StateEntry) map[string][]byte {
	if len(entries) == 0 {
		return nil
	}
	cache := make(map[string][]byte, len(entries))
	for _, e := range entries {
		cache[string(e.GetKey())] = append([]byte(nil), e.GetValue()...)
	}
	return cache
}
