package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// frameSource yields the StartMessage and replay frames the session
// driver consumes before invoking the handler. The streaming-bidi
// transport pulls these from the request body; the (future) Connect
// request/response transport will wrap a pre-loaded journal slice.
type frameSource interface {
	Recv() (*protocolv1.Frame, error)
}

// frameSink consumes the frames the handler produces: new command
// messages emitted by wireContext methods, and the terminal
// Output/Suspension/Error frame the driver writes at session end.
type frameSink interface {
	Send(*protocolv1.Frame) error
}

// frameStream is a duplex source+sink. The HTTP/2 transport satisfies
// it with one object (request body + response writer), so bidi callers
// pass the same value as both source and sink to runSession.
type frameStream interface {
	frameSource
	frameSink
}

// runSession drives one handler invocation: read StartMessage, replay
// any prior journal entries the engine ships, look up the handler, run
// it, then emit OutputCommandMessage + EndMessage (or
// SuspensionMessage / ErrorMessage). route is the transport-supplied
// (service, handler) hint — Connect RPC supplies an empty Route because
// the procedure URL carries no per-handler addressing, leaving
// StartMessage as the authoritative routing source. When both are
// populated they MUST agree.
//
// The returned error is logged by the transport; it is NOT mirrored as
// an ErrorMessage on the wire (that frame is reserved for protocol-level
// failures the engine should treat as terminal). Transport-level
// cleanup (closing the response body) is the caller's responsibility.
func runSession(
	ctx context.Context,
	src frameSource,
	sink frameSink,
	registry *Registry,
	codec wire.Codec,
	route wire.Route,
) error {
	start, err := readStart(src, codec)
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
		return sendError(sink, codec, 571,
			fmt.Sprintf("StartMessage.service_name=%q disagrees with URL path service=%q",
				start.GetServiceName(), route.Service))
	}
	if route.Handler != "" && handler != route.Handler {
		return sendError(sink, codec, 571,
			fmt.Sprintf("StartMessage.handler_name=%q disagrees with URL path handler=%q",
				start.GetHandlerName(), route.Handler))
	}
	if service == "" || handler == "" {
		return sendError(sink, codec, 571,
			"session missing (service, handler) routing: provide either "+
				"StartMessage.service_name/handler_name or an HTTP/2 URL path")
	}
	fn, _, ok := registry.Lookup(&Target{Service: service, Handler: handler})
	if !ok {
		return sendError(sink, codec, 404,
			fmt.Sprintf("no handler registered for %s/%s", service, handler))
	}

	// Consume the replay phase: read known_entries frames, build the
	// replay buffer + capture the JEInput payload as the handler input.
	input, replay, err := readReplay(src, codec, start.GetKnownEntries())
	if err != nil {
		return fmt.Errorf("read replay: %w", err)
	}

	// Build the wire context and run the handler. A panic is recovered
	// and translated to an ErrorMessage so the engine doesn't hang on
	// the stream.
	invID := &enginev1.InvocationId{
		Uuid:         start.GetId(),
		PartitionKey: start.GetPartitionKey(),
	}
	stateCache := stateMapToCache(start.GetStateMap())
	wctx := newWireContext(ctx, invID, input, sink, codec, stateCache, replay, start.GetPartitionKey(), start.GetMaxJournalEntries())
	wctx.partialState = start.GetPartialState()

	output, runErr := runHandler(wctx, fn, input)

	// Suspension path: handler called a primitive whose result wasn't
	// in the replay buffer. Emit SuspensionMessage and exit; the engine
	// proposes InvokerEffect_Suspended and respawns the session once
	// the awaited completion lands in the journal.
	if errors.Is(runErr, ErrSuspended) {
		return sendSuspension(sink, codec, wctx.snapshotAwaiting())
	}

	out := &protocolv1.OutputCommandMessage{}
	if runErr != nil {
		if f, ok := AsFailure(runErr); ok {
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
	if err := sink.Send(wire.FrameFor(wire.TypeCmdOutput, outBytes)); err != nil {
		return fmt.Errorf("send OutputCommandMessage: %w", err)
	}
	endBytes, err := codec.Marshal(&protocolv1.EndMessage{})
	if err != nil {
		return fmt.Errorf("marshal EndMessage: %w", err)
	}
	if err := sink.Send(wire.FrameFor(wire.TypeEnd, endBytes)); err != nil {
		return fmt.Errorf("send EndMessage: %w", err)
	}
	return nil
}

// runHandler invokes h(wctx, input) under a panic recover so a buggy
// handler cannot tear down the session goroutine.
func runHandler(wctx *wireContext, h Handler, input []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(wctx, input)
}

// readStart consumes the StartMessage frame. Errors if the first frame
// is not TypeStart or fails to decode.
func readStart(src frameSource, codec wire.Codec) (*protocolv1.StartMessage, error) {
	f, err := src.Recv()
	if err != nil {
		return nil, err
	}
	if err := wire.ValidatePayload(f); err != nil {
		return nil, err
	}
	typeCode, _, _ := wire.UnpackHeader(f.GetHeader())
	if typeCode != wire.TypeStart {
		return nil, fmt.Errorf("first frame type 0x%04x; expected StartMessage (0x%04x)",
			typeCode, wire.TypeStart)
	}
	var start protocolv1.StartMessage
	if err := codec.Unmarshal(f.GetPayload(), &start); err != nil {
		return nil, fmt.Errorf("decode StartMessage: %w", err)
	}
	return &start, nil
}

// readReplay consumes the count replay frames the engine ships after
// StartMessage. Returns the JEInput payload bytes (lazily decoded from
// the slot-0 entry — handlers consult it once via ctx.Input()) plus the
// replay buffer keyed by frame.slot.
//
// Each frame's slot is engine-stamped (see invoker/wire_replay
// translateEntry), so this loop is a pure byte-mirror: validate, place
// at frame.slot, store. Typed decoding for command/notification
// payloads now happens lazily inside the wireContext primitives when
// the handler actually consults a slot — entries past the handler's
// suspension point pay no decode cost.
//
// Cursor inference is retained as a defensive fallback for the
// pathological case where a frame arrives without a stamped slot (slot
// 0 collides with JEInput); in that case we synthesize a unique slot
// past the entry count. Production engines always stamp.
func readReplay(src frameSource, codec wire.Codec, count uint32) ([]byte, map[uint32]*replayEntry, error) {
	replay := make(map[uint32]*replayEntry, count)
	if count == 0 {
		return nil, replay, nil
	}
	var (
		input        []byte
		fallbackSlot uint32 = count // unique synthetic slot for legacy frames
	)
	for i := range count {
		f, err := src.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil, fmt.Errorf("replay truncated at frame %d/%d", i, count)
			}
			return nil, nil, err
		}
		if err := wire.ValidatePayload(f); err != nil {
			return nil, nil, err
		}
		typeCode, _, _ := wire.UnpackHeader(f.GetHeader())

		slot := f.GetSlot()
		if slot == 0 && typeCode != wire.TypeCmdInput {
			// Legacy / unstamped frame. Park it past the entry count
			// so it doesn't collide with JEInput's slot 0.
			slot = fallbackSlot
			fallbackSlot++
		}
		replay[slot] = &replayEntry{typeCode: typeCode, payload: f.GetPayload()}

		// JEInput is the one frame we decode eagerly so ctx.Input() can
		// hand the handler its input bytes without re-doing this per
		// call. Single decode per session, cheap.
		if typeCode == wire.TypeCmdInput {
			var in protocolv1.InputCommandMessage
			if err := codec.Unmarshal(f.GetPayload(), &in); err != nil {
				return nil, nil, fmt.Errorf("decode replay InputCommandMessage: %w", err)
			}
			input = in.GetValue().GetContent()
		}
	}
	return input, replay, nil
}

// sendSuspension emits a SuspensionMessage frame and closes the
// session cleanly. The engine maps awaitingTokens into
// InvocationSuspended.awaiting_on for observability; the wake path
// itself is respawn-driven by Suspended→Invoked transitions on the
// next completion event.
func sendSuspension(sink frameSink, codec wire.Codec, awaitingTokens []string) error {
	sm := &protocolv1.SuspensionMessage{}
	// Translate token strings back to typed waiting_* fields. Tokens
	// shaped "completion:<N>" land in waiting_completions; "signal:<N>"
	// in waiting_signals; everything else falls through to
	// waiting_named_signals. The strings are descriptive (the engine
	// stuffs them straight into awaiting_on for observability) so this
	// translation is lossless even if a token doesn't match any prefix.
	for _, t := range awaitingTokens {
		if id, ok := parseTokenSuffix(t, "completion:"); ok {
			sm.WaitingCompletions = append(sm.WaitingCompletions, id)
			continue
		}
		if id, ok := parseTokenSuffix(t, "signal:"); ok {
			sm.WaitingSignals = append(sm.WaitingSignals, id)
			continue
		}
		sm.WaitingNamedSignals = append(sm.WaitingNamedSignals, t)
	}
	body, err := codec.Marshal(sm)
	if err != nil {
		return fmt.Errorf("marshal SuspensionMessage: %w", err)
	}
	return sink.Send(wire.FrameFor(wire.TypeSuspension, body))
}

// sendError emits an ErrorMessage frame, terminating the session. The
// engine treats ErrorMessage as a terminal failure with the supplied
// code + message round-tripped into InvocationStatus.Completed.
func sendError(sink frameSink, codec wire.Codec, code uint32, message string) error {
	em := &protocolv1.ErrorMessage{Code: code, Message: message}
	body, err := codec.Marshal(em)
	if err != nil {
		return fmt.Errorf("marshal ErrorMessage: %w", err)
	}
	return sink.Send(wire.FrameFor(wire.TypeError, body))
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

// parseTokenSuffix returns the uint32 trailing prefix in t, or (0,
// false) when t doesn't start with prefix or the suffix isn't a
// well-formed unsigned integer. Stricter than fmt.Sscanf, which would
// accept "completion:5xxx" and yield 5.
func parseTokenSuffix(t, prefix string) (uint32, bool) {
	if !strings.HasPrefix(t, prefix) {
		return 0, false
	}
	n, err := strconv.ParseUint(t[len(prefix):], 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}
