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

// runSession drives one handler invocation: read StartMessage +
// InputCommandMessage, look up the handler, run it, then emit
// OutputCommandMessage + EndMessage (or ErrorMessage on protocol /
// framing failure). route is the transport-supplied (service, handler)
// hint — HTTP/2 fills it from the URL path /invoke/<service>/<handler>;
// StartMessage echoes the same tuple. When both are populated they MUST
// agree, or the session fails with a protocol-violation ErrorMessage.
//
// The returned error is logged by the transport; it is NOT mirrored as
// an ErrorMessage on the wire (that frame is reserved for protocol-level
// failures the engine should treat as terminal). Transport-level cleanup
// (closing the response body) is the caller's responsibility.
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
	input, err := readInput(stream, codec)
	if err != nil {
		return fmt.Errorf("read InputCommandMessage: %w", err)
	}

	// Resolve the route. StartMessage is authoritative when populated;
	// HTTP/2's URL path acts as a cross-check that the engine and
	// handler agree on which (service, handler) the session is for.
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

	// Build the wire context and run the handler. A panic is recovered
	// and translated to an ErrorMessage so the engine doesn't hang on
	// the stream.
	invID := &enginev1.InvocationId{Uuid: start.GetId()}
	wctx := newWireContext(ctx, invID, input, stream, codec)

	output, runErr := runHandler(wctx, fn, input)

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

// readInput consumes the InputCommandMessage frame.
func readInput(stream frameStream, codec handlerclient.Codec) ([]byte, error) {
	f, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Engine closed the stream without sending input; treat as
			// "no input" rather than failing the session.
			return nil, nil
		}
		return nil, err
	}
	if err := handlerclient.ValidatePayload(f); err != nil {
		return nil, err
	}
	typeCode, _, _ := handlerclient.UnpackHeader(f.GetHeader())
	if typeCode != handlerclient.TypeCmdInput {
		return nil, fmt.Errorf("expected InputCommandMessage (0x%04x); got 0x%04x",
			handlerclient.TypeCmdInput, typeCode)
	}
	var in protocolv1.InputCommandMessage
	if err := codec.Unmarshal(f.GetPayload(), &in); err != nil {
		return nil, fmt.Errorf("decode InputCommandMessage: %w", err)
	}
	return in.GetValue().GetContent(), nil
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
