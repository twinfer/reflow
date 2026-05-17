package server

import (
	"context"
	"errors"
	"io"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/sdk"
	"github.com/twinfer/reflow/proto/handlerv1/handlerv1connect"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// handlerService implements handlerv1connect.HandlerServiceHandler.
// InvokeStream drives one session through runSession; route info is
// pulled from the StartMessage payload — Connect's procedure path
// carries no per-handler addressing.
type handlerService struct {
	handlerv1connect.UnimplementedHandlerServiceHandler
	registry *sdk.Registry
	codec    handlerclient.Codec
}

func (s *handlerService) InvokeStream(ctx context.Context, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	cs := &connectStream{stream: stream}
	if err := runSession(ctx, cs, cs, s.registry, s.codec, handlerclient.Route{}); err != nil {
		// runSession's terminal frame (Output / Suspension / Error) has
		// already been sent on the wire. Don't promote the err to a
		// connect.Error — that would attach gRPC status trailers the
		// engine treats as a transport failure, masking the protocol
		// frame the SDK already wrote.
		return nil
	}
	return nil
}

// connectStream adapts a connect.BidiStream onto frameSource +
// frameSink. Connect wraps the underlying stream's EOF in its own
// error, so Recv unwraps before returning io.EOF.
type connectStream struct {
	stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]
}

func (c *connectStream) Recv() (*protocolv1.Frame, error) {
	f, err := c.stream.Receive()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	return f, nil
}

func (c *connectStream) Send(f *protocolv1.Frame) error {
	return c.stream.Send(f)
}
