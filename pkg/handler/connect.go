package handler

import (
	"context"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/pkg/handler/wire"
	handlerv1 "github.com/twinfer/reflw/proto/handlerv1"
	"github.com/twinfer/reflw/proto/handlerv1/handlerv1connect"
)

// handlerService implements handlerv1connect.HandlerServiceHandler. Invoke
// drives one session through runSession; route info is pulled from the
// StartMessage payload — Connect's procedure path carries no per-handler
// addressing.
type handlerService struct {
	handlerv1connect.UnimplementedHandlerServiceHandler
	registry *Registry
	codec    wire.Codec
}

// Invoke runs one full invocation session. req.Msg.Frames is the
// StartMessage frame followed by the replay frames; the response frames
// are the handler's emitted command frames followed by the terminal
// Output/Suspension/Error frame.
//
// A session error is reported in-band: runInvoke captures runSession's
// terminal frame in the response slice, so the outcome rides the frames.
// We never promote it to a connect.Error — that would attach status
// trailers the engine treats as a transport failure, masking the protocol
// frame the SDK already wrote.
func (s *handlerService) Invoke(ctx context.Context, req *connect.Request[handlerv1.InvokeRequest]) (*connect.Response[handlerv1.InvokeResponse], error) {
	frames := runInvoke(ctx, s.registry, s.codec, req.Msg.GetFrames())
	return connect.NewResponse(&handlerv1.InvokeResponse{Frames: frames}), nil
}
