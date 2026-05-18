package engine_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	"github.com/twinfer/reflow/proto/discoveryv1/discoveryv1connect"
	"github.com/twinfer/reflow/proto/handlerv1/handlerv1connect"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// connectFakeSession is invoked once per InvokeStream call. The
// implementation reads frames via stream.Receive and writes them via
// stream.Send. Returning an error fails the stream.
type connectFakeSession func(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error

// connectFakeImpl adapts a connectFakeSession into a Connect
// HandlerServiceHandler. The session function captures whatever
// per-test state the fake wants to track.
type connectFakeImpl struct {
	handlerv1connect.UnimplementedHandlerServiceHandler
	t       *testing.T
	session connectFakeSession
}

func (c connectFakeImpl) InvokeStream(_ context.Context, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	return c.session(c.t, stream)
}

// mountFakeHandler builds an http.Handler that mimics a real reflow
// handler deployment: DiscoveryService.Discover returning the provided
// DiscoveryResponse, and HandlerService.InvokeStream driven by session.
// Used by every wire integration test in this package.
func mountFakeHandler(t *testing.T, discovery *discoveryv1.DiscoveryResponse, session connectFakeSession) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	discoverPath, discoverHandler := discoveryv1connect.NewDiscoveryServiceHandler(
		fakeDiscoverImpl{resp: discovery},
	)
	mux.Handle(discoverPath, discoverHandler)
	invokePath, invokeHandler := handlerv1connect.NewHandlerServiceHandler(
		connectFakeImpl{t: t, session: session},
	)
	mux.Handle(invokePath, invokeHandler)
	return mux
}

// fakeDiscoverImpl replies to DiscoveryService.Discover with a canned
// response. Tests construct it once per fake handler.
type fakeDiscoverImpl struct {
	discoveryv1connect.UnimplementedDiscoveryServiceHandler
	resp *discoveryv1.DiscoveryResponse
}

func (f fakeDiscoverImpl) Discover(_ context.Context, _ *connect.Request[discoveryv1.DiscoveryRequest]) (*connect.Response[discoveryv1.DiscoveryResponse], error) {
	return connect.NewResponse(f.resp), nil
}

// frameFor builds a protocolv1.Frame with header packed from typeCode +
// payload length. Used by fake-handler sessions to send framed messages
// onto the Connect stream.
func frameFor(typeCode uint16, payload []byte) *protocolv1.Frame {
	return &protocolv1.Frame{
		Header:  handlerclient.PackHeader(typeCode, 0, uint32(len(payload))),
		Payload: payload,
	}
}

// drainStream reads from stream until it returns io.EOF, then exits.
// Used by fake handlers that have written their terminal frames but
// want to let the engine CloseSend before returning (returning while
// the engine still has pending sends races with the HTTP/2 close).
func drainStream(stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	for {
		if _, err := stream.Receive(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// Any other error here is fine — the engine closed early.
			return nil
		}
	}
}
