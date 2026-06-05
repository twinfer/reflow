package engine_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflw/proto/discoveryv1"
	"github.com/twinfer/reflw/proto/discoveryv1/discoveryv1connect"
	handlerv1 "github.com/twinfer/reflw/proto/handlerv1"
	"github.com/twinfer/reflw/proto/handlerv1/handlerv1connect"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
)

// fakeBidi adapts the unary InvokeRequest/InvokeResponse onto the
// Receive/Send shape the fake-handler sessions are written against:
// Receive pops the next request frame (io.EOF once exhausted), Send
// appends to the response. It lets the per-test session bodies stay
// unchanged across the bidi→unary transport switch.
type fakeBidi struct {
	in   []*protocolv1.Frame
	pos  int
	sent []*protocolv1.Frame
}

func (b *fakeBidi) Receive() (*protocolv1.Frame, error) {
	if b.pos >= len(b.in) {
		return nil, io.EOF
	}
	f := b.in[b.pos]
	b.pos++
	return f, nil
}

func (b *fakeBidi) Send(f *protocolv1.Frame) error {
	b.sent = append(b.sent, f)
	return nil
}

// connectFakeSession is invoked once per Invoke call. The implementation
// reads frames via stream.Receive and writes them via stream.Send;
// returning an error fails the session.
type connectFakeSession func(t *testing.T, stream *fakeBidi) error

// connectFakeImpl adapts a connectFakeSession into a Connect
// HandlerServiceHandler. The session function captures whatever
// per-test state the fake wants to track.
type connectFakeImpl struct {
	handlerv1connect.UnimplementedHandlerServiceHandler
	t       *testing.T
	session connectFakeSession
}

func (c connectFakeImpl) Invoke(_ context.Context, req *connect.Request[handlerv1.InvokeRequest]) (*connect.Response[handlerv1.InvokeResponse], error) {
	b := &fakeBidi{in: req.Msg.GetFrames()}
	if err := c.session(c.t, b); err != nil {
		return nil, err
	}
	return connect.NewResponse(&handlerv1.InvokeResponse{Frames: b.sent}), nil
}

// mountFakeHandler builds an http.Handler that mimics a real reflw
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
		Header:  wire.PackHeader(typeCode, 0, uint32(len(payload))),
		Payload: payload,
	}
}

// drainStream reads any remaining request frames until io.EOF. With the
// unary transport the whole request is already in memory, so this is
// effectively a no-op kept for source-compatibility with sessions that
// call it after writing their terminal frames.
func drainStream(stream *fakeBidi) error {
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
