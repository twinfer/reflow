package reflw

import (
	"context"
	"testing"

	"github.com/twinfer/reflw/pkg/handler"
	"github.com/twinfer/reflw/pkg/handler/wire"
	handlerv1 "github.com/twinfer/reflw/proto/handlerv1"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
)

// TestInprocClient_RoundTrip exercises the in-process transport bridge end
// to end without an engine: build a StartMessage + InputCommandMessage
// request, dispatch it through inprocDialer → inprocClient →
// handler.InvokeInProc → runSession, and assert the handler's output comes
// back in the response frames with no HTTP involved.
func TestInprocClient_RoundTrip(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("inproc:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	dial := inprocDialer(reg, wire.DefaultCodec())
	client, err := dial("dep-inproc", inprocDeploymentURL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	codec := wire.DefaultCodec()
	route := wire.Route{Service: "Echo", Handler: "echo"}
	start := &protocolv1.StartMessage{
		Id:           []byte("inproc-uuid-16b!"),
		ServiceName:  route.Service,
		HandlerName:  route.Handler,
		Kind:         protocolv1.Kind_KIND_SERVICE,
		KnownEntries: 1,
	}
	startBytes, err := codec.Marshal(start)
	if err != nil {
		t.Fatalf("marshal StartMessage: %v", err)
	}
	inMsg := &protocolv1.InputCommandMessage{Value: &protocolv1.Value{Content: []byte("hi")}}
	inBytes, err := codec.Marshal(inMsg)
	if err != nil {
		t.Fatalf("marshal InputCommandMessage: %v", err)
	}
	req := &handlerv1.InvokeRequest{Frames: []*protocolv1.Frame{
		wire.FrameFor(wire.TypeStart, startBytes),
		wire.FrameFor(wire.TypeCmdInput, inBytes),
	}}

	resp, err := client.Invoke(context.Background(), route, req)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	var out *protocolv1.OutputCommandMessage
	for _, f := range resp.GetFrames() {
		code, _, _ := wire.UnpackHeader(f.GetHeader())
		if code == wire.TypeCmdOutput {
			var o protocolv1.OutputCommandMessage
			if err := codec.Unmarshal(f.GetPayload(), &o); err != nil {
				t.Fatalf("decode OutputCommandMessage: %v", err)
			}
			out = &o
		}
	}
	if out == nil {
		t.Fatal("no OutputCommandMessage in response frames")
	}
	val, ok := out.GetResult().(*protocolv1.OutputCommandMessage_Value)
	if !ok {
		t.Fatalf("output result = %T; want Value", out.GetResult())
	}
	if got, want := string(val.Value.GetContent()), "inproc:hi"; got != want {
		t.Errorf("output = %q; want %q", got, want)
	}
}
