package server_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/engine/handlerclient/http2client"
	"github.com/twinfer/reflow/pkg/sdk"
	"github.com/twinfer/reflow/pkg/sdk/server"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// TestHTTP2Server_RoundTrip drives a registered handler end-to-end via
// pkg/sdk/server.NewHTTP2 + internal/engine/handlerclient/http2client.
// The engine's wire_session is bypassed — this test asserts the
// handler-side half on its own: input arrives, handler runs, output
// flows back over raw HTTP/2 (h2c).
func TestHTTP2Server_RoundTrip(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ sdk.Context, in []byte) ([]byte, error) {
		return append([]byte("h2-echo:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	srv, err := server.NewHTTP2(server.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewHTTP2: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	url := "http://" + ln.Addr().String()
	client, err := http2client.New("test-dep", url, true, nil)
	if err != nil {
		t.Fatalf("http2client.New: %v", err)
	}
	defer func() { _ = client.Close() }()

	output := runOneSession(t, client, handlerclient.Route{Service: "Echo", Handler: "echo"}, []byte("hi"))
	if got, want := string(output), "h2-echo:hi"; got != want {
		t.Errorf("output = %q; want %q", got, want)
	}
}

// TestHTTP2Server_FailureRoundTrip verifies a handler returning *Failure
// surfaces as OutputCommandMessage.failure on the wire.
func TestHTTP2Server_FailureRoundTrip(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Echo", "boom", func(_ sdk.Context, _ []byte) ([]byte, error) {
		return nil, sdk.NewFailure(42, "boom")
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	srv, err := server.NewHTTP2(server.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewHTTP2: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	url := "http://" + ln.Addr().String()
	client, err := http2client.New("test-dep", url, true, nil)
	if err != nil {
		t.Fatalf("http2client.New: %v", err)
	}
	defer func() { _ = client.Close() }()

	failure := runOneSessionExpectFailure(t, client,
		handlerclient.Route{Service: "Echo", Handler: "boom"}, []byte("input"))
	if failure.GetCode() != 42 || failure.GetMessage() != "boom" {
		t.Errorf("failure = (code=%d, msg=%q); want (42, %q)",
			failure.GetCode(), failure.GetMessage(), "boom")
	}
}

// TestHTTP2Server_Discover asserts /discover returns the registered
// handlers grouped by (service, kind).
func TestHTTP2Server_Discover(t *testing.T) {
	reg := sdk.NewRegistry()
	for _, sh := range [][3]string{
		{"Echo", "echo", "service"},
		{"Cart", "checkout", "object"},
		{"Cart", "addItem", "object"},
		{"Wf", "run", "workflow"},
	} {
		var err error
		switch sh[2] {
		case "service":
			err = reg.RegisterService(sh[0], sh[1], stubHandler)
		case "object":
			err = reg.RegisterObject(sh[0], sh[1], stubHandler)
		case "workflow":
			err = reg.RegisterWorkflow(sh[0], sh[1], stubHandler)
		}
		if err != nil {
			t.Fatalf("register %s/%s: %v", sh[0], sh[1], err)
		}
	}

	srv, err := server.NewHTTP2(server.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewHTTP2: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	// Discovery via the engine's admin path is exercised in
	// internal/engine integration tests — here we want a direct check
	// against /discover that doesn't drag in the full cluster bootstrap.
	resp := discoverHTTP2(t, "http://"+ln.Addr().String())
	if resp.GetProtocolVersion() != "v1" {
		t.Errorf("protocol_version = %q; want %q", resp.GetProtocolVersion(), "v1")
	}
	if got, want := len(resp.GetHandlers()), 3; got != want {
		t.Fatalf("len(handlers) = %d; want %d (Cart object handlers should fold into one group)", got, want)
	}
	// Assert Cart appears once with both handlers.
	var cart *discoveryv1.DiscoveredHandler
	for _, h := range resp.GetHandlers() {
		if h.GetService() == "Cart" {
			cart = h
			break
		}
	}
	if cart == nil {
		t.Fatal("Cart service not in discovery response")
	}
	if cart.GetKind() != protocolv1.Kind_KIND_OBJECT {
		t.Errorf("Cart.kind = %v; want KIND_OBJECT", cart.GetKind())
	}
	if got, want := len(cart.GetHandlerNames()), 2; got != want {
		t.Errorf("Cart handlers = %v; want 2 entries", cart.GetHandlerNames())
	}
}

// runOneSession opens a stream against client, sends StartMessage +
// InputCommandMessage matching the engine's wire-session protocol, and
// reads frames until EndMessage. Returns the OutputCommandMessage.value
// payload; fails the test on any unexpected condition.
func runOneSession(t *testing.T, client handlerclient.Client, route handlerclient.Route, input []byte) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Invoke(ctx, route)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	defer func() { _ = stream.CloseSend() }()

	if err := sendStart(stream, route, input); err != nil {
		t.Fatalf("send start: %v", err)
	}

	codec := handlerclient.DefaultCodec()
	var got *protocolv1.OutputCommandMessage
	for {
		f, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if err := handlerclient.ValidatePayload(f); err != nil {
			t.Fatalf("frame validate: %v", err)
		}
		typeCode, _, _ := handlerclient.UnpackHeader(f.GetHeader())
		switch typeCode {
		case handlerclient.TypeCmdOutput:
			var out protocolv1.OutputCommandMessage
			if err := codec.Unmarshal(f.GetPayload(), &out); err != nil {
				t.Fatalf("decode OutputCommandMessage: %v", err)
			}
			got = &out
		case handlerclient.TypeEnd:
			if got == nil {
				t.Fatal("EndMessage before OutputCommandMessage")
			}
			val, ok := got.GetResult().(*protocolv1.OutputCommandMessage_Value)
			if !ok {
				t.Fatalf("output result = %T; want Value", got.GetResult())
			}
			return val.Value.GetContent()
		case handlerclient.TypeError:
			var em protocolv1.ErrorMessage
			_ = codec.Unmarshal(f.GetPayload(), &em)
			t.Fatalf("ErrorMessage from handler: code=%d msg=%q", em.GetCode(), em.GetMessage())
		default:
			t.Fatalf("unexpected frame type 0x%04x", typeCode)
		}
	}
}

// runOneSessionExpectFailure mirrors runOneSession but expects the
// terminal OutputCommandMessage.result to be a Failure.
func runOneSessionExpectFailure(t *testing.T, client handlerclient.Client, route handlerclient.Route, input []byte) *protocolv1.Failure {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Invoke(ctx, route)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	defer func() { _ = stream.CloseSend() }()

	if err := sendStart(stream, route, input); err != nil {
		t.Fatalf("send start: %v", err)
	}

	codec := handlerclient.DefaultCodec()
	var got *protocolv1.OutputCommandMessage
	for {
		f, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		typeCode, _, _ := handlerclient.UnpackHeader(f.GetHeader())
		switch typeCode {
		case handlerclient.TypeCmdOutput:
			var out protocolv1.OutputCommandMessage
			if err := codec.Unmarshal(f.GetPayload(), &out); err != nil {
				t.Fatalf("decode OutputCommandMessage: %v", err)
			}
			got = &out
		case handlerclient.TypeEnd:
			if got == nil {
				t.Fatal("EndMessage before OutputCommandMessage")
			}
			fail, ok := got.GetResult().(*protocolv1.OutputCommandMessage_Failure)
			if !ok {
				t.Fatalf("output result = %T; want Failure", got.GetResult())
			}
			return fail.Failure
		default:
			t.Fatalf("unexpected frame type 0x%04x", typeCode)
		}
	}
}

// sendStart pushes the Start + Input frame pair the engine normally
// sends. The reflow-internal wire_session is bypassed here, so the
// helper has to do the encoding itself. known_entries=1 because the
// only replay frame is the InputCommandMessage at slot 0.
func sendStart(stream handlerclient.Stream, route handlerclient.Route, input []byte) error {
	codec := handlerclient.DefaultCodec()
	start := &protocolv1.StartMessage{
		Id:           []byte("test-uuid-16bytes"[:16]),
		DebugId:      "t/" + route.Service + "/" + route.Handler,
		ServiceName:  route.Service,
		HandlerName:  route.Handler,
		Kind:         protocolv1.Kind_KIND_SERVICE,
		KnownEntries: 1,
	}
	startBytes, err := codec.Marshal(start)
	if err != nil {
		return err
	}
	if err := stream.Send(handlerclient.FrameFor(handlerclient.TypeStart, startBytes)); err != nil {
		return err
	}
	in := &protocolv1.InputCommandMessage{Value: &protocolv1.Value{Content: input}}
	inputBytes, err := codec.Marshal(in)
	if err != nil {
		return err
	}
	return stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdInput, inputBytes))
}

// discoverHTTP2 issues GET /discover over h2c and decodes the response.
// The engine's admin path uses the same shape (see
// internal/engine/admin/server.go.discoverHTTP); here we hit the
// endpoint directly so the test stays self-contained.
func discoverHTTP2(t *testing.T, baseURL string) *discoveryv1.DiscoveryResponse {
	t.Helper()
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	tr.Protocols.SetHTTP1(false)
	hc := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	defer tr.CloseIdleConnections()

	target := strings.TrimRight(baseURL, "/") + "/discover"
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("build /discover request: %v", err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("GET /discover: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		t.Fatalf("/discover returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /discover body: %v", err)
	}
	var dr discoveryv1.DiscoveryResponse
	if err := proto.Unmarshal(body, &dr); err != nil {
		t.Fatalf("unmarshal DiscoveryResponse: %v", err)
	}
	return &dr
}

// stubHandler is a no-op handler used to populate the registry for the
// discovery test.
var stubHandler sdk.Handler = func(_ sdk.Context, _ []byte) ([]byte, error) { return nil, nil }

// TestHTTP2Server_NoBodyLeak runs many sequential sessions through one
// http2client.Client and asserts the transport doesn't accumulate idle
// connections after the engine's typical "defer CloseSend" teardown.
// Regression for the bug where Recv terminating on EOF never closed
// resp.Body and the underlying HTTP/2 stream was reaped only at GC.
func TestHTTP2Server_NoBodyLeak(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ sdk.Context, in []byte) ([]byte, error) {
		return append([]byte("h2-echo:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}
	srv, err := server.NewHTTP2(server.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewHTTP2: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	url := "http://" + ln.Addr().String()
	client, err := http2client.New("test-dep", url, true, nil)
	if err != nil {
		t.Fatalf("http2client.New: %v", err)
	}
	defer func() { _ = client.Close() }()

	// 32 sequential sessions: if Recv/CloseSend doesn't close resp.Body,
	// stdlib accumulates HTTP/2 stream tracking state. We can't directly
	// observe the leak without internal hooks, but a session count well
	// above the per-conn concurrency cap exercises the recycle path; a
	// real leak would manifest as stalls or stream-limit errors.
	for i := range 32 {
		output := runOneSession(t, client,
			handlerclient.Route{Service: "Echo", Handler: "echo"}, []byte("ping"))
		if got, want := string(output), "h2-echo:ping"; got != want {
			t.Fatalf("session %d output = %q; want %q", i, got, want)
		}
	}
}
