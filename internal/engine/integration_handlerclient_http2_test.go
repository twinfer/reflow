package engine_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// callRegisterDeployment invokes admin.Server.RegisterDeployment via
// gRPC loopback so the request rides the same auth + dispatch path
// production uses. Avoids reaching into the unexported method.
func callRegisterDeployment(ctx context.Context, srv *admin.Server, url string) (*adminv1.RegisterDeploymentResponse, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(ln) }()
	defer func() { gs.GracefulStop(); _ = ln.Close() }()

	cc, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cc.Close() }()
	return adminv1.NewAdminClient(cc).RegisterDeployment(ctx, &adminv1.RegisterDeploymentRequest{Url: url})
}

// fakeHandlerHTTP2 is the handler-side h2c server used by the HTTP/2
// round-trip + deployment-swap tests. Two endpoints:
//
//   - GET /discover    → discoveryv1.DiscoveryResponse protobuf body
//   - POST /invoke/.../...
//     reads the engine's StartMessage frame, writes OutputCommandMessage
//     and EndMessage frames back, then drains the request body until EOF.
//
// onStart, if non-nil, is invoked synchronously after the StartMessage is
// decoded. Tests use it to gate completion (e.g. the deployment-swap test
// blocks the slow deployment until released).
type fakeHandlerHTTP2 struct {
	output  []byte
	onStart func(start *protocolv1.StartMessage) error
}

func (f *fakeHandlerHTTP2) discoveryBody(t *testing.T) []byte {
	t.Helper()
	resp := &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Echo", Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{"echo"}},
		},
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal DiscoveryResponse: %v", err)
	}
	return body
}

func (f *fakeHandlerHTTP2) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/discover":
			w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
			_, _ = w.Write(f.discoveryBody(t))
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/invoke/"):
			f.serveInvoke(t, w, r)
			return
		default:
			http.NotFound(w, r)
		}
	})
}

func (f *fakeHandlerHTTP2) serveInvoke(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	// Read the engine's first frame; it must be a StartMessage.
	start, err := readFrame(r.Body)
	if err != nil {
		http.Error(w, "read start: "+err.Error(), http.StatusBadRequest)
		return
	}
	typeCode, _, _ := handlerclient.UnpackHeader(start.GetHeader())
	if typeCode != handlerclient.TypeStart {
		http.Error(w, "first frame not StartMessage", http.StatusBadRequest)
		return
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(start.GetPayload(), &sm); err != nil {
		http.Error(w, "decode StartMessage: "+err.Error(), http.StatusBadRequest)
		return
	}
	if f.onStart != nil {
		if err := f.onStart(&sm); err != nil {
			http.Error(w, "onStart: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		// h2c always supplies a Flusher; reaching here means the test
		// harness regressed onto HTTP/1.1.
		http.Error(w, "server: ResponseWriter is not a Flusher", http.StatusInternalServerError)
		return
	}

	out := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.output},
		},
	}
	payload, err := proto.Marshal(out)
	if err != nil {
		return
	}
	if err := writeFrame(w, handlerclient.TypeCmdOutput, payload); err != nil {
		return
	}
	flusher.Flush()
	endPayload, err := proto.Marshal(&protocolv1.EndMessage{})
	if err != nil {
		return
	}
	if err := writeFrame(w, handlerclient.TypeEnd, endPayload); err != nil {
		return
	}
	flusher.Flush()

	// Wait for the engine to CloseSend; returning early would close the
	// response body before the engine has read EndMessage on slow CI runs.
	_, _ = io.Copy(io.Discard, r.Body)
}

// readFrame decodes one [8-byte BE header][payload] frame from r.
func readFrame(r io.Reader) (*protocolv1.Frame, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	h := binary.BigEndian.Uint64(hdr[:])
	_, _, length := handlerclient.UnpackHeader(h)
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("read payload (%d bytes): %w", length, err)
		}
	}
	return &protocolv1.Frame{Header: h, Payload: payload}, nil
}

// writeFrame writes a [8-byte BE header][payload] frame to w.
func writeFrame(w io.Writer, typeCode uint16, payload []byte) error {
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], handlerclient.PackHeader(typeCode, 0, uint32(len(payload))))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// startFakeHandlerHTTP2 binds an h2c server hosting f.handler on a free
// port and returns its addr + a teardown. Uses stdlib's unencrypted HTTP/2
// support via http.Server.Protocols (the x/net/http2/h2c package is
// deprecated in favor of this).
func startFakeHandlerHTTP2(t *testing.T, f *fakeHandlerHTTP2) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{
		Handler:   f.handler(t),
		Protocols: new(http.Protocols),
	}
	// Accept both HTTP/1.1 (for misconfigured probes that return 4xx) and
	// h2c (the engine-side path under test).
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(true)
	go func() {
		_ = srv.Serve(ln)
	}()
	teardown := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = ln.Close()
	}
	return ln.Addr().String(), teardown
}

// TestWireDispatch_HTTP2_RoundTrip exercises the full engine ↔ handler
// wire path over raw HTTP/2: admin.RegisterDeployment discovers the
// fake h2c handler via GET /discover, the partition's invoker resolves
// the deployment and opens an HTTP/2 POST to /invoke/Echo/echo, and the
// fake handler completes the session with a fixed output. Mirrors
// TestWireDispatch_GRPC_RoundTrip but over HTTP/2.
func TestWireDispatch_HTTP2_RoundTrip(t *testing.T) {
	const wantOutput = "wired-http2:hello"

	fake := &fakeHandlerHTTP2{output: []byte(wantOutput)}
	fakeAddr, teardown := startFakeHandlerHTTP2(t, fake)
	defer teardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:        3,
		Handlers: sdk.NewRegistry(),
	})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host

	srv, err := admin.NewServer(admin.Config{
		Host:   host,
		Runner: host.MetadataRunner(),
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	regResp, err := callRegisterDeployment(regCtx, srv, "http://"+fakeAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}
	if regResp.GetDeploymentId() == "" {
		t.Fatal("RegisterDeployment returned empty deployment_id")
	}

	id := buildID(1, "wire-rt-http2")
	target := &enginev1.InvocationTarget{ServiceName: "Echo", HandlerName: "echo"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-http2", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, 1, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != wantOutput {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}

// TestWireDispatch_HTTP2_DeploymentSwap registers two deployments
// pointing at distinct handler endpoints with the same (service, handler)
// surface, then submits two invocations explicitly pinned to D1 and D2.
// Asserts the engine routes each invocation to the URL recorded in its
// stamped deployment_id — a regression here would let an invocation
// re-route across a deployment swap, breaking the journal-replay
// guarantee that future versions of a handler can co-exist with their
// past selves until in-flight invocations drain.
func TestWireDispatch_HTTP2_DeploymentSwap(t *testing.T) {
	const (
		wantD1 = "d1:hello"
		wantD2 = "d2:hello"
	)

	// Deployment 1: blocks on a release channel so we can observe pinning
	// to D1 even after D2 has been registered. The output asserts D1
	// served this invocation rather than D2.
	d1Released := make(chan struct{})
	fakeD1 := &fakeHandlerHTTP2{
		output: []byte(wantD1),
		onStart: func(_ *protocolv1.StartMessage) error {
			<-d1Released
			return nil
		},
	}
	d1Addr, d1Teardown := startFakeHandlerHTTP2(t, fakeD1)
	defer d1Teardown()

	fakeD2 := &fakeHandlerHTTP2{output: []byte(wantD2)}
	d2Addr, d2Teardown := startFakeHandlerHTTP2(t, fakeD2)
	defer d2Teardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:        3,
		Handlers: sdk.NewRegistry(),
	})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host

	srv, err := admin.NewServer(admin.Config{
		Host:   host,
		Runner: host.MetadataRunner(),
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	d1Resp, err := callRegisterDeployment(regCtx, srv, "http://"+d1Addr)
	if err != nil {
		t.Fatalf("RegisterDeployment D1: %v", err)
	}

	// Submit invocation A pinned to D1 while D1 is still alone.
	idA := buildID(1, "swap-A")
	target := &enginev1.InvocationTarget{ServiceName: "Echo", HandlerName: "echo"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/swap-A", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idA,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: d1Resp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress A: %v", err)
	}

	// Now register D2 (same (service, handler), different URL).
	d2Resp, err := callRegisterDeployment(regCtx, srv, "http://"+d2Addr)
	if err != nil {
		t.Fatalf("RegisterDeployment D2: %v", err)
	}
	if d1Resp.GetDeploymentId() == d2Resp.GetDeploymentId() {
		t.Fatalf("D1 and D2 share deployment_id %q; expected distinct ids", d1Resp.GetDeploymentId())
	}

	// Submit invocation B pinned to D2. With D1 still blocked, B should
	// complete via D2's fast endpoint first.
	idB := buildID(1, "swap-B")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/swap-B", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idB,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: d2Resp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress B: %v", err)
	}

	completedB := awaitCompleted(t, host, 1, idB, 10*time.Second)
	if got := string(completedB.GetOutput()); got != wantD2 {
		t.Errorf("B output = %q; want %q (B should pin to D2)", got, wantD2)
	}

	// Release D1; A must now complete with D1's output (NOT D2's), proving
	// the engine kept routing A to its pinned deployment after D2's
	// registration.
	close(d1Released)

	completedA := awaitCompleted(t, host, 1, idA, 10*time.Second)
	if got := string(completedA.GetOutput()); got != wantD1 {
		t.Errorf("A output = %q; want %q (A pinned to D1)", got, wantD1)
	}
}

// findMetadataLeader scans cluster.Nodes for the rig whose metadata
// runner reports IsLeader. Errors the test if no leader is found —
// AwaitAnyMetadataLeader is the precondition.
func findMetadataLeader(t *testing.T, cluster *loadgen.Cluster) *loadgen.InProcessNode {
	t.Helper()
	for _, n := range cluster.Nodes {
		ip := n.(*loadgen.InProcessNode)
		if mr := ip.Host.MetadataRunner(); mr != nil && mr.IsLeader() {
			return ip
		}
	}
	t.Fatal("no metadata leader after AwaitAnyMetadataLeader")
	return nil
}
