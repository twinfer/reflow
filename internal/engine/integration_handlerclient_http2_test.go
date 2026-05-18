package engine_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/admin"
	"github.com/twinfer/reflow/internal/connectserver"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/loadgen"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	"github.com/twinfer/reflow/proto/adminv1/adminv1connect"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// callRegisterDeployment invokes admin.Server.RegisterDeployment via a
// Connect h2c loopback so the request rides the same auth + dispatch
// path production uses.
func callRegisterDeployment(ctx context.Context, srv *admin.Server, url string) (*adminv1.RegisterDeploymentResponse, error) {
	path, h := srv.NewHandler()
	cs, err := connectserver.New(ctx, connectserver.Config{
		Addr: "127.0.0.1:0",
	}, connectserver.Route{Path: path, Handler: h})
	if err != nil {
		return nil, err
	}
	defer cs.Close()

	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	tr.Protocols.SetHTTP1(false)
	defer tr.CloseIdleConnections()
	cli := adminv1connect.NewAdminClient(&http.Client{Transport: tr}, "http://"+cs.Addr())
	resp, err := cli.RegisterDeployment(ctx, connect.NewRequest(&adminv1.RegisterDeploymentRequest{Url: url}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// fakeHandlerHTTP2 is the handler-side h2c server used by the HTTP/2
// round-trip + deployment-swap tests. Two endpoints:
//
//   - GET /discover                                    → DiscoveryResponse
//   - POST /reflow.handler.v1.HandlerService/InvokeStream → Connect bidi
//     reads the engine's StartMessage frame, writes OutputCommandMessage
//     and EndMessage frames back, then drains the request stream.
//
// onStart, if non-nil, is invoked synchronously after the StartMessage is
// decoded. Tests use it to gate completion (e.g. the deployment-swap test
// blocks the slow deployment until released).
type fakeHandlerHTTP2 struct {
	output  []byte
	onStart func(start *protocolv1.StartMessage) error
}

func (f *fakeHandlerHTTP2) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Echo", Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{"echo"}},
		},
	}
}

func (f *fakeHandlerHTTP2) handler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerHTTP2) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	t.Helper()
	// Read the engine's first frame; it must be a StartMessage.
	start, err := stream.Receive()
	if err != nil {
		return err
	}
	typeCode, _, _ := handlerclient.UnpackHeader(start.GetHeader())
	if typeCode != handlerclient.TypeStart {
		return errors.New("first frame not StartMessage")
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(start.GetPayload(), &sm); err != nil {
		return err
	}
	if f.onStart != nil {
		if err := f.onStart(&sm); err != nil {
			return err
		}
	}

	out := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.output},
		},
	}
	payload, err := proto.Marshal(out)
	if err != nil {
		return err
	}
	if err := stream.Send(frameFor(handlerclient.TypeCmdOutput, payload)); err != nil {
		return err
	}
	endPayload, err := proto.Marshal(&protocolv1.EndMessage{})
	if err != nil {
		return err
	}
	if err := stream.Send(frameFor(handlerclient.TypeEnd, endPayload)); err != nil {
		return err
	}

	// Drain remaining client-sent frames so the HTTP/2 stream closes
	// cleanly; returning while the engine still has frames to write
	// would race with CloseSend.
	for {
		if _, err := stream.Receive(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return nil
		}
	}
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
// wire path over Connect RPC: admin.RegisterDeployment discovers the
// fake h2c handler via GET /discover, the partition's invoker resolves
// the deployment and opens HandlerService.InvokeStream, and the fake
// handler completes the session with a fixed output.
func TestWireDispatch_HTTP2_RoundTrip(t *testing.T) {
	const wantOutput = "wired-http2:hello"

	fake := &fakeHandlerHTTP2{output: []byte(wantOutput)}
	fakeAddr, teardown := startFakeHandlerHTTP2(t, fake)
	defer teardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N: 3,
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
		N: 3,
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
