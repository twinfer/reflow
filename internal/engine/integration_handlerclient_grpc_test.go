package engine_test

import (
	"context"
	"errors"
	"io"
	"net"
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
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerServer is the handler-side gRPC implementation used by
// TestWireDispatch_GRPC_RoundTrip. It returns a fixed output on the
// first Invoke and serves Discovery responses describing one
// (Echo, echo) handler. Real handler-side wiring lands in
// pkg/sdk/server (5e); the in-test fake is sufficient for asserting
// the engine ↔ wire ↔ handler round-trip.
type fakeHandlerServer struct {
	protocolv1.UnimplementedSessionServiceServer
	protocolv1.UnimplementedDiscoveryServiceServer

	output []byte
}

func (f *fakeHandlerServer) Invoke(stream grpc.BidiStreamingServer[protocolv1.Frame, protocolv1.Frame]) error {
	// Receive the StartMessage frame (engine → handler).
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	typeCode, _, _ := handlerclient.UnpackHeader(first.GetHeader())
	if typeCode != handlerclient.TypeStart {
		return errors.New("first frame is not a StartMessage")
	}
	var start protocolv1.StartMessage
	if err := proto.Unmarshal(first.GetPayload(), &start); err != nil {
		return err
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
	if err := stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdOutput, payload)); err != nil {
		return err
	}
	endPayload, err := proto.Marshal(&protocolv1.EndMessage{})
	if err != nil {
		return err
	}
	if err := stream.Send(handlerclient.FrameFor(handlerclient.TypeEnd, endPayload)); err != nil {
		return err
	}
	// Wait for the engine to CloseSend (signals it received EndMessage)
	// or for the context to terminate. Returning before the client side
	// finishes can race the engine's propose.
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (f *fakeHandlerServer) Discover(_ context.Context, _ *protocolv1.DiscoveryRequest) (*protocolv1.DiscoveryResponse, error) {
	return &protocolv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*protocolv1.DiscoveredHandler{
			{
				Service:      "Echo",
				Kind:         protocolv1.Kind_KIND_SERVICE,
				HandlerNames: []string{"echo"},
			},
		},
	}, nil
}

// startFakeHandlerGRPC binds a gRPC server hosting both protocolv1
// services on a free port and returns its addr + a teardown.
func startFakeHandlerGRPC(t *testing.T, output []byte) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	fake := &fakeHandlerServer{output: output}
	protocolv1.RegisterSessionServiceServer(gs, fake)
	protocolv1.RegisterDiscoveryServiceServer(gs, fake)
	go func() {
		_ = gs.Serve(ln)
	}()
	return ln.Addr().String(), func() {
		gs.GracefulStop()
		_ = ln.Close()
	}
}

// TestWireDispatch_GRPC_RoundTrip exercises the full engine ↔ handler
// wire path: admin.RegisterDeployment discovers a fake gRPC handler,
// persists a DeploymentRecord on shard 0, ingress stamps the
// deployment_id on an invocation, the partition's invoker resolves the
// record, opens a wire stream, and the fake handler completes the
// session with a fixed output.
func TestWireDispatch_GRPC_RoundTrip(t *testing.T) {
	const wantOutput = "wired:hello"

	fakeAddr, fakeTeardown := startFakeHandlerGRPC(t, []byte(wantOutput))
	defer fakeTeardown()

	// N=3 because dragonboat's gossip layer requires at least one
	// non-self seed; single-node "multi-node" bootstrap (N=1) fails
	// NewNodeHost with "seed nodes not set". The N=3 cluster keeps
	// every test invariant intact while satisfying the seed
	// requirement.
	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:        3,
		Handlers: sdk.NewRegistry(), // no in-proc handlers; the wire path is the only route
	})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}

	var leaderRig *loadgen.InProcessNode
	for _, n := range cluster.Nodes {
		ip := n.(*loadgen.InProcessNode)
		if mr := ip.Host.MetadataRunner(); mr != nil && mr.IsLeader() {
			leaderRig = ip
			break
		}
	}
	if leaderRig == nil {
		t.Fatal("no metadata leader after AwaitAnyMetadataLeader")
	}
	host := leaderRig.Host

	srv, err := admin.NewServer(admin.Config{
		Host:   host,
		Runner: host.MetadataRunner(),
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}

	// Register the fake handler via the admin RPC. Routes through the
	// real gRPC discovery + Raft propose path.
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	regResp, err := callRegisterDeployment(regCtx, srv, "grpc://"+fakeAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}
	if regResp.GetDeploymentId() == "" {
		t.Fatal("RegisterDeployment returned empty deployment_id")
	}

	// Submit an invocation pinned to the new deployment. Use the host's
	// partition-1 proposer directly so we can stamp DeploymentId — the
	// loadgen Node.SubmitInvocation helper does not yet take it.
	id := buildID(1, "wire-rt")
	target := &enginev1.InvocationTarget{ServiceName: "Echo", HandlerName: "echo"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-grpc", 1, &enginev1.Command{
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
