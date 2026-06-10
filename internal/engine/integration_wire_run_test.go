package engine_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/admin"
	"github.com/twinfer/reflw/internal/loadgen"
	"github.com/twinfer/reflw/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflw/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
)

// fakeHandlerRun is a wire handler that exercises ctx.Run end-to-end:
//
//   - First invocation (known_entries=1, just JEInput): emit
//     RunCommandMessage + ProposeRunCompletionMessage carrying a value
//     (non-retryable), then OutputCommandMessage + EndMessage wrapping
//     the run output.
//   - Replay invocation (known_entries=2, Input + JERun): if the engine
//     ever replays this handler, the replay buffer carries the cached
//     value; the handler still emits the same outcome shape so the
//     completed status round-trips correctly.
type fakeHandlerRun struct {
	runValue   []byte
	outputWrap []byte
}

func (f *fakeHandlerRun) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Compute", Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{"run"}},
		},
	}
}

func (f *fakeHandlerRun) handler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerRun) serveInvoke(t *testing.T, stream *fakeBidi) error {
	t.Helper()

	startFrame, err := stream.Receive()
	if err != nil {
		return err
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		return err
	}
	for range sm.GetKnownEntries() {
		if _, err := stream.Receive(); err != nil {
			return err
		}
	}

	runCmd := &protocolv1.RunCommandMessage{ResultCompletionId: 1, Name: "compute"}
	runPayload, _ := proto.Marshal(runCmd)
	if err := stream.Send(frameFor(wire.TypeCmdRun, runPayload)); err != nil {
		return err
	}

	prop := &protocolv1.ProposeRunCompletionMessage{
		ResultCompletionId: 1,
		Result: &protocolv1.ProposeRunCompletionMessage_Value{
			Value: f.runValue,
		},
	}
	propPayload, _ := proto.Marshal(prop)
	if err := stream.Send(frameFor(wire.TypeProposeRunDone, propPayload)); err != nil {
		return err
	}

	final := append([]byte{}, f.outputWrap...)
	final = append(final, f.runValue...)
	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: final},
		},
	}
	outPayload, _ := proto.Marshal(outMsg)
	if err := stream.Send(frameFor(wire.TypeCmdOutput, outPayload)); err != nil {
		return err
	}
	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	if err := stream.Send(frameFor(wire.TypeEnd, endPayload)); err != nil {
		return err
	}
	return drainStream(stream)
}

// TestWireDispatch_HTTP2_Run drives a wire handler doing ctx.Run end-to-end:
//
//  1. Handler emits RunCommandMessage + ProposeRunCompletionMessage(value="42").
//  2. Engine proposes InvokerEffect_RunProposal → FSM journals JERun.
//  3. Handler emits OutputCommandMessage{"run:42"} + EndMessage.
//  4. Invocation completes; status.Completed.Output == "run:42".
func TestWireDispatch_HTTP2_Run(t *testing.T) {
	const wantOutput = "run:42"

	fake := &fakeHandlerRun{
		runValue:   []byte("42"),
		outputWrap: []byte("run:"),
	}
	addr, teardown := startFakeHandlerHTTP2WithHandler(t, fake.handler(t))
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
	regResp, err := callRegisterDeployment(regCtx, srv, "http://"+addr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	id := buildID(1, "wire-run")
	target := &enginev1.InvocationTarget{ServiceName: "Compute", HandlerName: "run"}
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-run", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != wantOutput {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}
