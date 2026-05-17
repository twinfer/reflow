package engine_test

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/loadgen"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerRunRetry simulates a transient failure on the first
// attempt and success on the second. The first invocation emits a
// retryable ProposeRunCompletion (no Failure code → engine writes
// JERun{retryable=true} and schedules a backoff timer). The second
// invocation, replayed after the timer fires, sees the retryable
// JERun marker without a TypeNoteRunDone notification, so wireContext's
// SDK-equivalent re-runs fn. We simulate that here by emitting a
// non-retryable ProposeRunCompletion with the success value.
type fakeHandlerRunRetry struct {
	attempt   atomic.Int32
	finalOut  []byte
	retryFail string
}

func (f *fakeHandlerRunRetry) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Compute", Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{"runRetry"}},
		},
	}
}

func (f *fakeHandlerRunRetry) handler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerRunRetry) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
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

	attempt := f.attempt.Add(1)

	runCmd := &protocolv1.RunCommandMessage{ResultCompletionId: 1, Name: "compute"}
	runPayload, _ := proto.Marshal(runCmd)
	if err := stream.Send(frameFor(handlerclient.TypeCmdRun, runPayload)); err != nil {
		return err
	}

	prop := &protocolv1.ProposeRunCompletionMessage{ResultCompletionId: 1}
	if attempt == 1 {
		prop.Retryable = true
		prop.Result = &protocolv1.ProposeRunCompletionMessage_Failure{
			Failure: &protocolv1.Failure{Message: f.retryFail},
		}
		propPayload, _ := proto.Marshal(prop)
		if err := stream.Send(frameFor(handlerclient.TypeProposeRunDone, propPayload)); err != nil {
			return err
		}
		susp := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{1}}
		suspPayload, _ := proto.Marshal(susp)
		if err := stream.Send(frameFor(handlerclient.TypeSuspension, suspPayload)); err != nil {
			return err
		}
		return drainStream(stream)
	}

	prop.Result = &protocolv1.ProposeRunCompletionMessage_Value{Value: f.finalOut}
	propPayload, _ := proto.Marshal(prop)
	if err := stream.Send(frameFor(handlerclient.TypeProposeRunDone, propPayload)); err != nil {
		return err
	}

	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.finalOut},
		},
	}
	outPayload, _ := proto.Marshal(outMsg)
	if err := stream.Send(frameFor(handlerclient.TypeCmdOutput, outPayload)); err != nil {
		return err
	}
	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	if err := stream.Send(frameFor(handlerclient.TypeEnd, endPayload)); err != nil {
		return err
	}
	return drainStream(stream)
}

// TestWireDispatch_HTTP2_RunRetryable exercises the retryable ctx.Run
// path end-to-end on the wire:
//
//  1. First attempt: handler emits RunCommandMessage +
//     ProposeRunCompletion{retryable=true, failure=...} + Suspension.
//  2. Engine writes JERun{retryable=true}, schedules a 50ms backoff
//     (default retry policy), proposes Suspended.
//  3. Backoff timer fires; partition respawns the session.
//  4. wire_replay.translateEntry now emits TypeCmdRun marker only
//     (no TypeNoteRunDone for retryable JERun).
//  5. Handler re-invokes fn (attempt counter increments), emits
//     non-retryable ProposeRunCompletion with the success value, then
//     OutputCommandMessage + EndMessage.
//  6. Invocation completes with the success value.
func TestWireDispatch_HTTP2_RunRetryable(t *testing.T) {
	wantOutput := []byte("retried-ok")

	fake := &fakeHandlerRunRetry{
		finalOut:  wantOutput,
		retryFail: "transient",
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

	srv, err := admin.NewServer(admin.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	regResp, err := callRegisterDeployment(regCtx, srv, "http://"+addr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	id := buildID(1, "wire-run-retry")
	target := &enginev1.InvocationTarget{ServiceName: "Compute", HandlerName: "runRetry"}
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-run-retry", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("input"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 15*time.Second)
	if got := string(completed.GetOutput()); got != string(wantOutput) {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}
	if msg := completed.GetFailureMessage(); msg != "" {
		t.Errorf("failure_message = %q; want empty", msg)
	}
	if got := fake.attempt.Load(); got != 2 {
		t.Errorf("attempt count = %d; want 2 (first retryable failure + retry success)", got)
	}
}
