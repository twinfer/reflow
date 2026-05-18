package engine_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/admin"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerSleeper is a wire handler that emits a Sleep command with
// a far-future wake-up and suspends. Used to put an invocation in
// long-lived Suspended state so the cancel path can terminate it.
type fakeHandlerSleeper struct {
	service string
	handler string
}

func (f *fakeHandlerSleeper) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: f.service, Kind: protocolv1.Kind_KIND_OBJECT, HandlerNames: []string{f.handler}},
		},
	}
}

func (f *fakeHandlerSleeper) httpHandler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerSleeper) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
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

	// Sleep for an hour — the test cancels well before this fires.
	sleepCmd := &protocolv1.SleepCommandMessage{
		WakeUpTime:         uint64(time.Now().Add(time.Hour).UnixMilli()),
		ResultCompletionId: 2,
	}
	sleepPayload, _ := proto.Marshal(sleepCmd)
	if err := stream.Send(frameFor(wire.TypeCmdSleep, sleepPayload)); err != nil {
		return err
	}

	susp := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}}
	suspPayload, _ := proto.Marshal(susp)
	if err := stream.Send(frameFor(wire.TypeSuspension, suspPayload)); err != nil {
		return err
	}
	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	if err := stream.Send(frameFor(wire.TypeEnd, endPayload)); err != nil {
		return err
	}
	return drainStream(stream)
}

// TestWireDispatch_HTTP2_CancelInvocation asserts the end-to-end cancel
// flow:
//
//  1. Register a Virtual Object handler that suspends on a 1-hour Sleep.
//  2. Submit a keyed invocation; verify it reaches Suspended.
//  3. Call ingress.CancelInvocation with that id.
//  4. Verify the invocation transitions to Completed with
//     FailureCode=CancelledCode and FailureMessage="invocation cancelled".
func TestWireDispatch_HTTP2_CancelInvocation(t *testing.T) {
	sleeper := &fakeHandlerSleeper{
		service: "Counter",
		handler: "Increment",
	}
	handlerAddr, teardown := startFakeHandlerHTTP2WithHandler(t, sleeper.httpHandler(t))
	defer teardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer cluster.Close()
	defer loadgen.StartEmbeddedHandlers(t, cluster, nil)()

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
	deploymentResp, err := callRegisterDeployment(regCtx, srv, "http://"+handlerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	// Build a keyed invocation. PartitionKey must match the (service,
	// object_key) hash so onInvoke populates KeyLeaseTable on the
	// owning shard — the cancel path's KeyLeaseTable lookup depends on
	// it landing on the same shard the signal routes to.
	const objectKey = "alice"
	target := &enginev1.InvocationTarget{
		ServiceName: sleeper.service,
		HandlerName: sleeper.handler,
		ObjectKey:   objectKey,
	}
	pk := routing.PartitionKey(target.GetServiceName(), target.GetObjectKey())
	id := buildID(pk, "wire-cancel-id1")
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}

	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-cancel", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: deploymentResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	// Wait for the invocation to reach Suspended (the sleeper handler
	// emits Sleep + Suspension).
	_ = awaitSuspended(t, host, shardID, id, 10*time.Second)

	// Build an ingress server and call CancelInvocation through it.
	ingSrv := ingress.NewServer(host, nil)
	cancelCtx, cancelCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCancel()
	resp, err := ingSrv.CancelInvocation(cancelCtx, connect.NewRequest(&ingressv1.CancelInvocationRequest{
		InvocationId: ingress.FormatInvocationID(id),
	}))
	if err != nil {
		t.Fatalf("ingress.CancelInvocation: %v", err)
	}
	if !resp.Msg.GetAccepted() {
		t.Errorf("CancelInvocation accepted = false; want true")
	}

	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if completed.GetFailureCode() != wire.CancelledCode {
		t.Errorf("failure_code = %d; want %d (CancelledCode)", completed.GetFailureCode(), wire.CancelledCode)
	}
	if completed.GetFailureMessage() != "invocation cancelled" {
		t.Errorf("failure_message = %q; want %q", completed.GetFailureMessage(), "invocation cancelled")
	}
}
