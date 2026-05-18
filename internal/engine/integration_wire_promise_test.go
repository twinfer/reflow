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
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerPromiseAwaiter is a workflow handler that calls
// Promise("done").Result() and returns the resolved payload as the
// invocation output. Used to exercise the workflow-promise end-to-end
// flow with an external ResolveWorkflowPromise call.
type fakeHandlerPromiseAwaiter struct {
	service     string
	handler     string
	promiseName string
}

func (f *fakeHandlerPromiseAwaiter) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: f.service, Kind: protocolv1.Kind_KIND_WORKFLOW, HandlerNames: []string{f.handler}},
		},
	}
}

func (f *fakeHandlerPromiseAwaiter) httpHandler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerPromiseAwaiter) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	t.Helper()
	startFrame, err := stream.Receive()
	if err != nil {
		return err
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		return err
	}

	// Walk replay frames. On the first run only JEInput is present
	// (known_entries=1). On respawn after the promise resolves we see
	// JEInput + JEGetPromise + JEPromiseResult (known_entries=3); the
	// result frame carries the resolved value.
	var resolvedPayload []byte
	resultDelivered := false
	for range sm.GetKnownEntries() {
		f, err := stream.Receive()
		if err != nil {
			return err
		}
		tc, _, _ := wire.UnpackHeader(f.GetHeader())
		if tc == wire.TypeNoteGetPromise {
			var note protocolv1.GetPromiseCompletionNotificationMessage
			if err := proto.Unmarshal(f.GetPayload(), &note); err != nil {
				return err
			}
			if v, ok := note.GetResult().(*protocolv1.GetPromiseCompletionNotificationMessage_Value); ok {
				resolvedPayload = v.Value.GetContent()
				resultDelivered = true
			}
		}
	}

	if !resultDelivered {
		// Fresh run: emit GetPromise at cmd slot 1 (result slot 2),
		// then suspend pending the resolve.
		getCmd := &protocolv1.GetPromiseCommandMessage{
			Name:               f.promiseName,
			Key:                sm.GetKey(),
			ResultCompletionId: 2,
		}
		getPayload, _ := proto.Marshal(getCmd)
		if err := stream.Send(frameFor(wire.TypeCmdGetPromise, getPayload)); err != nil {
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

	// Replay surfaced the promise payload — return it as the
	// invocation's output.
	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: resolvedPayload},
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

// TestWireDispatch_HTTP2_PromiseIngressResolve exercises the
// workflow-promise + ingress.ResolveWorkflowPromise flow end-to-end:
//
//  1. Submit a workflow invocation that calls Promise("done").Result()
//     and suspends.
//  2. Verify it reaches Suspended.
//  3. Propose an InvokerEffect.PromiseCompleted via the partition's
//     ingress proposer (mimicking the ResolveWorkflowPromise RPC).
//  4. Verify the workflow completes with the resolved value as output.
func TestWireDispatch_HTTP2_PromiseIngressResolve(t *testing.T) {
	const wantPayload = "promise-resolved-payload"
	awaiter := &fakeHandlerPromiseAwaiter{
		service:     "Orders",
		handler:     "run",
		promiseName: "done",
	}
	handlerAddr, teardown := startFakeHandlerHTTP2WithHandler(t, awaiter.httpHandler(t))
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

	target := &enginev1.InvocationTarget{
		ServiceName: awaiter.service,
		HandlerName: awaiter.handler,
		ObjectKey:   "order-1",
	}
	pk := routing.PartitionKey(target.GetServiceName(), target.GetObjectKey())
	id := buildID(pk, "wire-promise-id1")
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}

	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-promise", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("input"),
			DeploymentId: deploymentResp.GetDeploymentId(),
			Kind:         uint32(protocolv1.Kind_KIND_WORKFLOW),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress invoke: %v", err)
	}

	_ = awaitSuspended(t, host, shardID, id, 10*time.Second)

	// Resolve the promise (mimicking ingress.ResolveWorkflowPromise).
	resCtx, resCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer resCancel()
	if err := pr.Proposer().ProposeIngress(resCtx, "test/promise-resolve", shardID, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_PromiseCompleted{PromiseCompleted: &enginev1.PromiseCompleted{
				Service:     target.GetServiceName(),
				WorkflowKey: target.GetObjectKey(),
				PromiseName: "done",
				Value:       []byte(wantPayload),
			}},
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress promise resolve: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != wantPayload {
		t.Errorf("output = %q; want %q", got, wantPayload)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}
