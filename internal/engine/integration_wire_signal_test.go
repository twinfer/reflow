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

// fakeHandlerSignalAwaiter is a wire handler that calls WaitSignal on
// the named signal and returns its payload as the invocation output.
// Used by integration tests that exercise the signal inbox + awaiter
// stitch path end-to-end.
type fakeHandlerSignalAwaiter struct {
	service    string
	handler    string
	signalName string
}

func (f *fakeHandlerSignalAwaiter) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: f.service, Kind: protocolv1.Kind_KIND_OBJECT, HandlerNames: []string{f.handler}},
		},
	}
}

func (f *fakeHandlerSignalAwaiter) httpHandler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerSignalAwaiter) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	t.Helper()
	startFrame, err := stream.Receive()
	if err != nil {
		return err
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		return err
	}

	// Walk the replay buffer. On first run only JEInput (1 entry) is
	// present. On respawn after a signal lands we see JEInput +
	// JEAwaitSignal + JESignalResult (3 entries). Pluck the signal
	// payload from the result frame if present.
	var resolvedPayload []byte
	resultDelivered := false
	for range sm.GetKnownEntries() {
		f, err := stream.Receive()
		if err != nil {
			return err
		}
		tc, _, _ := wire.UnpackHeader(f.GetHeader())
		if tc == wire.TypeNoteSignal {
			var note protocolv1.SignalNotificationMessage
			if err := proto.Unmarshal(f.GetPayload(), &note); err != nil {
				return err
			}
			if v, ok := note.GetResult().(*protocolv1.SignalNotificationMessage_Value); ok {
				resolvedPayload = v.Value.GetContent()
				resultDelivered = true
			}
		}
	}

	if !resultDelivered {
		// Fresh run (or inbox miss): emit AwaitSignal at cmdSlot 1
		// (resultSlot 2), then suspend.
		awaitCmd := &protocolv1.AwaitSignalCommandMessage{
			SignalName:         f.signalName,
			ResultCompletionId: 2,
		}
		awaitPayload, _ := proto.Marshal(awaitCmd)
		if err := stream.Send(frameFor(wire.TypeCmdAwaitSignal, awaitPayload)); err != nil {
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

	// Replay surfaced the signal payload — return it as the
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

// TestWireDispatch_HTTP2_SignalAwaitArrives exercises the await-then-
// signal flow end-to-end:
//
//  1. Submit a keyed invocation that calls WaitSignal("ready") and
//     suspends.
//  2. Verify it reaches Suspended.
//  3. Propose an InvokerEffect.SignalDelivered{Target, "ready",
//     payload} via the partition's ingress proposer (mimicking what
//     happens when another invocation sends a signal via outbox).
//  4. Verify the invocation completes with the signal payload as output.
//
// This covers the awaiter-stitch path (apply arm finds a pending
// SignalAwaiter when the signal arrives).
func TestWireDispatch_HTTP2_SignalAwaitArrives(t *testing.T) {
	const wantPayload = "signal-payload-1"
	awaiter := &fakeHandlerSignalAwaiter{
		service:    "Counter",
		handler:    "Increment",
		signalName: "ready",
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
		ObjectKey:   "alice",
	}
	pk := routing.PartitionKey(target.GetServiceName(), target.GetObjectKey())
	id := buildID(pk, "wire-signal-id1")
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}

	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-signal", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: deploymentResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	// Wait for the handler to call WaitSignal and suspend.
	_ = awaitSuspended(t, host, shardID, id, 10*time.Second)

	// Propose the signal directly (mimicking outbox delivery).
	sigCtx, sigCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sigCancel()
	if err := pr.Proposer().ProposeIngress(sigCtx, "test/signal-arrive", shardID, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_SignalDelivered{SignalDelivered: &enginev1.SignalDelivered{
				Target:     target,
				SignalName: "ready",
				Payload:    []byte(wantPayload),
			}},
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress signal: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != wantPayload {
		t.Errorf("output = %q; want %q", got, wantPayload)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}

// TestWireDispatch_HTTP2_SignalBufferedBeforeAwait exercises the inbox
// buffered path: a signal lands BEFORE the handler calls WaitSignal.
// The apply arm buffers it in signal_inbox; when the handler runs and
// emits JEAwaitSignal, the apply arm probes the inbox, finds the
// buffered entry, writes JESignalResult inline at the result slot, and
// deletes the inbox row. Handler resumes and returns the payload
// without suspending.
func TestWireDispatch_HTTP2_SignalBufferedBeforeAwait(t *testing.T) {
	const wantPayload = "buffered-payload"
	awaiter := &fakeHandlerSignalAwaiter{
		service:    "Counter",
		handler:    "Increment",
		signalName: "ready",
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
		ObjectKey:   "alice-buffered",
	}
	pk := routing.PartitionKey(target.GetServiceName(), target.GetObjectKey())
	id := buildID(pk, "wire-buffered-id1")
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}

	// First: invoke (transitions Free → Scheduled → Invoked and
	// populates KeyLeaseTable.current_invocation so the signal can
	// route). Submit doesn't wait on the handler; the SignalDelivered
	// must land while the lease is held.
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-buffered", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: deploymentResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress invoke: %v", err)
	}

	// Race: we want the signal to arrive before the handler's
	// JEAwaitSignal commits. Propose it immediately. If the handler
	// happens to win the race and writes the awaiter first, the
	// awaiter-stitch path runs; either way the final output is the
	// payload, so the assertion stands. This test exercises the
	// buffered side as the typical case under heavy load.
	sigCtx, sigCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sigCancel()
	if err := pr.Proposer().ProposeIngress(sigCtx, "test/buffered-signal", shardID, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_SignalDelivered{SignalDelivered: &enginev1.SignalDelivered{
				Target:     target,
				SignalName: "ready",
				Payload:    []byte(wantPayload),
			}},
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress signal: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != wantPayload {
		t.Errorf("output = %q; want %q", got, wantPayload)
	}
}
