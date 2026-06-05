package engine_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/config"
	"github.com/twinfer/reflw/internal/loadgen"
	"github.com/twinfer/reflw/pkg/handler"
	"github.com/twinfer/reflw/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflw/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
)

// fakeHandlerCaller is a wire handler that exercises ctx.Call by:
//   - On the first invocation (known_entries=1, just JEInput): emit
//     CallCommandMessage targeting (calleeService, calleeHandler), then
//     SuspensionMessage waiting on slot 2 (the result slot).
//   - On respawn (known_entries=3, Input+Call+CallResult): scan the
//     replay frames for the CallCompletionNotificationMessage and emit
//     OutputCommandMessage echoing the callee's value.
type fakeHandlerCaller struct {
	callerService string
	callerHandler string
	calleeService string
	calleeHandler string
	calleeInput   []byte
	outputPrefix  []byte
}

func (f *fakeHandlerCaller) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: f.callerService, Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{f.callerHandler}},
		},
	}
}

func (f *fakeHandlerCaller) handler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerCaller) serveInvoke(t *testing.T, stream *fakeBidi) error {
	t.Helper()

	startFrame, err := stream.Receive()
	if err != nil {
		return err
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		return err
	}

	known := sm.GetKnownEntries()
	var callResult []byte
	for range known {
		frame, err := stream.Receive()
		if err != nil {
			return err
		}
		tc, _, _ := wire.UnpackHeader(frame.GetHeader())
		if tc == wire.TypeNoteCallDone {
			var note protocolv1.CallCompletionNotificationMessage
			if err := proto.Unmarshal(frame.GetPayload(), &note); err == nil {
				if v, ok := note.GetResult().(*protocolv1.CallCompletionNotificationMessage_Value); ok {
					callResult = v.Value.GetContent()
				}
			}
		}
	}

	if callResult == nil {
		callCmd := &protocolv1.CallCommandMessage{
			ServiceName:        f.calleeService,
			HandlerName:        f.calleeHandler,
			Parameter:          f.calleeInput,
			ResultCompletionId: 2,
		}
		payload, err := proto.Marshal(callCmd)
		if err != nil {
			return err
		}
		if err := stream.Send(frameFor(wire.TypeCmdCall, payload)); err != nil {
			return err
		}

		sus := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}}
		susPayload, _ := proto.Marshal(sus)
		if err := stream.Send(frameFor(wire.TypeSuspension, susPayload)); err != nil {
			return err
		}
		return drainStream(stream)
	}

	final := append([]byte{}, f.outputPrefix...)
	final = append(final, callResult...)
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

// TestWireDispatch_HTTP2_Call drives an end-to-end Call between two
// HTTP/2-registered handlers:
//
//  1. Handler A does ctx.Call(B, "ping") → emits CallCommandMessage +
//     SuspensionMessage.
//  2. Engine proposes JECall on A and Invoke envelope into outbox
//     toward B; A's status moves to Suspended.
//  3. Handler B receives the invoke, returns "pong".
//  4. B's Completed apply notices parent_link and journals JECallResult
//     on A's journal; A transitions Suspended → Invoked.
//  5. A's session respawns with known_entries=3 (Input + Call +
//     CallResult); the wire handler observes CallCompletionNotification
//     in replay and emits Output{"caller:pong"}.
//  6. Invocation A completes with output "caller:pong".
func TestWireDispatch_HTTP2_Call(t *testing.T) {
	const (
		wantOutput  = "caller:pong"
		calleeInput = "ping"
	)

	// Handler B is the callee — a real SDK handler returning "pong".
	// Registered via pkg/handler so its deployment is durable in
	// shard 0 and (Callee, echo) → deployment_id resolves at outbox
	// dispatch time.
	calleeReg := handler.NewRegistry()
	if err := calleeReg.RegisterService("Callee", "echo", func(_ handler.Context, _ []byte) ([]byte, error) {
		return []byte("pong"), nil
	}); err != nil {
		t.Fatalf("RegisterService Callee: %v", err)
	}

	// Handler A is a fake HTTP/2 endpoint that issues ctx.Call(B, ...).
	caller := &fakeHandlerCaller{
		callerService: "Caller",
		callerHandler: "call_b",
		calleeService: "Callee",
		calleeHandler: "echo",
		calleeInput:   []byte(calleeInput),
		outputPrefix:  []byte("caller:"),
	}
	callerAddr, callerTeardown := startFakeHandlerHTTP2WithHandler(t, caller.handler(t))
	defer callerTeardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N: 3,
	})
	defer cluster.Close()
	defer loadgen.StartEmbeddedHandlers(t, cluster, calleeReg)()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host

	srv, err := config.NewServer(config.Config{
		Host:   host,
		Runner: host.MetadataRunner(),
	})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	callerResp, err := callRegisterDeployment(regCtx, srv, "http://"+callerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment caller: %v", err)
	}

	id := buildID(1, "wire-call")
	target := &enginev1.InvocationTarget{ServiceName: "Caller", HandlerName: "call_b"}
	// Partitioner maps partition_key=1 to a specific shard; submit
	// to that shard so the callback path can find A's row by reading
	// from the same shard.
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-call", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: callerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 15*time.Second)
	if got := string(completed.GetOutput()); got != wantOutput {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}
