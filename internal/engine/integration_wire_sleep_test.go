package engine_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/config"
	"github.com/twinfer/reflw/internal/loadgen"
	"github.com/twinfer/reflw/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflw/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
)

// fakeHandlerSleep is a wire handler that exercises the suspend-and-replay
// cycle for ctx.Sleep(d).
//
// On the first invocation (StartMessage.known_entries == 1, just JEInput):
//   - Read StartMessage + InputCommandMessage replay frame
//   - Emit SleepCommandMessage with wake_up_time = now + sleepMs
//   - Emit SuspensionMessage{waiting_completions: [2]} (slot 2 is the result)
//   - Close the response body
//
// On respawn (known_entries == 3, JEInput + JESleep + JESleepResult):
//   - Read StartMessage + 3 replay frames
//   - Emit OutputCommandMessage + EndMessage with the configured output
type fakeHandlerSleep struct {
	sleepMs uint64
	output  []byte
}

func (f *fakeHandlerSleep) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Sleeper", Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{"nap"}},
		},
	}
}

func (f *fakeHandlerSleep) handler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerSleep) serveInvoke(t *testing.T, stream *fakeBidi) error {
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
	for range known {
		if _, err := stream.Receive(); err != nil {
			return err
		}
	}

	// known_entries == 1 means only JEInput was journaled → fresh invocation.
	// Anything larger means the engine already wrote JESleep + JESleepResult
	// in a prior session; resume by emitting Output + End.
	if known <= 1 {
		sleepCmd := &protocolv1.SleepCommandMessage{
			WakeUpTime:         uint64(time.Now().UnixMilli()) + f.sleepMs,
			ResultCompletionId: 2,
		}
		payload, err := proto.Marshal(sleepCmd)
		if err != nil {
			return err
		}
		if err := stream.Send(frameFor(wire.TypeCmdSleep, payload)); err != nil {
			return err
		}
		sus := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}}
		susPayload, err := proto.Marshal(sus)
		if err != nil {
			return err
		}
		if err := stream.Send(frameFor(wire.TypeSuspension, susPayload)); err != nil {
			return err
		}
		return drainStream(stream)
	}

	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.output},
		},
	}
	outPayload, err := proto.Marshal(outMsg)
	if err != nil {
		return err
	}
	if err := stream.Send(frameFor(wire.TypeCmdOutput, outPayload)); err != nil {
		return err
	}
	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	if err := stream.Send(frameFor(wire.TypeEnd, endPayload)); err != nil {
		return err
	}
	return drainStream(stream)
}

// TestWireDispatch_HTTP2_Sleep runs an invocation whose handler suspends
// on ctx.Sleep(50ms) and resumes after the timer fires. End-to-end
// flow validated:
//
//  1. First session: handler emits SleepCommandMessage + SuspensionMessage;
//     engine proposes JESleep + InvokerEffect_Suspended; status moves
//     to Suspended.
//  2. Timer fires; FSM writes JESleepResult; status transitions
//     Suspended → Invoked; ActInvoke fires.
//  3. Second session opens with known_entries=3 (Input + Sleep +
//     SleepResult); handler observes that and emits Output + End.
//  4. Invocation completes with the expected output.
func TestWireDispatch_HTTP2_Sleep(t *testing.T) {
	const wantOutput = "slept:50ms"

	fake := &fakeHandlerSleep{
		sleepMs: 50,
		output:  []byte(wantOutput),
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

	srv, err := config.NewServer(config.Config{
		Host:   host,
		Runner: host.MetadataRunner(),
	})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	regResp, err := callRegisterDeployment(regCtx, srv, "http://"+addr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	id := buildID(1, "wire-sleep")
	target := &enginev1.InvocationTarget{ServiceName: "Sleeper", HandlerName: "nap"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-sleep", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, 1, id, 15*time.Second)
	if got := string(completed.GetOutput()); got != wantOutput {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}
