package engine_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeStatePreloadHandler is a wire handler that echoes a single state
// value (looked up from StartMessage.state_map by key) back as the
// invocation's output. Use it to assert the engine populated the eager
// state snapshot correctly. When commands is non-empty the handler
// emits those frames first, before reading the captured value — so the
// same fixture can do "write then re-invoke and read" cycles.
type fakeStatePreloadHandler struct {
	echoKey  string         // state_map key to echo as output (after any commands)
	commands []stateCommand // optional pre-output command frames
	output   []byte         // if set, override the echoKey lookup with this fixed output
}

func (f *fakeStatePreloadHandler) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Counter", Kind: protocolv1.Kind_KIND_OBJECT, HandlerNames: []string{"tick"}},
		},
	}
}

func (f *fakeStatePreloadHandler) handler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeStatePreloadHandler) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	t.Helper()

	startFrame, err := stream.Receive()
	if err != nil {
		return err
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		return err
	}
	// Read InputCommandMessage replay frame.
	if _, err := stream.Receive(); err != nil {
		return err
	}

	out := f.output
	if out == nil && f.echoKey != "" {
		for _, e := range sm.GetStateMap() {
			if string(e.GetKey()) == f.echoKey {
				out = append([]byte(nil), e.GetValue()...)
				break
			}
		}
	}

	for _, c := range f.commands {
		if err := stream.Send(frameFor(c.typeCode, c.payload)); err != nil {
			return err
		}
	}

	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: out},
		},
	}
	payload, err := proto.Marshal(outMsg)
	if err != nil {
		return err
	}
	if err := stream.Send(frameFor(wire.TypeCmdOutput, payload)); err != nil {
		return err
	}

	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	if err := stream.Send(frameFor(wire.TypeEnd, endPayload)); err != nil {
		return err
	}
	return drainStream(stream)
}

// TestWireDispatch_HTTP2_StatePreload verifies the engine ships the
// eager state snapshot via StartMessage.state_map for keyed
// invocations.
//
// Step 1: invocation A writes "counter=42" via a SetState frame.
// Step 2: invocation B (same target + object_key) opens; the handler
// reads counter out of StartMessage.state_map and echoes its value as
// the invocation output. Assert B's output equals "42" — proving the
// engine preloaded the StateTable row into state_map.
func TestWireDispatch_HTTP2_StatePreload(t *testing.T) {
	const (
		stateKey   = "counter"
		stateValue = "42"
	)

	setPayload, err := proto.Marshal(&protocolv1.SetStateCommandMessage{
		Key:   []byte(stateKey),
		Value: &protocolv1.Value{Content: []byte(stateValue)},
	})
	if err != nil {
		t.Fatalf("marshal SetState: %v", err)
	}

	// Handler that writes the state row before returning fixed output.
	writerHandler := &fakeStatePreloadHandler{
		output: []byte("written"),
		commands: []stateCommand{
			{typeCode: wire.TypeCmdSetState, payload: setPayload},
		},
	}
	writerAddr, writerTeardown := startFakeHandlerHTTP2WithHandler(t, writerHandler.handler(t))
	defer writerTeardown()

	// Handler that echoes state_map[stateKey] back as output.
	echoHandler := &fakeStatePreloadHandler{echoKey: stateKey}
	echoAddr, echoTeardown := startFakeHandlerHTTP2WithHandler(t, echoHandler.handler(t))
	defer echoTeardown()

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
	writerResp, err := callRegisterDeployment(regCtx, srv, "http://"+writerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment writer: %v", err)
	}
	echoResp, err := callRegisterDeployment(regCtx, srv, "http://"+echoAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment echo: %v", err)
	}

	target := &enginev1.InvocationTarget{
		ServiceName: "Counter",
		HandlerName: "tick",
		ObjectKey:   "user-1",
	}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()

	// Invocation A: write state via writer handler.
	idA := buildID(1, "preload-write")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/preload-write", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idA,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: writerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress A: %v", err)
	}
	_ = awaitCompleted(t, host, 1, idA, 10*time.Second)

	// Invocation B: same (service, object_key); engine should preload
	// state_map={counter: 42}; echo handler returns counter's value.
	idB := buildID(1, "preload-read")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/preload-read", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idB,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: echoResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress B: %v", err)
	}
	completedB := awaitCompleted(t, host, 1, idB, 10*time.Second)
	if got := string(completedB.GetOutput()); got != stateValue {
		t.Errorf("B output = %q; want %q (engine should have preloaded state_map)", got, stateValue)
	}
}
