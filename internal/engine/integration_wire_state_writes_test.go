package engine_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerStateWrites is a wire-handler stub that emits a sequence
// of SetState / ClearState / ClearAllState command frames before
// returning OutputCommandMessage + EndMessage. Mirrors fakeHandlerHTTP2
// but exercises the 5f.1 state-write driveLoop arms.
type fakeHandlerStateWrites struct {
	// frames to emit, in order, before OutputCommandMessage. Each entry
	// is (typeCode, payload) — the test driver marshals/frames inline.
	commands []stateCommand
	output   []byte
}

type stateCommand struct {
	typeCode uint16
	payload  []byte
}

func (f *fakeHandlerStateWrites) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Counter", Kind: protocolv1.Kind_KIND_OBJECT, HandlerNames: []string{"tick"}},
		},
	}
}

func (f *fakeHandlerStateWrites) handler(t *testing.T) http.Handler {
	t.Helper()
	return mountFakeHandler(t, f.discovery(), f.serveInvoke)
}

func (f *fakeHandlerStateWrites) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	t.Helper()
	// Read StartMessage + InputCommandMessage (engine writes both before
	// the handler emits anything).
	for range 2 {
		if _, err := stream.Receive(); err != nil {
			return err
		}
	}

	for _, c := range f.commands {
		if err := stream.Send(frameFor(c.typeCode, c.payload)); err != nil {
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
	if err := stream.Send(frameFor(wire.TypeCmdOutput, payload)); err != nil {
		return err
	}
	endPayload, err := proto.Marshal(&protocolv1.EndMessage{})
	if err != nil {
		return err
	}
	if err := stream.Send(frameFor(wire.TypeEnd, endPayload)); err != nil {
		return err
	}
	return drainStream(stream)
}

// TestWireDispatch_HTTP2_StateWrites runs an end-to-end invocation
// whose handler emits SetState, then ClearState (for a different key),
// then ClearAllState before completing. Verifies:
//
//   - the invocation completes with the expected output (engine handled
//     the state command frames without erroring out);
//   - the StateTable for the (service, object_key) target is empty
//     after the ClearAllState wipes the prior SetState write — proving
//     the apply path materialized JESetState then JEClearAllState in
//     order.
func TestWireDispatch_HTTP2_StateWrites(t *testing.T) {
	const wantOutput = "state-writes:ok"

	setPayload, err := proto.Marshal(&protocolv1.SetStateCommandMessage{
		Key:   []byte("counter"),
		Value: &protocolv1.Value{Content: []byte("42")},
	})
	if err != nil {
		t.Fatalf("marshal SetState: %v", err)
	}
	clearPayload, err := proto.Marshal(&protocolv1.ClearStateCommandMessage{
		Key: []byte("stale"),
	})
	if err != nil {
		t.Fatalf("marshal ClearState: %v", err)
	}
	clearAllPayload, err := proto.Marshal(&protocolv1.ClearAllStateCommandMessage{})
	if err != nil {
		t.Fatalf("marshal ClearAllState: %v", err)
	}

	fake := &fakeHandlerStateWrites{
		output: []byte(wantOutput),
		commands: []stateCommand{
			{typeCode: wire.TypeCmdSetState, payload: setPayload},
			{typeCode: wire.TypeCmdClearState, payload: clearPayload},
			{typeCode: wire.TypeCmdClearAllState, payload: clearAllPayload},
		},
	}
	fakeAddr, teardown := startFakeHandlerHTTP2WithHandler(t, fake.handler(t))
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
	regResp, err := callRegisterDeployment(regCtx, srv, "http://"+fakeAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	id := buildID(1, "wire-state-writes")
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
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-state-writes", 1, &enginev1.Command{
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

	// After ClearAllState the StateTable for (Counter, user-1) should be
	// empty — proves both JESetState and JEClearAllState made it through
	// the apply path in order.
	store := pr.Snapshotter().Store()
	st := tables.StateTable{S: store}
	lp := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	count := 0
	if err := st.ScanObject(lp, keys.TenantDefault, target, func(_ string, _ []byte) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("StateTable.ScanObject: %v", err)
	}
	if count != 0 {
		t.Errorf("StateTable for %s/%s has %d row(s) after ClearAllState; want 0",
			target.GetServiceName(), target.GetObjectKey(), count)
	}

	// Sanity check: drive a follow-up SetState through a fresh invocation
	// to confirm the apply path is still healthy (and that the table
	// genuinely persists writes, not just absorbs them silently).
	setOnly := &fakeHandlerStateWrites{
		output: []byte("set-only:ok"),
		commands: []stateCommand{
			{typeCode: wire.TypeCmdSetState, payload: setPayload},
		},
	}
	setOnlyAddr, setOnlyTeardown := startFakeHandlerHTTP2WithHandler(t, setOnly.handler(t))
	defer setOnlyTeardown()

	setOnlyResp, err := callRegisterDeployment(regCtx, srv, "http://"+setOnlyAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment set-only: %v", err)
	}

	id2 := buildID(1, "wire-state-set-only")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-state-set-only", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id2,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: setOnlyResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress set-only: %v", err)
	}
	_ = awaitCompleted(t, host, 1, id2, 10*time.Second)

	value, present, err := st.Get(lp, keys.TenantDefault, target, "counter")
	if err != nil {
		t.Fatalf("StateTable.Get: %v", err)
	}
	if !present {
		t.Fatal("StateTable.Get(counter): not present after SetState")
	}
	if got := string(value); got != "42" {
		t.Errorf("StateTable.Get(counter) = %q; want %q", got, "42")
	}
}

// startFakeHandlerHTTP2WithHandler binds an h2c server hosting h on a
// free port and returns its addr + a teardown. Mirrors
// startFakeHandlerHTTP2 but takes a raw http.Handler so tests can supply
// their own request handling without subclassing fakeHandlerHTTP2.
func startFakeHandlerHTTP2WithHandler(t *testing.T, h http.Handler) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: h, Protocols: new(http.Protocols)}
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = srv.Serve(ln) }()
	teardown := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = ln.Close()
	}
	return ln.Addr().String(), teardown
}
