package engine_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/internal/storage/tables"
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

func (f *fakeHandlerStateWrites) discoveryBody(t *testing.T) []byte {
	t.Helper()
	resp := &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Counter", Kind: protocolv1.Kind_KIND_OBJECT, HandlerNames: []string{"tick"}},
		},
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal DiscoveryResponse: %v", err)
	}
	return body
}

func (f *fakeHandlerStateWrites) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/discover":
			w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
			_, _ = w.Write(f.discoveryBody(t))
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/invoke/"):
			f.serveInvoke(t, w, r)
			return
		default:
			http.NotFound(w, r)
		}
	})
}

func (f *fakeHandlerStateWrites) serveInvoke(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	// Read StartMessage + InputCommandMessage (engine writes both before
	// the handler emits anything).
	for range 2 {
		if _, err := readFrame(r.Body); err != nil {
			http.Error(w, "read engine frame: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "server: ResponseWriter is not a Flusher", http.StatusInternalServerError)
		return
	}

	// Emit state-write commands.
	for _, c := range f.commands {
		if err := writeFrame(w, c.typeCode, c.payload); err != nil {
			return
		}
		flusher.Flush()
	}

	// Emit terminal output + end.
	out := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.output},
		},
	}
	payload, err := proto.Marshal(out)
	if err != nil {
		return
	}
	if err := writeFrame(w, handlerclient.TypeCmdOutput, payload); err != nil {
		return
	}
	flusher.Flush()
	endPayload, err := proto.Marshal(&protocolv1.EndMessage{})
	if err != nil {
		return
	}
	if err := writeFrame(w, handlerclient.TypeEnd, endPayload); err != nil {
		return
	}
	flusher.Flush()
	_, _ = io.Copy(io.Discard, r.Body)
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
			{typeCode: handlerclient.TypeCmdSetState, payload: setPayload},
			{typeCode: handlerclient.TypeCmdClearState, payload: clearPayload},
			{typeCode: handlerclient.TypeCmdClearAllState, payload: clearAllPayload},
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

	srv, err := admin.NewServer(admin.Config{
		Host:   host,
		Runner: host.MetadataRunner(),
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
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
	count := 0
	if err := st.ScanObject(target, func(_ string, _ []byte) error {
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
			{typeCode: handlerclient.TypeCmdSetState, payload: setPayload},
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

	value, present, err := st.Get(target, "counter")
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
