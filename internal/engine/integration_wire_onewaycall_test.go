package engine_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerOneWayCall is a wire handler that fires a OneWayCall and
// completes immediately without waiting for the callee's result.
type fakeHandlerOneWayCall struct {
	callerService string
	callerHandler string
	calleeService string
	calleeHandler string
	calleeInput   []byte
	output        []byte
}

func (f *fakeHandlerOneWayCall) discoveryBody(t *testing.T) []byte {
	t.Helper()
	resp := &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: f.callerService, Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{f.callerHandler}},
		},
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal DiscoveryResponse: %v", err)
	}
	return body
}

func (f *fakeHandlerOneWayCall) handler(t *testing.T) http.Handler {
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

func (f *fakeHandlerOneWayCall) serveInvoke(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	startFrame, err := readFrame(r.Body)
	if err != nil {
		http.Error(w, "read start: "+err.Error(), http.StatusBadRequest)
		return
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		http.Error(w, "decode StartMessage: "+err.Error(), http.StatusBadRequest)
		return
	}
	for range sm.GetKnownEntries() {
		if _, err := readFrame(r.Body); err != nil {
			http.Error(w, "read replay frame: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "ResponseWriter is not a Flusher", http.StatusInternalServerError)
		return
	}

	// Emit OneWayCall + Output + End in one shot. OneWayCall doesn't
	// suspend the handler so we complete this invocation immediately.
	owCmd := &protocolv1.OneWayCallCommandMessage{
		ServiceName: f.calleeService,
		HandlerName: f.calleeHandler,
		Parameter:   f.calleeInput,
	}
	payload, err := proto.Marshal(owCmd)
	if err != nil {
		return
	}
	if err := writeFrame(w, handlerclient.TypeCmdOneWayCall, payload); err != nil {
		return
	}
	flusher.Flush()

	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.output},
		},
	}
	outPayload, _ := proto.Marshal(outMsg)
	_ = writeFrame(w, handlerclient.TypeCmdOutput, outPayload)
	flusher.Flush()
	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	_ = writeFrame(w, handlerclient.TypeEnd, endPayload)
	flusher.Flush()
	_, _ = io.Copy(io.Discard, r.Body)
}

// TestWireDispatch_HTTP2_OneWayCall asserts the wire OneWayCall flow
// dispatches the callee asynchronously and lets the caller complete
// immediately. Verifies:
//
//  1. Wire handler A's OneWayCall(B) emits OneWayCallCommandMessage.
//  2. A completes with its own output (no callee result needed).
//  3. The callee invocation B is dispatched and runs (the calleeRuns
//     counter increments).
func TestWireDispatch_HTTP2_OneWayCall(t *testing.T) {
	const wantOutput = "caller-done"

	var calleeRuns atomic.Int32
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Callee", "fired", func(_ sdk.Context, _ []byte) ([]byte, error) {
		calleeRuns.Add(1)
		return []byte("ack"), nil
	}); err != nil {
		t.Fatalf("RegisterService Callee: %v", err)
	}

	caller := &fakeHandlerOneWayCall{
		callerService: "Caller",
		callerHandler: "fire_b",
		calleeService: "Callee",
		calleeHandler: "fired",
		calleeInput:   []byte("ping"),
		output:        []byte(wantOutput),
	}
	callerAddr, callerTeardown := startFakeHandlerHTTP2WithHandler(t, caller.handler(t))
	defer callerTeardown()

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
	callerResp, err := callRegisterDeployment(regCtx, srv, "http://"+callerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment caller: %v", err)
	}

	id := buildID(1, "wire-one-way")
	target := &enginev1.InvocationTarget{ServiceName: "Caller", HandlerName: "fire_b"}
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-one-way", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: callerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != wantOutput {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}

	// Wait briefly for the callee outbox dispatch to run; the
	// OneWayCall semantics don't block the caller on this so we give it
	// a short window then assert.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if calleeRuns.Load() >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := calleeRuns.Load(); got < 1 {
		t.Errorf("calleeRuns = %d; want >= 1 (OneWayCall should have dispatched B)", got)
	}
}
