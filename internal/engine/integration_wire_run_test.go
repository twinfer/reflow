package engine_test

import (
	"context"
	"io"
	"net/http"
	"strings"
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

// fakeHandlerRun is a wire handler that exercises ctx.Run end-to-end:
//
//   - First invocation (known_entries=1, just JEInput): emit
//     RunCommandMessage + ProposeRunCompletionMessage carrying a value
//     (non-retryable), then OutputCommandMessage + EndMessage wrapping
//     the run output.
//   - Replay invocation (known_entries=2, Input + JERun): if the engine
//     ever replays this handler, the replay buffer carries the cached
//     value; the handler still emits the same outcome shape so the
//     completed status round-trips correctly.
type fakeHandlerRun struct {
	runValue   []byte
	outputWrap []byte
}

func (f *fakeHandlerRun) discoveryBody(t *testing.T) []byte {
	t.Helper()
	resp := &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Compute", Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{"run"}},
		},
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal DiscoveryResponse: %v", err)
	}
	return body
}

func (f *fakeHandlerRun) handler(t *testing.T) http.Handler {
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

func (f *fakeHandlerRun) serveInvoke(t *testing.T, w http.ResponseWriter, r *http.Request) {
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
	flusher, _ := w.(http.Flusher)

	// Emit RunCommandMessage at slot 1 (claims the slot) + ProposeRun with the value.
	runCmd := &protocolv1.RunCommandMessage{ResultCompletionId: 1, Name: "compute"}
	runPayload, _ := proto.Marshal(runCmd)
	_ = writeFrame(w, handlerclient.TypeCmdRun, runPayload)
	flusher.Flush()

	prop := &protocolv1.ProposeRunCompletionMessage{
		ResultCompletionId: 1,
		Result: &protocolv1.ProposeRunCompletionMessage_Value{
			Value: f.runValue,
		},
	}
	propPayload, _ := proto.Marshal(prop)
	_ = writeFrame(w, handlerclient.TypeProposeRunDone, propPayload)
	flusher.Flush()

	// Wrap the run value into the final output.
	final := append([]byte{}, f.outputWrap...)
	final = append(final, f.runValue...)
	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: final},
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

// TestWireDispatch_HTTP2_Run drives a wire handler doing ctx.Run end-to-end:
//
//  1. Handler emits RunCommandMessage + ProposeRunCompletionMessage(value="42").
//  2. Engine proposes InvokerEffect_RunProposal → FSM journals JERun.
//  3. Handler emits OutputCommandMessage{"run:42"} + EndMessage.
//  4. Invocation completes; status.Completed.Output == "run:42".
func TestWireDispatch_HTTP2_Run(t *testing.T) {
	const wantOutput = "run:42"

	fake := &fakeHandlerRun{
		runValue:   []byte("42"),
		outputWrap: []byte("run:"),
	}
	addr, teardown := startFakeHandlerHTTP2WithHandler(t, fake.handler(t))
	defer teardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:        3,
		Handlers: sdk.NewRegistry(),
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
	regResp, err := callRegisterDeployment(regCtx, srv, "http://"+addr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	id := buildID(1, "wire-run")
	target := &enginev1.InvocationTarget{ServiceName: "Compute", HandlerName: "run"}
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-run", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != wantOutput {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}
