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

func (f *fakeHandlerCaller) discoveryBody(t *testing.T) []byte {
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

func (f *fakeHandlerCaller) handler(t *testing.T) http.Handler {
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

func (f *fakeHandlerCaller) serveInvoke(t *testing.T, w http.ResponseWriter, r *http.Request) {
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

	known := sm.GetKnownEntries()
	// Read each replay frame and capture any CallCompletionNotificationMessage.
	var callResult []byte
	for range known {
		f, err := readFrame(r.Body)
		if err != nil {
			http.Error(w, "read replay frame: "+err.Error(), http.StatusBadRequest)
			return
		}
		tc, _, _ := handlerclient.UnpackHeader(f.GetHeader())
		if tc == handlerclient.TypeNoteCallDone {
			var note protocolv1.CallCompletionNotificationMessage
			if err := proto.Unmarshal(f.GetPayload(), &note); err == nil {
				if v, ok := note.GetResult().(*protocolv1.CallCompletionNotificationMessage_Value); ok {
					callResult = v.Value.GetContent()
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "ResponseWriter is not a Flusher", http.StatusInternalServerError)
		return
	}

	if callResult == nil {
		// Fresh path: emit CallCommandMessage targeting B + SuspensionMessage.
		callCmd := &protocolv1.CallCommandMessage{
			ServiceName:        f.calleeService,
			HandlerName:        f.calleeHandler,
			Parameter:          f.calleeInput,
			ResultCompletionId: 2,
		}
		payload, err := proto.Marshal(callCmd)
		if err != nil {
			return
		}
		if err := writeFrame(w, handlerclient.TypeCmdCall, payload); err != nil {
			return
		}
		flusher.Flush()

		sus := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}}
		susPayload, _ := proto.Marshal(sus)
		_ = writeFrame(w, handlerclient.TypeSuspension, susPayload)
		flusher.Flush()
		_, _ = io.Copy(io.Discard, r.Body)
		return
	}

	// Resume path: emit the prefix + callResult and finish.
	final := append([]byte{}, f.outputPrefix...)
	final = append(final, callResult...)
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
	// Registered via pkg/sdk/server so its deployment is durable in
	// shard 0 and (Callee, echo) → deployment_id resolves at outbox
	// dispatch time.
	calleeReg := sdk.NewRegistry()
	if err := calleeReg.RegisterService("Callee", "echo", func(_ sdk.Context, _ []byte) ([]byte, error) {
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
