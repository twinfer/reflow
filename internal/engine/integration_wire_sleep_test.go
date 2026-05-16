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
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
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

func (f *fakeHandlerSleep) discoveryBody(t *testing.T) []byte {
	t.Helper()
	resp := &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Sleeper", Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{"nap"}},
		},
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal DiscoveryResponse: %v", err)
	}
	return body
}

func (f *fakeHandlerSleep) handler(t *testing.T) http.Handler {
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

func (f *fakeHandlerSleep) serveInvoke(t *testing.T, w http.ResponseWriter, r *http.Request) {
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
	// Drain the replay frames so the engine side proceeds to read our
	// response without buffering against a stalled body.
	for range known {
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

	// known_entries == 1 means only JEInput was journaled → fresh invocation.
	// Anything larger means the engine already wrote JESleep + JESleepResult
	// in a prior session; resume by emitting Output + End.
	if known <= 1 {
		// Emit SleepCommandMessage for slot 1 (result lands at slot 2).
		sleepCmd := &protocolv1.SleepCommandMessage{
			WakeUpTime:         uint64(time.Now().UnixMilli()) + f.sleepMs,
			ResultCompletionId: 2,
		}
		payload, err := proto.Marshal(sleepCmd)
		if err != nil {
			return
		}
		if err := writeFrame(w, handlerclient.TypeCmdSleep, payload); err != nil {
			return
		}
		flusher.Flush()

		// Emit SuspensionMessage waiting on completion 2.
		sus := &protocolv1.SuspensionMessage{
			WaitingCompletions: []uint32{2},
		}
		susPayload, err := proto.Marshal(sus)
		if err != nil {
			return
		}
		_ = writeFrame(w, handlerclient.TypeSuspension, susPayload)
		flusher.Flush()
		_, _ = io.Copy(io.Discard, r.Body)
		return
	}

	// Resume path: replay carried the sleep result, so finish the invocation.
	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.output},
		},
	}
	outPayload, err := proto.Marshal(outMsg)
	if err != nil {
		return
	}
	if err := writeFrame(w, handlerclient.TypeCmdOutput, outPayload); err != nil {
		return
	}
	flusher.Flush()
	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	_ = writeFrame(w, handlerclient.TypeEnd, endPayload)
	flusher.Flush()
	_, _ = io.Copy(io.Discard, r.Body)
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
