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
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerRunRetry simulates a transient failure on the first
// attempt and success on the second. The first invocation emits a
// retryable ProposeRunCompletion (no Failure code → engine writes
// JERun{retryable=true} and schedules a backoff timer). The second
// invocation, replayed after the timer fires, sees the retryable
// JERun marker without a TypeNoteRunDone notification, so wireContext's
// SDK-equivalent re-runs fn. We simulate that here by emitting a
// non-retryable ProposeRunCompletion with the success value.
type fakeHandlerRunRetry struct {
	attempt   atomic.Int32
	finalOut  []byte
	retryFail string
}

func (f *fakeHandlerRunRetry) discoveryBody(t *testing.T) []byte {
	t.Helper()
	resp := &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Compute", Kind: protocolv1.Kind_KIND_SERVICE, HandlerNames: []string{"runRetry"}},
		},
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal DiscoveryResponse: %v", err)
	}
	return body
}

func (f *fakeHandlerRunRetry) handler(t *testing.T) http.Handler {
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

func (f *fakeHandlerRunRetry) serveInvoke(t *testing.T, w http.ResponseWriter, r *http.Request) {
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

	attempt := f.attempt.Add(1)

	runCmd := &protocolv1.RunCommandMessage{ResultCompletionId: 1, Name: "compute"}
	runPayload, _ := proto.Marshal(runCmd)
	_ = writeFrame(w, handlerclient.TypeCmdRun, runPayload)
	flusher.Flush()

	prop := &protocolv1.ProposeRunCompletionMessage{ResultCompletionId: 1}
	if attempt == 1 {
		// First attempt: retryable failure (no Failure code = engine
		// treats as transient and schedules a backoff timer).
		prop.Retryable = true
		prop.Result = &protocolv1.ProposeRunCompletionMessage_Failure{
			Failure: &protocolv1.Failure{Message: f.retryFail},
		}
		propPayload, _ := proto.Marshal(prop)
		_ = writeFrame(w, handlerclient.TypeProposeRunDone, propPayload)
		flusher.Flush()
		// SDK suspends after a retryable failure — emit SuspensionMessage
		// so the engine proposes Suspended cleanly.
		susp := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{1}}
		suspPayload, _ := proto.Marshal(susp)
		_ = writeFrame(w, handlerclient.TypeSuspension, suspPayload)
		flusher.Flush()
		_, _ = io.Copy(io.Discard, r.Body)
		return
	}

	// Second attempt (after backoff timer fires + respawn): fn succeeds.
	prop.Result = &protocolv1.ProposeRunCompletionMessage_Value{Value: f.finalOut}
	propPayload, _ := proto.Marshal(prop)
	_ = writeFrame(w, handlerclient.TypeProposeRunDone, propPayload)
	flusher.Flush()

	outMsg := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.finalOut},
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

// TestWireDispatch_HTTP2_RunRetryable exercises the retryable ctx.Run
// path end-to-end on the wire:
//
//  1. First attempt: handler emits RunCommandMessage +
//     ProposeRunCompletion{retryable=true, failure=...} + Suspension.
//  2. Engine writes JERun{retryable=true}, schedules a 50ms backoff
//     (default retry policy), proposes Suspended.
//  3. Backoff timer fires; partition respawns the session.
//  4. wire_replay.translateEntry now emits TypeCmdRun marker only
//     (no TypeNoteRunDone for retryable JERun).
//  5. Handler re-invokes fn (attempt counter increments), emits
//     non-retryable ProposeRunCompletion with the success value, then
//     OutputCommandMessage + EndMessage.
//  6. Invocation completes with the success value.
func TestWireDispatch_HTTP2_RunRetryable(t *testing.T) {
	wantOutput := []byte("retried-ok")

	fake := &fakeHandlerRunRetry{
		finalOut:  wantOutput,
		retryFail: "transient",
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

	srv, err := admin.NewServer(admin.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	regResp, err := callRegisterDeployment(regCtx, srv, "http://"+addr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	id := buildID(1, "wire-run-retry")
	target := &enginev1.InvocationTarget{ServiceName: "Compute", HandlerName: "runRetry"}
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-run-retry", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("input"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 15*time.Second)
	if got := string(completed.GetOutput()); got != string(wantOutput) {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}
	if msg := completed.GetFailureMessage(); msg != "" {
		t.Errorf("failure_message = %q; want empty", msg)
	}
	if got := fake.attempt.Load(); got != 2 {
		t.Errorf("attempt count = %d; want 2 (first retryable failure + retry success)", got)
	}
}
