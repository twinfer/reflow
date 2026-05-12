package engine_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestPhase2_HandlerSurvivesKill is the Phase 2 exit criterion. A handler
// doing SetState → Sleep → Run is brought up, suspended on the Sleep
// timer, the Host is closed (simulating a crash before the Sleep fires),
// the same DataDir is reopened on a fresh Host, and the invocation must
// resume to Completed with the Run body executed exactly once.
//
// Why a single Run-count check proves "kill -9 safety":
//   - The runCount atomic lives in the test process memory and survives
//     the close/reopen of the engine. The handler closure captures it on
//     both bring-ups.
//   - The Run body is journaled as a JERun row. On the resumed session,
//     ctx.Run consults the journal first; if the row is present the body
//     is skipped. We never reach 2 calls unless durability breaks.
//   - SetState is journaled as JESetState; resumption replays the journal
//     and skips the SDK-side proposal entirely.
//   - Sleep is journaled as JESleep; TimerService rebuilds the pending
//     timer from PendingTimersTable on leader gain.
func TestPhase2_HandlerSurvivesKill(t *testing.T) {
	var runCount atomic.Int32

	handler := func(c sdk.Context, in []byte) ([]byte, error) {
		if err := c.SetState("k", in); err != nil {
			return nil, err
		}
		// 1500ms gives the timer headroom past the Close+reopen cycle on
		// dev hardware (dragonboat NewNodeHost is ~100–300ms, then a
		// leader election adds another ~300–800ms). The Sleep is what
		// makes the kill land *mid-execution*: the SDK has suspended
		// waiting on the timer when we Close the host. Past-due fires
		// would race with leader-gain timing in a way that doesn't
		// belong in this exit-criterion test.
		if err := c.Sleep(1500 * time.Millisecond); err != nil {
			return nil, err
		}
		v, err := c.Run("compute", func() ([]byte, error) {
			runCount.Add(1)
			return []byte("computed"), nil
		})
		if err != nil {
			return nil, err
		}
		return v, nil
	}

	reg := sdk.NewRegistry()
	if err := reg.Register("Survivor", "go", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")
	raftAddr := freeLocalAddr(t)

	h1, err := engine.NewHost(engine.HostConfig{
		NodeID:         1,
		RaftAddr:       raftAddr,
		DataDir:        dataDir,
		RTTMillisecond: 50,
		Handlers:       reg,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	r, err := h1.StartPartition(1)
	if err != nil {
		_ = h1.Close()
		t.Fatalf("StartPartition: %v", err)
	}
	startCtx, startCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := h1.AwaitLeader(startCtx, 1); err != nil {
		startCancel()
		_ = h1.Close()
		t.Fatalf("AwaitLeader: %v", err)
	}
	startCancel()

	id := buildID(1, "survivor")
	target := &enginev1.InvocationTarget{ServiceName: "Survivor", HandlerName: "go"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = r.Proposer().ProposeIngress(propCtx, "test/survivor", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("input"),
		}},
	})
	propCancel()
	if err != nil {
		_ = h1.Close()
		t.Fatalf("ProposeIngress: %v", err)
	}

	// Wait until the Sleep entry (index 2: Input=0, SetState=1, Sleep=2)
	// is journaled. That confirms the handler suspended on Sleep and any
	// crash now lands "mid-execution".
	if err := waitForJournalEntry(h1, id, 2, 3*time.Second); err != nil {
		_ = h1.Close()
		t.Fatalf("Sleep entry not observed: %v", err)
	}

	// At this point the Run body MUST NOT have executed yet (the handler
	// is suspended on Sleep). Guard against a future SDK regression that
	// accidentally races ahead.
	if got := runCount.Load(); got != 0 {
		t.Fatalf("run body executed before crash: count = %d; want 0", got)
	}

	// Simulate the crash. Close stops the partition runner and the
	// NodeHost without flushing in-flight SDK work, equivalent in effect
	// to a SIGKILL for replay purposes (the Sleep timer state is durable
	// on disk; the in-process session is gone).
	if err := h1.Close(); err != nil {
		t.Fatalf("Close (crash sim): %v", err)
	}

	// Reopen on the same DataDir with the same handler binding.
	h2, err := engine.NewHost(engine.HostConfig{
		NodeID:         1,
		RaftAddr:       raftAddr,
		DataDir:        dataDir,
		RTTMillisecond: 50,
		Handlers:       reg,
	})
	if err != nil {
		t.Fatalf("NewHost (restart): %v", err)
	}
	defer h2.Close()

	if _, err := h2.StartPartition(1); err != nil {
		t.Fatalf("StartPartition (restart): %v", err)
	}
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := h2.AwaitLeader(awaitCtx, 1); err != nil {
		awaitCancel()
		t.Fatalf("AwaitLeader (restart): %v", err)
	}
	awaitCancel()

	// 30s deadline: under heavy CI/parallel-test load dragonboat
	// leader election + timer-fire-then-apply can stretch past the 10s
	// the SDK-level tests use. The wake path is the same; we just need
	// to give it enough wall-clock to land.
	completed := awaitCompleted(t, h2, 1, id, 30*time.Second)
	if string(completed.GetOutput()) != "computed" {
		t.Errorf("post-restart output = %q; want computed", completed.GetOutput())
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("post-restart failure_message = %q; want empty", completed.GetFailureMessage())
	}
	if got := runCount.Load(); got != 1 {
		t.Errorf("run body executions across restart = %d; want exactly 1", got)
	}
}

// TestPhase2_OutgoingCallSurvivesRestart verifies outbox durability
// across a host crash. Handler A invokes handler B via ctx.Call; the
// Host is closed after A's JECall is journaled. After reopen, the
// outbox shuffler re-injects B's InvokeCommand and B runs to Completed.
//
// Scope note: this test asserts the Callee reaches Completed under the
// new leader (which requires both the outbox rebuild and the Invoker
// resume-on-leader-gain path to be wired). The Caller→Callee return
// path — translating the Callee's InvocationCompleted into a
// JECallResult on the Caller's journal — is NOT YET wired in Phase 2,
// so we do not assert that Caller reaches Completed here. That is a
// Phase 2.5 follow-up: extend onCompleted to look up the parent
// invocation_id stamped on the callee's status (proto change required)
// and propose a JournalAppended { JECallResult } against the parent.
func TestPhase2_OutgoingCallSurvivesRestart(t *testing.T) {
	var calleeRuns atomic.Int32

	reg := sdk.NewRegistry()
	if err := reg.Register("Callee", "do", func(_ sdk.Context, in []byte) ([]byte, error) {
		calleeRuns.Add(1)
		return append([]byte("from-callee:"), in...), nil
	}); err != nil {
		t.Fatalf("Register Callee: %v", err)
	}
	if err := reg.Register("Caller", "go", func(c sdk.Context, in []byte) ([]byte, error) {
		out, err := c.Call(sdk.Target{Service: "Callee", Handler: "do"}, in)
		if err != nil {
			return nil, err
		}
		return out, nil
	}); err != nil {
		t.Fatalf("Register Caller: %v", err)
	}

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")
	raftAddr := freeLocalAddr(t)

	h1, err := engine.NewHost(engine.HostConfig{
		NodeID:         1,
		RaftAddr:       raftAddr,
		DataDir:        dataDir,
		RTTMillisecond: 50,
		Handlers:       reg,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	r, err := h1.StartPartition(1)
	if err != nil {
		_ = h1.Close()
		t.Fatalf("StartPartition: %v", err)
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	if err := h1.AwaitLeader(ctx1, 1); err != nil {
		cancel1()
		_ = h1.Close()
		t.Fatalf("AwaitLeader: %v", err)
	}
	cancel1()

	callerID := buildID(1, "caller")
	target := &enginev1.InvocationTarget{ServiceName: "Caller", HandlerName: "go"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = r.Proposer().ProposeIngress(propCtx, "test/caller", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: callerID, Target: target, Input: []byte("hello"),
		}},
	})
	propCancel()
	if err != nil {
		_ = h1.Close()
		t.Fatalf("ProposeIngress: %v", err)
	}

	// Wait for the JECall to be journaled at index 1 (Input=0, Call=1).
	// That marks the moment Caller has suspended waiting on Callee and
	// the outbox row has been appended.
	if err := waitForJournalEntry(h1, callerID, 1, 3*time.Second); err != nil {
		_ = h1.Close()
		t.Fatalf("Call entry not observed: %v", err)
	}

	if err := h1.Close(); err != nil {
		t.Fatalf("Close (crash sim): %v", err)
	}

	h2, err := engine.NewHost(engine.HostConfig{
		NodeID:         1,
		RaftAddr:       raftAddr,
		DataDir:        dataDir,
		RTTMillisecond: 50,
		Handlers:       reg,
	})
	if err != nil {
		t.Fatalf("NewHost (restart): %v", err)
	}
	defer h2.Close()
	if _, err := h2.StartPartition(1); err != nil {
		t.Fatalf("StartPartition (restart): %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	if err := h2.AwaitLeader(ctx2, 1); err != nil {
		cancel2()
		t.Fatalf("AwaitLeader (restart): %v", err)
	}
	cancel2()

	// The Callee id is deterministically derived from (caller_id, call
	// entry index = 1) — see mintCalleeInvocationID in partition.go. We
	// can poll the Callee's status directly without needing the SDK to
	// expose it.
	calleeID := deriveCalleeID(callerID, 1)
	completed := awaitCompleted(t, h2, 1, calleeID, 10*time.Second)
	if got := string(completed.GetOutput()); got != "from-callee:hello" {
		t.Errorf("callee post-restart output = %q; want from-callee:hello", got)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("callee failure_message = %q; want empty", completed.GetFailureMessage())
	}
	// Outbox dedup must prevent the callee from running twice even if
	// the shuffler re-injects on restart. The callee body is the only
	// path that bumps calleeRuns; replays in the SDK consult the
	// journaled JERun and skip the body.
	if got := calleeRuns.Load(); got != 1 {
		t.Errorf("callee executions across restart = %d; want exactly 1 (outbox dedup)", got)
	}
}

// deriveCalleeID mirrors engine.mintCalleeInvocationID: SHA-256 of the
// parent uuid + 4 big-endian bytes of the call entry index, truncated to
// 16 bytes. Kept here as a local helper because the engine function is
// package-private.
func deriveCalleeID(parent *enginev1.InvocationId, entryIdx uint32) *enginev1.InvocationId {
	h := sha256.New()
	h.Write(parent.GetUuid())
	var idxBuf [4]byte
	binary.BigEndian.PutUint32(idxBuf[:], entryIdx)
	h.Write(idxBuf[:])
	sum := h.Sum(nil)
	return &enginev1.InvocationId{
		PartitionKey: parent.GetPartitionKey(),
		Uuid:         sum[:16],
	}
}

// TestPhase2_AwakeableResolvedByIngress wires the awakeable path through
// the real Step 13 ingress: handler mints an awakeable, suspends, an
// external HTTP POST to /awakeable/{id}/resolve resolves it, and the
// handler wakes returning the resolved bytes.
func TestPhase2_AwakeableResolvedByIngress(t *testing.T) {
	// awakeableCh carries the awakeable ID minted by the handler out to
	// the test body, which then calls the ingress resolve endpoint.
	awakeableCh := make(chan string, 1)
	var emitted atomic.Bool

	reg := sdk.NewRegistry()
	if err := reg.Register("Awaiter", "wait", func(c sdk.Context, _ []byte) ([]byte, error) {
		id, fut := c.Awakeable()
		// Only publish the id once across replays. After resolution the
		// session respawns and re-enters this branch; emitting again
		// would block forever on a full unbuffered channel and risk
		// flaking the test.
		if emitted.CompareAndSwap(false, true) {
			select {
			case awakeableCh <- id:
			default:
			}
		}
		v, err := fut.Result()
		if err != nil {
			return nil, err
		}
		return append([]byte("woke:"), v...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	r := h.Partition(1)
	if r == nil {
		t.Fatal("partition 1 missing")
	}

	id := buildID(1, "awaiter")
	target := &enginev1.InvocationTarget{ServiceName: "Awaiter", HandlerName: "wait"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := r.Proposer().ProposeIngress(propCtx, "test/awaiter", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	})
	propCancel()
	if err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	var awakeableID string
	select {
	case awakeableID = <-awakeableCh:
	case <-time.After(5 * time.Second):
		t.Fatal("awakeable id never published by handler")
	}
	if awakeableID == "" {
		t.Fatal("empty awakeable id")
	}

	resolveURL := base + "/awakeable/" + awakeableID + "/resolve"
	resolveBody, _ := json.Marshal(map[string]any{
		"value": base64.StdEncoding.EncodeToString([]byte("payload")),
	})
	// Poll briefly: the JEAwakeable journal write races with the ingress
	// resolve call; on the AwakeableTable.Get miss the server returns
	// NotFound and we retry.
	var (
		resolveResp *http.Response
		resolveRaw  []byte
	)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodPost, resolveURL, bytes.NewReader(resolveBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("resolve POST: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			resolveResp = resp
			resolveRaw = body
			break
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("resolve POST: code=%d body=%s", resp.StatusCode, string(body))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if resolveResp == nil {
		t.Fatalf("awakeable resolve never reached OK (last polled %s)", resolveURL)
	}
	if !bytes.Contains(resolveRaw, []byte(`"resolved":true`)) {
		t.Errorf("resolve response missing resolved:true; got %s", string(resolveRaw))
	}

	completed := awaitCompleted(t, h, 1, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != "woke:payload" {
		t.Errorf("output = %q; want woke:payload", got)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}

// bringUpHostWithIngress is the engine_test copy of the ingress test
// helper: a single-node Host on a temp dir, ingress on ephemeral HTTP
// + gRPC ports, both torn down by t.Cleanup. Kept local so this file
// doesn't depend on ingress_test internals.
func bringUpHostWithIngress(t *testing.T, reg *sdk.Registry) (*engine.Host, *ingress.Runtime) {
	t.Helper()
	dir := t.TempDir()
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:         1,
		RaftAddr:       freeLocalAddr(t),
		DataDir:        filepath.Join(dir, "node1"),
		RTTMillisecond: 50,
		Handlers:       reg,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.StartPartition(1); err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.AwaitLeader(ctx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		HTTPAddr: "127.0.0.1:0",
		GRPCAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return h, rt
}
