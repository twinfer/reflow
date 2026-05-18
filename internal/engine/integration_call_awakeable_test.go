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

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/pkg/handler"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestHandlerSurvivesKill verifies kill-9 safety. A handler doing
// SetState → Sleep → Run is brought up, suspended on the Sleep timer,
// the Host is closed (simulating a crash before the Sleep fires), the
// same DataDir is reopened on a fresh Host, and the invocation must
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
func TestHandlerSurvivesKill(t *testing.T) {
	var runCount atomic.Int32

	fn := func(c handler.Context, in []byte) ([]byte, error) {
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
		if _, err := c.Sleep(1500 * time.Millisecond).Result(); err != nil {
			return nil, err
		}
		v, err := c.Run("compute", func(*handler.RunContext) ([]byte, error) {
			runCount.Add(1)
			return []byte("computed"), nil
		})
		if err != nil {
			return nil, err
		}
		return v, nil
	}

	reg := handler.NewRegistry()
	if err := reg.RegisterService("Survivor", "go", fn); err != nil {
		t.Fatalf("Register: %v", err)
	}
	handlerURL := startSDKServer(t, reg)

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")
	raftAddr := freeLocalAddr(t)

	h1 := openSingleNodeOnDir(t, dataDir, raftAddr)
	registerDeploymentURL(t, h1, handlerURL)
	r := h1.Partition(1)

	id := buildID(1, "survivor")
	target := &enginev1.InvocationTarget{ServiceName: "Survivor", HandlerName: "go"}
	depID := resolveDeploymentID(t, h1, target.ServiceName, target.HandlerName)
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := r.Proposer().ProposeIngress(propCtx, "test/survivor", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("input"), DeploymentId: depID,
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
	// on disk; the wire session is gone).
	if err := h1.Close(); err != nil {
		t.Fatalf("Close (crash sim): %v", err)
	}

	// Reopen on the same DataDir. Deployment registration is durable in
	// shard 0; the SDK server URL stayed up across the close.
	h2 := openSingleNodeOnDir(t, dataDir, raftAddr)

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

// TestOutgoingCallSurvivesRestart verifies outbox durability across a
// host crash and the Callee→Caller return path: handler Caller invokes
// handler Callee via ctx.Call; the Host is closed after Caller's JECall
// is journaled. After reopen, the outbox shuffler re-injects Callee's
// InvokeCommand, Callee runs to Completed, and the partition's Completed
// apply arm journals a JECallResult on Caller's journal. Caller's
// suspended session resumes, observes the JECallResult on replay, wraps
// the value, and returns.
//
// This test simultaneously covers:
//   - Outbox durability + dedup: Callee runs exactly once across restart
//     (the calleeRuns counter would exceed 1 if the outbox shuffler
//     re-injected without dedup).
//   - Caller's Completed.Output proves the JECallResult journal entry
//     was delivered and the session woke up.
func TestOutgoingCallSurvivesRestart(t *testing.T) {
	var calleeRuns atomic.Int32

	reg := handler.NewRegistry()
	if err := reg.RegisterService("Callee", "do", func(_ handler.Context, in []byte) ([]byte, error) {
		calleeRuns.Add(1)
		return append([]byte("from-callee:"), in...), nil
	}); err != nil {
		t.Fatalf("Register Callee: %v", err)
	}
	if err := reg.RegisterService("Caller", "go", func(c handler.Context, in []byte) ([]byte, error) {
		out, err := c.Call(handler.Target{Service: "Callee", Handler: "do"}, in).Result()
		if err != nil {
			return nil, err
		}
		// Wrap so the assertion below proves Caller ran past the Call
		// rather than just observing the raw callee output somewhere.
		return append([]byte("caller-wrap:"), out...), nil
	}); err != nil {
		t.Fatalf("Register Caller: %v", err)
	}
	handlerURL := startSDKServer(t, reg)

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")
	raftAddr := freeLocalAddr(t)

	h1 := openSingleNodeOnDir(t, dataDir, raftAddr)
	registerDeploymentURL(t, h1, handlerURL)
	r := h1.Partition(1)

	callerID := buildID(1, "caller")
	target := &enginev1.InvocationTarget{ServiceName: "Caller", HandlerName: "go"}
	depID := resolveDeploymentID(t, h1, target.ServiceName, target.HandlerName)
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := r.Proposer().ProposeIngress(propCtx, "test/caller", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: callerID, Target: target, Input: []byte("hello"), DeploymentId: depID,
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

	h2 := openSingleNodeOnDir(t, dataDir, raftAddr)

	// The Callee id is deterministically derived from (caller_id, call
	// entry index = 1) — see mintCalleeInvocationID in partition.go. We
	// can poll the Callee's status directly without needing the SDK to
	// expose it.
	calleeID := deriveCalleeID(callerID, 1, &enginev1.InvocationTarget{ServiceName: "Callee", HandlerName: "do"})
	calleeDone := awaitCompleted(t, h2, 1, calleeID, 10*time.Second)
	if got := string(calleeDone.GetOutput()); got != "from-callee:hello" {
		t.Errorf("callee post-restart output = %q; want from-callee:hello", got)
	}
	if calleeDone.GetFailureMessage() != "" {
		t.Errorf("callee failure_message = %q; want empty", calleeDone.GetFailureMessage())
	}
	// Outbox dedup must prevent the callee from running twice even if
	// the shuffler re-injects on restart. The callee body is the only
	// path that bumps calleeRuns; replays in the SDK consult the
	// journaled JERun and skip the body.
	if got := calleeRuns.Load(); got != 1 {
		t.Errorf("callee executions across restart = %d; want exactly 1 (outbox dedup)", got)
	}

	// Caller must also reach Completed after the JECallResult is journaled
	// on its side and its session is resumed.
	callerDone := awaitCompleted(t, h2, 1, callerID, 10*time.Second)
	if got := string(callerDone.GetOutput()); got != "caller-wrap:from-callee:hello" {
		t.Errorf("caller post-restart output = %q; want caller-wrap:from-callee:hello", got)
	}
	if callerDone.GetFailureMessage() != "" {
		t.Errorf("caller failure_message = %q; want empty", callerDone.GetFailureMessage())
	}

	// JECallResult must be journaled at call_index+1 = 2 on the caller.
	// (Caller journal: Input=0, Call=1, CallResult=2, Output=3.)
	store := h2.Partition(1).Snapshotter().Store()
	jt := journalTableFor(store)
	entry, err := jt.Read(callerID, 2)
	if err != nil {
		t.Fatalf("caller journal read at idx 2: %v", err)
	}
	cr := entry.GetCallResult()
	if cr == nil {
		t.Fatalf("caller journal idx 2 = %T; want JECallResult", entry.GetEntry())
	}
	if cr.GetCallIndex() != 1 {
		t.Errorf("JECallResult.CallIndex = %d; want 1", cr.GetCallIndex())
	}
	if string(cr.GetResult()) != "from-callee:hello" {
		t.Errorf("JECallResult.Result = %q; want from-callee:hello", cr.GetResult())
	}
}

// TestCallResultDeliveredInline exercises the Callee→Caller
// return path on a single host with no crash: Caller calls Callee,
// Callee returns synchronously, Caller wakes and wraps the value. The
// straight-line happy path proves the apply arm + FSM transition wiring
// before the crash variants stress the resume paths.
func TestCallResultDeliveredInline(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("B", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("pong:"), in...), nil
	}); err != nil {
		t.Fatalf("Register B: %v", err)
	}
	if err := reg.RegisterService("A", "go", func(c handler.Context, in []byte) ([]byte, error) {
		out, err := c.Call(handler.Target{Service: "B", Handler: "echo"}, in).Result()
		if err != nil {
			return nil, err
		}
		return append([]byte("got:"), out...), nil
	}); err != nil {
		t.Fatalf("Register A: %v", err)
	}

	h := singleNodeWithHandlers(t, reg)
	r := h.Partition(1)
	if r == nil {
		t.Fatal("partition 1 missing")
	}

	callerID := buildID(1, "caller25")
	target := &enginev1.InvocationTarget{ServiceName: "A", HandlerName: "go"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)
	propCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := r.Proposer().ProposeIngress(propCtx, "test/caller25", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: callerID, Target: target, Input: []byte("ping"), DeploymentId: depID,
		}},
	}); err != nil {
		cancel()
		t.Fatalf("ProposeIngress: %v", err)
	}
	cancel()

	completed := awaitCompleted(t, h, 1, callerID, 10*time.Second)
	if got := string(completed.GetOutput()); got != "got:pong:ping" {
		t.Errorf("caller output = %q; want got:pong:ping", got)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("caller failure_message = %q; want empty", completed.GetFailureMessage())
	}
}

// TestCallResultSurvivesCallerCrash kills the host while
// Caller is Suspended awaiting Callee's result. After reopen the
// outbox + invoker resume paths spin up Callee, the Completed apply
// arm journals JECallResult on Caller, and Caller resumes to Completed.
func TestCallResultSurvivesCallerCrash(t *testing.T) {
	var calleeRuns atomic.Int32
	reg := handler.NewRegistry()
	if err := reg.RegisterService("B", "do", func(c handler.Context, in []byte) ([]byte, error) {
		// Sleep gives the test window to crash the host before Callee
		// finishes — Callee's status is Invoked (mid-handler) at crash time.
		if _, err := c.Sleep(800 * time.Millisecond).Result(); err != nil {
			return nil, err
		}
		calleeRuns.Add(1)
		return append([]byte("b:"), in...), nil
	}); err != nil {
		t.Fatalf("Register B: %v", err)
	}
	if err := reg.RegisterService("A", "go", func(c handler.Context, in []byte) ([]byte, error) {
		out, err := c.Call(handler.Target{Service: "B", Handler: "do"}, in).Result()
		if err != nil {
			return nil, err
		}
		return append([]byte("a:"), out...), nil
	}); err != nil {
		t.Fatalf("Register A: %v", err)
	}
	handlerURL := startSDKServer(t, reg)

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")
	raftAddr := freeLocalAddr(t)

	h1 := openSingleNodeOnDir(t, dataDir, raftAddr)
	registerDeploymentURL(t, h1, handlerURL)
	r1 := h1.Partition(1)

	callerID := buildID(1, "caller-crash")
	target := &enginev1.InvocationTarget{ServiceName: "A", HandlerName: "go"}
	depID := resolveDeploymentID(t, h1, target.ServiceName, target.HandlerName)
	propCtx, cancelP := context.WithTimeout(context.Background(), 5*time.Second)
	if err := r1.Proposer().ProposeIngress(propCtx, "test/caller-crash", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: callerID, Target: target, Input: []byte("x"), DeploymentId: depID,
		}},
	}); err != nil {
		cancelP()
		_ = h1.Close()
		t.Fatalf("ProposeIngress: %v", err)
	}
	cancelP()

	// Wait until Caller has journaled JECall (index 1), meaning Caller
	// is Suspended awaiting JECallResult and Callee has been invoked
	// (its 800ms Sleep is ticking).
	if err := waitForJournalEntry(h1, callerID, 1, 3*time.Second); err != nil {
		_ = h1.Close()
		t.Fatalf("caller JECall not observed: %v", err)
	}

	if err := h1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	h2 := openSingleNodeOnDir(t, dataDir, raftAddr)

	callerDone := awaitCompleted(t, h2, 1, callerID, 30*time.Second)
	if got := string(callerDone.GetOutput()); got != "a:b:x" {
		t.Errorf("caller post-restart output = %q; want a:b:x", got)
	}
	if got := calleeRuns.Load(); got != 1 {
		t.Errorf("callee executions across crash = %d; want 1", got)
	}
}

// TestCallResultSurvivesCalleeCrash is the symmetric variant:
// the host crashes while Callee is mid-handler (its Sleep is ticking).
// On restart Callee's session re-spawns via ResumeNonTerminal, replays
// the journal deterministically, finishes its Sleep, returns, and the
// JECallResult delivery wakes Caller.
//
// (Note: in single-node mode the "callee crash" and "caller crash"
// scenarios both crash the same host, so structurally the lifecycle is
// the same. The differentiator is *what state each invocation is in*
// when we crash — here we crash later, after Callee has begun and
// before its result lands.)
func TestCallResultSurvivesCalleeCrash(t *testing.T) {
	var calleeRuns atomic.Int32
	reg := handler.NewRegistry()
	if err := reg.RegisterService("B", "do", func(c handler.Context, in []byte) ([]byte, error) {
		if _, err := c.Sleep(1200 * time.Millisecond).Result(); err != nil {
			return nil, err
		}
		calleeRuns.Add(1)
		return append([]byte("b:"), in...), nil
	}); err != nil {
		t.Fatalf("Register B: %v", err)
	}
	if err := reg.RegisterService("A", "go", func(c handler.Context, in []byte) ([]byte, error) {
		out, err := c.Call(handler.Target{Service: "B", Handler: "do"}, in).Result()
		if err != nil {
			return nil, err
		}
		return append([]byte("a:"), out...), nil
	}); err != nil {
		t.Fatalf("Register A: %v", err)
	}
	handlerURL := startSDKServer(t, reg)

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")
	raftAddr := freeLocalAddr(t)

	h1 := openSingleNodeOnDir(t, dataDir, raftAddr)
	registerDeploymentURL(t, h1, handlerURL)
	r1 := h1.Partition(1)

	callerID := buildID(1, "caller-cb")
	target := &enginev1.InvocationTarget{ServiceName: "A", HandlerName: "go"}
	depID := resolveDeploymentID(t, h1, target.ServiceName, target.HandlerName)
	propCtx, cancelP := context.WithTimeout(context.Background(), 5*time.Second)
	if err := r1.Proposer().ProposeIngress(propCtx, "test/caller-cb", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: callerID, Target: target, Input: []byte("y"), DeploymentId: depID,
		}},
	}); err != nil {
		cancelP()
		_ = h1.Close()
		t.Fatalf("ProposeIngress: %v", err)
	}
	cancelP()

	// Wait for Callee's JESleep (idx 1) to be journaled, proving Callee
	// has begun and is suspended on its Sleep. (Callee journal: Input=0,
	// Sleep=1.) Crashing here lands the host with Callee Suspended-on-
	// timer, Caller Suspended-on-call-result.
	calleeID := deriveCalleeID(callerID, 1, &enginev1.InvocationTarget{ServiceName: "B", HandlerName: "do"})
	if err := waitForJournalEntry(h1, calleeID, 1, 3*time.Second); err != nil {
		_ = h1.Close()
		t.Fatalf("callee JESleep not observed: %v", err)
	}

	if err := h1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	h2 := openSingleNodeOnDir(t, dataDir, raftAddr)

	// Wait for Callee first — JECallResult delivery requires Callee to
	// reach Completed. If this never happens, the timer-fired→resume
	// path is the regression, not the parent-delivery path.
	calleeDone := awaitCompleted(t, h2, 1, calleeID, 30*time.Second)
	if got := string(calleeDone.GetOutput()); got != "b:y" {
		t.Errorf("callee post-restart output = %q; want b:y", got)
	}
	callerDone := awaitCompleted(t, h2, 1, callerID, 30*time.Second)
	if got := string(callerDone.GetOutput()); got != "a:b:y" {
		t.Errorf("caller post-restart output = %q; want a:b:y", got)
	}
	if got := calleeRuns.Load(); got != 1 {
		t.Errorf("callee executions across crash = %d; want 1", got)
	}
}

// deriveCalleeID mirrors engine.mintCalleeInvocationID: SHA-256 of the
// parent uuid + 4 big-endian bytes of the call entry index for the
// 16-byte uuid; the PartitionKey is derived from the callee's target
// tuple (service, object_key). Kept here as a local helper because the
// engine function is package-private.
func deriveCalleeID(parent *enginev1.InvocationId, entryIdx uint32, target *enginev1.InvocationTarget) *enginev1.InvocationId {
	h := sha256.New()
	h.Write(parent.GetUuid())
	var idxBuf [4]byte
	binary.BigEndian.PutUint32(idxBuf[:], entryIdx)
	h.Write(idxBuf[:])
	sum := h.Sum(nil)
	return &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()),
		Uuid:         sum[:16],
	}
}

// TestAwakeableResolvedByIngress wires the awakeable path through
// the real Step 13 ingress: handler mints an awakeable, suspends, an
// external HTTP POST to /awakeable/{id}/resolve resolves it, and the
// handler wakes returning the resolved bytes.
func TestAwakeableResolvedByIngress(t *testing.T) {
	// awakeableCh carries the awakeable ID minted by the handler out to
	// the test body, which then calls the ingress resolve endpoint.
	awakeableCh := make(chan string, 1)
	var emitted atomic.Bool

	reg := handler.NewRegistry()
	if err := reg.RegisterService("Awaiter", "wait", func(c handler.Context, _ []byte) ([]byte, error) {
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
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := r.Proposer().ProposeIngress(propCtx, "test/awaiter", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, DeploymentId: depID,
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
