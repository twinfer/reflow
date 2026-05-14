package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// awaitCompleted polls SyncRead until the invocation reaches Completed
// or the deadline expires. Used by Phase 2 wiring tests that don't yet
// have an ingress-side response channel.
func awaitCompleted(t *testing.T, h *engine.Host, shardID uint64, id *enginev1.InvocationId, timeout time.Duration) *enginev1.Completed {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastStatus *enginev1.InvocationStatus
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		s, err := h.LookupInvocationStatus(ctx, shardID, id)
		cancel()
		if err == nil && s != nil {
			lastStatus = s
			if c, ok := s.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				return c.Completed
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("status never reached Completed within %s; last observed = %T", timeout, lastStatus.GetStatus())
	return nil
}

// buildID returns an InvocationId with a 16-byte uuid derived from name
// (left-padded with zeros, truncated if too long).
func buildID(pk uint64, name string) *enginev1.InvocationId {
	b := make([]byte, 16)
	copy(b, name)
	return &enginev1.InvocationId{PartitionKey: pk, Uuid: b}
}

// TestInvokerWiringEchoCompletes is the smallest end-to-end test
// that exercises the Step 12 wiring: HostConfig.Handlers → Invoker →
// session → handler → InvokerEffect.Completed → FSM. A pure echo handler
// is registered, an Invoke command is proposed via the partition's
// ingress proposer, and the invocation status is polled until Completed.
//
// Failure modes this guards against:
//   - HostConfig.Handlers being ignored (registry lookup never finds the
//     handler, session is dropped silently).
//   - PartitionRunner.dispatchActions not routing ActInvoke to the Invoker.
//   - Invoker not started on leader gain (StartInvocation logs a warning
//     and returns).
//   - session.publishOutcome failing to propose Completed (status stays
//     Invoked forever).
func TestInvokerWiringEchoCompletes(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")

	reg := sdk.NewRegistry()
	if err := reg.Register("Echo", "echo", func(_ sdk.Context, input []byte) ([]byte, error) {
		return append([]byte("echo:"), input...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	raftAddr := freeLocalAddr(t)
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            dataDir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		Handlers:           reg,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	r, err := h.StartPartition(1)
	if err != nil {
		t.Fatalf("StartPartition: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.AwaitLeader(ctx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	id := buildID(1, "echo-test-id")
	target := &enginev1.InvocationTarget{ServiceName: "Echo", HandlerName: "echo"}

	if err := r.Proposer().ProposeIngress(ctx, "test/echo", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("hello"),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, h, 1, id, 5*time.Second)
	if string(completed.GetOutput()) != "echo:hello" {
		t.Errorf("output = %q; want echo:hello", completed.GetOutput())
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}

// TestInvokerWiringMissingHandlerStaysScheduled verifies that an
// Invoke whose target is NOT in the registry leaves the invocation
// Scheduled (not Completed, not Invoked). The Invoker logs a warning and
// drops the StartInvocation rather than panicking or transitioning state.
func TestInvokerWiringMissingHandlerStaysScheduled(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")

	raftAddr := freeLocalAddr(t)
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            dataDir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		Handlers:           sdk.NewRegistry(), // empty
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	r, err := h.StartPartition(1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.AwaitLeader(ctx, 1); err != nil {
		t.Fatal(err)
	}

	id := buildID(1, "missing-handler")
	target := &enginev1.InvocationTarget{ServiceName: "Nope", HandlerName: "x"}
	if err := r.Proposer().ProposeIngress(ctx, "test/missing", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// Give the runner a moment to process the action.
	time.Sleep(200 * time.Millisecond)
	status, err := h.LookupInvocationStatus(ctx, 1, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := status.GetStatus().(*enginev1.InvocationStatus_Scheduled); !ok {
		t.Errorf("status = %T; want Scheduled (handler missing)", status.GetStatus())
	}
}
