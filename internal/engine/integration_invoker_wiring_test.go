package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/pkg/handler"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// awaitCompleted polls SyncRead until the invocation reaches Completed
// or the deadline expires. Used by wiring tests that don't yet have an
// ingress-side response channel.
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
// that exercises the wiring: admin.RegisterDeployment → Invoker → wire
// session → handler → InvokerEffect.Completed → FSM. A pure echo handler
// is registered via pkg/handler + admin.RegisterDeployment, an Invoke
// command is proposed via the partition's ingress proposer, and the
// invocation status is polled until Completed.
//
// Failure modes this guards against:
//   - Deployment registration failing or never landing on shard 0.
//   - PartitionRunner.dispatchActions not routing ActInvoke to the Invoker.
//   - Invoker not started on leader gain (StartInvocation logs a warning
//     and returns).
//   - wireSession.completeTerminal failing to propose Completed (status
//     stays Invoked forever).
func TestInvokerWiringEchoCompletes(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, input []byte) ([]byte, error) {
		return append([]byte("echo:"), input...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h := singleNodeWithHandlers(t, reg)
	r := h.Partition(1)

	id := buildID(1, "echo-test-id")
	target := &enginev1.InvocationTarget{ServiceName: "Echo", HandlerName: "echo"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Proposer().ProposeIngress(ctx, "test/echo", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("hello"), DeploymentId: depID,
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

// TestInvokerWiring_StampsDeploymentID asserts the deployment_id rides
// through the Free→Scheduled→Invoked→Completed transitions intact: the
// InvokeCommand carries it on ingress, the apply arm copies it onto the
// new InvocationStatus, and downstream transitions preserve it.
func TestInvokerWiring_StampsDeploymentID(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Stamper", "go", func(_ handler.Context, _ []byte) ([]byte, error) {
		return []byte("ok"), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h := singleNodeWithHandlers(t, reg)
	r := h.Partition(1)

	id := buildID(1, "stamp-test")
	target := &enginev1.InvocationTarget{ServiceName: "Stamper", HandlerName: "go"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Proposer().ProposeIngress(ctx, "test/stamp", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, DeploymentId: depID,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	_ = awaitCompleted(t, h, 1, id, 5*time.Second)

	// The status row must carry the stamped deployment_id even in the
	// Completed terminal state — this is what makes pinned replays route
	// to the same deployment even after a deployment swap.
	statusCtx, statusCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer statusCancel()
	status, err := h.LookupInvocationStatus(statusCtx, 1, id)
	if err != nil {
		t.Fatalf("LookupInvocationStatus: %v", err)
	}
	if got := status.GetDeploymentId(); got != depID {
		t.Errorf("status deployment_id = %q; want %q", got, depID)
	}
}

// TestInvokerWiringMissingHandlerFailsTerminally verifies that an
// Invoke for a (service, handler) with no registered deployment
// terminates with a non-empty failure_message rather than panicking,
// hanging, or transitioning to an Invoked state without a session.
func TestInvokerWiringMissingHandlerFailsTerminally(t *testing.T) {
	// Register one handler so a deployment exists for the host's auto-
	// seeding path, but the invocation targets a different (service,
	// handler) tuple so the handler lookup misses.
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Real", "go", func(_ handler.Context, _ []byte) ([]byte, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h := singleNodeWithHandlers(t, reg)
	r := h.Partition(1)

	id := buildID(1, "missing-target")
	target := &enginev1.InvocationTarget{ServiceName: "Ghost", HandlerName: "unknown"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Proposer().ProposeIngress(ctx, "test/missing", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	completed := awaitCompleted(t, h, 1, id, 5*time.Second)
	if completed.GetFailureMessage() == "" {
		t.Errorf("failure_message empty; want non-empty (no deployment for Ghost/unknown)")
	}
	if len(completed.GetOutput()) != 0 {
		t.Errorf("output = %q; want empty on failure", completed.GetOutput())
	}
}
