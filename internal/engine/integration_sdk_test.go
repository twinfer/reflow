package engine_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/pkg/handler"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// bringUpForSDKTest opens a fresh single-node Host with shard 0 +
// shard 1, starts a pkg/handler hosting reg, and registers its URL
// as a deployment so invocations dispatch to it.
func bringUpForSDKTest(t *testing.T, reg *handler.Registry) (*engine.Host, *engine.PartitionRunner) {
	t.Helper()
	h := singleNodeWithHandlers(t, reg)
	return h, h.Partition(1)
}

// TestSDK_RunReturnsJournaledValue exercises ctx.Run end-to-end:
// fn is invoked once on the live path, the JERunProposal is committed via
// Raft, and the handler's return value flows out as the invocation
// Completed output.
func TestSDK_RunReturnsJournaledValue(t *testing.T) {
	var runCount atomic.Int32

	reg := handler.NewRegistry()
	if err := reg.RegisterService("Runner", "go", func(c handler.Context, _ []byte) ([]byte, error) {
		v, err := c.Run("compute", func(*handler.RunContext) ([]byte, error) {
			runCount.Add(1)
			return []byte("computed"), nil
		})
		if err != nil {
			return nil, err
		}
		return v, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h, r := bringUpForSDKTest(t, reg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id := buildID(1, "run-test")
	target := &enginev1.InvocationTarget{ServiceName: "Runner", HandlerName: "go"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)
	if err := r.Proposer().ProposeIngress(ctx, "test/run", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, DeploymentId: depID,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	c := awaitCompleted(t, h, 1, id, 5*time.Second)
	if string(c.GetOutput()) != "computed" {
		t.Errorf("output = %q; want computed", c.GetOutput())
	}
	if c.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", c.GetFailureMessage())
	}
	// The Run body executed on the live path; subsequent session
	// respawns (e.g. via Sleep) would consult the journal and skip the
	// body. This test only exercises the live path, so we expect exactly
	// one invocation.
	if got := runCount.Load(); got != 1 {
		t.Errorf("run body executions = %d; want 1", got)
	}
}

// TestSDK_SleepResumesAfterTimerFires drives the full
// Suspended → timer fire → respawn-session → fast-replay → Completed
// cycle. Demonstrates that the timer service wakes the invocation and the
// second session run reads the journaled JESleepResult without re-issuing
// the timer.
func TestSDK_SleepResumesAfterTimerFires(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Sleeper", "wait", func(c handler.Context, in []byte) ([]byte, error) {
		if _, err := c.Sleep(80 * time.Millisecond).Result(); err != nil {
			return nil, err
		}
		return append([]byte("woke:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h, r := bringUpForSDKTest(t, reg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id := buildID(1, "sleep-test")
	target := &enginev1.InvocationTarget{ServiceName: "Sleeper", HandlerName: "wait"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)
	if err := r.Proposer().ProposeIngress(ctx, "test/sleep", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("hi"), DeploymentId: depID,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	c := awaitCompleted(t, h, 1, id, 5*time.Second)
	if string(c.GetOutput()) != "woke:hi" {
		t.Errorf("output = %q; want woke:hi", c.GetOutput())
	}
}

// TestSDK_SetStateCompletesOK verifies that ctx.SetState produces a
// committed journal entry without suspending — the handler returns its
// result on the same session run. The state write itself is verified
// transitively (the handler returns a constant; what we really cover here
// is that the SetState path doesn't accidentally suspend on a missing
// completion).
func TestSDK_SetStateCompletesOK(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Stater", "set", func(c handler.Context, in []byte) ([]byte, error) {
		if err := c.SetState("k", in); err != nil {
			return nil, err
		}
		return []byte("ok"), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h, r := bringUpForSDKTest(t, reg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := buildID(1, "set-state")
	target := &enginev1.InvocationTarget{ServiceName: "Stater", HandlerName: "set"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)
	if err := r.Proposer().ProposeIngress(ctx, "test/set-state", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("v"), DeploymentId: depID,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	c := awaitCompleted(t, h, 1, id, 5*time.Second)
	if string(c.GetOutput()) != "ok" {
		t.Errorf("output = %q; want ok", c.GetOutput())
	}
}

// TestSDK_StepBudgetExhausts asserts a handler that runs past its
// per-invocation journal-entry cap completes with a StepBudgetExhausted
// failure rather than running unbounded. Budget=3 (JEInput + 2 SetState
// fit; the 3rd SetState would push to index 4 and is rejected); the
// handler emits a *Failure that propagates out as the invocation's
// terminal failure_message.
func TestSDK_StepBudgetExhausts(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Greedy", "loop", func(c handler.Context, _ []byte) ([]byte, error) {
		for range 10 {
			if err := c.SetState("k", []byte("v")); err != nil {
				return nil, err
			}
		}
		return []byte("never"), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Custom bringup: register the deployment with a tight budget. Reuses
	// singleNodeWithHandlers's plumbing minus the default-budget register.
	h := singleNodeWithoutHandlers(t)
	url := startSDKServer(t, reg)
	registerDeploymentURLWithBudget(t, h, url, 3)
	r := h.Partition(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := buildID(1, "step-budget")
	target := &enginev1.InvocationTarget{ServiceName: "Greedy", HandlerName: "loop"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)
	if err := r.Proposer().ProposeIngress(ctx, "test/step-budget", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, DeploymentId: depID,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	c := awaitCompleted(t, h, 1, id, 10*time.Second)
	if msg := c.GetFailureMessage(); !strings.Contains(msg, "step budget") {
		t.Errorf("failure_message = %q; want 'step budget' substring", msg)
	}
	if got := string(c.GetOutput()); got != "" {
		t.Errorf("output = %q; want empty on step-budget failure", got)
	}
}

// TestSDK_RunFailureSurfacesAsFailure verifies that an error
// returned from the Run body lands in InvocationStatus.Completed with a
// non-empty failure_message, and the handler return path translates the
// failure cleanly to the engine.
func TestSDK_RunFailureSurfacesAsFailure(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Boom", "fail", func(c handler.Context, _ []byte) ([]byte, error) {
		_, err := c.Run("nope", func(*handler.RunContext) ([]byte, error) {
			return nil, handler.NewFailure(7, "kaboom")
		})
		return nil, err
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h, r := bringUpForSDKTest(t, reg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := buildID(1, "boom")
	target := &enginev1.InvocationTarget{ServiceName: "Boom", HandlerName: "fail"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)
	if err := r.Proposer().ProposeIngress(ctx, "test/boom", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, DeploymentId: depID,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	c := awaitCompleted(t, h, 1, id, 5*time.Second)
	if c.GetFailureMessage() == "" {
		t.Errorf("failure_message empty; want non-empty (kaboom)")
	}
	if len(c.GetOutput()) != 0 {
		t.Errorf("output = %q; want empty on failure", c.GetOutput())
	}
}
