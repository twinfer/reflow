package engine_test

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// bringUpForSDKTest opens a fresh single-node Host registered with reg and
// awaits leadership on shard 1. Cleanup closes the host on test exit.
func bringUpForSDKTest(t *testing.T, reg *sdk.Registry) (*engine.Host, *engine.PartitionRunner) {
	t.Helper()
	dir := t.TempDir()
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeLocalAddr(t),
		DataDir:            filepath.Join(dir, "node1"),
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		Handlers:           reg,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	r, err := h.StartPartition(1)
	if err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.AwaitLeader(ctx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}
	return h, r
}

// TestSDK_RunReturnsJournaledValue exercises ctx.Run end-to-end:
// fn is invoked once on the live path, the JERunProposal is committed via
// Raft, and the handler's return value flows out as the invocation
// Completed output.
func TestSDK_RunReturnsJournaledValue(t *testing.T) {
	var runCount atomic.Int32

	reg := sdk.NewRegistry()
	if err := reg.Register("Runner", "go", func(c sdk.Context, _ []byte) ([]byte, error) {
		v, err := c.Run("compute", func() ([]byte, error) {
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
	if err := r.Proposer().ProposeIngress(ctx, "test/run", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
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
	reg := sdk.NewRegistry()
	if err := reg.Register("Sleeper", "wait", func(c sdk.Context, in []byte) ([]byte, error) {
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
	if err := r.Proposer().ProposeIngress(ctx, "test/sleep", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("hi"),
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
	reg := sdk.NewRegistry()
	if err := reg.Register("Stater", "set", func(c sdk.Context, in []byte) ([]byte, error) {
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
	if err := r.Proposer().ProposeIngress(ctx, "test/set-state", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target, Input: []byte("v"),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	c := awaitCompleted(t, h, 1, id, 5*time.Second)
	if string(c.GetOutput()) != "ok" {
		t.Errorf("output = %q; want ok", c.GetOutput())
	}
}

// TestSDK_RunFailureSurfacesAsFailure verifies that an error
// returned from the Run body lands in InvocationStatus.Completed with a
// non-empty failure_message, and the handler return path translates the
// failure cleanly to the engine.
func TestSDK_RunFailureSurfacesAsFailure(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.Register("Boom", "fail", func(c sdk.Context, _ []byte) ([]byte, error) {
		_, err := c.Run("nope", func() ([]byte, error) {
			return nil, sdk.NewFailure(7, "kaboom")
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
	if err := r.Proposer().ProposeIngress(ctx, "test/boom", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
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
