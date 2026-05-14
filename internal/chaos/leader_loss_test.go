//go:build loadtest

package chaos_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/chaos"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
)

// TestChaos_LeaderLoss runs a steady-state workload, kills the
// metadata leader once mid-flight, and verifies invariants hold:
// every issued invocation reaches Completed within the post-run
// deadline. The killed node stays down for the rest of the run.
//
// KNOWN FAILURE — this test is expected to fail until the
// leader-failover recovery gap below is closed:
//
// Under-load leader kill reproducibly leaves a fraction of
// invocations stuck in InvocationStatus_Scheduled on the new
// leader's partition replicas. The resume scan in
// PartitionRunner.onBecomeLeader / Invoker.ResumeNonTerminal
// (internal/engine/runner.go:174, internal/engine/invoker/invoker.go:248)
// appears to miss Scheduled rows that get applied after the scan
// completes — observed loss is ~5-10% of invocations issued at
// rate=50 RPS over a 30s window with a single mid-flight kill.
//
// The test stays enabled so the harness surfaces the bug loudly
// in every loadtest run; close it on green once the recovery gap
// is fixed.
//
// Invocation:
//
//	go test -tags=loadtest -timeout=10m -run=TestChaos_LeaderLoss \
//	    -v ./internal/chaos/...
func TestChaos_LeaderLoss(t *testing.T) {
	const (
		service     = "loadgen.Hello"
		handler     = "echo"
		rate        = 50.0
		concurrency = 16
		duration    = 30 * time.Second
		killAfter   = 5 * time.Second
	)

	reg := sdk.NewRegistry()
	if err := reg.Register(service, handler, loadgen.HelloHandler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3, Handlers: reg})
	defer cluster.Close()

	sampler := loadgen.NewSampler()
	ctx, cancel := context.WithTimeout(context.Background(), duration+240*time.Second)
	defer cancel()

	sampleCtx, stopSampling := context.WithCancel(ctx)
	go sampler.SampleEvery(sampleCtx, 1*time.Second, cluster.Nodes, uint64(len(cluster.Nodes)))

	// Fire the kill on a goroutine `killAfter` into the run; the
	// workload picks up from the surviving 2-replica majority.
	killDone := make(chan struct{})
	go func() {
		defer close(killDone)
		select {
		case <-time.After(killAfter):
		case <-ctx.Done():
			return
		}
		chaos.LeaderKill(t, cluster, 30*time.Second)
	}()

	wl := loadgen.WorkloadConfig{
		Cluster:      cluster,
		Service:      service,
		Handler:      handler,
		RatePerSec:   rate,
		Concurrency:  concurrency,
		Duration:     duration,
		PollInterval: 100 * time.Millisecond,
	}
	stats, issued, err := wl.Run(ctx, sampler)
	if err != nil {
		stopSampling()
		t.Fatalf("workload: %v", err)
	}
	stopSampling()
	<-killDone

	violations := loadgen.AwaitCompletion(ctx, cluster, issued, 120*time.Second)

	summary, err := (loadgen.ResultDir{Path: filepath.Join(t.TempDir(), "results")}).
		WriteAll(stats, sampler, violations)
	if err != nil {
		t.Fatalf("write results: %v", err)
	}
	t.Logf("summary: %s", summary)
	t.Logf("stats: issued=%d completed=%d failed=%d in_flight_end=%d",
		stats.Issued, stats.Completed, stats.Failed, stats.InFlightAtEnd)

	if len(violations) > 0 {
		for _, v := range violations[:min(len(violations), 5)] {
			t.Logf("violation: %s — %s", v.Kind, v.Detail)
		}
		t.Fatalf("%d invariant violation(s) after leader kill", len(violations))
	}
	if stats.Issued == 0 {
		t.Fatal("workload issued zero invocations")
	}
}
