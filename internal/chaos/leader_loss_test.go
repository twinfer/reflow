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
// AnnounceLeader-on-failover gap below is closed:
//
// After the metadata leader is killed mid-flight, dragonboat
// elects new raft leaders for each partition shard on the
// surviving nodes (visible in the raft logs as "[NNNNN:NNNNN]
// t3 became leader"), but PartitionRunner.onBecomeLeader is
// never invoked on those new leaders. Symptom: the
// "partition: became leader" INFO log fires once during
// initial bring-up and then never again after the kill, while
// dragonboat-level "raft became leader" events do fire on
// the surviving replicas. Because onBecomeLeader never runs,
// Invoker.Start is never called and ResumeNonTerminal is never
// invoked — every Scheduled row whose session lived on the
// killed node is stranded until the test cleanup tears the
// cluster down. Observed loss tracks the in-flight window at
// kill time (~10% at rate=50 RPS, 30s, single kill).
//
// Suspected location: the Leadership.OnRaftLeaderChange →
// runCandidate → propose AnnounceLeader → OnAnnounceLeader
// chain in internal/engine/leadership.go. Either the propose
// never fires, never commits, or commits with the wrong
// (epoch, nodeId) so OnAnnounceLeader's Candidate→Leader
// transition is skipped. Investigation is the next PR's scope.
//
// The pre-Start StartInvocation buffer added in the same
// commit as this docstring update closes a separate, narrower
// race (apply-pump emits ActInvoke between Leadership flipping
// IsLeader=true synchronously and the onBecomeLeader goroutine
// reaching invoker.Start) and is independently correct, but
// does not close this test on its own.
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
