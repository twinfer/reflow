//go:build e2e

// Package perf_test hosts the containerized perf baseline — the
// e2e-tier counterpart to internal/loadgen/steady_test.go's
// TestLoad_SteadyState. Both tests share the workload driver, the
// sampler, the invariant checker, and the result writer (all of
// `internal/loadgen`); only the cluster bootstrap differs:
//
//	in-proc baseline (loadtest tag)   -> loadgen.NewCluster
//	containerized baseline (e2e tag)  -> e2e.NewContainerCluster
//
// The in-proc tier stays the canonical "did we regress?" smoke for
// dev-machine work because it isn't gated on Docker. This e2e tier
// adds a second baseline that exercises the real reflowd binary,
// real TCP, real Docker network hops, real handler-container mTLS-
// free engine→handler dispatch — numbers will trail in-proc by 2–5×
// on latency because of those hops.
package perf_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/e2e"
	"github.com/twinfer/reflw/internal/loadgen"
)

const (
	// Service / handler are baked into cmd/loadhandler:
	// reg.RegisterService("e2e.Echo", "echo", echo)
	perfService = "e2e.Echo"
	perfHandler = "echo"

	// Workload shape mirrors loadgen.TestLoad_SteadyState (50 qps,
	// concurrency 16, 20s) so the two summaries diff cleanly.
	perfRate         = 50.0
	perfConcurrency  = 16
	perfDuration     = 20 * time.Second
	perfPollInterval = 50 * time.Millisecond

	// AwaitCompletion budget — generous vs. the in-proc baseline
	// (120s there) because container roundtrip latency makes the
	// final-state probe loop spend more wall time per pass. Tuning
	// note: this is the time the harness gives the engine to drain
	// every in-flight invocation after the workload's Duration
	// expires, not the workload duration itself.
	perfAwaitTerminal = 180 * time.Second
)

// TestE2EPerf_SteadyState is the containerized perf baseline. Brings
// up a 3-node reflowd cluster + loadhandler sidecar, drives the same
// 50qps/20s workload as the in-proc loadtest, and writes summary.md
// (counts + latency percentiles) plus an empty pebble-stats.csv
// under t.TempDir.
//
// Pebble stats are intentionally empty: the sampler reads
// engine.Host internals which only exist on *loadgen.InProcessNode,
// so on a containerized cluster SamplePebble is a no-op. Latency
// percentiles, completion counts, and invariant violations are what
// this tier asserts on.
//
// Run with:
//
//	go test -tags=e2e -timeout=10m -count=1 \
//	    -run=TestE2EPerf_SteadyState -v ./internal/e2e/perf/...
//
// Set REFLOW_E2E_LOGS=1 to stream reflowd container logs into t.Logf
// for diagnosing bring-up issues.
func TestE2EPerf_SteadyState(t *testing.T) {
	cluster := e2e.NewContainerCluster(t, e2e.ContainerClusterOptions{
		N:         3,
		NumShards: 1,
	})
	h := e2e.StartHandlerContainer(t, cluster.Net)
	regCtx, regCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer regCancel()
	if err := e2e.RegisterHandler(regCtx, cluster, h); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	sampler := loadgen.NewSampler()

	// Outer budget must cover the workload + AwaitCompletion + a
	// grace margin. If it expires early, AwaitCompletion's final-
	// state probes derive from a done ctx and surface bogus
	// "lookup_err=invalid deadline" violations even when the row
	// actually reached Completed.
	ctx, cancel := context.WithTimeout(context.Background(),
		perfDuration+perfAwaitTerminal+90*time.Second)
	defer cancel()

	wl := loadgen.WorkloadConfig{
		Cluster:      cluster,
		Partitioner:  cluster.Partitioner,
		Service:      perfService,
		Handler:      perfHandler,
		RatePerSec:   perfRate,
		Concurrency:  perfConcurrency,
		Duration:     perfDuration,
		PollInterval: perfPollInterval,
	}
	stats, issued, err := wl.Run(ctx, sampler)
	if err != nil {
		t.Fatalf("workload: %v", err)
	}

	violations := loadgen.AwaitCompletion(ctx, cluster, issued, perfAwaitTerminal)

	resultDir := filepath.Join(t.TempDir(), "results")
	summary, err := (loadgen.ResultDir{Path: resultDir}).WriteAll(stats, sampler, violations)
	if err != nil {
		t.Fatalf("write results: %v", err)
	}
	t.Logf("summary: %s", summary)
	if body, readErr := os.ReadFile(summary); readErr == nil {
		t.Logf("--- summary.md ---\n%s", body)
	}
	t.Logf("stats: issued=%d completed=%d failed=%d in_flight_end=%d",
		stats.Issued, stats.Completed, stats.Failed, stats.InFlightAtEnd)
	for i, s := range stats.FailedSamples {
		t.Logf("failed_sample[%d]: %s", i, s)
	}

	if len(violations) > 0 {
		for i, v := range violations {
			if i >= 5 {
				t.Logf("... and %d more violations elided", len(violations)-i)
				break
			}
			t.Logf("violation[%d]: kind=%s detail=%s", i, v.Kind, v.Detail)
		}
		t.Fatalf("%d invariant violation(s); see %s", len(violations), summary)
	}
	if stats.Issued == 0 {
		t.Fatal("workload issued zero invocations")
	}
	// Same tolerance as the in-proc baseline: end-of-run propose
	// cancellations (proposes still in flight when runCtx expired)
	// are expected. Anything past ~1% of issued indicates real
	// backpressure or a defect — and bears looking at given the
	// containerized tier already absorbs more end-to-end jitter.
	if stats.Failed*100 > stats.Issued {
		t.Errorf("workload reported %d failed proposes out of %d issued (>1%%); samples=%v",
			stats.Failed, stats.Issued, stats.FailedSamples)
	}
}
