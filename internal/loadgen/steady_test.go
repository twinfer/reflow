//go:build loadtest

package loadgen_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
)

// TestLoad_SteadyState exercises a 3-node in-process cluster at
// sustained QPS for a short interval. Invariant: every issued
// invocation reaches Completed (no orphans, no synthesized failures).
//
// Run with:
//
//	go test -tags=loadtest -timeout=10m \
//	    -run=TestLoad_SteadyState -v ./internal/loadgen/...
//
// The summary (counts, latency percentiles, peak L0 file count,
// mean write-amp) is logged with the result-dir path so the
// operator can ingest pebble-stats.csv into a notebook for
// tuning analysis.
func TestLoad_SteadyState(t *testing.T) {
	const (
		service     = "loadgen.Hello"
		handler     = "echo"
		rate        = 50.0
		concurrency = 16
		duration    = 20 * time.Second
		sampleEvery = 1 * time.Second
	)

	reg := sdk.NewRegistry()
	if err := reg.Register(service, handler, loadgen.HelloHandler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:        3,
		Handlers: reg,
	})
	defer cluster.Close()

	sampler := loadgen.NewSampler()

	// Outer budget must comfortably cover the workload duration plus the
	// AwaitCompletion timeout below (120s) plus a small grace margin. If
	// it expires early, AwaitCompletion's final-state lookups derive
	// from a done ctx and surface as bogus "lookup_err=invalid deadline"
	// violations even when the invocation actually reached Completed.
	ctx, cancel := context.WithTimeout(context.Background(), duration+150*time.Second)
	defer cancel()

	// Background Pebble sampling — runs every sampleEvery until ctx
	// is cancelled by the deferred cancel above or by the explicit
	// cancellation after the workload completes.
	sampleCtx, stopSampling := context.WithCancel(ctx)
	go sampler.SampleEvery(sampleCtx, sampleEvery, cluster.Nodes, uint64(len(cluster.Nodes)))

	wl := loadgen.WorkloadConfig{
		Cluster:      cluster,
		Service:      service,
		Handler:      handler,
		RatePerSec:   rate,
		Concurrency:  concurrency,
		Duration:     duration,
		PollInterval: 50 * time.Millisecond,
	}
	stats, issued, err := wl.Run(ctx, sampler)
	if err != nil {
		stopSampling()
		t.Fatalf("workload: %v", err)
	}
	stopSampling()

	violations := loadgen.AwaitCompletion(ctx, cluster, issued, 120*time.Second)

	resultDir := filepath.Join(t.TempDir(), "results")
	summary, err := (loadgen.ResultDir{Path: resultDir}).WriteAll(stats, sampler, violations)
	if err != nil {
		t.Fatalf("write results: %v", err)
	}
	t.Logf("summary: %s", summary)
	t.Logf("stats: issued=%d completed=%d failed=%d in_flight_end=%d",
		stats.Issued, stats.Completed, stats.Failed, stats.InFlightAtEnd)
	for i, s := range stats.FailedSamples {
		t.Logf("failed_sample[%d]: %s", i, s)
	}

	if len(violations) > 0 {
		for i, v := range violations {
			t.Logf("violation[%d]: kind=%s detail=%s", i, v.Kind, v.Detail)
		}
		t.Fatalf("%d invariant violation(s); see %s", len(violations), summary)
	}
	if stats.Issued == 0 {
		t.Fatal("workload issued zero invocations")
	}
	// Tolerate a handful of end-of-run propose cancellations
	// (proposes still in flight when runCtx expired). Anything past
	// ~1% of issued indicates real backpressure or a defect.
	if stats.Failed*100 > stats.Issued {
		t.Errorf("workload reported %d failed proposes out of %d issued (>1%%); samples=%v",
			stats.Failed, stats.Issued, stats.FailedSamples)
	}
}
