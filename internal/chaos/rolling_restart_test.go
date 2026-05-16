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

// TestChaos_RollingRestart runs a steady-state workload while
// every node is killed and restarted exactly once in sequence.
//
// Invocation:
//
//	go test -tags=loadtest -timeout=15m -run=TestChaos_RollingRestart \
//	    -v ./internal/chaos/...
func TestChaos_RollingRestart(t *testing.T) {
	const (
		service     = "loadgen.Hello"
		handler     = "echo"
		rate        = 30.0
		concurrency = 8
		duration    = 90 * time.Second
		settle      = 3 * time.Second
	)

	reg := sdk.NewRegistry()
	if err := reg.RegisterService(service, handler, loadgen.HelloHandler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer cluster.Close()
	defer loadgen.StartEmbeddedHandlers(t, cluster, reg)()

	sampler := loadgen.NewSampler()
	ctx, cancel := context.WithTimeout(context.Background(), duration+180*time.Second)
	defer cancel()

	sampleCtx, stopSampling := context.WithCancel(ctx)
	go sampler.SampleEvery(sampleCtx, 1*time.Second, cluster.Nodes, uint64(len(cluster.Nodes)))

	// Drive the rolling restart on a goroutine concurrent with the
	// workload. The scenario gates its own settle pauses.
	chaosDone := make(chan struct{})
	go func() {
		defer close(chaosDone)
		// Give the workload a few seconds of clean traffic first so
		// the result includes some pre-chaos baseline samples.
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return
		}
		chaos.RollingRestart(t, cluster, settle)
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
	<-chaosDone

	violations := loadgen.AwaitCompletion(ctx, cluster, issued, 90*time.Second)

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
		t.Fatalf("%d invariant violation(s) after rolling restart", len(violations))
	}
	if stats.Issued == 0 {
		t.Fatal("workload issued zero invocations")
	}
}
