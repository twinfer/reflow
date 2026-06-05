//go:build e2e

package chaos_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/e2e"
	"github.com/twinfer/reflw/internal/loadgen"
	"github.com/twinfer/reflw/pkg/handler"
)

// scenarioConfig parameterizes the standard "workload + chaos + check"
// envelope every under-load chaos test in this directory wraps. Each
// test fills in only the fields that differ (rate, duration, when /
// what to fault) and calls runScenario; the boilerplate
// (cluster, sampler, post-run invariants, results CSV) is shared.
type scenarioConfig struct {
	// withToxiproxy tells the cluster harness to install per-source
	// sidecars. Required for IsolateNode / PartitionLeader scenarios;
	// optional for SIGKILL / RollingRestart.
	withToxiproxy bool

	// workload parameters; sane defaults applied when zero.
	rate         float64
	concurrency  int
	duration     time.Duration
	pollInterval time.Duration

	// awaitTerminal is the post-run window to wait for in-flight
	// invocations to settle before declaring violations.
	awaitTerminal time.Duration

	// chaos is the fault-injection goroutine fired concurrent with the
	// workload, after `chaosAfter` of clean traffic. Receives a fresh
	// ctx bounded by the whole run.
	chaos      func(t *testing.T, c *e2e.ContainerCluster)
	chaosAfter time.Duration
}

const (
	svc        = "e2e.Echo"
	handlerFn  = "echo"
)

// runScenario brings up the cluster + handler, registers, runs the
// workload concurrent with cfg.chaos, then asserts every issued
// invocation reaches a terminal state within awaitTerminal. Result
// CSVs are written under t.TempDir for post-mortem.
func runScenario(t *testing.T, cfg scenarioConfig) {
	t.Helper()
	if cfg.rate == 0 {
		cfg.rate = 30
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = 8
	}
	if cfg.duration == 0 {
		cfg.duration = 30 * time.Second
	}
	if cfg.pollInterval == 0 {
		cfg.pollInterval = 100 * time.Millisecond
	}
	if cfg.awaitTerminal == 0 {
		cfg.awaitTerminal = 120 * time.Second
	}

	cluster := e2e.NewContainerCluster(t, e2e.ContainerClusterOptions{
		N:             3,
		NumShards:     1,
		WithToxiproxy: cfg.withToxiproxy,
	})
	h := e2e.StartHandlerContainer(t, cluster.Net)
	regCtx, regCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer regCancel()
	if err := e2e.RegisterHandler(regCtx, cluster, h); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	sampler := loadgen.NewSampler()
	ctx, cancel := context.WithTimeout(context.Background(),
		cfg.duration+cfg.awaitTerminal+90*time.Second)
	defer cancel()

	// SamplePebble on a containerized cluster is a no-op (the
	// per-node sampler reads Host.Snapshotter().Store(), which only
	// exists for in-process loadgen.InProcessNode). The sampler still
	// records latency, so keep it running for the percentile data.
	sampleCtx, stopSampling := context.WithCancel(ctx)
	nodesIface := containerNodesAsLoadgen(cluster)
	go sampler.SampleEvery(sampleCtx, 1*time.Second, nodesIface, uint64(len(cluster.Nodes)))

	// Fire the chaos on a goroutine after a brief baseline window.
	chaosDone := make(chan struct{})
	go func() {
		defer close(chaosDone)
		if cfg.chaos == nil {
			return
		}
		select {
		case <-time.After(cfg.chaosAfter):
		case <-ctx.Done():
			return
		}
		cfg.chaos(t, cluster)
	}()

	wl := loadgen.WorkloadConfig{
		Cluster:      cluster,
		Partitioner:  cluster.Partitioner,
		Service:      svc,
		Handler:      handlerFn,
		RatePerSec:   cfg.rate,
		Concurrency:  cfg.concurrency,
		Duration:     cfg.duration,
		PollInterval: cfg.pollInterval,
	}
	stats, issued, err := wl.Run(ctx, sampler)
	if err != nil {
		stopSampling()
		t.Fatalf("workload: %v", err)
	}
	stopSampling()
	<-chaosDone

	violations := loadgen.AwaitCompletion(ctx, cluster, issued, cfg.awaitTerminal)

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
		t.Fatalf("%d invariant violation(s)", len(violations))
	}
	if stats.Issued == 0 {
		t.Fatal("workload issued zero invocations")
	}
}

// containerNodesAsLoadgen projects ContainerCluster.Nodes into the
// []loadgen.Node shape SamplePebble consumes. Live nodes only — a
// terminated/killed slot maps to a nil entry the sampler skips.
func containerNodesAsLoadgen(c *e2e.ContainerCluster) []loadgen.Node {
	out := make([]loadgen.Node, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		if n == nil || !n.IsLive() {
			out = append(out, nil)
			continue
		}
		out = append(out, n)
	}
	return out
}

// Compile-time check that the handler.Registry surface is the one
// the harness depends on. (Goes through the handler package without
// using it elsewhere in this file, so the import doesn't go stale.)
var _ = handler.NewRegistry
