//go:build loadtest

package chaos_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/chaos"
	"github.com/twinfer/reflow/internal/loadgen"
)

// TestChaos_LeaderIsolated runs a steady-state workload, isolates the
// metadata leader from every other peer for `isolateFor` mid-flight,
// then heals. Because dragonboat traffic flows through the bufconn-
// backed transport (internal/loadgen.BufconnTransportFactory), the
// Cut is exact: the survivors keep raft links to each other and
// re-elect among themselves; the isolated node stays online (no
// Host.Close) but cannot reach the majority.
//
// Post-run invariant: every issued invocation reaches a terminal
// state within the AwaitCompletion deadline.
//
// Invocation:
//
//	go test -tags=loadtest -timeout=10m -run=TestChaos_LeaderIsolated \
//	    -v ./internal/chaos/...
func TestChaos_LeaderIsolated(t *testing.T) {
	const (
		service       = "loadgen.Hello"
		handler       = "echo"
		rate          = 50.0
		concurrency   = 16
		duration      = 30 * time.Second
		isolateAfter  = 5 * time.Second
		isolateFor    = 10 * time.Second
		awaitTerminal = 120 * time.Second
	)

	reg := handler.NewRegistry()
	if err := reg.RegisterService(service, handlerName, loadgen.HelloHandler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	hub := loadgen.NewBufconnHub()
	matrix := loadgen.NewPartitionMatrix()
	factory := loadgen.NewBufconnTransportFactory(hub, matrix)

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:                    3,
		RaftTransportFactory: factory,
	})
	defer cluster.Close()
	defer loadgen.StartEmbeddedHandlers(t, cluster, reg)()

	sampler := loadgen.NewSampler()
	ctx, cancel := context.WithTimeout(context.Background(), duration+awaitTerminal+60*time.Second)
	defer cancel()

	sampleCtx, stopSampling := context.WithCancel(ctx)
	go sampler.SampleEvery(sampleCtx, 1*time.Second, cluster.Nodes, uint64(len(cluster.Nodes)))

	// Fire the isolation on a goroutine `isolateAfter` into the run.
	// The leader is offline from the survivors' perspective for the
	// `isolateFor` window, then healed.
	partitionDone := make(chan struct{})
	go func() {
		defer close(partitionDone)
		select {
		case <-time.After(isolateAfter):
		case <-ctx.Done():
			return
		}
		chaos.PartitionLeader(t, cluster, matrix, isolateFor)
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
	<-partitionDone

	violations := loadgen.AwaitCompletion(ctx, cluster, issued, awaitTerminal)

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
		t.Fatalf("%d invariant violation(s) after leader isolation", len(violations))
	}
	if stats.Issued == 0 {
		t.Fatal("workload issued zero invocations")
	}
}
