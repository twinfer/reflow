//go:build loadtest

package chaos_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
)

// TestChaos_LeaderSIGKILL runs a steady-state workload against a
// 3-node subprocess cluster, SIGKILLs the leader of partition 1
// mid-flight, restarts that node from the same dataDir, and asserts
// every issued invocation reaches a terminal state.
//
// This is the test graceful Host.Close cannot exercise: SIGKILL leaves
// Pebble's WAL un-fsynced, so recovery on restart genuinely replays
// the torn write. The in-process Kill() path runs Host.Close which
// drains the WAL — same observable behavior in the happy path but
// not the same code path.
//
// Invocation:
//
//	go test -tags=loadtest -timeout=10m -run=TestChaos_LeaderSIGKILL \
//	    -v ./internal/chaos/...
func TestChaos_LeaderSIGKILL(t *testing.T) {
	const (
		service       = "loadgen.Hello"
		handler       = "echo"
		rate          = 30.0
		concurrency   = 8
		duration      = 30 * time.Second
		killAfter     = 5 * time.Second
		awaitNewLead  = 60 * time.Second
		awaitTerminal = 120 * time.Second
	)

	bin := loadgen.BuildLoadnodeBinary(t)

	// Pre-allocate ingress addresses; the subprocess cluster needs to
	// know where to dial each child.
	ingressAddrs := []string{
		loadgen.FreeLocalAddr(t),
		loadgen.FreeLocalAddr(t),
		loadgen.FreeLocalAddr(t),
	}

	reg := sdk.NewRegistry()
	if err := reg.Register(service, handler, loadgen.HelloHandler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:                      3,
		Handlers:               reg,
		SubprocessNodes:        true,
		LoadnodeBinaryPath:     bin,
		SubprocessIngressAddrs: ingressAddrs,
	})
	defer cluster.Close()

	sampler := loadgen.NewSampler()
	ctx, cancel := context.WithTimeout(context.Background(), duration+awaitTerminal+90*time.Second)
	defer cancel()

	// SamplePebble would skip subprocess nodes anyway, but start the
	// sampler so the results CSV stays consistent across modes.
	sampleCtx, stopSampling := context.WithCancel(ctx)
	go sampler.SampleEvery(sampleCtx, 1*time.Second, cluster.Nodes, uint64(len(cluster.Nodes)))

	// Find the leader of partition 1 and SIGKILL it `killAfter` into
	// the run. Restart the same dataDir + addrs so the node recovers
	// from the torn WAL.
	killDone := make(chan struct{})
	go func() {
		defer close(killDone)
		select {
		case <-time.After(killAfter):
		case <-ctx.Done():
			return
		}
		idx := findPartitionLeaderIdx(t, cluster, 1)
		if idx < 0 {
			t.Errorf("no leader for partition 1 at kill time")
			return
		}
		t.Logf("chaos: SIGKILL node=%d (leader of partition 1)", idx+1)
		cluster.KillNode(idx)

		// Wait briefly so the OS reaps the child before we re-spawn
		// against the same dataDir.
		time.Sleep(1 * time.Second)

		t.Logf("chaos: restarting node=%d from torn WAL", idx+1)
		if err := cluster.RestartNode(t, idx); err != nil {
			t.Errorf("RestartNode(%d): %v", idx+1, err)
			return
		}
		awaitCtx, cancelA := context.WithTimeout(ctx, awaitNewLead)
		defer cancelA()
		if err := cluster.AwaitAnyPartitionLeader(awaitCtx, 1); err != nil {
			t.Errorf("partition 1 leader never elected after restart: %v", err)
		}
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

	violations := loadgen.AwaitCompletion(ctx, cluster, issued, awaitTerminal)

	summary, err := (loadgen.ResultDir{Path: filepath.Join(t.TempDir(), "results")}).
		WriteAll(stats, sampler, violations)
	if err != nil {
		t.Fatalf("write results: %v", err)
	}
	t.Logf("summary: %s", summary)
	t.Logf("stats: issued=%d completed=%d failed=%d in_flight_end=%d",
		stats.Issued, stats.Completed, stats.Failed, stats.InFlightAtEnd)
	for i, s := range stats.FailedSamples {
		t.Logf("failed sample %d: %s", i+1, s)
	}

	if len(violations) > 0 {
		for _, v := range violations[:min(len(violations), 5)] {
			t.Logf("violation: %s — %s", v.Kind, v.Detail)
		}
		t.Fatalf("%d invariant violation(s) after SIGKILL+restart", len(violations))
	}
	if stats.Issued == 0 {
		t.Fatal("workload issued zero invocations")
	}
}

// findPartitionLeaderIdx returns the index of cluster.Nodes[i] whose
// ListPartitions reports shardID as leader, or -1.
func findPartitionLeaderIdx(t *testing.T, c *loadgen.Cluster, shardID uint64) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, n := range c.Nodes {
		if n == nil {
			continue
		}
		parts, err := n.ListPartitions(ctx)
		if err != nil {
			continue
		}
		for _, p := range parts {
			if p.ShardID == shardID && p.IsLeader {
				return i
			}
		}
	}
	return -1
}
