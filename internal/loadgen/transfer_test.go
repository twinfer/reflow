//go:build loadtest

package loadgen_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/engine/rebalance"
	"github.com/twinfer/reflw/internal/loadgen"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/pkg/handler"
)

// envInt reads a positive-int scale knob from the environment, falling
// back to def when unset. A malformed or non-positive value fails the
// test loudly rather than silently running at the default.
func envInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("env %s=%q: want a positive int (%v)", key, v, err)
	}
	return n
}

// buildTransferLPs returns n populated high-LPs to chain-transfer, spread across
// the transfer region [transferRegionLP, LPCount) — the LPs the live workload
// never routes to. Region-relative so it stays valid for any LPCount. n == 5 is
// the canonical set (spread with increasing gaps); for any other n the LPs are
// spread evenly. Spreading raises the chance that, as LPs chain around shards
// and accumulate, their key ranges interleave on a shared dest — the condition
// under which a plain Ingest SST lands in L0 instead of sinking to L6.
func buildTransferLPs(n int) []uint32 {
	region := keys.LPCount - loadgen.TransferRegionLP
	if n == 5 {
		// Fractions of the region, clustered low with increasing gaps.
		return []uint32{
			loadgen.TransferRegionLP + 0,
			loadgen.TransferRegionLP + region/32,
			loadgen.TransferRegionLP + region/11,
			loadgen.TransferRegionLP + region/4,
			loadgen.TransferRegionLP + region/2,
		}
	}
	out := make([]uint32, n)
	for i := range out {
		out[i] = loadgen.TransferRegionLP + uint32(i)*region/uint32(n)
	}
	return out
}

// TestLoad_TransferUnderLoad measures what LP-transfer SST Ingest does to a
// destination shard's Pebble L0 / write-amp while that shard is busy
// serving a steady workload. It exists to make the "ship SSTs via
// IngestAndExcise?" decision data-driven: plain pebble.DB.Ingest places an
// SST that overlaps existing sstables into L0, and because LPs interleave
// within each namespace, a transferred LP's SST can overlap the dest's L6
// even when its own LP sub-range is logically empty. IngestAndExcise would
// drop it to L6 instead. This test surfaces whether that L0 pressure is
// real on this workload — if peak dest L0 stays low here, plain Ingest is
// fine and excise is not worth the format-version bump + per-namespace
// excise complexity.
//
// Shape:
//   - 3-node in-process cluster, autonomous rebalancer OFF (we drive the
//     transfers manually).
//   - A steady 50qps workload runs against the reserved low LPs (0..63) the
//     workload confines itself to — as background write-load on every shard.
//   - Concurrently, several populated high-LPs (>= transferRegionLP) are
//     chain-transferred shard-to-shard. Live traffic never routes to those
//     LPs, so the transfers cannot misroute the workload (the in-process
//     host routes statically — it does not run the routing reconciler).
//   - Each hop samples the dest shard's worst-replica L0 + write-amp right
//     after CLEANED.
//
// The L0 / write-amp numbers are REPORTED, not asserted: this is a
// measurement, read off the logged summary + transfers.csv + pebble-stats.csv.
// The test DOES assert that the workload stays correct under transfer load
// (every invocation completes) and that transfers actually ran.
//
// Run with:
//
//	go test -tags=loadtest -timeout=10m \
//	    -run=TestLoad_TransferUnderLoad -v ./internal/loadgen/...
func TestLoad_TransferUnderLoad(t *testing.T) {
	const (
		service     = "loadgen.Hello"
		handlerName = "echo"
		rate        = 50.0
		concurrency = 16
		sampleEvery = 1 * time.Second
	)

	// Scale knobs — env-overridable so this test doubles as a tunable
	// probe for the "ship SSTs via IngestAndExcise?" threshold. Defaults
	// reproduce the committed reference numbers; raise them to push L0:
	//   REFLW_LOADTEST_SEED_ROWS        (default 3000)  rows seeded per LP/replica → per-hop SST size
	//   REFLW_LOADTEST_SEED_VALUE_BYTES (default 512)   value bytes per seeded row
	//   REFLW_LOADTEST_DURATION_SEC     (default 30)    workload + transfer-chain duration
	//   REFLW_LOADTEST_LP_COUNT         (default 5)     number of high-LPs chain-transferred
	//   REFLW_LOADTEST_HOP_TIMEOUT_SEC  (default 60)    per-hop saga timeout (raise for big SSTs)
	seedRows := envInt(t, "REFLW_LOADTEST_SEED_ROWS", 3000)
	seedValueBytes := envInt(t, "REFLW_LOADTEST_SEED_VALUE_BYTES", 512)
	duration := time.Duration(envInt(t, "REFLW_LOADTEST_DURATION_SEC", 30)) * time.Second
	hopTimeout := time.Duration(envInt(t, "REFLW_LOADTEST_HOP_TIMEOUT_SEC", 60)) * time.Second

	// Populated LPs to chain-transfer. All >= transferRegionLP (64) so the
	// workload never routes to them.
	transferLPs := buildTransferLPs(envInt(t, "REFLW_LOADTEST_LP_COUNT", 5))
	t.Logf("scale: seed_rows=%d seed_value_bytes=%d (~%d KiB/hop SST) duration=%s lp_count=%d hop_timeout=%s",
		seedRows, seedValueBytes, seedRows*seedValueBytes/1024, duration, len(transferLPs), hopTimeout)

	reg := handler.NewRegistry()
	if err := reg.RegisterService(service, handlerName, loadgen.HelloHandler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N:         3,
		Rebalance: rebalance.Config{Mode: rebalance.ModeOff},
	})
	defer cluster.Close()
	defer loadgen.StartEmbeddedHandlers(t, cluster, reg)()

	sampler := loadgen.NewSampler()

	ctx, cancel := context.WithTimeout(context.Background(), duration+150*time.Second)
	defer cancel()

	// Transfers need a seeded LPOwnersTable to resolve current owners.
	if _, err := loadgen.AwaitLPOwnersSeeded(ctx, cluster, 30*time.Second); err != nil {
		t.Fatalf("LPOwners never seeded: %v", err)
	}

	// Background Pebble sampling for the whole run (feeds pebble-stats.csv).
	sampleCtx, stopSampling := context.WithCancel(ctx)
	go sampler.SampleEvery(sampleCtx, sampleEvery, cluster.Nodes, uint64(len(cluster.Nodes)))

	// Concurrent transfer chains, stopped when the workload finishes.
	chainCtx, stopChains := context.WithCancel(ctx)
	var events []loadgen.LPTransferEvent
	chainsDone := make(chan struct{})
	go func() {
		defer close(chainsDone)
		events = loadgen.RunLPTransferChains(chainCtx, cluster, loadgen.LPTransferLoadConfig{
			LPs:            transferLPs,
			Service:        "lp-xfer-load",
			SeedRows:       seedRows,
			SeedValueBytes: seedValueBytes,
			NumShards:      uint64(len(cluster.Nodes)),
			HopTimeout:     hopTimeout,
		})
	}()

	wl := loadgen.WorkloadConfig{
		Cluster:      cluster,
		Partitioner:  cluster.Partitioner,
		Service:      service,
		Handler:      handlerName,
		RatePerSec:   rate,
		Concurrency:  concurrency,
		Duration:     duration,
		PollInterval: 50 * time.Millisecond,
	}
	stats, issued, err := wl.Run(ctx, sampler)
	if err != nil {
		stopChains()
		<-chainsDone
		stopSampling()
		t.Fatalf("workload: %v", err)
	}
	stopChains()
	<-chainsDone
	stopSampling()

	violations := loadgen.AwaitCompletion(ctx, cluster, issued, 120*time.Second)

	resultDir := filepath.Join(t.TempDir(), "results")
	summary, werr := (loadgen.ResultDir{Path: resultDir}).WriteAll(stats, sampler, violations)
	if werr != nil {
		t.Fatalf("write results: %v", werr)
	}
	transfersCSV := filepath.Join(resultDir, "transfers.csv")
	if cerr := loadgen.WriteTransferCSV(transfersCSV, events); cerr != nil {
		t.Fatalf("write transfers.csv: %v", cerr)
	}

	// --- Report: transfer-under-load measurement. ---
	type lpAgg struct {
		hops, completed, failed int
		peakDestL0              int
		maxWriteAmp             float64
	}
	byLP := make(map[uint32]*lpAgg)
	var totalHops, totalCompleted, totalFailed, peakDestL0 int
	var maxWriteAmp float64
	for _, e := range events {
		a := byLP[e.LP]
		if a == nil {
			a = &lpAgg{}
			byLP[e.LP] = a
		}
		a.hops++
		totalHops++
		if e.Err != nil {
			a.failed++
			totalFailed++
			continue
		}
		a.completed++
		totalCompleted++
		if e.DestL0AfterFiles > a.peakDestL0 {
			a.peakDestL0 = e.DestL0AfterFiles
		}
		if e.DestL0AfterFiles > peakDestL0 {
			peakDestL0 = e.DestL0AfterFiles
		}
		if e.DestWriteAmpAfter > a.maxWriteAmp {
			a.maxWriteAmp = e.DestWriteAmpAfter
		}
		if e.DestWriteAmpAfter > maxWriteAmp {
			maxWriteAmp = e.DestWriteAmpAfter
		}
	}

	t.Logf("summary: %s", summary)
	if body, readErr := os.ReadFile(summary); readErr == nil {
		t.Logf("--- summary.md ---\n%s", body)
	}
	t.Logf("transfers.csv: %s", transfersCSV)
	t.Logf("workload: issued=%d completed=%d failed=%d in_flight_end=%d",
		stats.Issued, stats.Completed, stats.Failed, stats.InFlightAtEnd)
	t.Logf("transfers: hops=%d completed=%d failed=%d | peak_dest_L0_after_ingest=%d max_dest_write_amp=%.3f",
		totalHops, totalCompleted, totalFailed, peakDestL0, maxWriteAmp)
	for _, lp := range transferLPs {
		if a := byLP[lp]; a != nil {
			t.Logf("  lp=%d hops=%d completed=%d failed=%d peak_dest_L0=%d max_dest_write_amp=%.3f",
				lp, a.hops, a.completed, a.failed, a.peakDestL0, a.maxWriteAmp)
		}
	}
	logged := 0
	for _, e := range events {
		if e.Err == nil || logged >= 8 {
			continue
		}
		t.Logf("  failed hop: lp=%d transfer_id=%s src=%d dest=%d phase=%s err=%v",
			e.LP, e.TransferID, e.SourceShard, e.DestShard, e.Phase, e.Err)
		logged++
	}

	// --- Assertions: the harness ran and the workload stayed correct. ---
	if len(violations) > 0 {
		for i, v := range violations {
			t.Logf("violation[%d]: kind=%s detail=%s", i, v.Kind, v.Detail)
		}
		t.Fatalf("%d invariant violation(s) under transfer load; see %s", len(violations), summary)
	}
	if stats.Issued == 0 {
		t.Fatal("workload issued zero invocations")
	}
	if stats.Failed*100 > stats.Issued {
		t.Errorf("workload reported %d failed proposes out of %d issued (>1%%); samples=%v",
			stats.Failed, stats.Issued, stats.FailedSamples)
	}
	if totalCompleted == 0 {
		t.Fatalf("no LP transfer reached CLEANED; scenario measured nothing (hops=%d failed=%d)",
			totalHops, totalFailed)
	}
	if totalCompleted < len(transferLPs) {
		t.Logf("WARN: only %d/%d LPs completed at least one hop; numbers are thin — consider a longer duration",
			totalCompleted, len(transferLPs))
	}
}
