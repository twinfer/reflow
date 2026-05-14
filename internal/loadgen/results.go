package loadgen

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// ResultDir is the harness's output directory. The caller picks the
// path (often t.TempDir() in a test); the writer drops:
//
//	pebble-stats.csv  — one row per per-shard Pebble sample
//	summary.md        — human-readable summary (counts, p50/p99,
//	                    peak L0 file count across all nodes/shards,
//	                    median write-amp)
type ResultDir struct {
	Path string
}

// WriteAll serializes the workload+sampler results into the
// directory. Returns the absolute path of summary.md so the caller
// can log it for the test runner to surface.
func (r ResultDir) WriteAll(stats WorkloadStats, sampler *Sampler, violations []Violation) (string, error) {
	if err := os.MkdirAll(r.Path, 0o755); err != nil {
		return "", err
	}
	if err := r.writePebbleCSV(sampler); err != nil {
		return "", err
	}
	return r.writeSummary(stats, sampler, violations)
}

func (r ResultDir) writePebbleCSV(sampler *Sampler) error {
	if sampler == nil {
		return nil
	}
	f, err := os.Create(filepath.Join(r.Path, "pebble-stats.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{
		"when_unix_ms", "node_id", "shard_id",
		"l0_files", "disk_usage_bytes", "write_amp",
		"block_cache_hits", "block_cache_miss", "compaction_count",
	}); err != nil {
		return err
	}
	for _, obs := range sampler.PebbleObservations() {
		row := []string{
			strconv.FormatInt(obs.When.UnixMilli(), 10),
			strconv.FormatUint(obs.NodeID, 10),
			strconv.FormatUint(obs.ShardID, 10),
			strconv.Itoa(obs.L0Files),
			strconv.FormatUint(obs.DiskUsage, 10),
			strconv.FormatFloat(obs.WriteAmp, 'f', 3, 64),
			strconv.FormatInt(obs.BlockCacheHits, 10),
			strconv.FormatInt(obs.BlockCacheMiss, 10),
			strconv.FormatInt(obs.CompactionCount, 10),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func (r ResultDir) writeSummary(stats WorkloadStats, sampler *Sampler, violations []Violation) (string, error) {
	path := filepath.Join(r.Path, "summary.md")
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	fmt.Fprintf(f, "# Load run summary\n\n")
	fmt.Fprintf(f, "- Issued: %d\n", stats.Issued)
	fmt.Fprintf(f, "- Completed: %d\n", stats.Completed)
	fmt.Fprintf(f, "- Failed: %d\n", stats.Failed)
	fmt.Fprintf(f, "- InFlightAtEnd: %d\n", stats.InFlightAtEnd)
	fmt.Fprintf(f, "- Duration: %s\n\n", stats.Elapsed)

	if sampler != nil {
		snap := sampler.Latency()
		hist := hdrhistogram.Import(snap)
		if hist.TotalCount() > 0 {
			fmt.Fprintf(f, "## Latency (end-to-end, µs)\n\n")
			fmt.Fprintf(f, "- p50:  %d\n", hist.ValueAtQuantile(50))
			fmt.Fprintf(f, "- p90:  %d\n", hist.ValueAtQuantile(90))
			fmt.Fprintf(f, "- p99:  %d\n", hist.ValueAtQuantile(99))
			fmt.Fprintf(f, "- p999: %d\n", hist.ValueAtQuantile(99.9))
			fmt.Fprintf(f, "- max:  %d\n\n", hist.Max())
		}

		obs := sampler.PebbleObservations()
		if len(obs) > 0 {
			peakL0 := 0
			var sumWA float64
			for _, o := range obs {
				if o.L0Files > peakL0 {
					peakL0 = o.L0Files
				}
				sumWA += o.WriteAmp
			}
			fmt.Fprintf(f, "## Pebble\n\n")
			fmt.Fprintf(f, "- samples: %d\n", len(obs))
			fmt.Fprintf(f, "- peak L0 files (any shard, any node): %d\n", peakL0)
			fmt.Fprintf(f, "- mean write-amp across samples: %.3f\n\n", sumWA/float64(len(obs)))
		}
	}

	if len(violations) > 0 {
		fmt.Fprintf(f, "## Invariant violations (%d)\n\n", len(violations))
		for _, v := range violations {
			fmt.Fprintf(f, "- [%s] %s\n", v.Kind, v.Detail)
		}
		fmt.Fprintln(f)
	} else {
		fmt.Fprintf(f, "## Invariants\n\nAll invariants passed.\n\n")
	}
	fmt.Fprintf(f, "_Generated %s_\n", time.Now().Format(time.RFC3339))
	return path, nil
}
