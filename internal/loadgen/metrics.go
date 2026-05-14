package loadgen

import (
	"context"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/cockroachdb/pebble/v2"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/storage"
)

// Sampler captures end-to-end invocation latency and periodic
// snapshots of per-node Pebble metrics during a workload run. All
// methods are goroutine-safe.
type Sampler struct {
	mu        sync.Mutex
	latency   *hdrhistogram.Histogram
	pebbleObs []PebbleObservation
}

// PebbleObservation is a per-node Pebble metrics snapshot taken at a
// single point in time. The harness exposes the raw *pebble.Metrics
// alongside a few precomputed counters for ease of CSV ingestion.
type PebbleObservation struct {
	When      time.Time
	NodeID    uint64
	ShardID   uint64 // 0 for metadata, 1..N for partitions
	L0Files   int
	DiskUsage uint64
	// WriteAmp is the live write-amplification metric Pebble exposes
	// (data written by compactions divided by ingress data). Rough
	// but adequate for tuning comparisons.
	WriteAmp        float64
	BlockCacheHits  int64
	BlockCacheMiss  int64
	CompactionCount int64
	Raw             *pebble.Metrics
}

// NewSampler returns a Sampler with a histogram tuned for sub-second
// latencies: tracks 1µs..30s with 3 significant figures.
func NewSampler() *Sampler {
	return &Sampler{
		latency: hdrhistogram.New(1, int64((30 * time.Second).Microseconds()), 3),
	}
}

// ObserveLatency records one end-to-end invocation latency.
func (s *Sampler) ObserveLatency(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.latency.RecordValue(d.Microseconds())
}

// Latency returns a snapshot copy of the latency histogram so the
// caller can read percentiles after the run.
func (s *Sampler) Latency() *hdrhistogram.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latency.Export()
}

// SamplePebble appends one PebbleObservation per per-shard Pebble DB
// on each node. The on-disk DB paths follow the host.go layout —
// <DataDir>/meta/state for shard 0 and <DataDir>/p{shardID}/state
// for partition shards. The sampler doesn't open new connections;
// it reads metrics from the live DBs through the
// engine.Host.SnapshotterFor accessor.
//
// nodes is the live Cluster.Nodes slice; partitionShards is the
// closed range 1..N. The metadata shard 0 is sampled separately.
func (s *Sampler) SamplePebble(nodes []Node, partitionShards uint64) {
	now := time.Now()
	for _, node := range nodes {
		ip, ok := node.(*InProcessNode)
		if !ok || ip == nil {
			// Subprocess nodes have no local Pebble metrics; skip.
			continue
		}
		nodeID := ip.Host.NodeID()
		for sh := uint64(1); sh <= partitionShards; sh++ {
			pr := ip.Host.Partition(sh)
			if pr == nil {
				continue
			}
			snap := runnerSnapshotter(pr)
			if snap == nil {
				continue
			}
			ps, ok := snap.Store().(*storage.PebbleStore)
			if !ok {
				continue
			}
			s.recordPebble(now, nodeID, sh, ps.Metrics())
		}
	}
}

// SampleEvery runs SamplePebble at every tick of interval until ctx
// is cancelled. Call as a goroutine for background sampling during
// Run.
func (s *Sampler) SampleEvery(ctx context.Context, interval time.Duration, nodes []Node, partitionShards uint64) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s.SamplePebble(nodes, partitionShards)
	}
}

// PebbleObservations returns a copy of the per-tick observations
// recorded so far.
func (s *Sampler) PebbleObservations() []PebbleObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PebbleObservation, len(s.pebbleObs))
	copy(out, s.pebbleObs)
	return out
}

func (s *Sampler) recordPebble(when time.Time, nodeID, shardID uint64, m *pebble.Metrics) {
	if m == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	total := m.Total()
	var writeAmp float64
	if total.TableBytesIn > 0 {
		writeAmp = float64(total.TableBytesCompacted+total.TableBytesFlushed) /
			float64(total.TableBytesIn)
	}
	s.pebbleObs = append(s.pebbleObs, PebbleObservation{
		When:            when,
		NodeID:          nodeID,
		ShardID:         shardID,
		L0Files:         int(m.Levels[0].TablesCount),
		DiskUsage:       m.DiskSpaceUsage(),
		WriteAmp:        writeAmp,
		BlockCacheHits:  m.BlockCache.Hits,
		BlockCacheMiss:  m.BlockCache.Misses,
		CompactionCount: m.Compact.Count,
		Raw:             m,
	})
}

// runnerSnapshotter peeks at the partition runner's snapshotter so
// the harness can reach the underlying *storage.PebbleStore. It
// relies on the existing engine.PartitionRunner accessor surface.
func runnerSnapshotter(pr *engine.PartitionRunner) *engine.Snapshotter {
	if pr == nil {
		return nil
	}
	return pr.Snapshotter()
}
