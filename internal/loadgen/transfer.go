package loadgen

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// This file holds the LP-transfer-under-load primitives the loadtest
// (transfer_test.go) composes. They mirror what
// internal/engine/integration_lp_transfer_test.go does for a single
// seeded transfer, generalized into a reusable driver that chains
// transfers of populated logical partitions while a workload runs.
//
// The driver is deliberately scoped to LPs the live workload never routes to.
// The loadgen workload confines its keys to LPs below FirstTenantedLP (see
// randomObjectKey); any LP >= FirstTenantedLP is therefore never the target of a
// live invocation, so transferring it cannot misroute traffic. That matters
// because the in-process loadgen host does NOT run the routing reconciler (only
// pkg/reflow.Run does), so Host.Partitioner() routes statically and would not
// follow an LP that flipped owners mid-run.

// FirstTenantedLP is the lowest LP in the region reserved for the transfer
// driver. The loadgen workload confines itself to [0, FirstTenantedLP), so LPs
// in [FirstTenantedLP, keys.LPCount) are safe to chain-transfer under load. 64
// low LPs give the workload ample partition spread while leaving the bulk of the
// LP space free for transfer targets.
const FirstTenantedLP uint32 = 64

// MetadataLeaderHost returns the engine.Host currently leading shard 0,
// or nil when no in-process node leads it. Re-resolve per use: leadership
// can move (it won't under the fault-free loadtest, but the driver stays
// correct if it does).
func MetadataLeaderHost(cluster *Cluster) *engine.Host { return findMetadataLeaderHost(cluster) }

// AwaitLPOwnersSeeded blocks until the metadata leader's LPOwnersTable has
// been seeded (the BulkUpsertLPOwners proposal at metadata-leader gain
// committed) so OwnerOfLP can resolve a current owner. Returns the leader
// host that observed the seed.
func AwaitLPOwnersSeeded(ctx context.Context, cluster *Cluster, timeout time.Duration) (*engine.Host, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		host := MetadataLeaderHost(cluster)
		if host != nil {
			rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			ow, err := host.LPOwners(rctx)
			cancel()
			if err == nil && ow != nil && len(ow.Records) > 0 {
				return host, nil
			}
		}
		if !sleepCtx(ctx, 100*time.Millisecond) {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("loadgen: LPOwners never seeded within %s", timeout)
}

// OwnerOfLP returns the partition shard that currently owns lp per shard
// 0's LPOwnersTable.
func OwnerOfLP(ctx context.Context, host *engine.Host, lp uint32) (uint64, error) {
	owners, err := host.LPOwners(ctx)
	if err != nil {
		return 0, err
	}
	for _, rec := range owners.Records {
		if rec.GetLp() == lp {
			return rec.GetShardId(), nil
		}
	}
	return 0, fmt.Errorf("loadgen: lp %d has no current owner", lp)
}

// SeedLPState writes `rows` state-table rows under logical partition lp on
// the given partition shard, replicated to every in-process node hosting
// the shard. The rows land directly in each replica's Pebble store
// (bypassing Raft) so the source-side SST scanner finds identical data
// wherever the shard's leader election landed — the same strategy
// TestIntegrationLPTransfer_SeededRowsShipViaSST uses, scaled up so the
// shipped SST is large enough to exercise the dest's L0.
//
// Returns the per-replica payload byte estimate (rows * (key + value)) so
// the caller can correlate SST size with the dest's L0 response. State is
// a single LP-prefixed namespace, so buildLPSSTs emits one SST for it;
// real transfers span more namespaces but total byte volume is the L0
// driver this scenario measures.
func SeedLPState(cluster *Cluster, shard uint64, lp uint32, svc string, rows, valueBytes int) (perReplicaBytes int, err error) {
	val := bytes.Repeat([]byte{'x'}, valueBytes)
	for ni, n := range cluster.Nodes {
		ip, ok := n.(*InProcessNode)
		if !ok || ip == nil {
			continue
		}
		pr := ip.Host.Partition(shard)
		if pr == nil {
			return 0, fmt.Errorf("loadgen: SeedLPState: node %d has no runner for shard %d", ni+1, shard)
		}
		batch := pr.Snapshotter().Store().NewBatch()
		for i := range rows {
			key := keys.StateKey(lp, svc, "obj", fmt.Sprintf("k%08d", i))
			if serr := batch.Set(key, val); serr != nil {
				batch.Close()
				return 0, fmt.Errorf("loadgen: SeedLPState: set: %w", serr)
			}
			if ni == 0 {
				perReplicaBytes += len(key) + len(val)
			}
		}
		if cerr := batch.Commit(true); cerr != nil {
			return 0, fmt.Errorf("loadgen: SeedLPState: commit: %w", cerr)
		}
	}
	return perReplicaBytes, nil
}

// InitiateLPTransfer proposes Command_InitiateLPTransfer on shard 0 via the
// metadata leader's proposer — the same command the reflowd cluster
// transfer-lp CLI and the autonomous balancer emit. The lpMover saga on
// the metadata leader drives the phases. destShard MUST differ from the
// LP's current owner (the apply arm rejects a self-transfer).
func InitiateLPTransfer(ctx context.Context, host *engine.Host, transferID string, lp uint32, destShard uint64) error {
	mr := host.MetadataRunner()
	if mr == nil {
		return fmt.Errorf("loadgen: host has no metadata runner")
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_InitiateLpTransfer{
			InitiateLpTransfer: &enginev1.InitiateLPTransfer{
				TransferId: transferID,
				Lp:         lp,
				DestShard:  destShard,
			},
		},
	}
	return mr.Proposer().ProposeSelf(ctx, cmd)
}

// AwaitLPTransferPhase polls shard 0's LPTransfersTable until the named
// transfer reaches want, or the deadline elapses. Returns the last phase
// observed; a non-nil error means want was not reached.
func AwaitLPTransferPhase(ctx context.Context, host *engine.Host, transferID string, want enginev1.LPTransferPhase, deadline time.Duration) (enginev1.LPTransferPhase, error) {
	end := time.Now().Add(deadline)
	last := enginev1.LPTransferPhase_LP_TRANSFER_PHASE_UNSPECIFIED
	for time.Now().Before(end) {
		rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		list, err := host.LPTransfers(rctx)
		cancel()
		if err == nil && list != nil {
			for _, rec := range list.Records {
				if rec.GetTransferId() == transferID {
					last = rec.GetPhase()
					break
				}
			}
		}
		if last == want {
			return last, nil
		}
		if !sleepCtx(ctx, 150*time.Millisecond) {
			return last, ctx.Err()
		}
	}
	return last, fmt.Errorf("loadgen: transfer %s never reached %s; last=%s", transferID, want, last)
}

// ShardPebbleStat is a point-in-time read of one partition shard's Pebble
// state, reduced to the worst case across every in-process replica.
type ShardPebbleStat struct {
	MaxL0Files  int
	MaxWriteAmp float64
}

// ShardPebble samples the live Pebble metrics for shard on every
// in-process node and returns the worst-replica L0 file count and
// write-amp. Used to capture the dest's state immediately after a transfer
// hop's Ingest — the direct "what did ingesting this SST do to L0" signal.
func ShardPebble(nodes []Node, shard uint64) ShardPebbleStat {
	var out ShardPebbleStat
	for _, n := range nodes {
		ip, ok := n.(*InProcessNode)
		if !ok || ip == nil {
			continue
		}
		pr := ip.Host.Partition(shard)
		if pr == nil {
			continue
		}
		ps, ok := pr.Snapshotter().Store().(*storage.PebbleStore)
		if !ok {
			continue
		}
		m := ps.Metrics()
		if m == nil {
			continue
		}
		if l0 := int(m.Levels[0].TablesCount); l0 > out.MaxL0Files {
			out.MaxL0Files = l0
		}
		total := m.Total()
		if total.TableBytesIn > 0 {
			wa := float64(total.TableBytesCompacted+total.TableBytesFlushed) / float64(total.TableBytesIn)
			if wa > out.MaxWriteAmp {
				out.MaxWriteAmp = wa
			}
		}
	}
	return out
}

// LPTransferEvent records one completed (or failed) transfer hop.
type LPTransferEvent struct {
	LP          uint32
	TransferID  string
	SourceShard uint64
	DestShard   uint64
	// SeedBytes is the per-replica payload seeded for this LP (non-zero
	// only on the first hop, which seeds; later hops ship what the prior
	// hop's dest ingested).
	SeedBytes int
	Phase     enginev1.LPTransferPhase
	Elapsed   time.Duration
	// DestL0AfterFiles / DestWriteAmpAfter are the dest shard's
	// worst-replica Pebble state sampled immediately after the hop reached
	// CLEANED. Zero when the hop failed.
	DestL0AfterFiles  int
	DestWriteAmpAfter float64
	Err               error
}

// LPTransferLoadConfig parameterizes RunLPTransferChains.
type LPTransferLoadConfig struct {
	// LPs are the logical partitions to chain-transfer. Use LPs >=
	// FirstTenantedLP so live (band-0) traffic never routes to them.
	LPs []uint32
	// Service labels the seeded state rows.
	Service string
	// SeedRows / SeedValueBytes size each LP's seeded payload (and thus
	// the shipped SST).
	SeedRows       int
	SeedValueBytes int
	// NumShards is the partition shard count — the dest-selection space.
	NumShards uint64
	// HopTimeout bounds how long one hop may take to reach CLEANED.
	HopTimeout time.Duration
}

// RunLPTransferChains drives transfers single-flight, round-robin across
// cfg.LPs, until ctx is cancelled — returning every hop's event. Each LP is
// seeded on its current owner once, then transferred one shard forward per
// round, so its data ping-pongs across shards and each hop drives a fresh
// dest-side Ingest.
//
// Single-flight is deliberate, not a throughput compromise: the lpMover
// flips ownership through one CAS revision on shard 0's LPOwnersTable
// (STAGED → UpsertLpOwner with if_table_revision_eq; a losing CAS aborts
// the transfer). Two transfers reaching the flip concurrently means one
// aborts. Serializing matches how the autonomous rebalancer actuates
// transfers, and keeps every hop's dest L0 reading attributable to exactly
// one Ingest. The concurrent write-load that makes this "under load" comes
// from the workload running alongside, not from overlapping transfers.
func RunLPTransferChains(ctx context.Context, cluster *Cluster, cfg LPTransferLoadConfig) []LPTransferEvent {
	var events []LPTransferEvent
	seeded := make(map[uint32]bool, len(cfg.LPs))
	hops := make(map[uint32]int, len(cfg.LPs))
	for ctx.Err() == nil {
		advanced := false
		for _, lp := range cfg.LPs {
			if ctx.Err() != nil {
				return events
			}
			ev, ran := transferOneHop(ctx, cluster, lp, cfg, seeded, hops)
			if !ran {
				continue
			}
			events = append(events, ev)
			advanced = true
			if ev.Err != nil {
				// Back off on failure (no leader, CAS drift, cancellation)
				// so a transient doesn't spin the round.
				if !sleepCtx(ctx, 300*time.Millisecond) {
					return events
				}
			}
		}
		if !advanced {
			if !sleepCtx(ctx, 200*time.Millisecond) {
				return events
			}
		}
	}
	return events
}

// transferOneHop transfers lp from its current owner to the next shard,
// seeding lp on first use. Returns (event, true) when a hop was attempted;
// (zero, false) when prerequisites aren't ready (no leader / no owner /
// single shard) so the caller retries next round.
func transferOneHop(ctx context.Context, cluster *Cluster, lp uint32, cfg LPTransferLoadConfig, seeded map[uint32]bool, hops map[uint32]int) (LPTransferEvent, bool) {
	host := MetadataLeaderHost(cluster)
	if host == nil {
		return LPTransferEvent{}, false
	}
	ownerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	owner, err := OwnerOfLP(ownerCtx, host, lp)
	cancel()
	if err != nil {
		return LPTransferEvent{}, false
	}
	dest := nextDestShard(owner, cfg.NumShards)
	if dest == owner {
		return LPTransferEvent{}, false // single-shard cluster: nothing to do.
	}

	seedBytes := 0
	if !seeded[lp] {
		b, serr := SeedLPState(cluster, owner, lp, cfg.Service, cfg.SeedRows, cfg.SeedValueBytes)
		if serr != nil {
			return LPTransferEvent{LP: lp, SourceShard: owner, Err: fmt.Errorf("seed: %w", serr)}, true
		}
		seedBytes = b
		seeded[lp] = true
	}

	hop := hops[lp]
	hops[lp] = hop + 1
	transferID := fmt.Sprintf("loadxfer-lp%d-hop%d", lp, hop)
	ev := LPTransferEvent{LP: lp, TransferID: transferID, SourceShard: owner, DestShard: dest, SeedBytes: seedBytes}
	start := time.Now()

	initCtx, icancel := context.WithTimeout(ctx, 10*time.Second)
	ierr := InitiateLPTransfer(initCtx, host, transferID, lp, dest)
	icancel()
	if ierr != nil {
		ev.Err = fmt.Errorf("initiate: %w", ierr)
		ev.Elapsed = time.Since(start)
		return ev, true
	}

	phase, perr := AwaitLPTransferPhase(ctx, host, transferID, enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED, cfg.HopTimeout)
	ev.Phase = phase
	ev.Elapsed = time.Since(start)
	if perr != nil {
		ev.Err = perr
		return ev, true
	}
	// Dest has Ingested the SST and the source has range-deleted — read the
	// dest's Pebble state now, before background compaction drains L0.
	stat := ShardPebble(cluster.Nodes, dest)
	ev.DestL0AfterFiles = stat.MaxL0Files
	ev.DestWriteAmpAfter = stat.MaxWriteAmp
	return ev, true
}

// nextDestShard rotates owner forward to a different shard in [1, numShards].
func nextDestShard(owner, numShards uint64) uint64 {
	if numShards <= 1 {
		return owner
	}
	return owner%numShards + 1
}

// sleepCtx sleeps for d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// WriteTransferCSV writes one row per transfer hop to path. Columns line up
// with the dest L0 / write-amp readings so a notebook can join them against
// pebble-stats.csv on (dest_shard, time).
func WriteTransferCSV(path string, events []LPTransferEvent) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{
		"lp", "transfer_id", "source_shard", "dest_shard", "seed_bytes",
		"phase", "elapsed_ms", "dest_l0_after", "dest_write_amp_after", "err",
	}); err != nil {
		return err
	}
	for _, e := range events {
		errStr := ""
		if e.Err != nil {
			errStr = e.Err.Error()
		}
		if err := w.Write([]string{
			strconv.FormatUint(uint64(e.LP), 10),
			e.TransferID,
			strconv.FormatUint(e.SourceShard, 10),
			strconv.FormatUint(e.DestShard, 10),
			strconv.Itoa(e.SeedBytes),
			e.Phase.String(),
			strconv.FormatInt(e.Elapsed.Milliseconds(), 10),
			strconv.Itoa(e.DestL0AfterFiles),
			strconv.FormatFloat(e.DestWriteAmpAfter, 'f', 3, 64),
			errStr,
		}); err != nil {
			return err
		}
	}
	return nil
}
