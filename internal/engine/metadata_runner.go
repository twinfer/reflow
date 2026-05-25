package engine

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/rebalance"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// MetadataRunner is the per-Host singleton driving shard 0 (the metadata
// Raft group). Mirrors the partition-side PartitionRunner shape but with a
// much smaller surface area: there are no timers, no outbox, and no
// Invoker — shard 0 only stores cluster ownership state.
//
// Responsibilities:
//   - Track leadership via Leadership + raftEventListener (re-used from
//     the partition path).
//   - On leader gain, propose UpdatePartitionTable with the static
//     assignment derived from cfg.Peers, plus a RegisterNode for every
//     peer in the static set. Idempotent re-propose-on-restart is safe;
//     UpdatePartitionTable is a singleton overwrite, RegisterNode upserts.
//   - Expose a Lookup helper so the Host can resolve the partition table
//     via dragonboat SyncRead.
type MetadataRunner struct {
	ShardID uint64

	snapshotter *Snapshotter
	proposer    *RaftProposer
	leadership  *Leadership
	log         *slog.Logger

	// peers is the static cluster snapshot fed in at construction. It
	// drives the bootstrap UpdatePartitionTable + RegisterNode proposals.
	peers []Peer

	// numPartitionShards is the cluster-wide partition shard count (ids
	// 1..S), independent of peer count. Drives buildBootstrapTable.
	numPartitionShards uint64

	// host is the back-reference handed in by Host.StartMetadataShard so
	// the rebalancer can call dragonboat membership APIs + Host helpers
	// (PartitionTable, Membership, nodeHostIDOf).
	host *Host

	mu           sync.Mutex
	leaderCancel context.CancelFunc
	// inflightOnLeader tracks in-flight onBecomeLeader goroutines so
	// onStepDown waits for them before reading r.leaderCancel. Mirrors the
	// guard in PartitionRunner: without the wait, onStepDown can race the
	// `r.leaderCancel = cancel` assignment and leave bootstrap/rebalancer
	// goroutines with a leaderCtx nobody will cancel.
	inflightOnLeader sync.WaitGroup
	// leaderGoroutines tracks the long-running leader-scoped goroutines
	// (bootstrap, rebalancer, lpMover) so onStepDown can wait for them
	// to fully exit before returning. Without this, a goroutine doing a
	// SyncRead against shard 0 can outlive the leader transition and
	// race with the host's Snapshotter teardown on the next start.
	leaderGoroutines sync.WaitGroup
}

// Snapshotter exposes the underlying snapshotter for tests.
func (r *MetadataRunner) Snapshotter() *Snapshotter { return r.snapshotter }

// Proposer returns the metadata shard's RaftProposer.
func (r *MetadataRunner) Proposer() *RaftProposer { return r.proposer }

// Leadership returns the metadata shard's leadership state.
func (r *MetadataRunner) Leadership() *Leadership { return r.leadership }

// IsLeader reports whether this node currently believes it is the
// metadata-shard leader. Advisory: leadership is confirmed only when an
// AnnounceLeader command commits through Raft.
func (r *MetadataRunner) IsLeader() bool { return r.leadership.IsLeader() }

// onBecomeLeader proposes the bootstrap partition table + per-peer
// RegisterNode rows. Runs in its own goroutine off the FSM apply path; safe
// to block on Raft proposals.
func (r *MetadataRunner) onBecomeLeader() {
	r.inflightOnLeader.Add(1)
	defer r.inflightOnLeader.Done()

	r.log.Info("metadata: became leader",
		"shard", r.ShardID, "epoch", r.leadership.LeaderEpoch())

	leaderCtx, cancel := context.WithCancel(context.Background())
	r.mu.Lock()
	if r.leaderCancel != nil {
		r.leaderCancel()
	}
	r.leaderCancel = cancel
	r.mu.Unlock()

	// Each leader-scoped goroutine takes its own short-lived store lease
	// around the section that touches pebble. Snapshotter.Close blocks on
	// outstanding leases, so a clean teardown waits for the bootstrap or
	// rebalancer to finish its current iteration before the pebble.DB is
	// torn down.
	r.leaderGoroutines.Go(func() {
		r.bootstrap(leaderCtx)
	})
	if r.host != nil {
		r.leaderGoroutines.Add(2)
		go func() {
			defer r.leaderGoroutines.Done()
			newMetadataRebalancer(r.host, r).run(leaderCtx)
		}()
		go func() {
			defer r.leaderGoroutines.Done()
			newLPMover(r.host, r).run(leaderCtx)
		}()
		// Autonomous LP rebalancer (PR 5.0). Started only when
		// Mode != "off". Shares leaderCtx + leaderGoroutines so
		// onStepDown's Wait() drains the loop cleanly before the
		// snapshotter teardown. The drain-table notifier is taken from
		// ClusterNotifiers; nil-safe because TableNotifier.Subscribe
		// returns nil on a nil receiver and select-on-nil is a no-op.
		if r.host.cfg.Rebalance.Mode != "" && r.host.cfg.Rebalance.Mode != rebalance.ModeOff {
			r.leaderGoroutines.Go(func() {
				bal := rebalance.New(
					r.host.cfg.Rebalance,
					r.host,
					r.proposer,
					r.host.cfg.ClusterNotifiers.RebalanceDrainTable.Subscribe(),
					r.host.cfg.Metrics,
					r.log,
				)
				bal.Run(leaderCtx)
			})
		}
		// Audit-log retention scrubber. Started only when retention is
		// enabled (RetentionDuration > 0); the goroutine itself short-
		// circuits at the same gate but skipping the spawn keeps the
		// goroutine count honest for tests that count leader goroutines.
		if r.host.cfg.Audit.RetentionDuration > 0 {
			r.leaderGoroutines.Go(func() {
				newAuditGC(r, r.host.cfg.Audit).run(leaderCtx)
			})
		}
	}
}

// onStepDown cancels the leader-scoped context. The bootstrap and
// rebalancer goroutines exit when leaderCtx fires; each holds short
// store leases around its pebble reads, so Snapshotter.Close waits for
// them to drop those leases before tearing the underlying DB down.
func (r *MetadataRunner) onStepDown() {
	// Wait for any in-flight onBecomeLeader to install r.leaderCancel
	// before we capture it; see r.inflightOnLeader doc.
	r.inflightOnLeader.Wait()

	r.log.Info("metadata: stepped down", "shard", r.ShardID)
	r.mu.Lock()
	cancel := r.leaderCancel
	r.leaderCancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	// Wait for the leader-scoped goroutines to actually exit, so any
	// SyncReads they had in flight against shard 0 finish before the
	// next leader gain (or the host's snapshotter teardown) begins.
	r.leaderGoroutines.Wait()
}

// bootstrap proposes the static partition table and a RegisterNode for
// every peer. The static assignment gives every partition shard
// 1..len(peers) to every node (RF=N). The number of partition shards is
// taken from the larger of (a) the assignment we'd write here and (b) the
// already-persisted table.
//
// Re-proposed on every leader gain. UpdatePartitionTable is an idempotent
// singleton overwrite; RegisterNode is an upsert. Slow stale leaders that
// come back online write the same content — harmless. Solo deployments
// (len(peers) == 1) bootstrap a 1-shard table for self so subsequent
// AddNode calls can grow the cluster without restart.
func (r *MetadataRunner) bootstrap(ctx context.Context) {
	for _, p := range r.peers {
		mem := &enginev1.NodeMembership{
			NodeId:     p.NodeID,
			RaftAddr:   p.RaftAddr,
			NodeHostId: p.resolvedNodeHostID(),
			LastSeenMs: time.Now().UnixMilli(),
		}
		cmd := &enginev1.Command{
			Kind: &enginev1.Command_RegisterNode{
				RegisterNode: &enginev1.RegisterNode{Member: mem},
			},
		}
		if err := r.proposer.ProposeSelf(ctx, cmd); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, ErrShardClosed) {
				return
			}
			r.log.Warn("metadata: RegisterNode propose failed",
				"node_id", p.NodeID, "err", err)
		}
	}

	store, release, ok := r.snapshotter.Acquire()
	if !ok {
		return
	}
	existing, _ := (cluster.PartitionTableTable{S: store}).Get()
	release()
	pt := buildBootstrapTable(r.peers, r.numPartitionShards, existing, r.leadership.LeaderEpoch())
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpdatePartitionTable{
			UpdatePartitionTable: &enginev1.UpdatePartitionTable{Table: pt},
		},
	}
	if err := r.proposer.ProposeSelf(ctx, cmd); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, ErrShardClosed) {
			return
		}
		r.log.Warn("metadata: UpdatePartitionTable propose failed", "err", err)
		return
	}

	r.seedLPOwners(ctx, pt)

	r.log.Info("metadata: bootstrap proposals committed",
		"shard", r.ShardID, "partition_count", len(pt.Shards))
}

// seedLPOwners proposes BulkUpsertLPOwners with the consistent-hash
// assignment for all 4096 LPs, but only when the LPOwnersTable revision
// is 0 (table never written). The plan is computed deterministically from
// the bootstrap PartitionTable's shard ids by routing.NewPlanner — every
// metadata leader gets the same answer, so two leaders racing to seed
// produce byte-identical content. Subsequent leader-gain re-runs read
// revision > 0 and skip, preserving any PR 3 transfer commits that have
// since modified individual rows.
//
// The Precondition CAS mechanism treats if_table_revision_eq=0 as "no
// precondition", so idempotency is enforced via the pre-propose revision
// read rather than a CAS gate.
//
// The planner output here MUST match what routing.Partitioner falls back
// to during the warm-up window (NewPartitioner builds the same ring from
// shard ids 1..N). That's how invocations submitted before the routing
// reconciler has installed the snapshot still land on the same shard the
// post-seed table will own.
func (r *MetadataRunner) seedLPOwners(ctx context.Context, pt *enginev1.PartitionTable) {
	shardIDs := make([]uint64, 0, len(pt.GetShards()))
	for id := range pt.GetShards() {
		shardIDs = append(shardIDs, id)
	}
	if len(shardIDs) == 0 {
		return
	}
	store, release, ok := r.snapshotter.Acquire()
	if !ok {
		return
	}
	rev, err := (cluster.RevisionTable{S: store}).Get(cluster.RevisionTableLPOwners)
	release()
	if err != nil {
		r.log.Warn("metadata: load lpowners revision failed; skipping seed", "err", err)
		return
	}
	if rev != 0 {
		return
	}
	planner := routing.NewPlanner(shardIDs)
	if planner == nil {
		return
	}
	plan := planner.PlanAll()
	recs := make([]*enginev1.LPOwnerRecord, 0, len(plan))
	for lp := range keys.LPCount {
		recs = append(recs, &enginev1.LPOwnerRecord{
			Lp:      lp,
			ShardId: plan[lp],
		})
	}
	// Sort by lp so the serialized proto bytes are stable across
	// runs (the map iteration above does not guarantee order). Two
	// leaders racing the seed produce identical content + identical
	// bytes — useful for debug and not strictly required by the FSM.
	sort.Slice(recs, func(i, j int) bool { return recs[i].Lp < recs[j].Lp })
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_BulkUpsertLpOwners{
			BulkUpsertLpOwners: &enginev1.BulkUpsertLPOwners{Records: recs},
		},
	}
	if err := r.proposer.ProposeSelf(ctx, cmd); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, ErrShardClosed) {
			return
		}
		r.log.Warn("metadata: BulkUpsertLPOwners propose failed", "err", err)
		return
	}
	r.log.Info("metadata: lpowners consistent-hash seed committed",
		"shard", r.ShardID, "lp_count", len(recs), "num_partition_shards", len(shardIDs))
}

// buildBootstrapTable produces the PartitionTable the metadata-leader
// bootstrap proposer will send via UpdatePartitionTable. Pure function
// (no I/O, no logging) so the merge-vs-seed logic is unit-testable
// without spinning a real dragonboat.
//
// UpdatePartitionTable is a full overwrite, so the bootstrap proposer
// is responsible for sending the complete desired state every time.
// Both pt.Shards and pt.MetaReplicas obey the same rule: re-use the
// existing on-disk value when present (so a leader-gain re-run
// preserves whatever the rebalance pipeline has done since boot);
// otherwise seed from the static peer set (the fresh-bootstrap path).
//
// The static assignment creates numShards partition shards (ids 1..S),
// each replicated on every peer (RF=N). Shard count is independent of
// peer count — peers sets only the replica set.
func buildBootstrapTable(peers []Peer, numShards uint64, existing *enginev1.PartitionTable, leaderEpoch uint64) *enginev1.PartitionTable {
	pt := &enginev1.PartitionTable{AssignmentEpoch: leaderEpoch}
	replicas := make([]uint64, 0, len(peers))
	for _, p := range peers {
		replicas = append(replicas, p.NodeID)
	}
	if existing != nil && len(existing.GetShards()) > 0 {
		pt.Shards = existing.GetShards()
	} else {
		pt.Shards = make(map[uint64]*enginev1.ReplicaSet, numShards)
		for i := range numShards {
			shardID := i + 1
			// Defensive copy so future mutations don't alias.
			rs := append([]uint64(nil), replicas...)
			pt.Shards[shardID] = &enginev1.ReplicaSet{NodeIds: rs}
		}
	}
	if existing != nil && len(existing.GetMetaReplicas().GetNodeIds()) > 0 {
		pt.MetaReplicas = existing.GetMetaReplicas()
	} else {
		pt.MetaReplicas = &enginev1.ReplicaSet{NodeIds: append([]uint64(nil), replicas...)}
	}
	return pt
}
