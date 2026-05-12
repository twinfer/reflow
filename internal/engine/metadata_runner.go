package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// MetadataRunner is the per-Host singleton driving shard 0 (the metadata
// Raft group). Mirrors the partition-side PartitionRunner shape but with a
// much smaller surface area: there are no timers, no outbox, and no
// Invoker — shard 0 only stores cluster ownership state.
//
// Phase 4.1 responsibilities:
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

	// host is the back-reference handed in by Host.StartMetadataShard so
	// the rebalancer can call dragonboat membership APIs + Host helpers
	// (PartitionTable, Membership, nodeHostIDOf). Phase 4.2.
	host *Host

	mu           sync.Mutex
	leaderCtx    context.Context
	leaderCancel context.CancelFunc
}

// Snapshotter exposes the underlying snapshotter for tests.
func (r *MetadataRunner) Snapshotter() *Snapshotter { return r.snapshotter }

// Proposer returns the metadata shard's RaftProposer.
func (r *MetadataRunner) Proposer() *RaftProposer { return r.proposer }

// Leadership returns the metadata shard's leadership state.
func (r *MetadataRunner) Leadership() *Leadership { return r.leadership }

// IsLeader reports whether this node currently believes it is the
// metadata-shard leader. Advisory; the durable test is OnAnnounceLeader.
func (r *MetadataRunner) IsLeader() bool { return r.leadership.IsLeader() }

// onBecomeLeader proposes the bootstrap partition table + per-peer
// RegisterNode rows. Runs in its own goroutine off the FSM apply path; safe
// to block on Raft proposals.
func (r *MetadataRunner) onBecomeLeader() {
	r.log.Info("metadata: became leader",
		"shard", r.ShardID, "epoch", r.leadership.LeaderEpoch())

	leaderCtx, cancel := context.WithCancel(context.Background())
	r.mu.Lock()
	if r.leaderCancel != nil {
		r.leaderCancel()
	}
	r.leaderCtx = leaderCtx
	r.leaderCancel = cancel
	r.mu.Unlock()

	go r.bootstrap(leaderCtx)
	if r.host != nil {
		// Phase 4.2: spawn the rebalancer + failure-detection ticker
		// once shard 0 bootstrap has been kicked off. Both share the
		// leader-scoped context so step-down tears everything down.
		go newMetadataRebalancer(r.host, r).run(leaderCtx)
	}
}

// onStepDown cancels the leader-scoped context.
func (r *MetadataRunner) onStepDown() {
	r.log.Info("metadata: stepped down", "shard", r.ShardID)
	r.mu.Lock()
	cancel := r.leaderCancel
	r.leaderCtx = nil
	r.leaderCancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// bootstrap proposes the static partition table and a RegisterNode for
// every peer. Phase 4.1 assignment: every partition shard 1..len(peers) is
// hosted by every node (RF=N). The number of partition shards is taken
// from the larger of (a) the assignment we'd write here and (b) the
// already-persisted table — once 4.2 introduces dynamic shard counts we
// will read the count from a config field instead.
//
// Re-proposed on every leader gain. UpdatePartitionTable is an idempotent
// singleton overwrite; RegisterNode is an upsert. Slow stale leaders that
// come back online write the same content — harmless in 4.1.
func (r *MetadataRunner) bootstrap(ctx context.Context) {
	if len(r.peers) == 0 {
		// Single-node deployments don't reach here (StartMetadataShard is
		// only called when Peers is non-empty), but be defensive.
		return
	}

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

	// Phase 4.1 static assignment: one partition shard per peer index
	// (shard ids 1..N), every shard replicated on every peer. The
	// resulting table is identical across the cluster so any leader's
	// proposal yields the same byte sequence.
	pt := &enginev1.PartitionTable{
		Shards:          make(map[uint64]*enginev1.ReplicaSet, len(r.peers)),
		AssignmentEpoch: r.leadership.LeaderEpoch(),
	}
	replicas := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		replicas = append(replicas, p.NodeID)
	}
	for i := range r.peers {
		shardID := uint64(i + 1)
		// Defensive copy so future mutations don't alias.
		rs := append([]uint64(nil), replicas...)
		pt.Shards[shardID] = &enginev1.ReplicaSet{NodeIds: rs}
	}
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
	r.log.Info("metadata: bootstrap proposals committed",
		"shard", r.ShardID, "partition_count", len(pt.Shards))
}
