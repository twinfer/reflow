package engine

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/engine/cluster"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// metadataRebalancer is the metadata-leader's orchestrator for dragonboat
// membership changes and gossip-based failure detection. Owned by
// MetadataRunner; spawned in onBecomeLeader, torn down in onStepDown.
//
// Two leader-scoped goroutines:
//
//   - failureLoop polls dragonboat's gossip NodeHostRegistry every
//     pollInterval. When a peer's NodeHostMeta is unreadable (its
//     memberlist NotifyLeave has fired) for missThreshold consecutive
//     ticks, an EvictNode command is proposed against shard 0.
//
//   - stepLoop reads PartitionTable.pending and drives each entry forward
//     against the local dragonboat NodeHost (SyncRequestAddNonVoting /
//     SyncRequestAddReplica / SyncRequestDeleteReplica). On dragonboat
//     success, a CompleteRebalanceStep is proposed back through shard 0.
//
// Both loops are idempotent: re-running a propose-already-committed step
// is a shard-0 apply-arm no-op, and re-running a dragonboat membership
// change against the current membership returns harmlessly.
//
// Dragonboat's gossip events are not exposed to user code; see the Phase
// 4.2 plan for the rationale behind polling.
type metadataRebalancer struct {
	host    *Host
	runner  *MetadataRunner
	log     *slog.Logger
	nodeID  uint64
	shardID uint64

	pollInterval  time.Duration
	missThreshold int
	stepTimeout   time.Duration

	mu         sync.Mutex
	missCounts map[uint64]int // node_id -> consecutive missed observations
}

// rebalancerDefaults centralize the cadence knobs. Phase 4.2 ships them
// as constants; configurability is a 4.3+ concern.
const (
	defaultRebalancerPollInterval  = 1 * time.Second
	defaultRebalancerMissThreshold = 10
	defaultRebalancerStepTimeout   = 5 * time.Second
)

func newMetadataRebalancer(h *Host, r *MetadataRunner) *metadataRebalancer {
	return &metadataRebalancer{
		host:          h,
		runner:        r,
		log:           h.log,
		nodeID:        h.cfg.NodeID,
		shardID:       r.ShardID,
		pollInterval:  defaultRebalancerPollInterval,
		missThreshold: defaultRebalancerMissThreshold,
		stepTimeout:   defaultRebalancerStepTimeout,
		missCounts:    make(map[uint64]int),
	}
}

// run blocks until ctx is cancelled (leader step-down). Both loops run
// in goroutines; this function returns once both exit.
func (r *metadataRebalancer) run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r.failureLoop(ctx) }()
	go func() { defer wg.Done(); r.stepLoop(ctx) }()
	wg.Wait()
}

func (r *metadataRebalancer) failureLoop(ctx context.Context) {
	t := time.NewTicker(r.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.checkFailures(ctx)
		}
	}
}

// checkFailures polls every membership row's NodeHostMeta via the
// dragonboat gossip registry and accumulates miss-streaks. A peer with
// last_seen_ms == 0 is already evicted and skipped; self is never
// considered (the leader by definition is alive). On threshold, an
// EvictNode is proposed. Counters reset on the first successful
// observation after a miss streak.
func (r *metadataRebalancer) checkFailures(ctx context.Context) {
	readCtx, cancel := context.WithTimeout(ctx, r.stepTimeout)
	defer cancel()
	members, err := r.host.Membership(readCtx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.log.Debug("rebalancer: read membership failed", "err", err)
		}
		return
	}
	reg, ok := r.host.nh.GetNodeHostRegistry()
	if !ok {
		return
	}

	seen := make(map[uint64]struct{}, len(members))
	for _, m := range members {
		seen[m.GetNodeId()] = struct{}{}
		if m.GetNodeId() == r.nodeID {
			continue
		}
		if m.GetLastSeenMs() == 0 {
			// Already evicted; clear any lingering counter.
			r.mu.Lock()
			delete(r.missCounts, m.GetNodeId())
			r.mu.Unlock()
			continue
		}
		nhID := m.GetNodeHostId()
		if nhID == "" {
			continue
		}
		if _, alive := reg.GetMeta(nhID); alive {
			r.mu.Lock()
			delete(r.missCounts, m.GetNodeId())
			r.mu.Unlock()
			continue
		}
		r.mu.Lock()
		r.missCounts[m.GetNodeId()]++
		miss := r.missCounts[m.GetNodeId()]
		r.mu.Unlock()
		if miss < r.missThreshold {
			continue
		}
		r.log.Info("rebalancer: missed gossip threshold; proposing eviction",
			"node_id", m.GetNodeId(), "node_host_id", nhID, "misses", miss)
		r.proposeEvict(ctx, m.GetNodeId())
		r.mu.Lock()
		delete(r.missCounts, m.GetNodeId())
		r.mu.Unlock()
	}
	// Drop counters for nodes that vanished from membership (e.g.
	// concurrent re-registration after a network blip).
	r.mu.Lock()
	for id := range r.missCounts {
		if _, ok := seen[id]; !ok {
			delete(r.missCounts, id)
		}
	}
	r.mu.Unlock()
}

func (r *metadataRebalancer) proposeEvict(ctx context.Context, nodeID uint64) {
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_EvictNode{
			EvictNode: &enginev1.EvictNode{NodeId: nodeID},
		},
	}
	if err := r.runner.proposer.ProposeSelf(ctx, cmd); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrShardClosed) {
			r.log.Warn("rebalancer: EvictNode propose failed",
				"node_id", nodeID, "err", err)
		}
	}
}

func (r *metadataRebalancer) stepLoop(ctx context.Context) {
	t := time.NewTicker(r.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.driveRebalance(ctx)
		}
	}
}

// driveRebalance reads pending steps and dispatches each one. Failures
// are logged and retried on the next tick; the (shard, step_id) primary
// key in PartitionTable.pending is the idempotency token.
func (r *metadataRebalancer) driveRebalance(ctx context.Context) {
	readCtx, cancel := context.WithTimeout(ctx, r.stepTimeout)
	defer cancel()
	pt, err := r.host.PartitionTable(readCtx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.log.Debug("rebalancer: read partition table failed", "err", err)
		}
		return
	}
	if pt == nil {
		return
	}
	for _, step := range pt.GetPending() {
		if ctx.Err() != nil {
			return
		}
		r.runStep(ctx, step)
	}
}

func (r *metadataRebalancer) runStep(ctx context.Context, step *enginev1.RebalanceStep) {
	if r.host.nh == nil {
		return
	}
	if step.GetStepId() == 0 {
		return
	}
	// shard_id=0 is the metadata Raft group itself — dragonboat accepts
	// the same SyncRequestAdd*/Delete* calls for shard 0 as for partitions.
	stepCtx, cancel := context.WithTimeout(ctx, r.stepTimeout)
	defer cancel()

	var membershipErr error
	switch step.GetKind() {
	case enginev1.RebalanceStep_ADD_NON_VOTING:
		nhID := r.resolveNodeHostID(step.GetAddNodeId())
		if nhID == "" {
			r.log.Warn("rebalancer: ADD_NON_VOTING for unknown peer; ignoring",
				"node_id", step.GetAddNodeId())
			return
		}
		membershipErr = r.host.nh.SyncRequestAddNonVoting(
			stepCtx, step.GetShardId(), step.GetAddNodeId(), nhID, 0)
	case enginev1.RebalanceStep_PROMOTE_TO_VOTER:
		nhID := r.resolveNodeHostID(step.GetAddNodeId())
		if nhID == "" {
			r.log.Warn("rebalancer: PROMOTE_TO_VOTER for unknown peer; ignoring",
				"node_id", step.GetAddNodeId())
			return
		}
		membershipErr = r.host.nh.SyncRequestAddReplica(
			stepCtx, step.GetShardId(), step.GetAddNodeId(), nhID, 0)
	case enginev1.RebalanceStep_DELETE_REPLICA:
		membershipErr = r.host.nh.SyncRequestDeleteReplica(
			stepCtx, step.GetShardId(), step.GetRemoveNodeId(), 0)
	default:
		r.log.Warn("rebalancer: unknown step kind; dropping",
			"shard", step.GetShardId(), "step_id", step.GetStepId(),
			"kind", step.GetKind())
		return
	}

	if membershipErr != nil {
		// dragonboat returns context errors on deadline; treat those as
		// transient (next tick retries). For other errors log and move
		// on — the next tick reattempts the same step_id.
		if !errors.Is(membershipErr, context.DeadlineExceeded) &&
			!errors.Is(membershipErr, context.Canceled) {
			r.log.Warn("rebalancer: dragonboat membership change failed",
				"shard", step.GetShardId(), "step_id", step.GetStepId(),
				"kind", step.GetKind(), "err", membershipErr)
		}
		return
	}

	complete := &enginev1.Command{
		Kind: &enginev1.Command_CompleteRebalanceStep{
			CompleteRebalanceStep: &enginev1.CompleteRebalanceStep{
				ShardId: step.GetShardId(),
				StepId:  step.GetStepId(),
			},
		},
	}
	if err := r.runner.proposer.ProposeSelf(ctx, complete); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrShardClosed) {
			r.log.Warn("rebalancer: CompleteRebalanceStep propose failed",
				"shard", step.GetShardId(), "step_id", step.GetStepId(),
				"err", err)
		}
	}
}

// resolveNodeHostID returns the NodeHostID for nodeID, consulting the
// static peer list first and then falling back to the on-disk
// MembershipTable that RegisterNode populates. The fallback lets the
// rebalancer add nodes that joined after bootstrap (the
// `reflow-cluster add-node` workflow).
func (r *metadataRebalancer) resolveNodeHostID(nodeID uint64) string {
	if nhID := r.host.nodeHostIDOf(nodeID); nhID != "" {
		return nhID
	}
	if r.runner == nil || r.runner.snapshotter == nil {
		return ""
	}
	store := r.runner.snapshotter.Store()
	if store == nil {
		return ""
	}
	m, err := (cluster.MembershipTable{S: store}).Get(nodeID)
	if err != nil || m == nil {
		return ""
	}
	return m.GetNodeHostId()
}
