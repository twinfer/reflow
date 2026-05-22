package clusterctl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/rebalance"
	clusterctlv1 "github.com/twinfer/reflow/proto/clusterctlv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// RebalanceAdvise returns the rebalancer's view of the cluster on this
// peer right now: skew, drained shards, per-shard LP counts, and the
// moves the advisor would propose under hysteresis-free evaluation.
//
// Read-only — any peer can answer. The advisor is evaluated server-
// side off live SyncReads of shard 0, so the response is consistent
// w.r.t. the metadata FSM's commit state but does not reflect the
// in-process Balancer's engaged-bit (which is leader-only and
// transient anyway). Operators get the fresh-view answer.
func (s *Server) RebalanceAdvise(ctx context.Context, _ *connect.Request[clusterctlv1.RebalanceAdviseRequest]) (*connect.Response[clusterctlv1.RebalanceAdviseResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	state, err := s.gatherRebalanceState(callCtx)
	if err != nil {
		return nil, err
	}
	dec := rebalance.Advise(state)
	resp := &clusterctlv1.RebalanceAdviseResponse{
		Mode:          dec.Mode,
		Engaged:       dec.Engaged,
		SkewPct:       dec.SkewPct,
		InFlight:      uint32(dec.InFlight),
		SkippedReason: dec.SkippedReason,
		DrainedShards: append([]uint64(nil), dec.DrainedShards...),
		LpsPerShard:   make(map[uint64]uint32, len(dec.LPsPerShard)),
	}
	for shard, n := range dec.LPsPerShard {
		resp.LpsPerShard[shard] = uint32(n)
	}
	resp.WouldTransfer = make([]*clusterctlv1.RebalanceMove, 0, len(dec.Proposed))
	for _, mv := range dec.Proposed {
		resp.WouldTransfer = append(resp.WouldTransfer, &clusterctlv1.RebalanceMove{
			Lp:        mv.LP,
			FromShard: mv.FromShard,
			ToShard:   mv.ToShard,
		})
	}
	return connect.NewResponse(resp), nil
}

// gatherRebalanceState SyncReads shard 0 for the advisor's input.
// Mirrors the Balancer.snapshot path but uses the server's configured
// knobs and reports state derived from the live tables.
func (s *Server) gatherRebalanceState(ctx context.Context) (rebalance.State, error) {
	pt, err := s.host.PartitionTable(ctx)
	if err != nil {
		return rebalance.State{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read partition table: %w", err))
	}
	active := make([]uint64, 0)
	if pt != nil {
		for id := range pt.GetShards() {
			active = append(active, id)
		}
		slices.Sort(active)
	}

	owners, err := s.host.LPOwners(ctx)
	if err != nil {
		return rebalance.State{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read lp owners: %w", err))
	}
	current := make(map[uint32]uint64, len(owners.Records))
	for _, rec := range owners.Records {
		current[rec.GetLp()] = rec.GetShardId()
	}

	drains, err := s.host.RebalanceDrains(ctx)
	if err != nil {
		return rebalance.State{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read rebalance_drain: %w", err))
	}
	drained := make([]uint64, 0, len(drains.Records))
	for _, rec := range drains.Records {
		drained = append(drained, rec.GetShardId())
	}

	transfers, err := s.host.LPTransfers(ctx)
	if err != nil {
		return rebalance.State{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read lp transfers: %w", err))
	}
	inFlight := 0
	var mostRecent uint64
	for _, rec := range transfers.Records {
		switch rec.GetPhase() {
		case enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED,
			enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTED:
		default:
			inFlight++
		}
		if rec.GetStartedAtMs() > mostRecent {
			mostRecent = rec.GetStartedAtMs()
		}
	}

	mode := s.rebalance.Mode
	if mode == "" {
		mode = rebalance.ModeOff
	}
	return rebalance.State{
		Mode:              mode,
		ActiveShards:      active,
		DrainedShards:     drained,
		CurrentOwners:     current,
		InFlight:          inFlight,
		MostRecentStartMs: mostRecent,
		NowMs:             uint64(time.Now().UnixMilli()),
		// PreviouslyEngaged left false — server-side advice evaluates
		// against the engage threshold without hysteresis carry-over.
		MaxConcurrent:     s.rebalance.MaxConcurrentTransfers,
		MinSecondsBetween: s.rebalance.MinSecondsBetweenTransfers,
		SkewEngagePct:     s.rebalance.SkewEngagePct,
		SkewDisengagePct:  s.rebalance.SkewDisengagePct,
	}, nil
}

// RebalanceDrain adds or removes a partition shard from the
// RebalanceDrainTable. Leader-only. CAS via if_table_revision_eq;
// mismatch returns FailedPrecondition.
func (s *Server) RebalanceDrain(ctx context.Context, req *connect.Request[clusterctlv1.RebalanceDrainRequest]) (*connect.Response[clusterctlv1.RebalanceDrainResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	shardID := req.Msg.GetShardId()
	if shardID == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("clusterctl: shard_id must be a partition shard id (≥ 1)"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	// Sanity-check shard_id against the current partition table.
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read partition table: %w", err))
	}
	if pt == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("clusterctl: partition table not yet bootstrapped"))
	}
	if _, ok := pt.GetShards()[shardID]; !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("clusterctl: shard_id %d is not a known partition shard", shardID))
	}

	cmd := &enginev1.Command{
		Kind: &enginev1.Command_SetRebalanceDrain{
			SetRebalanceDrain: &enginev1.SetRebalanceDrain{
				ShardId: shardID,
				Drain:   req.Msg.GetDrain(),
			},
		},
	}
	var pre *enginev1.Precondition
	if ifRev := req.Msg.GetIfTableRevisionEq(); ifRev != 0 {
		pre = &enginev1.Precondition{IfTableRevisionEq: ifRev}
	}
	val, err := s.runner.Proposer().ProposeSelfCAS(callCtx, cmd, pre)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: propose SetRebalanceDrain: %w", err))
	}
	if val == cluster.ResultValueFailedPrecondition {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("clusterctl: rebalance_drain CAS mismatch (if_table_revision_eq=%d)",
				req.Msg.GetIfTableRevisionEq()))
	}
	// Read the post-apply revision so callers can chain CAS roundtrips.
	rev, err := s.readRebalanceDrainRevision(callCtx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&clusterctlv1.RebalanceDrainResponse{
		TableRevision: rev,
	}), nil
}

// readRebalanceDrainRevision SyncReads the RebalanceDrainTable list
// just to return the post-apply revision; small surface area, no
// new accessor needed.
func (s *Server) readRebalanceDrainRevision(ctx context.Context) (uint64, error) {
	drains, err := s.host.RebalanceDrains(ctx)
	if err != nil {
		return 0, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read rebalance_drain revision: %w", err))
	}
	return drains.TableRevision, nil
}
