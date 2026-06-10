package admin

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	connect "connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/twinfer/reflw/internal/apimap"
	"github.com/twinfer/reflw/internal/auth"
	"github.com/twinfer/reflw/internal/engine/cluster"
	"github.com/twinfer/reflw/internal/engine/rebalance"
	"github.com/twinfer/reflw/internal/engine/snapshot"
	"github.com/twinfer/reflw/internal/storage/keys"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	apiv1 "github.com/twinfer/reflw/proto/apiv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// AddNode registers a new peer and schedules a PROMOTE_TO_VOTER step for every
// existing partition shard.
func (s *Server) AddNode(ctx context.Context, req *connect.Request[adminv1.AddNodeRequest]) (*connect.Response[adminv1.AddNodeResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	out, err := s.addNodeInternal(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(out), nil
}

// SelfJoin is AddNode initiated by the joiner itself. Authorization requires the
// caller's principal to be node/<req.node_id> (defense in depth behind the Cedar
// policy that already gates this method to node/* principals).
func (s *Server) SelfJoin(ctx context.Context, req *connect.Request[adminv1.AddNodeRequest]) (*connect.Response[adminv1.AddNodeResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if err := checkSelfJoinPrincipal(ctx, req.Msg.GetNodeId()); err != nil {
		return nil, err
	}
	out, err := s.addNodeInternal(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(out), nil
}

// checkSelfJoinPrincipal enforces the principal-equals-NodeID gate for SelfJoin.
// Extracted so it's unit-testable without standing up an engine.Host.
func checkSelfJoinPrincipal(ctx context.Context, nodeID uint64) error {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal.Kind != "node" {
		return connect.NewError(connect.CodePermissionDenied,
			errors.New("admin: SelfJoin requires a node-kind principal"))
	}
	if principal.Subject != strconv.FormatUint(nodeID, 10) {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("admin: SelfJoin principal node/%s does not match req.node_id=%d",
				principal.Subject, nodeID))
	}
	return nil
}

// addNodeInternal contains the FSM-driving body shared by AddNode and SelfJoin.
func (s *Server) addNodeInternal(ctx context.Context, req *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
	if req.GetNodeId() == 0 || req.GetRaftAddr() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: node_id and raft_addr are required"))
	}
	mem := &enginev1.NodeMembership{
		NodeId:     req.GetNodeId(),
		RaftAddr:   req.GetRaftAddr(),
		NodeHostId: req.GetNodeHostId(),
		LastSeenMs: time.Now().UnixMilli(),
	}
	regCmd := &enginev1.Command{
		Kind: &enginev1.Command_RegisterNode{
			RegisterNode: &enginev1.RegisterNode{Member: mem},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.runner.Proposer().ProposeSelf(callCtx, regCmd); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: propose RegisterNode: %w", err))
	}

	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read partition table: %w", err))
	}
	if pt == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("admin: partition table not yet bootstrapped"))
	}
	addShard := func(shardID uint64, rs *enginev1.ReplicaSet) error {
		if replicaSetContainsID(rs.GetNodeIds(), req.GetNodeId()) {
			return nil
		}
		step := &enginev1.RebalanceStep{
			ShardId:   shardID,
			Kind:      enginev1.RebalanceStep_PROMOTE_TO_VOTER,
			AddNodeId: req.GetNodeId(),
			StepId:    nextStepIDForShard(pt.GetPending(), shardID),
		}
		beginCmd := &enginev1.Command{
			Kind: &enginev1.Command_BeginRebalanceStep{
				BeginRebalanceStep: &enginev1.BeginRebalanceStep{Step: step},
			},
		}
		if err := s.runner.Proposer().ProposeSelf(callCtx, beginCmd); err != nil {
			return connect.NewError(connect.CodeInternal,
				fmt.Errorf("admin: propose BeginRebalanceStep shard=%d: %w", shardID, err))
		}
		return nil
	}
	if err := addShard(0, pt.GetMetaReplicas()); err != nil {
		return nil, err
	}
	for shardID, rs := range pt.GetShards() {
		if err := addShard(shardID, rs); err != nil {
			return nil, err
		}
	}
	pt2, err := s.host.PartitionTable(callCtx)
	if err != nil || pt2 == nil {
		return &adminv1.AddNodeResponse{}, nil
	}
	return &adminv1.AddNodeResponse{AssignmentEpoch: pt2.GetAssignmentEpoch()}, nil
}

// RemoveNode proposes EvictNode; the apply arm + rebalancer drive the rest.
func (s *Server) RemoveNode(ctx context.Context, req *connect.Request[adminv1.RemoveNodeRequest]) (*connect.Response[adminv1.RemoveNodeResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.Msg.GetNodeId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: node_id required"))
	}
	if req.Msg.GetNodeId() == s.host.NodeID() {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("admin: refusing to remove self; redirect to another node first"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_EvictNode{
			EvictNode: &enginev1.EvictNode{NodeId: req.Msg.GetNodeId()},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.runner.Proposer().ProposeSelf(callCtx, cmd); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: propose EvictNode: %w", err))
	}
	pt, _ := s.host.PartitionTable(callCtx)
	var epoch uint64
	if pt != nil {
		epoch = pt.GetAssignmentEpoch()
	}
	return connect.NewResponse(&adminv1.RemoveNodeResponse{AssignmentEpoch: epoch}), nil
}

// ListNodes returns the current Membership rows (as views).
func (s *Server) ListNodes(ctx context.Context, _ *connect.Request[adminv1.ListNodesRequest]) (*connect.Response[adminv1.ListNodesResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	members, err := s.host.Membership(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read membership: %w", err))
	}
	return connect.NewResponse(&adminv1.ListNodesResponse{Nodes: apimap.NodeViews(members)}), nil
}

// ListPartitions returns the current PartitionTable (as a view).
func (s *Server) ListPartitions(ctx context.Context, _ *connect.Request[adminv1.ListPartitionsRequest]) (*connect.Response[adminv1.ListPartitionsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read partition table: %w", err))
	}
	return connect.NewResponse(&adminv1.ListPartitionsResponse{Table: apimap.PartitionTableView(pt)}), nil
}

// NodeLeadership returns this node's live runtime view of the shards it hosts and
// whether it currently leads each — read from Host.Partitions(), so it reflects
// an in-progress election the shard-0 assignment table cannot. Not leader-gated.
func (s *Server) NodeLeadership(_ context.Context, _ *connect.Request[adminv1.NodeLeadershipRequest]) (*connect.Response[adminv1.NodeLeadershipResponse], error) {
	parts := s.host.Partitions()
	out := make([]*apiv1.PartitionLeadershipView, 0, len(parts))
	for shardID, runner := range parts {
		out = append(out, &apiv1.PartitionLeadershipView{
			ShardId:     shardID,
			IsLeader:    runner.Leadership().IsLeader(),
			LeaderEpoch: runner.Leadership().LeaderEpoch(),
		})
	}
	slices.SortFunc(out, func(a, b *apiv1.PartitionLeadershipView) int {
		return cmp.Compare(a.GetShardId(), b.GetShardId())
	})
	return connect.NewResponse(&adminv1.NodeLeadershipResponse{Partitions: out}), nil
}

// CreateSnapshot delegates to snapshot.SnapshotOnce. Leader-only.
func (s *Server) CreateSnapshot(ctx context.Context, req *connect.Request[adminv1.CreateSnapshotRequest]) (*connect.Response[adminv1.CreateSnapshotResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if s.repo == nil || s.src == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("admin: snapshot repository not configured"))
	}
	if req.Msg.GetShardId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: shard_id required"))
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := snapshot.SnapshotOnce(callCtx, snapshot.ProducerConfig{
		ShardID:    req.Msg.GetShardId(),
		Source:     s.src,
		Repo:       s.repo,
		ScratchDir: s.scratchDir,
		Log:        s.log,
	}); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: snapshot: %w", err))
	}
	refs, err := s.repo.List(callCtx, req.Msg.GetShardId())
	if err != nil || len(refs) == 0 {
		return connect.NewResponse(&adminv1.CreateSnapshotResponse{ShardId: req.Msg.GetShardId()}), nil
	}
	latest := refs[0]
	for _, r := range refs[1:] {
		if r.Index > latest.Index {
			latest = r
		}
	}
	return connect.NewResponse(&adminv1.CreateSnapshotResponse{
		ShardId:   req.Msg.GetShardId(),
		Index:     latest.Index,
		SizeBytes: latest.SizeBytes,
	}), nil
}

// DeleteSnapshot removes (shard_id, index) from the configured repository.
// Idempotent. Leader-only.
func (s *Server) DeleteSnapshot(ctx context.Context, req *connect.Request[adminv1.DeleteSnapshotRequest]) (*connect.Response[adminv1.DeleteSnapshotResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if s.repo == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("admin: snapshot repository not configured"))
	}
	if req.Msg.GetShardId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: shard_id required"))
	}
	if req.Msg.GetIndex() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: index required"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.repo.Delete(callCtx, req.Msg.GetShardId(), req.Msg.GetIndex()); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: delete snapshot: %w", err))
	}
	return connect.NewResponse(&adminv1.DeleteSnapshotResponse{}), nil
}

// ListSnapshots delegates to Repository.List.
func (s *Server) ListSnapshots(ctx context.Context, req *connect.Request[adminv1.ListSnapshotsRequest]) (*connect.Response[adminv1.ListSnapshotsResponse], error) {
	if s.repo == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("admin: snapshot repository not configured"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	refs, err := s.repo.List(callCtx, req.Msg.GetShardId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: list snapshots: %w", err))
	}
	out := make([]*adminv1.SnapshotRef, 0, len(refs))
	for _, r := range refs {
		out = append(out, &adminv1.SnapshotRef{
			ShardId:         r.ShardID,
			Index:           r.Index,
			SizeBytes:       r.SizeBytes,
			CreatedAtUnixMs: r.CreatedAt.UnixMilli(),
		})
	}
	return connect.NewResponse(&adminv1.ListSnapshotsResponse{Snapshots: out}), nil
}

// TransferLP initiates a cross-shard transfer for one logical partition.
// Leader-only.
func (s *Server) TransferLP(ctx context.Context, req *connect.Request[adminv1.TransferLPRequest]) (*connect.Response[adminv1.TransferLPResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	lp := req.Msg.GetLp()
	destShard := req.Msg.GetDestShard()
	if lp >= keys.LPCount {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("admin: lp %d out of range [0, %d)", lp, keys.LPCount))
	}
	if destShard == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: dest_shard must be a partition shard id (>= 1)"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read partition table: %w", err))
	}
	if pt == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("admin: partition table not yet bootstrapped"))
	}
	if _, ok := pt.GetShards()[destShard]; !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("admin: dest_shard %d is not a known partition shard", destShard))
	}
	owners, err := s.host.LPOwners(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read lp owners: %w", err))
	}
	var currentOwner uint64
	for _, rec := range owners.Records {
		if rec.GetLp() == lp {
			currentOwner = rec.GetShardId()
			break
		}
	}
	if currentOwner == 0 {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("admin: lp %d has no current owner (table not yet seeded)", lp))
	}
	if currentOwner == destShard {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("admin: lp %d already on shard %d", lp, destShard))
	}
	transferID := uuid.NewString()
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_InitiateLpTransfer{
			InitiateLpTransfer: &enginev1.InitiateLPTransfer{
				TransferId: transferID,
				Lp:         lp,
				DestShard:  destShard,
			},
		},
	}
	if err := s.runner.Proposer().ProposeSelf(callCtx, cmd); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: propose InitiateLPTransfer: %w", err))
	}
	return connect.NewResponse(&adminv1.TransferLPResponse{TransferId: transferID}), nil
}

// ListLPTransfers returns every LPTransferRecord (as views) plus the table's
// current CAS revision. No leader gate.
func (s *Server) ListLPTransfers(ctx context.Context, _ *connect.Request[adminv1.ListLPTransfersRequest]) (*connect.Response[adminv1.ListLPTransfersResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.LPTransfers(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read lp transfers: %w", err))
	}
	return connect.NewResponse(&adminv1.ListLPTransfersResponse{
		Transfers:     apimap.LPTransferViews(list.Records),
		TableRevision: list.TableRevision,
	}), nil
}

// RebalanceAdvise returns the rebalancer's view of the cluster on this peer right
// now. Read-only — any peer can answer.
func (s *Server) RebalanceAdvise(ctx context.Context, _ *connect.Request[adminv1.RebalanceAdviseRequest]) (*connect.Response[adminv1.RebalanceAdviseResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	state, err := s.gatherRebalanceState(callCtx)
	if err != nil {
		return nil, err
	}
	dec := rebalance.Advise(state)
	resp := &adminv1.RebalanceAdviseResponse{
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
	resp.WouldTransfer = make([]*adminv1.RebalanceMove, 0, len(dec.Proposed))
	for _, mv := range dec.Proposed {
		resp.WouldTransfer = append(resp.WouldTransfer, &adminv1.RebalanceMove{
			Lp:        mv.LP,
			FromShard: mv.FromShard,
			ToShard:   mv.ToShard,
		})
	}
	return connect.NewResponse(resp), nil
}

// gatherRebalanceState SyncReads shard 0 for the advisor's input.
func (s *Server) gatherRebalanceState(ctx context.Context) (rebalance.State, error) {
	pt, err := s.host.PartitionTable(ctx)
	if err != nil {
		return rebalance.State{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read partition table: %w", err))
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
			fmt.Errorf("admin: read lp owners: %w", err))
	}
	current := make(map[uint32]uint64, len(owners.Records))
	for _, rec := range owners.Records {
		current[rec.GetLp()] = rec.GetShardId()
	}

	drains, err := s.host.RebalanceDrains(ctx)
	if err != nil {
		return rebalance.State{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read rebalance_drain: %w", err))
	}
	drained := make([]uint64, 0, len(drains.Records))
	for _, rec := range drains.Records {
		drained = append(drained, rec.GetShardId())
	}

	transfers, err := s.host.LPTransfers(ctx)
	if err != nil {
		return rebalance.State{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read lp transfers: %w", err))
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
		MaxConcurrent:     s.rebalance.MaxConcurrentTransfers,
		MinSecondsBetween: s.rebalance.MinSecondsBetweenTransfers,
		SkewEngagePct:     s.rebalance.SkewEngagePct,
		SkewDisengagePct:  s.rebalance.SkewDisengagePct,
	}, nil
}

// RebalanceDrain adds or removes a partition shard from the RebalanceDrainTable.
// Leader-only. CAS via if_table_revision_eq.
func (s *Server) RebalanceDrain(ctx context.Context, req *connect.Request[adminv1.RebalanceDrainRequest]) (*connect.Response[adminv1.RebalanceDrainResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	shardID := req.Msg.GetShardId()
	if shardID == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: shard_id must be a partition shard id (≥ 1)"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read partition table: %w", err))
	}
	if pt == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("admin: partition table not yet bootstrapped"))
	}
	if _, ok := pt.GetShards()[shardID]; !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("admin: shard_id %d is not a known partition shard", shardID))
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
			fmt.Errorf("admin: propose SetRebalanceDrain: %w", err))
	}
	if val == cluster.ResultValueFailedPrecondition {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("admin: rebalance_drain CAS mismatch (if_table_revision_eq=%d)",
				req.Msg.GetIfTableRevisionEq()))
	}
	rev, err := s.readRebalanceDrainRevision(callCtx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&adminv1.RebalanceDrainResponse{TableRevision: rev}), nil
}

// readRebalanceDrainRevision SyncReads the RebalanceDrainTable list for the
// post-apply revision.
func (s *Server) readRebalanceDrainRevision(ctx context.Context) (uint64, error) {
	drains, err := s.host.RebalanceDrains(ctx)
	if err != nil {
		return 0, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read rebalance_drain revision: %w", err))
	}
	return drains.TableRevision, nil
}

// replicaSetContainsID is a small predicate; cluster has the same logic but its
// package is below ours in the import graph.
func replicaSetContainsID(ids []uint64, nodeID uint64) bool {
	return slices.Contains(ids, nodeID)
}

// nextStepIDForShard returns max(pending[shard].step_id)+1 or 1.
func nextStepIDForShard(pending []*enginev1.RebalanceStep, shardID uint64) uint64 {
	var highest uint64
	for _, p := range pending {
		if p.GetShardId() == shardID && p.GetStepId() > highest {
			highest = p.GetStepId()
		}
	}
	return highest + 1
}
