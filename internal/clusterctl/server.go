// Package clusterctl implements reflow's ClusterCtl Connect RPC
// surface — the cluster-operator side of the admin port. It owns the
// per-RPC business logic for cluster topology (AddNode, SelfJoin,
// RemoveNode, ListNodes, ListPartitions), DR snapshots (Create / List
// / Delete), and routing transfers (TransferLP, ListLPTransfers).
// App config (deployments, event sources, webhooks, secrets) lives in
// internal/config under a parallel Connect service.
//
// Every mutating RPC translates into a shard-0 Raft proposal via
// MetadataRunner.Proposer().ProposeSelf, so all calls must reach the
// metadata leader. Non-leader nodes return CodeUnavailable with a
// clusterctlv1.LeaderHint detail attached;
// pkg/reflowclient.CallWithLeaderRedirect is the canonical retry
// helper.
package clusterctl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	clusterctlv1 "github.com/twinfer/reflow/proto/clusterctlv1"
	"github.com/twinfer/reflow/proto/clusterctlv1/clusterctlv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Server implements clusterctlv1connect.ClusterCtlHandler.
type Server struct {
	clusterctlv1connect.UnimplementedClusterCtlHandler

	host   *engine.Host
	runner *engine.MetadataRunner
	repo   snapshot.Repository
	src    snapshot.Source
	log    *slog.Logger

	scratchDir       string
	adminCallTimeout time.Duration
}

// Config groups the constructor inputs.
type Config struct {
	Host       *engine.Host
	Runner     *engine.MetadataRunner
	Repo       snapshot.Repository
	Source     snapshot.Source
	Log        *slog.Logger
	ScratchDir string
}

// NewServer constructs the ClusterCtl server. Repo and Source are
// required for snapshot RPCs; without them, snapshot endpoints return
// FailedPrecondition.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Host == nil || cfg.Runner == nil {
		return nil, errors.New("clusterctl: Host and Runner are required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.ScratchDir == "" {
		cfg.ScratchDir = filepath.Join(os.TempDir(), "reflow-clusterctl-scratch")
	}
	if err := os.MkdirAll(cfg.ScratchDir, 0o755); err != nil {
		return nil, fmt.Errorf("clusterctl: scratch dir: %w", err)
	}
	return &Server{
		host:             cfg.Host,
		runner:           cfg.Runner,
		repo:             cfg.Repo,
		src:              cfg.Source,
		log:              cfg.Log,
		scratchDir:       cfg.ScratchDir,
		adminCallTimeout: 30 * time.Second,
	}, nil
}

// NewHandler returns the path + http.Handler pair to mount on a
// connectserver. opts is forwarded to the generated handler.
func (s *Server) NewHandler(opts ...connect.HandlerOption) (string, http.Handler) {
	return clusterctlv1connect.NewClusterCtlHandler(s, opts...)
}

// requireLeader returns CodeUnavailable when this node is not the
// metadata leader, attaching a LeaderHint detail (node_id +
// admin_endpoint resolved via gossip NodeHostMeta) so clients can
// redirect via pkg/reflowclient.CallWithLeaderRedirect.
func (s *Server) requireLeader() error {
	if s.runner.IsLeader() {
		return nil
	}
	hintID, _ := s.host.PartitionLeaderHint(0)
	cerr := connect.NewError(connect.CodeUnavailable,
		fmt.Errorf("clusterctl: not the metadata leader (hint=%d)", hintID))
	if hintID != 0 {
		if addr, ok := s.host.NodeAdminEndpoint(hintID); ok {
			if d, derr := connect.NewErrorDetail(&clusterctlv1.LeaderHint{
				NodeId:        hintID,
				AdminEndpoint: addr,
			}); derr == nil {
				cerr.AddDetail(d)
			}
		}
	}
	return cerr
}

// AddNode registers a new peer and schedules a PROMOTE_TO_VOTER step
// for every existing partition shard.
func (s *Server) AddNode(ctx context.Context, req *connect.Request[clusterctlv1.AddNodeRequest]) (*connect.Response[clusterctlv1.AddNodeResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	out, err := s.addNodeInternal(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(out), nil
}

// SelfJoin is AddNode initiated by the joiner itself. Authorization
// requires the caller's SPIFFE identity to be node/<req.node_id>.
// Transport authz already gates this method to node/* principals; this
// in-handler check is the second gate ensuring the node_id in the
// request matches the SPIFFE subject (defense in depth).
func (s *Server) SelfJoin(ctx context.Context, req *connect.Request[clusterctlv1.AddNodeRequest]) (*connect.Response[clusterctlv1.AddNodeResponse], error) {
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

// checkSelfJoinPrincipal enforces the SPIFFE-equals-NodeID gate for
// SelfJoin. Extracted so it's unit-testable without standing up an
// engine.Host / MetadataRunner.
func checkSelfJoinPrincipal(ctx context.Context, nodeID uint64) error {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal.Kind != "node" {
		return connect.NewError(connect.CodePermissionDenied,
			errors.New("clusterctl: SelfJoin requires a node SPIFFE identity"))
	}
	if principal.Subject != strconv.FormatUint(nodeID, 10) {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("clusterctl: SelfJoin SPIFFE node/%s does not match req.node_id=%d",
				principal.Subject, nodeID))
	}
	return nil
}

// addNodeInternal contains the FSM-driving body shared by AddNode and
// SelfJoin.
func (s *Server) addNodeInternal(ctx context.Context, req *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
	if req.GetNodeId() == 0 || req.GetRaftAddr() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("clusterctl: node_id and raft_addr are required"))
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
			fmt.Errorf("clusterctl: propose RegisterNode: %w", err))
	}

	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("clusterctl: read partition table: %w", err))
	}
	if pt == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("clusterctl: partition table not yet bootstrapped"))
	}
	addShard := func(shardID uint64, rs *enginev1.ReplicaSet) error {
		if replicaSetContainsID(rs.GetNodeIds(), req.GetNodeId()) {
			return nil
		}
		// nextStepIDForShard filters by shardID and each shard is visited
		// once per call, so the local pt.Pending mutation has no effect
		// on subsequent iterations — no need to append.
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
				fmt.Errorf("clusterctl: propose BeginRebalanceStep shard=%d: %w",
					shardID, err))
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
		return &clusterctlv1.AddNodeResponse{}, nil
	}
	return &clusterctlv1.AddNodeResponse{AssignmentEpoch: pt2.GetAssignmentEpoch()}, nil
}

// RemoveNode proposes EvictNode; the apply arm + rebalancer drive the rest.
func (s *Server) RemoveNode(ctx context.Context, req *connect.Request[clusterctlv1.RemoveNodeRequest]) (*connect.Response[clusterctlv1.RemoveNodeResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.Msg.GetNodeId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("clusterctl: node_id required"))
	}
	if req.Msg.GetNodeId() == s.host.NodeID() {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("clusterctl: refusing to remove self; redirect to another node first"))
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
			fmt.Errorf("clusterctl: propose EvictNode: %w", err))
	}
	pt, _ := s.host.PartitionTable(callCtx)
	var epoch uint64
	if pt != nil {
		epoch = pt.GetAssignmentEpoch()
	}
	return connect.NewResponse(&clusterctlv1.RemoveNodeResponse{AssignmentEpoch: epoch}), nil
}

// ListNodes returns the current Membership rows.
func (s *Server) ListNodes(ctx context.Context, _ *connect.Request[clusterctlv1.ListNodesRequest]) (*connect.Response[clusterctlv1.ListNodesResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	members, err := s.host.Membership(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("clusterctl: read membership: %w", err))
	}
	return connect.NewResponse(&clusterctlv1.ListNodesResponse{Nodes: members}), nil
}

// ListPartitions returns the current PartitionTable.
func (s *Server) ListPartitions(ctx context.Context, _ *connect.Request[clusterctlv1.ListPartitionsRequest]) (*connect.Response[clusterctlv1.ListPartitionsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("clusterctl: read partition table: %w", err))
	}
	return connect.NewResponse(&clusterctlv1.ListPartitionsResponse{Table: pt}), nil
}

// CreateSnapshot delegates to snapshot.SnapshotOnce. Leader-only: every
// node has its own local store, so the leader's snapshot is the only one
// guaranteed to contain the latest applied state, and writes to the
// shared blob repo from multiple nodes would race.
func (s *Server) CreateSnapshot(ctx context.Context, req *connect.Request[clusterctlv1.CreateSnapshotRequest]) (*connect.Response[clusterctlv1.CreateSnapshotResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if s.repo == nil || s.src == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("clusterctl: snapshot repository not configured"))
	}
	if req.Msg.GetShardId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("clusterctl: shard_id required"))
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
			fmt.Errorf("clusterctl: snapshot: %w", err))
	}
	refs, err := s.repo.List(callCtx, req.Msg.GetShardId())
	if err != nil || len(refs) == 0 {
		return connect.NewResponse(&clusterctlv1.CreateSnapshotResponse{ShardId: req.Msg.GetShardId()}), nil
	}
	latest := refs[0]
	for _, r := range refs[1:] {
		if r.Index > latest.Index {
			latest = r
		}
	}
	return connect.NewResponse(&clusterctlv1.CreateSnapshotResponse{
		ShardId:   req.Msg.GetShardId(),
		Index:     latest.Index,
		SizeBytes: latest.SizeBytes,
	}), nil
}

// DeleteSnapshot removes (shard_id, index) from the configured
// repository. Idempotent — succeeds when the snapshot is already absent.
// Leader-only: serializing through the leader keeps concurrent reaper /
// admin deletes from racing on the shared blob repo. ListSnapshots is
// intentionally not gated — read-only, served by any node.
func (s *Server) DeleteSnapshot(ctx context.Context, req *connect.Request[clusterctlv1.DeleteSnapshotRequest]) (*connect.Response[clusterctlv1.DeleteSnapshotResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if s.repo == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("clusterctl: snapshot repository not configured"))
	}
	if req.Msg.GetShardId() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("clusterctl: shard_id required"))
	}
	if req.Msg.GetIndex() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("clusterctl: index required"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.repo.Delete(callCtx, req.Msg.GetShardId(), req.Msg.GetIndex()); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("clusterctl: delete snapshot: %w", err))
	}
	return connect.NewResponse(&clusterctlv1.DeleteSnapshotResponse{}), nil
}

// ListSnapshots delegates to Repository.List.
func (s *Server) ListSnapshots(ctx context.Context, req *connect.Request[clusterctlv1.ListSnapshotsRequest]) (*connect.Response[clusterctlv1.ListSnapshotsResponse], error) {
	if s.repo == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("clusterctl: snapshot repository not configured"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	refs, err := s.repo.List(callCtx, req.Msg.GetShardId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("clusterctl: list snapshots: %w", err))
	}
	out := make([]*clusterctlv1.SnapshotRef, 0, len(refs))
	for _, r := range refs {
		out = append(out, &clusterctlv1.SnapshotRef{
			ShardId:         r.ShardID,
			Index:           r.Index,
			SizeBytes:       r.SizeBytes,
			CreatedAtUnixMs: r.CreatedAt.UnixMilli(),
		})
	}
	return connect.NewResponse(&clusterctlv1.ListSnapshotsResponse{Snapshots: out}), nil
}

// replicaSetContainsID is a small predicate; cluster has the same logic
// but its package is below ours in the import graph.
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
