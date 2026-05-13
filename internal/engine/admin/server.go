// Package admin implements reflow's mTLS-protected cluster Admin gRPC
// surface. Phase 4.2.
//
// Every mutating RPC translates into a shard-0 Raft proposal via
// MetadataRunner.Proposer().ProposeSelf, so all admin calls must reach
// the metadata leader. Non-leader nodes return codes.Unavailable with
// a leader hint in the error message; the reflow-cluster CLI is the
// canonical client and is responsible for retrying.
//
// Authorization is gross-grained: any client whose certificate chain
// verifies against the configured operator CA (terminated by the
// transport layer in BuildAdminServerTLS) is treated as admin. The
// caller's CommonName is logged on every call so audit trails remain
// useful.
package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Server implements adminv1.AdminServer.
type Server struct {
	adminv1.UnimplementedAdminServer

	host   *engine.Host
	runner *engine.MetadataRunner
	repo   snapshot.Repository
	src    snapshot.Source
	log    *slog.Logger

	// scratchDir holds export directories created for CreateSnapshot.
	// Each call writes into a fresh sub-directory.
	scratchDir string

	// adminCallTimeout caps the wall-clock time of a single admin RPC.
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

// NewServer constructs the Admin server. Repo and Source are required
// for snapshot RPCs; without them, snapshot endpoints return
// FailedPrecondition.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Host == nil || cfg.Runner == nil {
		return nil, errors.New("admin: Host and Runner are required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.ScratchDir == "" {
		cfg.ScratchDir = filepath.Join(os.TempDir(), "reflow-admin-scratch")
	}
	if err := os.MkdirAll(cfg.ScratchDir, 0o755); err != nil {
		return nil, fmt.Errorf("admin: scratch dir: %w", err)
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

// Register installs s on gs.
func (s *Server) Register(gs *grpc.Server) {
	adminv1.RegisterAdminServer(gs, s)
}

// AuditInterceptor is the unary interceptor that pulls caller identity
// from the verified TLS cert and logs it alongside the RPC name. Reject
// callers without a verified chain — BuildAdminServerTLS already
// enforces this at the transport, but a defense-in-depth check here
// keeps the audit log honest if someone wires the server without TLS.
func AuditInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	if log == nil {
		log = slog.Default()
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		caller := callerIdentity(ctx)
		if caller == "" {
			return nil, status.Error(codes.Unauthenticated, "admin: client certificate not verified")
		}
		log.Info("admin: rpc",
			"method", info.FullMethod, "caller", caller)
		return handler(ctx, req)
	}
}

func callerIdentity(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}
	if len(tlsInfo.State.VerifiedChains) == 0 {
		return ""
	}
	leaf := tlsInfo.State.VerifiedChains[0][0]
	return leaf.Subject.CommonName
}

// requireLeader returns Unavailable when this node is not the metadata
// leader. The CLI retries against another node.
func (s *Server) requireLeader() error {
	if s.runner.IsLeader() {
		return nil
	}
	hint, _ := s.host.PartitionLeaderHint(0)
	return status.Errorf(codes.Unavailable,
		"admin: not the metadata leader (hint=%d)", hint)
}

// AddNode registers a new peer and schedules a PROMOTE_TO_VOTER step
// for every existing partition shard. Catch-up happens automatically
// inside dragonboat when SyncRequestAddReplica is called by the
// rebalancer.
func (s *Server) AddNode(ctx context.Context, req *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.GetNodeId() == 0 || req.GetRaftAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "admin: node_id and raft_addr are required")
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
		return nil, status.Errorf(codes.Internal, "admin: propose RegisterNode: %v", err)
	}

	// Read the current partition table to find every shard that needs
	// the new node added.
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "admin: read partition table: %v", err)
	}
	if pt == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin: partition table not yet bootstrapped")
	}
	for shardID, rs := range pt.GetShards() {
		if replicaSetContainsID(rs.GetNodeIds(), req.GetNodeId()) {
			continue
		}
		step := &enginev1.RebalanceStep{
			ShardId:   shardID,
			Kind:      enginev1.RebalanceStep_PROMOTE_TO_VOTER,
			AddNodeId: req.GetNodeId(),
			StepId:    nextStepIDForShard(pt.GetPending(), shardID),
		}
		// Mutate the in-memory copy so successive iterations pick a
		// fresh step_id without re-reading from shard 0.
		pt.Pending = append(pt.Pending, step)
		beginCmd := &enginev1.Command{
			Kind: &enginev1.Command_BeginRebalanceStep{
				BeginRebalanceStep: &enginev1.BeginRebalanceStep{Step: step},
			},
		}
		if err := s.runner.Proposer().ProposeSelf(callCtx, beginCmd); err != nil {
			return nil, status.Errorf(codes.Internal, "admin: propose BeginRebalanceStep shard=%d: %v",
				shardID, err)
		}
	}
	// Re-read the table for the response's assignment_epoch.
	pt2, err := s.host.PartitionTable(callCtx)
	if err != nil || pt2 == nil {
		return &adminv1.AddNodeResponse{}, nil
	}
	return &adminv1.AddNodeResponse{AssignmentEpoch: pt2.GetAssignmentEpoch()}, nil
}

// RemoveNode proposes EvictNode; the apply arm + rebalancer drive the
// rest.
func (s *Server) RemoveNode(ctx context.Context, req *adminv1.RemoveNodeRequest) (*adminv1.RemoveNodeResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.GetNodeId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "admin: node_id required")
	}
	if req.GetNodeId() == s.host.NodeID() {
		return nil, status.Error(codes.FailedPrecondition,
			"admin: refusing to remove self; redirect to another node first")
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_EvictNode{
			EvictNode: &enginev1.EvictNode{NodeId: req.GetNodeId()},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.runner.Proposer().ProposeSelf(callCtx, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "admin: propose EvictNode: %v", err)
	}
	pt, _ := s.host.PartitionTable(callCtx)
	var epoch uint64
	if pt != nil {
		epoch = pt.GetAssignmentEpoch()
	}
	return &adminv1.RemoveNodeResponse{AssignmentEpoch: epoch}, nil
}

// ListNodes streams the current Membership rows.
func (s *Server) ListNodes(ctx context.Context, _ *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	members, err := s.host.Membership(callCtx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "admin: read membership: %v", err)
	}
	return &adminv1.ListNodesResponse{Nodes: members}, nil
}

// ListPartitions returns the current PartitionTable.
func (s *Server) ListPartitions(ctx context.Context, _ *adminv1.ListPartitionsRequest) (*adminv1.ListPartitionsResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "admin: read partition table: %v", err)
	}
	return &adminv1.ListPartitionsResponse{Table: pt}, nil
}

// CreateSnapshot delegates to snapshot.SnapshotOnce.
func (s *Server) CreateSnapshot(ctx context.Context, req *adminv1.CreateSnapshotRequest) (*adminv1.CreateSnapshotResponse, error) {
	if s.repo == nil || s.src == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin: snapshot repository not configured")
	}
	if req.GetShardId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "admin: shard_id required")
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := snapshot.SnapshotOnce(callCtx, snapshot.ProducerConfig{
		ShardID:    req.GetShardId(),
		Source:     s.src,
		Repo:       s.repo,
		ScratchDir: s.scratchDir,
		Log:        s.log,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "admin: snapshot: %v", err)
	}
	refs, err := s.repo.List(callCtx, req.GetShardId())
	if err != nil || len(refs) == 0 {
		return &adminv1.CreateSnapshotResponse{ShardId: req.GetShardId()}, nil
	}
	last := refs[len(refs)-1]
	return &adminv1.CreateSnapshotResponse{
		ShardId:   req.GetShardId(),
		Index:     last.Index,
		SizeBytes: last.SizeBytes,
	}, nil
}

// ListSnapshots delegates to Repository.List.
func (s *Server) ListSnapshots(ctx context.Context, req *adminv1.ListSnapshotsRequest) (*adminv1.ListSnapshotsResponse, error) {
	if s.repo == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin: snapshot repository not configured")
	}
	refs, err := s.repo.List(ctx, req.GetShardId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "admin: list snapshots: %v", err)
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
	return &adminv1.ListSnapshotsResponse{Snapshots: out}, nil
}

// replicaSetContainsID is a small predicate; cluster has the same logic
// but its package is below ours in the import graph.
func replicaSetContainsID(ids []uint64, nodeID uint64) bool {
	return slices.Contains(ids, nodeID)
}

// nextStepIDForShard returns max(pending[shard].step_id)+1 or 1.
func nextStepIDForShard(pending []*enginev1.RebalanceStep, shardID uint64) uint64 {
	var max uint64
	for _, p := range pending {
		if p.GetShardId() == shardID && p.GetStepId() > max {
			max = p.GetStepId()
		}
	}
	return max + 1
}
