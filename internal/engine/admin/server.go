// Package admin implements reflow's mTLS-protected cluster Admin gRPC
// surface. It owns the gRPC handlers and the per-RPC business logic
// (Raft proposals against shard 0, snapshot orchestration). Identity,
// audit, and authorization all live in internal/auth — the same auth
// stack drives the Delivery service too.
//
// Every mutating RPC translates into a shard-0 Raft proposal via
// MetadataRunner.Proposer().ProposeSelf, so all admin calls must reach
// the metadata leader. Non-leader nodes return codes.Unavailable with
// a leader hint in the error message; the reflow-cluster CLI is the
// canonical client and is responsible for retrying.
package admin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// protocolVersion is the wire-protocol version this engine speaks; the
// handler-side discovery response must advertise the same string.
const protocolVersion = "v1"

// Server implements adminv1.AdminServer.
type Server struct {
	adminv1.UnimplementedAdminServer

	host   *engine.Host
	runner *engine.MetadataRunner
	repo   snapshot.Repository
	src    snapshot.Source
	log    *slog.Logger
	// signer, when non-nil, stamps Authorization: Bearer on outgoing
	// GET /discover requests so the handler's verifier accepts them.
	// Nil disables the header (single-node and insecure-creds posture).
	signer handlerclient.Signer

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
	// Signer, when non-nil, is used to mint a JWT on outgoing
	// GET /discover requests during RegisterDeployment. Same Signer
	// the handlerclient http2client uses.
	Signer handlerclient.Signer
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
		signer:           cfg.Signer,
		scratchDir:       cfg.ScratchDir,
		adminCallTimeout: 30 * time.Second,
	}, nil
}

// Register installs s on gs.
func (s *Server) Register(gs *grpc.Server) {
	adminv1.RegisterAdminServer(gs, s)
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
	// Build the union {0} ∪ partition-shard-ids so shard 0 is proposed
	// alongside the partition shards in one uniform loop.
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
		// Mutate the in-memory copy so successive iterations pick a
		// fresh step_id without re-reading from shard 0.
		pt.Pending = append(pt.Pending, step)
		beginCmd := &enginev1.Command{
			Kind: &enginev1.Command_BeginRebalanceStep{
				BeginRebalanceStep: &enginev1.BeginRebalanceStep{Step: step},
			},
		}
		if err := s.runner.Proposer().ProposeSelf(callCtx, beginCmd); err != nil {
			return status.Errorf(codes.Internal, "admin: propose BeginRebalanceStep shard=%d: %v",
				shardID, err)
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
	// Pick the highest-Index ref explicitly instead of trusting List's
	// slice order — the Repository contract sorts ascending but we
	// don't want the response to silently flip if that ever changes.
	latest := refs[0]
	for _, r := range refs[1:] {
		if r.Index > latest.Index {
			latest = r
		}
	}
	return &adminv1.CreateSnapshotResponse{
		ShardId:   req.GetShardId(),
		Index:     latest.Index,
		SizeBytes: latest.SizeBytes,
	}, nil
}

// DeleteSnapshot removes (shard_id, index) from the configured
// repository. Idempotent — succeeds when the snapshot is already absent.
func (s *Server) DeleteSnapshot(ctx context.Context, req *adminv1.DeleteSnapshotRequest) (*adminv1.DeleteSnapshotResponse, error) {
	if s.repo == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin: snapshot repository not configured")
	}
	if req.GetShardId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "admin: shard_id required")
	}
	if req.GetIndex() == 0 {
		return nil, status.Error(codes.InvalidArgument, "admin: index required")
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.repo.Delete(callCtx, req.GetShardId(), req.GetIndex()); err != nil {
		return nil, status.Errorf(codes.Internal, "admin: delete snapshot: %v", err)
	}
	return &adminv1.DeleteSnapshotResponse{}, nil
}

// ListSnapshots delegates to Repository.List.
func (s *Server) ListSnapshots(ctx context.Context, req *adminv1.ListSnapshotsRequest) (*adminv1.ListSnapshotsResponse, error) {
	if s.repo == nil {
		return nil, status.Error(codes.FailedPrecondition, "admin: snapshot repository not configured")
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	refs, err := s.repo.List(callCtx, req.GetShardId())
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

// RegisterDeployment accepts a remote-handler URL, dials its discovery
// endpoint, and proposes Command_RegisterDeployment to shard 0. The
// synthetic inproc deployment is registered internally at metadata-leader
// bootstrap, NOT via this RPC; operators do not see it.
//
// Wired schemes: http:// (h2c) and https:// (HTTP/2 + TLS). inproc:// is
// reserved internally and rejected.
func (s *Server) RegisterDeployment(ctx context.Context, req *adminv1.RegisterDeploymentRequest) (*adminv1.RegisterDeploymentResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	raw := req.GetUrl()
	if raw == "" {
		return nil, status.Error(codes.InvalidArgument, "admin: url required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "admin: parse url: %v", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if err := validateDeploymentScheme(scheme); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "admin: %v", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()

	resp, err := discoverHTTP(callCtx, raw, scheme == "http", s.signer)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "admin: discovery: %v", err)
	}
	if got := resp.GetProtocolVersion(); got != protocolVersion {
		return nil, status.Errorf(codes.FailedPrecondition,
			"admin: handler advertised protocol %q; engine speaks %q", got, protocolVersion)
	}

	deploymentID := uuid.NewString()
	rec := &enginev1.DeploymentRecord{
		Id:             deploymentID,
		Url:            raw,
		RegisteredAtMs: uint64(time.Now().UnixMilli()),
	}
	for _, h := range resp.GetHandlers() {
		for _, name := range h.GetHandlerNames() {
			rec.Handlers = append(rec.Handlers, &enginev1.DeploymentHandler{
				Service: h.GetService(),
				Handler: name,
				Kind:    uint32(h.GetKind()),
			})
		}
	}

	cmd := &enginev1.Command{
		Kind: &enginev1.Command_RegisterDeployment{
			RegisterDeployment: &enginev1.RegisterDeployment{Record: rec},
		},
	}
	if err := s.runner.Proposer().ProposeSelf(callCtx, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "admin: propose RegisterDeployment: %v", err)
	}
	return &adminv1.RegisterDeploymentResponse{DeploymentId: deploymentID}, nil
}

// validateDeploymentScheme rejects schemes the engine cannot dial. The
// only supported wire transport is HTTP/2 (h2c for http://, TLS for
// https://). inproc:// is reserved internally so operators can't shadow
// the synthetic in-proc deployment via this RPC.
func validateDeploymentScheme(scheme string) error {
	switch scheme {
	case "http", "https":
		return nil
	case "inproc":
		return errors.New("inproc:// is internal; cannot be registered via RegisterDeployment")
	case "":
		return errors.New("url missing scheme")
	default:
		return fmt.Errorf("unsupported scheme %q; only http:// and https:// are supported", scheme)
	}
}

// discoverHTTP issues GET <url>/discover over HTTP/2 (h2c for http://,
// TLS for https://). The response body is a protobuf-encoded
// DiscoveryResponse. When signer is non-nil the request carries
// Authorization: Bearer <jwt> with the deployment URL as audience so
// the handler's verifier (if configured) accepts it.
func discoverHTTP(ctx context.Context, rawURL string, plaintextH2C bool, signer handlerclient.Signer) (*discoveryv1.DiscoveryResponse, error) {
	tr := &http.Transport{Protocols: new(http.Protocols)}
	if plaintextH2C {
		tr.Protocols.SetUnencryptedHTTP2(true)
		tr.Protocols.SetHTTP1(false)
	} else {
		tr.Protocols.SetHTTP2(true)
		tr.Protocols.SetHTTP1(false)
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	hc := &http.Client{Transport: tr}
	defer tr.CloseIdleConnections()

	target := strings.TrimRight(rawURL, "/") + "/discover"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.reflow.invocation.v1+protobuf")
	if signer != nil {
		tok, serr := signer.Sign(rawURL)
		if serr != nil {
			return nil, fmt.Errorf("sign discover: %w", serr)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return nil, fmt.Errorf("handler returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	var out discoveryv1.DiscoveryResponse
	if err := proto.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &out, nil
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
