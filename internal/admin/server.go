// Package admin implements reflow's mTLS-protected cluster Admin
// Connect RPC surface. It owns the per-RPC business logic (Raft
// proposals against shard 0, snapshot orchestration). Identity, audit,
// and authorization all live in internal/auth — the same auth stack
// drives the Delivery service too.
//
// Every mutating RPC translates into a shard-0 Raft proposal via
// MetadataRunner.Proposer().ProposeSelf, so all admin calls must reach
// the metadata leader. Non-leader nodes return CodeUnavailable with a
// LeaderHint detail attached; pkg/adminclient.CallWithLeaderRedirect
// is the canonical retry helper.
package admin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	connect "connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	"github.com/twinfer/reflow/proto/adminv1/adminv1connect"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	"github.com/twinfer/reflow/proto/discoveryv1/discoveryv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// protocolVersion is the wire-protocol version this engine speaks; the
// handler-side discovery response must advertise the same string.
const protocolVersion = "v1"

// Server implements adminv1connect.AdminHandler.
type Server struct {
	adminv1connect.UnimplementedAdminHandler

	host   *engine.Host
	runner *engine.MetadataRunner
	repo   snapshot.Repository
	src    snapshot.Source
	log    *slog.Logger
	signer handlerclient.Signer

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
	// Signer, when non-nil, mints a JWT on outgoing
	// DiscoveryService.Discover requests during RegisterDeployment.
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

// NewHandler returns the path + http.Handler pair to mount on a
// connectserver. opts is forwarded to the generated handler (e.g.
// connect.WithInterceptors for cross-cutting concerns).
func (s *Server) NewHandler(opts ...connect.HandlerOption) (string, http.Handler) {
	return adminv1connect.NewAdminHandler(s, opts...)
}

// requireLeader returns CodeUnavailable when this node is not the
// metadata leader, attaching a LeaderHint detail (node_id +
// admin_endpoint resolved via gossip NodeHostMeta) so clients can
// redirect via pkg/adminclient.CallWithLeaderRedirect.
func (s *Server) requireLeader() error {
	if s.runner.IsLeader() {
		return nil
	}
	hintID, _ := s.host.PartitionLeaderHint(0)
	cerr := connect.NewError(connect.CodeUnavailable,
		fmt.Errorf("admin: not the metadata leader (hint=%d)", hintID))
	if hintID != 0 {
		if addr, ok := s.host.NodeAdminEndpoint(hintID); ok {
			if d, derr := connect.NewErrorDetail(&adminv1.LeaderHint{
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

// SelfJoin is AddNode initiated by the joiner itself. Authorization
// requires the caller's SPIFFE identity to be node/<req.node_id>.
// Transport authz already gates this method to node/* principals; this
// in-handler check is the second gate ensuring the node_id in the
// request matches the SPIFFE subject (defense in depth).
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

// checkSelfJoinPrincipal enforces the SPIFFE-equals-NodeID gate for
// SelfJoin. Extracted so it's unit-testable without standing up an
// engine.Host / MetadataRunner.
func checkSelfJoinPrincipal(ctx context.Context, nodeID uint64) error {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal.Kind != "node" {
		return connect.NewError(connect.CodePermissionDenied,
			errors.New("admin: SelfJoin requires a node SPIFFE identity"))
	}
	if principal.Subject != strconv.FormatUint(nodeID, 10) {
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("admin: SelfJoin SPIFFE node/%s does not match req.node_id=%d",
				principal.Subject, nodeID))
	}
	return nil
}

// addNodeInternal contains the FSM-driving body shared by AddNode and
// SelfJoin.
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
		pt.Pending = append(pt.Pending, step)
		beginCmd := &enginev1.Command{
			Kind: &enginev1.Command_BeginRebalanceStep{
				BeginRebalanceStep: &enginev1.BeginRebalanceStep{Step: step},
			},
		}
		if err := s.runner.Proposer().ProposeSelf(callCtx, beginCmd); err != nil {
			return connect.NewError(connect.CodeInternal,
				fmt.Errorf("admin: propose BeginRebalanceStep shard=%d: %w",
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

// ListNodes returns the current Membership rows.
func (s *Server) ListNodes(ctx context.Context, _ *connect.Request[adminv1.ListNodesRequest]) (*connect.Response[adminv1.ListNodesResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	members, err := s.host.Membership(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read membership: %w", err))
	}
	return connect.NewResponse(&adminv1.ListNodesResponse{Nodes: members}), nil
}

// ListPartitions returns the current PartitionTable.
func (s *Server) ListPartitions(ctx context.Context, _ *connect.Request[adminv1.ListPartitionsRequest]) (*connect.Response[adminv1.ListPartitionsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read partition table: %w", err))
	}
	return connect.NewResponse(&adminv1.ListPartitionsResponse{Table: pt}), nil
}

// CreateSnapshot delegates to snapshot.SnapshotOnce.
func (s *Server) CreateSnapshot(ctx context.Context, req *connect.Request[adminv1.CreateSnapshotRequest]) (*connect.Response[adminv1.CreateSnapshotResponse], error) {
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

// DeleteSnapshot removes (shard_id, index) from the configured
// repository. Idempotent — succeeds when the snapshot is already absent.
func (s *Server) DeleteSnapshot(ctx context.Context, req *connect.Request[adminv1.DeleteSnapshotRequest]) (*connect.Response[adminv1.DeleteSnapshotResponse], error) {
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

// RegisterDeployment accepts a remote-handler URL, dials its discovery
// endpoint, and proposes Command_RegisterDeployment to shard 0.
func (s *Server) RegisterDeployment(ctx context.Context, req *connect.Request[adminv1.RegisterDeploymentRequest]) (*connect.Response[adminv1.RegisterDeploymentResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	out, err := s.registerDeployment(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(out), nil
}

// registerDeployment is the leader-side body, also called by
// pkg/reflow/run.go's autoSeedEndpoints via AutoSeed.
func (s *Server) registerDeployment(ctx context.Context, req *adminv1.RegisterDeploymentRequest) (*adminv1.RegisterDeploymentResponse, error) {
	raw := req.GetUrl()
	if raw == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: url required"))
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("admin: parse url: %w", err))
	}
	scheme := strings.ToLower(u.Scheme)
	if err := validateDeploymentScheme(scheme); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("admin: %w", err))
	}

	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()

	resp, err := discoverConnect(callCtx, raw, scheme == "http", s.signer)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: discovery: %w", err))
	}
	if got := resp.GetProtocolVersion(); got != protocolVersion {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("admin: handler advertised protocol %q; engine speaks %q", got, protocolVersion))
	}

	deploymentID := uuid.NewString()
	rec := &enginev1.DeploymentRecord{
		Id:                deploymentID,
		Url:               raw,
		RegisteredAtMs:    uint64(time.Now().UnixMilli()),
		MaxJournalEntries: req.GetMaxJournalEntries(),
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
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: propose RegisterDeployment: %w", err))
	}
	return &adminv1.RegisterDeploymentResponse{DeploymentId: deploymentID}, nil
}

// AutoSeed is the in-process registration path used by run.go's
// autoSeedEndpoints and by engine integration tests. Same body as
// RegisterDeployment minus the leader gate (callers wait for leadership
// themselves). budget=0 → engine default.
func (s *Server) AutoSeed(ctx context.Context, url string) (string, error) {
	return s.AutoSeedWithBudget(ctx, url, 0)
}

// AutoSeedWithBudget mirrors AutoSeed and additionally stamps a per-
// invocation step-budget override onto the deployment record.
func (s *Server) AutoSeedWithBudget(ctx context.Context, url string, budget uint32) (string, error) {
	resp, err := s.registerDeployment(ctx, &adminv1.RegisterDeploymentRequest{
		Url:               url,
		MaxJournalEntries: budget,
	})
	if err != nil {
		return "", err
	}
	return resp.GetDeploymentId(), nil
}

// validateDeploymentScheme rejects schemes the engine cannot dial.
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

// discoverConnect calls DiscoveryService.Discover on the deployment URL
// over HTTP/2 (h2c for http://, TLS for https://). When signer is
// non-nil the request carries Authorization: Bearer <jwt> with the
// deployment URL as audience.
func discoverConnect(ctx context.Context, rawURL string, plaintextH2C bool, signer handlerclient.Signer) (*discoveryv1.DiscoveryResponse, error) {
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

	client := discoveryv1connect.NewDiscoveryServiceClient(hc, strings.TrimRight(rawURL, "/"))
	req := connect.NewRequest(&discoveryv1.DiscoveryRequest{ProtocolVersion: protocolVersion})
	if signer != nil {
		tok, serr := signer.Sign(rawURL)
		if serr != nil {
			return nil, fmt.Errorf("sign discover: %w", serr)
		}
		req.Header().Set("Authorization", "Bearer "+tok)
	}
	resp, err := client.Discover(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
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
