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
	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/engine/snapshot"
	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/webhook"
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

// CreateSnapshot delegates to snapshot.SnapshotOnce. Leader-only: every
// node has its own local store, so the leader's snapshot is the only one
// guaranteed to contain the latest applied state, and writes to the
// shared blob repo from multiple nodes would race.
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

// DeleteSnapshot removes (shard_id, index) from the configured
// repository. Idempotent — succeeds when the snapshot is already absent.
// Leader-only: serializing through the leader keeps concurrent reaper /
// admin deletes from racing on the shared blob repo. ListSnapshots is
// intentionally not gated — read-only, served by any node.
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

// proposeCAS wraps RaftProposer.ProposeSelfCAS, translating the
// FSM-side ResultValueFailedPrecondition sentinel into a connect-coded
// CodeFailedPrecondition error so callers see the CAS conflict
// uniformly across all CAS-aware Admin RPCs.
func (s *Server) proposeCAS(ctx context.Context, cmd *enginev1.Command, ifRev uint64) error {
	var pre *enginev1.Precondition
	if ifRev != 0 {
		pre = &enginev1.Precondition{IfTableRevisionEq: ifRev}
	}
	val, err := s.runner.Proposer().ProposeSelfCAS(ctx, cmd, pre)
	if err != nil {
		return err
	}
	if val == cluster.ResultValueFailedPrecondition {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("admin: table revision changed; re-read and retry"))
	}
	return nil
}

// UpsertEventSource inserts or replaces one row in shard 0's
// EventSourceTable. Validates the record, then proposes
// Command_UpsertEventSource with the operator-supplied CAS guard. After
// the proposal lands, returns the new table revision (re-reads via
// SyncRead — the FSM bumps the revision on apply, but the proposer
// has no direct view of post-apply state).
func (s *Server) UpsertEventSource(ctx context.Context, req *connect.Request[adminv1.UpsertEventSourceRequest]) (*connect.Response[adminv1.UpsertEventSourceResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: record required"))
	}
	if err := validateEventSourceRecord(rec); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertEventSource{
			UpsertEventSource: &enginev1.UpsertEventSource{Record: rec},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readEventSourceRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.UpsertEventSourceResponse{TableRevision: newRev}), nil
}

// DeleteEventSource removes the named row. CAS via if_table_revision_eq.
// Delete-of-absent still bumps the revision so the operator's CLI sees
// the proposal landed.
func (s *Server) DeleteEventSource(ctx context.Context, req *connect.Request[adminv1.DeleteEventSourceRequest]) (*connect.Response[adminv1.DeleteEventSourceResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: name required"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteEventSource{
			DeleteEventSource: &enginev1.DeleteEventSource{Name: name},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readEventSourceRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.DeleteEventSourceResponse{TableRevision: newRev}), nil
}

// ListEventSources returns every EventSourceRecord plus the table's
// current CAS revision. No leader gate — SyncRead routes to the local
// shard-0 replica, so any peer can serve.
func (s *Server) ListEventSources(ctx context.Context, _ *connect.Request[adminv1.ListEventSourcesRequest]) (*connect.Response[adminv1.ListEventSourcesResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.EventSources(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read event sources: %w", err))
	}
	return connect.NewResponse(&adminv1.ListEventSourcesResponse{
		Sources:       list.Sources,
		TableRevision: list.TableRevision,
	}), nil
}

// readEventSourceRevision is a SyncRead helper used by Upsert/Delete to
// echo the post-apply revision back to the operator.
func (s *Server) readEventSourceRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.EventSources(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// AutoSeedEventSource is the in-process bootstrap entrypoint for
// pkg/reflow/run.go's seed loop. Same body as UpsertEventSource minus
// the leader gate (callers wait for leadership themselves) and pinned
// to if_table_revision_eq=0 (only succeeds when the table is empty,
// so a racing operator-Apply at the same instant gets the conflict
// instead of silently losing).
func (s *Server) AutoSeedEventSource(ctx context.Context, rec *enginev1.EventSourceRecord) error {
	if rec == nil {
		return errors.New("admin: record required")
	}
	if err := validateEventSourceRecord(rec); err != nil {
		return err
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertEventSource{
			UpsertEventSource: &enginev1.UpsertEventSource{Record: rec},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	return s.proposeCAS(callCtx, cmd, 0)
}

// validateEventSourceRecord enforces the same minimum-field rules the
// in-process Manager applies to a koanf-loaded SourceConfig. The
// factory-type check is the only one that can change at runtime (an
// operator could `import _ "custom/factory"` from their handler binary),
// so it lives here rather than in the proto definition.
func validateEventSourceRecord(rec *enginev1.EventSourceRecord) error {
	if rec.GetName() == "" {
		return errors.New("name is required")
	}
	if rec.GetType() == "" {
		return errors.New("type is required")
	}
	if !eventsource.HasFactory(rec.GetType()) {
		return fmt.Errorf("unknown backend type %q (registered: %v)",
			rec.GetType(), eventsource.RegisteredTypes())
	}
	if rec.GetTopic() == "" {
		return errors.New("topic is required")
	}
	if rec.GetService() == "" {
		return errors.New("service is required")
	}
	if rec.GetHandler() == "" {
		return errors.New("handler is required")
	}
	return nil
}

// UpsertWebhookSource validates the record (incl. path uniqueness
// across other rows), then proposes Command_UpsertWebhookSource with
// the operator-supplied CAS guard. Returns the post-apply revision.
func (s *Server) UpsertWebhookSource(ctx context.Context, req *connect.Request[adminv1.UpsertWebhookSourceRequest]) (*connect.Response[adminv1.UpsertWebhookSourceResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: record required"))
	}
	if err := validateWebhookSourceRecord(rec); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.checkWebhookPathUnique(callCtx, rec); err != nil {
		return nil, err
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertWebhookSource{
			UpsertWebhookSource: &enginev1.UpsertWebhookSource{Record: rec},
		},
	}
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readWebhookSourceRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.UpsertWebhookSourceResponse{TableRevision: newRev}), nil
}

// DeleteWebhookSource removes the named row. CAS via if_table_revision_eq.
func (s *Server) DeleteWebhookSource(ctx context.Context, req *connect.Request[adminv1.DeleteWebhookSourceRequest]) (*connect.Response[adminv1.DeleteWebhookSourceResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: name required"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteWebhookSource{
			DeleteWebhookSource: &enginev1.DeleteWebhookSource{Name: name},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readWebhookSourceRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.DeleteWebhookSourceResponse{TableRevision: newRev}), nil
}

// ListWebhookSources returns every WebhookSourceRecord plus the
// table's current CAS revision. No leader gate.
func (s *Server) ListWebhookSources(ctx context.Context, _ *connect.Request[adminv1.ListWebhookSourcesRequest]) (*connect.Response[adminv1.ListWebhookSourcesResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.WebhookSources(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read webhook sources: %w", err))
	}
	return connect.NewResponse(&adminv1.ListWebhookSourcesResponse{
		Sources:       list.Sources,
		TableRevision: list.TableRevision,
	}), nil
}

// readWebhookSourceRevision is a SyncRead helper used by Upsert/Delete.
func (s *Server) readWebhookSourceRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.WebhookSources(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// AutoSeedWebhookSource is the in-process bootstrap entrypoint for
// pkg/reflow/run.go's seed loop. Same body as UpsertWebhookSource
// minus the leader gate, pinned to if_table_revision_eq=0.
func (s *Server) AutoSeedWebhookSource(ctx context.Context, rec *enginev1.WebhookSourceRecord) error {
	if rec == nil {
		return errors.New("admin: record required")
	}
	if err := validateWebhookSourceRecord(rec); err != nil {
		return err
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertWebhookSource{
			UpsertWebhookSource: &enginev1.UpsertWebhookSource{Record: rec},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	return s.proposeCAS(callCtx, cmd, 0)
}

// validateWebhookSourceRecord enforces shape rules; path uniqueness is
// checked separately via checkWebhookPathUnique because it needs a
// fresh SyncRead and is therefore expensive to inline.
func validateWebhookSourceRecord(rec *enginev1.WebhookSourceRecord) error {
	if rec.GetName() == "" {
		return errors.New("name is required")
	}
	path := rec.GetPath()
	if path == "" {
		return errors.New("path is required")
	}
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must start with '/' (got %q)", path)
	}
	if rec.GetVerifier() == "" {
		return errors.New("verifier is required")
	}
	if _, err := webhook.LookupVerifier(rec.GetVerifier()); err != nil {
		return fmt.Errorf("unknown verifier %q (registered: %v)",
			rec.GetVerifier(), webhook.RegisteredNames())
	}
	if rec.GetService() == "" {
		return errors.New("service is required")
	}
	if rec.GetHandler() == "" {
		return errors.New("handler is required")
	}
	if rec.GetSecretName() == "" {
		return errors.New("secret_name is required (reference a row in the SecretTable via UpsertSecret)")
	}
	// Existence-check of the named secret is intentionally NOT done here:
	// the admin RPC and SecretStore reconciler can race against fresh
	// clusters where webhook upsert lands before the secret upsert.
	// Resolve failure on next reconcile surfaces via metrics + log.
	return nil
}

// hasKnownBlobScheme returns true when uri starts with a gocloud.dev/blob
// scheme that Reflow links by default. Operators registering additional
// schemes from their main package can extend this list — keeping the
// allowlist explicit avoids accidental acceptance of garbage URIs.
func hasKnownBlobScheme(uri string) bool {
	for _, p := range []string{"s3://", "gs://", "azblob://", "file://", "mem://"} {
		if strings.HasPrefix(uri, p) {
			return true
		}
	}
	return false
}

// checkWebhookPathUnique returns a CodeAlreadyExists error when some
// other named row claims the same path. The check is best-effort: a
// concurrent operator-Apply against the same revision can race, but the
// CAS guard closes the window from each operator's perspective and the
// per-node Reconciler's sort-by-name + drop-on-collision keeps local
// state identical across nodes even if a duplicate slipped through.
func (s *Server) checkWebhookPathUnique(ctx context.Context, rec *enginev1.WebhookSourceRecord) error {
	list, err := s.host.WebhookSources(ctx)
	if err != nil {
		return connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read webhook sources: %w", err))
	}
	for _, existing := range list.Sources {
		if existing.GetName() == rec.GetName() {
			continue
		}
		if existing.GetPath() == rec.GetPath() {
			return connect.NewError(connect.CodeAlreadyExists,
				fmt.Errorf("path %q already in use by webhook %q",
					rec.GetPath(), existing.GetName()))
		}
	}
	return nil
}

// UpsertSecret validates the record then proposes Command_UpsertSecret
// with the operator-supplied CAS guard. Returns the post-apply revision.
func (s *Server) UpsertSecret(ctx context.Context, req *connect.Request[adminv1.UpsertSecretRequest]) (*connect.Response[adminv1.UpsertSecretResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: record required"))
	}
	if err := validateSecretRecord(rec); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertSecret{
			UpsertSecret: &enginev1.UpsertSecret{Record: rec},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readSecretRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.UpsertSecretResponse{TableRevision: newRev}), nil
}

// DeleteSecret removes the named row. CAS via if_table_revision_eq.
//
// Does NOT validate consumer references — webhook (and future) rows
// that name this secret will fail to resolve on next reconcile and
// preserve-prev. Operators see the metric and clean up.
func (s *Server) DeleteSecret(ctx context.Context, req *connect.Request[adminv1.DeleteSecretRequest]) (*connect.Response[adminv1.DeleteSecretResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: name required"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteSecret{
			DeleteSecret: &enginev1.DeleteSecret{Name: name},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readSecretRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.DeleteSecretResponse{TableRevision: newRev}), nil
}

// ListSecrets returns every SecretRecord plus the table's current CAS
// revision. No leader gate.
func (s *Server) ListSecrets(ctx context.Context, _ *connect.Request[adminv1.ListSecretsRequest]) (*connect.Response[adminv1.ListSecretsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Secrets(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read secrets: %w", err))
	}
	return connect.NewResponse(&adminv1.ListSecretsResponse{
		Records:       list.Records,
		TableRevision: list.TableRevision,
	}), nil
}

// readSecretRevision is a SyncRead helper used by Upsert/Delete.
func (s *Server) readSecretRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.Secrets(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// AutoSeedSecret is the in-process bootstrap entrypoint mirroring
// AutoSeedWebhookSource — pinned to if_table_revision_eq=0. Used by
// tests; pkg/reflow has no koanf-bootstrap path for secrets.
func (s *Server) AutoSeedSecret(ctx context.Context, rec *enginev1.SecretRecord) error {
	if rec == nil {
		return errors.New("admin: record required")
	}
	if err := validateSecretRecord(rec); err != nil {
		return err
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertSecret{
			UpsertSecret: &enginev1.UpsertSecret{Record: rec},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	return s.proposeCAS(callCtx, cmd, 0)
}

// validateSecretRecord enforces shape rules on a SecretRecord.
// Mirror of validateWebhookSourceRecord. No decrypt attempt — coupling
// admin RPC availability to KMS+blob reachability is the wrong
// trade-off; the SecretStore reconciler surfaces resolve errors via
// reflow_secretstore_decrypt_errors_total.
func validateSecretRecord(rec *enginev1.SecretRecord) error {
	if rec.GetName() == "" {
		return errors.New("name is required")
	}
	src := rec.GetRemoteEncrypted()
	if src == nil {
		return errors.New("source.remote_encrypted is required")
	}
	if src.GetBlobUri() == "" {
		return errors.New("remote_encrypted.blob_uri must be non-empty")
	}
	if !hasKnownBlobScheme(src.GetBlobUri()) {
		return fmt.Errorf("remote_encrypted.blob_uri %q has unknown scheme (want s3://, gs://, azblob://, file://, or mem://)", src.GetBlobUri())
	}
	if src.GetKekUri() == "" {
		return errors.New("remote_encrypted.kek_uri must be non-empty")
	}
	return nil
}
