// Package admin implements reflw's Admin Connect RPC surface — the unified
// operator/admin port. It merges the former internal/config (app config:
// deployments, models, secrets, cluster authz policy) and internal/clusterctl
// (cluster topology, DR snapshots, LP routing transfers, rebalance) behind one
// reflw.admin.v1.Admin service.
//
// Every mutating RPC translates into a shard-0 Raft proposal via
// MetadataRunner.Proposer, so all such calls must reach the metadata leader;
// non-leader nodes return CodeUnavailable with an adminv1.LeaderHint detail
// (pkg/reflwclient.CallWithLeaderRedirect follows it). Read RPCs SyncRead the
// local shard-0 replica and map enginev1 records to apiv1 view DTOs via
// internal/apimap, so the admin surface never returns the Raft/on-disk wire
// format.
//
// AutoSeed entry points are leader-gate-less in-process helpers used by
// pkg/reflw.Run during cold start to seed deployments from the bootstrap config.
package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/authz"
	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/engine/cluster"
	"github.com/twinfer/reflw/internal/engine/handlerclient"
	"github.com/twinfer/reflw/internal/engine/rebalance"
	"github.com/twinfer/reflw/internal/engine/snapshot"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	"github.com/twinfer/reflw/proto/adminv1/adminv1connect"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// Server implements adminv1connect.AdminHandler.
type Server struct {
	adminv1connect.UnimplementedAdminHandler

	host   *engine.Host
	runner *engine.MetadataRunner
	log    *slog.Logger

	// signer, when non-nil, mints a JWT on outgoing DiscoveryService.Discover
	// requests during RegisterDeployment.
	signer handlerclient.Signer
	// planModelSet validates a RegisterModelSet against the existing ModelTable
	// and returns the ModelRecords (with derived bundles) to write. Never nil
	// after NewServer (falls back to shallowPlanModelSet).
	planModelSet PlanModelSetFunc

	// repo/src/scratchDir back the snapshot RPCs; nil repo/src → snapshot
	// endpoints return FailedPrecondition.
	repo       snapshot.Repository
	src        snapshot.Source
	scratchDir string
	// rebalance configures the autonomous LP rebalancer's advice path.
	rebalance rebalance.Config

	adminCallTimeout time.Duration
}

// Config groups the constructor inputs (union of the former config.Config and
// clusterctl.Config).
type Config struct {
	Host   *engine.Host
	Runner *engine.MetadataRunner
	Log    *slog.Logger
	// Signer, when non-nil, mints a JWT on outgoing discovery requests.
	Signer handlerclient.Signer
	// PlanModelSet, when non-nil, validates a RegisterModelSet and derives each
	// model's bundle. nil falls back to a shallow well-formed-XML check.
	PlanModelSet PlanModelSetFunc
	// Repo/Source/ScratchDir back the snapshot RPCs.
	Repo       snapshot.Repository
	Source     snapshot.Source
	ScratchDir string
	// Rebalance is the autonomous LP rebalancer's configuration used by the
	// RebalanceAdvise RPC. Zero value renders Advise as "mode=off".
	Rebalance rebalance.Config
}

// NewServer constructs the Admin server.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Host == nil || cfg.Runner == nil {
		return nil, errors.New("admin: Host and Runner are required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	planModelSet := cfg.PlanModelSet
	if planModelSet == nil {
		planModelSet = shallowPlanModelSet
	}
	if cfg.ScratchDir == "" {
		cfg.ScratchDir = filepath.Join(os.TempDir(), "reflw-admin-scratch")
	}
	if err := os.MkdirAll(cfg.ScratchDir, 0o755); err != nil {
		return nil, fmt.Errorf("admin: scratch dir: %w", err)
	}
	return &Server{
		host:             cfg.Host,
		runner:           cfg.Runner,
		log:              cfg.Log,
		signer:           cfg.Signer,
		planModelSet:     planModelSet,
		repo:             cfg.Repo,
		src:              cfg.Source,
		scratchDir:       cfg.ScratchDir,
		rebalance:        cfg.Rebalance,
		adminCallTimeout: 30 * time.Second,
	}, nil
}

// NewHandler returns the path + http.Handler pair to mount on a connectserver.
// opts is forwarded to the generated handler.
func (s *Server) NewHandler(opts ...connect.HandlerOption) (string, http.Handler) {
	return adminv1connect.NewAdminHandler(s, opts...)
}

// requireLeader returns CodeUnavailable when this node is not the metadata
// leader, attaching a LeaderHint detail (node_id + admin_endpoint resolved via
// gossip) so clients can redirect via pkg/reflwclient.CallWithLeaderRedirect.
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

// proposeCAS wraps RaftProposer.ProposeSelfCAS, translating the FSM-side
// ResultValueFailedPrecondition sentinel into a connect-coded
// CodeFailedPrecondition error so callers see the CAS conflict uniformly.
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

// UpsertClusterAuthzPolicy validates policy_text against the Cedar schema
// (layer 1) and, on success, proposes it as the cluster-wide
// PlatformConfigRecord. An invalid policy is rejected at upload and never
// installed. CAS via if_table_revision_eq.
func (s *Server) UpsertClusterAuthzPolicy(ctx context.Context, req *connect.Request[adminv1.UpsertClusterAuthzPolicyRequest]) (*connect.Response[adminv1.UpsertClusterAuthzPolicyResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	text := req.Msg.GetPolicyText()
	if text == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("admin: policy_text required"))
	}
	if err := authz.ValidateClusterPolicy([]byte(text)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("admin: cluster authz policy failed validation: %w", err))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertPlatformConfig{
			UpsertPlatformConfig: &enginev1.UpsertPlatformConfig{
				Record: &enginev1.PlatformConfigRecord{ClusterAuthzPolicyText: text},
			},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	res, err := s.host.ClusterAuthzPolicy(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.UpsertClusterAuthzPolicyResponse{TableRevision: res.TableRevision}), nil
}

// GetClusterAuthzPolicy returns the current cluster authz policy text + the
// platform-config table revision. When no policy has been uploaded it returns
// the in-binary foundational policy with revision 0. No leader gate.
func (s *Server) GetClusterAuthzPolicy(ctx context.Context, _ *connect.Request[adminv1.GetClusterAuthzPolicyRequest]) (*connect.Response[adminv1.GetClusterAuthzPolicyResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	res, err := s.host.ClusterAuthzPolicy(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read cluster authz policy: %w", err))
	}
	text := authz.FoundationalClusterPolicies
	if res.Record != nil && res.Record.GetClusterAuthzPolicyText() != "" {
		text = res.Record.GetClusterAuthzPolicyText()
	}
	return connect.NewResponse(&adminv1.GetClusterAuthzPolicyResponse{
		PolicyText:    text,
		TableRevision: res.TableRevision,
	}), nil
}
