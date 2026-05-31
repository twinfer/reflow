// Package config implements reflow's Config Connect RPC surface —
// the app-config side of the admin port. It owns the per-RPC business
// logic for handler deployments, event sources, webhook sources, and
// the named-secret table consumers reference. Cluster topology, DR,
// and routing live in internal/clusterctl under a parallel Connect
// service.
//
// Every mutating RPC translates into a shard-0 Raft proposal via
// MetadataRunner.Proposer().ProposeSelfCAS, so all calls must reach
// the metadata leader. Non-leader nodes return CodeUnavailable with a
// configv1.LeaderHint detail attached;
// pkg/reflowclient.CallWithLeaderRedirect is the canonical retry
// helper.
//
// AutoSeed entry points (AutoSeed for deployments, AutoSeedEventSource
// for event sources) are leader-gate-less in-process helpers used by
// pkg/reflow.Run during cold start to seed config from the bootstrap
// koanf source.
package config

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	connect "connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/twinfer/reflow/internal/authz"
	"github.com/twinfer/reflow/internal/certmgr"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	configv1 "github.com/twinfer/reflow/proto/configv1"
	"github.com/twinfer/reflow/proto/configv1/configv1connect"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	"github.com/twinfer/reflow/proto/discoveryv1/discoveryv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// protocolVersion is the wire-protocol version this engine speaks; the
// handler-side discovery response must advertise the same string.
const protocolVersion = "v1"

// Server implements configv1connect.ConfigHandler.
type Server struct {
	configv1connect.UnimplementedConfigHandler

	host   *engine.Host
	runner *engine.MetadataRunner
	log    *slog.Logger
	signer handlerclient.Signer
	// operatorIssuer signs IssueOperator CSRs against the active cluster
	// CA. Optional: when nil (e.g. before a CA root exists), the
	// IssueOperator RPC returns FailedPrecondition.
	operatorIssuer *certmgr.ClusterIssuer

	adminCallTimeout time.Duration
}

// Config groups the constructor inputs.
type Config struct {
	Host   *engine.Host
	Runner *engine.MetadataRunner
	Log    *slog.Logger
	// Signer, when non-nil, mints a JWT on outgoing
	// DiscoveryService.Discover requests during RegisterDeployment.
	Signer handlerclient.Signer
	// OperatorIssuer, when non-nil, enables the IssueOperator RPC. Used
	// to sign operator-supplied CSRs against the active cluster CA.
	OperatorIssuer *certmgr.ClusterIssuer
}

// NewServer constructs the Config server.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Host == nil || cfg.Runner == nil {
		return nil, errors.New("config: Host and Runner are required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Server{
		host:             cfg.Host,
		runner:           cfg.Runner,
		log:              cfg.Log,
		signer:           cfg.Signer,
		operatorIssuer:   cfg.OperatorIssuer,
		adminCallTimeout: 30 * time.Second,
	}, nil
}

// NewHandler returns the path + http.Handler pair to mount on a
// connectserver. opts is forwarded to the generated handler.
func (s *Server) NewHandler(opts ...connect.HandlerOption) (string, http.Handler) {
	return configv1connect.NewConfigHandler(s, opts...)
}

// SetOperatorIssuer attaches a ClusterIssuer for the IssueOperator RPC.
// Late-bound because the issuer requires shard 0 to have an active CA
// row, which the operator may populate after the listener comes up.
// Calling with nil disables IssueOperator (returns FailedPrecondition).
func (s *Server) SetOperatorIssuer(issuer *certmgr.ClusterIssuer) {
	s.operatorIssuer = issuer
}

// requireLeader returns CodeUnavailable when this node is not the
// metadata leader, attaching a LeaderHint detail so clients can
// redirect.
func (s *Server) requireLeader() error {
	if s.runner.IsLeader() {
		return nil
	}
	hintID, _ := s.host.PartitionLeaderHint(0)
	cerr := connect.NewError(connect.CodeUnavailable,
		fmt.Errorf("config: not the metadata leader (hint=%d)", hintID))
	if hintID != 0 {
		if addr, ok := s.host.NodeAdminEndpoint(hintID); ok {
			if d, derr := connect.NewErrorDetail(&configv1.LeaderHint{
				NodeId:        hintID,
				AdminEndpoint: addr,
			}); derr == nil {
				cerr.AddDetail(d)
			}
		}
	}
	return cerr
}

// proposeCAS wraps RaftProposer.ProposeSelfCAS, translating the
// FSM-side ResultValueFailedPrecondition sentinel into a connect-coded
// CodeFailedPrecondition error so callers see the CAS conflict
// uniformly across all CAS-aware Config RPCs.
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
			fmt.Errorf("config: table revision changed; re-read and retry"))
	}
	return nil
}

// RegisterDeployment accepts a remote-handler URL, dials its discovery
// endpoint, and proposes Command_RegisterDeployment to shard 0.
func (s *Server) RegisterDeployment(ctx context.Context, req *connect.Request[configv1.RegisterDeploymentRequest]) (*connect.Response[configv1.RegisterDeploymentResponse], error) {
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
func (s *Server) registerDeployment(ctx context.Context, req *configv1.RegisterDeploymentRequest) (*configv1.RegisterDeploymentResponse, error) {
	raw := req.GetUrl()
	if raw == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: url required"))
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: parse url: %w", err))
	}
	scheme := strings.ToLower(u.Scheme)
	if err := validateDeploymentScheme(scheme); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: %w", err))
	}

	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()

	resp, err := discoverConnect(callCtx, raw, scheme == "http", s.signer)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: discovery: %w", err))
	}
	if got := resp.GetProtocolVersion(); got != protocolVersion {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("config: handler advertised protocol %q; engine speaks %q", got, protocolVersion))
	}

	deploymentID := uuid.NewString()
	rec := &enginev1.DeploymentRecord{
		Id:                    deploymentID,
		Url:                   raw,
		RegisteredAtMs:        uint64(time.Now().UnixMilli()),
		MaxJournalEntries:     req.GetMaxJournalEntries(),
		InvocationRetentionMs: req.GetInvocationRetentionMs(),
		WorkflowRetentionMs:   req.GetWorkflowRetentionMs(),
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
			fmt.Errorf("config: propose RegisterDeployment: %w", err))
	}
	return &configv1.RegisterDeploymentResponse{DeploymentId: deploymentID}, nil
}

// ListDeployments returns every DeploymentRecord on shard 0 plus the
// deployment table's CAS revision. SyncRead — any peer can serve.
func (s *Server) ListDeployments(ctx context.Context, _ *connect.Request[configv1.ListDeploymentsRequest]) (*connect.Response[configv1.ListDeploymentsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Deployments(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read deployments: %w", err))
	}
	return connect.NewResponse(&configv1.ListDeploymentsResponse{
		Deployments:   list.Records,
		TableRevision: list.TableRevision,
	}), nil
}

// DescribeDeployment returns one DeploymentRecord by id. CodeNotFound
// when no deployment claims the id.
func (s *Server) DescribeDeployment(ctx context.Context, req *connect.Request[configv1.DescribeDeploymentRequest]) (*connect.Response[configv1.DescribeDeploymentResponse], error) {
	id := req.Msg.GetDeploymentId()
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: deployment_id required"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	rec, err := s.host.Deployment(callCtx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read deployment %q: %w", id, err))
	}
	if rec == nil {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("config: deployment %q not found", id))
	}
	return connect.NewResponse(&configv1.DescribeDeploymentResponse{Deployment: rec}), nil
}

// DeleteDeployment removes one DeploymentRecord and evicts any
// (service, handler) → id index entries that pointed to it. Refuses
// without force=true — deletion may break in-flight invocations
// pinned to this deployment; force is the operator's acknowledgement
// of the risk.
func (s *Server) DeleteDeployment(ctx context.Context, req *connect.Request[configv1.DeleteDeploymentRequest]) (*connect.Response[configv1.DeleteDeploymentResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	id := req.Msg.GetDeploymentId()
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: deployment_id required"))
	}
	if !req.Msg.GetForce() {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("config: refusing to delete deployment %q without force=true; "+
				"in-flight invocations resolving this deployment will break", id))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteDeployment{
			DeleteDeployment: &enginev1.DeleteDeployment{Id: id},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readDeploymentRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.DeleteDeploymentResponse{TableRevision: newRev}), nil
}

// readDeploymentRevision is a SyncRead helper used by Delete to echo
// the post-apply revision back to the operator.
func (s *Server) readDeploymentRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.Deployments(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// AutoSeed is the in-process registration path used by pkg/reflow's
// autoSeedEndpoints and by engine integration tests. Same body as
// RegisterDeployment minus the leader gate (callers wait for
// leadership themselves). budget=0 → engine default.
func (s *Server) AutoSeed(ctx context.Context, url string) (string, error) {
	return s.AutoSeedWithBudget(ctx, url, 0)
}

// AutoSeedWithBudget mirrors AutoSeed and additionally stamps a per-
// invocation step-budget override onto the deployment record.
func (s *Server) AutoSeedWithBudget(ctx context.Context, url string, budget uint32) (string, error) {
	resp, err := s.registerDeployment(ctx, &configv1.RegisterDeploymentRequest{
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

// UpsertClusterAuthzPolicy validates policy_text against the Cedar schema
// (layer 1) and, on success, proposes it as the cluster-wide
// PlatformConfigRecord. An invalid policy is rejected at upload
// (InvalidArgument) and never installed, so a typo can't silently swap in a
// deny-everything policy on the next reconcile. CAS via if_table_revision_eq.
func (s *Server) UpsertClusterAuthzPolicy(ctx context.Context, req *connect.Request[configv1.UpsertClusterAuthzPolicyRequest]) (*connect.Response[configv1.UpsertClusterAuthzPolicyResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	text := req.Msg.GetPolicyText()
	if text == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: policy_text required"))
	}
	if err := authz.ValidateClusterPolicy([]byte(text)); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: cluster authz policy failed validation: %w", err))
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
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.UpsertClusterAuthzPolicyResponse{TableRevision: res.TableRevision}), nil
}

// GetClusterAuthzPolicy returns the current cluster authz policy text + the
// platform-config table revision. When no policy has been uploaded the row is
// empty, so it returns the in-binary foundational policy — the effective
// default the engine runs until an operator overrides it — with revision 0.
// No leader gate: SyncRead serves from the local shard-0 replica.
func (s *Server) GetClusterAuthzPolicy(ctx context.Context, _ *connect.Request[configv1.GetClusterAuthzPolicyRequest]) (*connect.Response[configv1.GetClusterAuthzPolicyResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	res, err := s.host.ClusterAuthzPolicy(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read cluster authz policy: %w", err))
	}
	text := authz.FoundationalClusterPolicies
	if res.Record != nil && res.Record.GetClusterAuthzPolicyText() != "" {
		text = res.Record.GetClusterAuthzPolicyText()
	}
	return connect.NewResponse(&configv1.GetClusterAuthzPolicyResponse{
		PolicyText:    text,
		TableRevision: res.TableRevision,
	}), nil
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

// UpsertSecret validates the record then proposes Command_UpsertSecret
// with the operator-supplied CAS guard. Returns the post-apply revision.
func (s *Server) UpsertSecret(ctx context.Context, req *connect.Request[configv1.UpsertSecretRequest]) (*connect.Response[configv1.UpsertSecretResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: record required"))
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
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.UpsertSecretResponse{TableRevision: newRev}), nil
}

// DeleteSecret removes the named row. CAS via if_table_revision_eq.
//
// Does NOT validate consumer references — webhook (and future) rows
// that name this secret will fail to resolve on next reconcile and
// preserve-prev. Operators see the metric and clean up.
func (s *Server) DeleteSecret(ctx context.Context, req *connect.Request[configv1.DeleteSecretRequest]) (*connect.Response[configv1.DeleteSecretResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: name required"))
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
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.DeleteSecretResponse{TableRevision: newRev}), nil
}

// ListSecrets returns every SecretRecord plus the table's current CAS
// revision. No leader gate.
func (s *Server) ListSecrets(ctx context.Context, _ *connect.Request[configv1.ListSecretsRequest]) (*connect.Response[configv1.ListSecretsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Secrets(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read secrets: %w", err))
	}
	return connect.NewResponse(&configv1.ListSecretsResponse{
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

// UpsertCARoot validates the record then proposes Command_UpsertCARoot
// with the operator-supplied CAS guard. Returns the post-apply revision.
func (s *Server) UpsertCARoot(ctx context.Context, req *connect.Request[configv1.UpsertCARootRequest]) (*connect.Response[configv1.UpsertCARootResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: record required"))
	}
	if err := validateCARootRecord(rec); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertCaRoot{
			UpsertCaRoot: &enginev1.UpsertCARoot{Record: rec},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readCARootRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.UpsertCARootResponse{TableRevision: newRev}), nil
}

// DeleteCARoot removes the named row. CAS via if_table_revision_eq.
// Deletes the active row are accepted: consumers (the per-node
// ClusterIssuer) preserve their in-memory snapshot on resolve error so
// in-flight handshakes keep working; renewals fail until a new row
// lands.
func (s *Server) DeleteCARoot(ctx context.Context, req *connect.Request[configv1.DeleteCARootRequest]) (*connect.Response[configv1.DeleteCARootResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: name required"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteCaRoot{
			DeleteCaRoot: &enginev1.DeleteCARoot{Name: name},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readCARootRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.DeleteCARootResponse{TableRevision: newRev}), nil
}

// ListCARoots returns every CARootRecord plus the table's current CAS
// revision. No leader gate.
func (s *Server) ListCARoots(ctx context.Context, _ *connect.Request[configv1.ListCARootsRequest]) (*connect.Response[configv1.ListCARootsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.CARoots(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read caroots: %w", err))
	}
	return connect.NewResponse(&configv1.ListCARootsResponse{
		Records:       list.Records,
		TableRevision: list.TableRevision,
	}), nil
}

// readCARootRevision is a SyncRead helper used by Upsert/Delete.
func (s *Server) readCARootRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.CARoots(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// auditLogHardLimit caps the per-request scan even when the caller
// asks for no limit (request.limit == 0). Keeps one operator query
// from monopolizing the shard-0 SyncRead — operators with deeper
// queries narrow the time window and paginate.
// validateCARootRecord enforces shape rules on a CARootRecord. The
// signing key is NOT loaded here: the per-node ClusterIssuer surfaces
// resolve errors via reflow_pki_ca_sign_errors_total, and coupling
// admin RPC availability to KMS+blob reachability would be wrong.
func validateCARootRecord(rec *enginev1.CARootRecord) error {
	if rec.GetName() == "" {
		return errors.New("name is required")
	}
	if len(rec.GetCertPem()) == 0 {
		return errors.New("cert_pem is required")
	}
	if rec.GetKeySecretName() == "" {
		return errors.New("key_secret_name is required")
	}
	if rec.GetFingerprint() == "" {
		return errors.New("fingerprint is required")
	}
	return nil
}

// validateSecretRecord enforces shape rules on a SecretRecord. No
// decrypt attempt — coupling admin RPC availability to KMS+blob
// reachability is the wrong trade-off; the SecretStore reconciler
// surfaces resolve errors via reflow_secretstore_decrypt_errors_total.
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
