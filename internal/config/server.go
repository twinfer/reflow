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

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/ingress/eventsource"
	"github.com/twinfer/reflow/pkg/webhook"
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
		adminCallTimeout: 30 * time.Second,
	}, nil
}

// NewHandler returns the path + http.Handler pair to mount on a
// connectserver. opts is forwarded to the generated handler.
func (s *Server) NewHandler(opts ...connect.HandlerOption) (string, http.Handler) {
	return configv1connect.NewConfigHandler(s, opts...)
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

// UpsertEventSource inserts or replaces one row in shard 0's
// EventSourceTable. Validates the record, then proposes
// Command_UpsertEventSource with the operator-supplied CAS guard. After
// the proposal lands, returns the new table revision (re-reads via
// SyncRead — the FSM bumps the revision on apply, but the proposer
// has no direct view of post-apply state).
func (s *Server) UpsertEventSource(ctx context.Context, req *connect.Request[configv1.UpsertEventSourceRequest]) (*connect.Response[configv1.UpsertEventSourceResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: record required"))
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
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.UpsertEventSourceResponse{TableRevision: newRev}), nil
}

// DeleteEventSource removes the named row. CAS via if_table_revision_eq.
// Delete-of-absent still bumps the revision so the operator's CLI sees
// the proposal landed.
func (s *Server) DeleteEventSource(ctx context.Context, req *connect.Request[configv1.DeleteEventSourceRequest]) (*connect.Response[configv1.DeleteEventSourceResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: name required"))
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
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.DeleteEventSourceResponse{TableRevision: newRev}), nil
}

// ListEventSources returns every EventSourceRecord plus the table's
// current CAS revision. No leader gate — SyncRead routes to the local
// shard-0 replica, so any peer can serve.
func (s *Server) ListEventSources(ctx context.Context, _ *connect.Request[configv1.ListEventSourcesRequest]) (*connect.Response[configv1.ListEventSourcesResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.EventSources(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read event sources: %w", err))
	}
	return connect.NewResponse(&configv1.ListEventSourcesResponse{
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
		return errors.New("config: record required")
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
	backend := eventsource.BackendConfig{}
	if b := rec.GetBackend(); b != nil {
		backend.Settings = b.GetSettings()
	}
	if err := eventsource.Validate(rec.GetType(), rec.GetTopic(), backend); err != nil {
		return err
	}
	return nil
}

// UpsertWebhookSource validates the record (incl. path uniqueness
// across other rows), then proposes Command_UpsertWebhookSource with
// the operator-supplied CAS guard. Returns the post-apply revision.
func (s *Server) UpsertWebhookSource(ctx context.Context, req *connect.Request[configv1.UpsertWebhookSourceRequest]) (*connect.Response[configv1.UpsertWebhookSourceResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: record required"))
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
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.UpsertWebhookSourceResponse{TableRevision: newRev}), nil
}

// DeleteWebhookSource removes the named row. CAS via if_table_revision_eq.
func (s *Server) DeleteWebhookSource(ctx context.Context, req *connect.Request[configv1.DeleteWebhookSourceRequest]) (*connect.Response[configv1.DeleteWebhookSourceResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: name required"))
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
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.DeleteWebhookSourceResponse{TableRevision: newRev}), nil
}

// ListWebhookSources returns every WebhookSourceRecord plus the
// table's current CAS revision. No leader gate.
func (s *Server) ListWebhookSources(ctx context.Context, _ *connect.Request[configv1.ListWebhookSourcesRequest]) (*connect.Response[configv1.ListWebhookSourcesResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.WebhookSources(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read webhook sources: %w", err))
	}
	return connect.NewResponse(&configv1.ListWebhookSourcesResponse{
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
			fmt.Errorf("config: read webhook sources: %w", err))
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

// UpsertTenant inserts or updates a TenantRecord. Pre-allocates
// record.id when zero by reading the current TenantList: if a row with
// the requested name already exists, the server reuses its id (update
// path); otherwise it picks max(existing.id)+1, starting at 1 (id=0 is
// the reserved default-tenant sentinel). The atomic ListTenants read
// gives the table_revision used for CAS so a racing operator's edit
// reproducibly conflicts. Returns the assigned id and the post-apply
// revision.
func (s *Server) UpsertTenant(ctx context.Context, req *connect.Request[configv1.UpsertTenantRequest]) (*connect.Response[configv1.UpsertTenantResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config: record required"))
	}
	if err := validateTenantRecord(rec); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()

	// One SyncRead serves two purposes: it gives the table_revision the
	// FSM will CAS against (so a racing operator's concurrent edit is
	// rejected with CodeFailedPrecondition), and it lets us resolve
	// create-vs-update + allocate the next id deterministically before
	// proposing.
	list, err := s.host.Tenants(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read tenants: %w", err))
	}
	// Honor an operator-supplied CAS revision when non-zero. Zero means
	// "use what we just read"; non-zero is the explicit operator-pinned
	// guard (we never silently widen it).
	casRev := req.Msg.GetIfTableRevisionEq()
	if casRev == 0 {
		casRev = list.TableRevision
	}

	assignedID := rec.GetId()
	if assignedID == 0 {
		var maxID uint32
		for _, t := range list.Tenants {
			if t.GetName() == rec.GetName() {
				assignedID = t.GetId()
				break
			}
			if t.GetId() > maxID {
				maxID = t.GetId()
			}
		}
		if assignedID == 0 {
			assignedID = maxID + 1
		}
	} else {
		// Operator-pinned id: reject name collisions with a different id.
		for _, t := range list.Tenants {
			if t.GetName() == rec.GetName() && t.GetId() != assignedID {
				return nil, connect.NewError(connect.CodeAlreadyExists,
					fmt.Errorf("config: tenant name %q already bound to id %d",
						rec.GetName(), t.GetId()))
			}
		}
	}
	rec.Id = assignedID

	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertTenant{
			UpsertTenant: &enginev1.UpsertTenant{Record: rec},
		},
	}
	if err := s.proposeCAS(callCtx, cmd, casRev); err != nil {
		return nil, err
	}
	newRev, err := s.readTenantRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.UpsertTenantResponse{
		TenantId:      assignedID,
		TableRevision: newRev,
	}), nil
}

// DeleteTenant removes the row identified by tenant_id. Delete-of-
// absent succeeds (the revision still bumps). Does NOT cascade-delete
// tenant data; operators clean up separately.
func (s *Server) DeleteTenant(ctx context.Context, req *connect.Request[configv1.DeleteTenantRequest]) (*connect.Response[configv1.DeleteTenantResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	id := req.Msg.GetTenantId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: tenant_id required (0 is the default-tenant sentinel)"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteTenant{
			DeleteTenant: &enginev1.DeleteTenant{Id: id},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readTenantRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.DeleteTenantResponse{TableRevision: newRev}), nil
}

// ListTenants returns every TenantRecord plus the table revision via
// one SyncRead. No leader gate.
func (s *Server) ListTenants(ctx context.Context, _ *connect.Request[configv1.ListTenantsRequest]) (*connect.Response[configv1.ListTenantsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Tenants(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read tenants: %w", err))
	}
	return connect.NewResponse(&configv1.ListTenantsResponse{
		Tenants:       list.Tenants,
		TableRevision: list.TableRevision,
	}), nil
}

// DescribeTenant returns one TenantRecord by id, or CodeNotFound.
func (s *Server) DescribeTenant(ctx context.Context, req *connect.Request[configv1.DescribeTenantRequest]) (*connect.Response[configv1.DescribeTenantResponse], error) {
	id := req.Msg.GetTenantId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: tenant_id required"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Tenants(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read tenants: %w", err))
	}
	for _, t := range list.Tenants {
		if t.GetId() == id {
			return connect.NewResponse(&configv1.DescribeTenantResponse{Tenant: t}), nil
		}
	}
	return nil, connect.NewError(connect.CodeNotFound,
		fmt.Errorf("config: tenant %d not found", id))
}

// readTenantRevision is a SyncRead helper used by Upsert/Delete to
// echo the post-apply revision back to the operator.
func (s *Server) readTenantRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.Tenants(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// validateTenantRecord enforces shape rules on a TenantRecord. id may
// be 0 on a create (server allocates); name is always required.
func validateTenantRecord(rec *enginev1.TenantRecord) error {
	if rec.GetName() == "" {
		return errors.New("name is required")
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
