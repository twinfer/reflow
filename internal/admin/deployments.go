package admin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	connect "connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/twinfer/reflw/internal/apimap"
	"github.com/twinfer/reflw/internal/engine/handlerclient"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	discoveryv1 "github.com/twinfer/reflw/proto/discoveryv1"
	"github.com/twinfer/reflw/proto/discoveryv1/discoveryv1connect"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// protocolVersion is the wire-protocol version this engine speaks; the
// handler-side discovery response must advertise the same string.
const protocolVersion = "v1"

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

// registerDeployment is the leader-side body, also reached by AutoSeed.
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
			fmt.Errorf("admin: propose RegisterDeployment: %w", err))
	}
	return &adminv1.RegisterDeploymentResponse{DeploymentId: deploymentID}, nil
}

// ListDeployments returns every DeploymentRecord on shard 0 (as views) plus the
// deployment table's CAS revision. SyncRead — any peer can serve.
func (s *Server) ListDeployments(ctx context.Context, _ *connect.Request[adminv1.ListDeploymentsRequest]) (*connect.Response[adminv1.ListDeploymentsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Deployments(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read deployments: %w", err))
	}
	return connect.NewResponse(&adminv1.ListDeploymentsResponse{
		Deployments:   apimap.DeploymentViews(list.Records),
		TableRevision: list.TableRevision,
	}), nil
}

// DescribeDeployment returns one DeploymentRecord by id (as a view). CodeNotFound
// when no deployment claims the id.
func (s *Server) DescribeDeployment(ctx context.Context, req *connect.Request[adminv1.DescribeDeploymentRequest]) (*connect.Response[adminv1.DescribeDeploymentResponse], error) {
	id := req.Msg.GetDeploymentId()
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: deployment_id required"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	rec, err := s.host.Deployment(callCtx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read deployment %q: %w", id, err))
	}
	if rec == nil {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("admin: deployment %q not found", id))
	}
	return connect.NewResponse(&adminv1.DescribeDeploymentResponse{Deployment: apimap.DeploymentView(rec)}), nil
}

// DeleteDeployment removes one DeploymentRecord and evicts any (service,
// handler) → id index entries. Refuses without force=true.
func (s *Server) DeleteDeployment(ctx context.Context, req *connect.Request[adminv1.DeleteDeploymentRequest]) (*connect.Response[adminv1.DeleteDeploymentResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	id := req.Msg.GetDeploymentId()
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: deployment_id required"))
	}
	if !req.Msg.GetForce() {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("admin: refusing to delete deployment %q without force=true; "+
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
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.DeleteDeploymentResponse{TableRevision: newRev}), nil
}

// readDeploymentRevision echoes the post-apply revision to Delete.
func (s *Server) readDeploymentRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.Deployments(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// AutoSeed is the in-process registration path used by pkg/reflw's cold-start
// seeding and by engine integration tests — the RegisterDeployment body minus
// the leader gate. budget=0 → engine default.
func (s *Server) AutoSeed(ctx context.Context, url string) (string, error) {
	return s.AutoSeedWithBudget(ctx, url, 0)
}

// AutoSeedWithBudget mirrors AutoSeed and additionally stamps a per-invocation
// step-budget override onto the deployment record.
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

// AutoSeedLocal registers an in-process deployment without a network discovery
// probe: the caller supplies the handler set directly and rawURL is the internal
// inproc:// address. Skips the leader gate. budget=0 → engine default.
func (s *Server) AutoSeedLocal(ctx context.Context, rawURL string, handlers []*discoveryv1.DiscoveredHandler, budget uint32) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("admin: parse url: %w", err))
	}
	if strings.ToLower(u.Scheme) != "inproc" {
		return "", connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("admin: AutoSeedLocal requires an inproc:// url, got %q", rawURL))
	}
	deploymentID := uuid.NewString()
	rec := &enginev1.DeploymentRecord{
		Id:                deploymentID,
		Url:               rawURL,
		RegisteredAtMs:    uint64(time.Now().UnixMilli()),
		MaxJournalEntries: budget,
	}
	for _, h := range handlers {
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
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.runner.Proposer().ProposeSelf(callCtx, cmd); err != nil {
		return "", connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: propose RegisterDeployment: %w", err))
	}
	return deploymentID, nil
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

// discoverConnect calls DiscoveryService.Discover on the deployment URL over
// HTTP/2 (h2c for http://, TLS for https://). When signer is non-nil the request
// carries Authorization: Bearer <jwt> with the deployment URL as audience.
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
