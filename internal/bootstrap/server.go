// Package bootstrap implements reflow's MeshSign Connect RPC — the
// kubeadm-style joiner credential exchange. A joiner that has a one-time
// `reflowd config create-join-token` plaintext sends a CertificateSigningRequest
// plus the token; the server verifies the token against shard 0's
// JoinTokenTable, proposes ConsumeJoinToken (which atomically marks
// single_use rows as spent), and on success signs the CSR against the
// active cluster CA via certmgr.ClusterIssuer.
//
// Authentication: the bootstrap port runs TLS but without
// RequireAndVerifyClientCert — the join token is the credential. The
// admin/clusterctl ports still require a mesh-issued client cert and
// are not reachable until a node has finished joining.
package bootstrap

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/certmgr"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/cluster"
	bootstrapv1 "github.com/twinfer/reflow/proto/bootstrapv1"
	"github.com/twinfer/reflow/proto/bootstrapv1/bootstrapv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Server implements bootstrapv1connect.MeshSignHandler.
type Server struct {
	bootstrapv1connect.UnimplementedMeshSignHandler

	host             *engine.Host
	runner           *engine.MetadataRunner
	issuer           *certmgr.ClusterIssuer
	log              *slog.Logger
	leafValidity     time.Duration
	adminCallTimeout time.Duration
}

// Config groups the constructor inputs.
type Config struct {
	Host   *engine.Host
	Runner *engine.MetadataRunner
	Issuer *certmgr.ClusterIssuer
	Log    *slog.Logger
	// LeafValidity is the validity period stamped onto signed leaves.
	// Zero defaults to 24h, matching certmgr's leaf default.
	LeafValidity time.Duration
	// AdminCallTimeout is the deadline applied to FSM-driving proposals
	// (ConsumeJoinToken). Zero defaults to 10s.
	AdminCallTimeout time.Duration
}

// NewServer constructs the MeshSign server.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Host == nil || cfg.Runner == nil || cfg.Issuer == nil {
		return nil, errors.New("bootstrap: Host, Runner, and Issuer are required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.LeafValidity == 0 {
		cfg.LeafValidity = 24 * time.Hour
	}
	if cfg.AdminCallTimeout == 0 {
		cfg.AdminCallTimeout = 10 * time.Second
	}
	return &Server{
		host:             cfg.Host,
		runner:           cfg.Runner,
		issuer:           cfg.Issuer,
		log:              cfg.Log,
		leafValidity:     cfg.LeafValidity,
		adminCallTimeout: cfg.AdminCallTimeout,
	}, nil
}

// NewHandler returns the (path, handler) pair for mounting on a
// connectserver. opts is forwarded to the generated handler.
func (s *Server) NewHandler(opts ...connect.HandlerOption) (string, http.Handler) {
	return bootstrapv1connect.NewMeshSignHandler(s, opts...)
}

// requireLeader returns CodeUnavailable when this node is not the
// metadata leader. Joiners follow the LeaderHint via the same
// CallWithLeaderRedirect helper the clusterctl client uses.
func (s *Server) requireLeader() error {
	if s.runner.IsLeader() {
		return nil
	}
	hintID, _ := s.host.PartitionLeaderHint(0)
	return connect.NewError(connect.CodeUnavailable,
		fmt.Errorf("bootstrap: not the metadata leader (hint=%d)", hintID))
}

// SignCSR is the only RPC: validate token → consume row atomically →
// sign CSR → return leaf + CA chain + assigned node_id.
func (s *Server) SignCSR(
	ctx context.Context,
	req *connect.Request[bootstrapv1.SignCSRRequest],
) (*connect.Response[bootstrapv1.SignCSRResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	plaintext := strings.TrimSpace(req.Msg.GetJoinToken())
	if plaintext == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("bootstrap: join_token is required"))
	}
	csrDER := req.Msg.GetCsrDer()
	if len(csrDER) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("bootstrap: csr_der is required"))
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("bootstrap: parse CSR: %w", err))
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("bootstrap: CSR signature invalid: %w", err))
	}

	hash := sha256.Sum256([]byte(plaintext))
	tokens, err := s.host.JoinTokens(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("bootstrap: read JoinTokenTable: %w", err))
	}
	rec := findToken(tokens.Records, hash[:])
	if rec == nil {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("bootstrap: join_token not recognised"))
	}
	if rec.GetSingleUse() && rec.GetUsed() {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("bootstrap: join_token already redeemed"))
	}
	nowMs := time.Now().UnixMilli()
	if exp := rec.GetExpiryMs(); exp != 0 && uint64(nowMs) >= exp {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("bootstrap: join_token expired"))
	}

	wantKind, err := principalKindFromToken(rec.GetKind())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	csrKind, csrName, ok := parseCSRCommonName(csr.Subject.CommonName)
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("bootstrap: CSR CN %q not in <kind>/<name> form", csr.Subject.CommonName))
	}
	if csrKind != wantKind {
		return nil, connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("bootstrap: CSR kind %q does not match token kind %q", csrKind, wantKind))
	}

	requested := rec.GetRequestedName()
	if requested != "auto" && csrName != requested {
		return nil, connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("bootstrap: CSR name %q does not match token requested_name %q",
				csrName, requested))
	}

	var assignedNodeID uint64
	finalName := csrName
	if requested == "auto" {
		if wantKind != "node" {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				errors.New("bootstrap: requested_name=auto only valid for node tokens"))
		}
		id, err := s.allocateNodeID(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal,
				fmt.Errorf("bootstrap: allocate node_id: %w", err))
		}
		assignedNodeID = id
		finalName = strconv.FormatUint(id, 10)
	} else if wantKind == "node" {
		n, perr := strconv.ParseUint(requested, 10, 64)
		if perr != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("bootstrap: node token requested_name=%q is not a uint64", requested))
		}
		assignedNodeID = n
	}

	cmd := &enginev1.Command{
		Kind: &enginev1.Command_ConsumeJoinToken{
			ConsumeJoinToken: &enginev1.ConsumeJoinToken{TokenHash: hash[:]},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	val, err := s.runner.Proposer().ProposeSelfCAS(callCtx, cmd, nil)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("bootstrap: propose ConsumeJoinToken: %w", err))
	}
	if val == cluster.ResultValueFailedPrecondition {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("bootstrap: join_token redemption rejected (already used or expired)"))
	}

	principal := wantKind + "/" + finalName
	kind := certmgr.LeafNode
	if wantKind == "operator" {
		kind = certmgr.LeafOperator
	}
	leafPEM, err := s.issuer.IssueForPrincipal(csr, principal, kind, nil, s.leafValidity)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("bootstrap: sign leaf: %w", err))
	}
	caPEM := s.issuer.ActiveCertPEM()
	if len(caPEM) == 0 {
		return nil, connect.NewError(connect.CodeInternal,
			errors.New("bootstrap: active CA snapshot missing"))
	}
	fpr := caFingerprint(caPEM)
	s.log.Info("bootstrap: signed leaf",
		"principal", principal,
		"assigned_node_id", assignedNodeID,
		"validity", s.leafValidity)
	return connect.NewResponse(&bootstrapv1.SignCSRResponse{
		CertPem:        leafPEM,
		CaChainPem:     caPEM,
		AssignedNodeId: assignedNodeID,
		CaFingerprint:  fpr,
	}), nil
}

func findToken(records []*enginev1.JoinTokenRecord, hash []byte) *enginev1.JoinTokenRecord {
	for _, r := range records {
		if bytesEqual(r.GetTokenHash(), hash) {
			return r
		}
	}
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func principalKindFromToken(k enginev1.JoinTokenKind) (string, error) {
	switch k {
	case enginev1.JoinTokenKind_JOIN_TOKEN_KIND_NODE:
		return "node", nil
	case enginev1.JoinTokenKind_JOIN_TOKEN_KIND_OPERATOR:
		return "operator", nil
	default:
		return "", fmt.Errorf("bootstrap: unknown JoinTokenKind %v", k)
	}
}

func parseCSRCommonName(cn string) (kind, name string, ok bool) {
	i := strings.IndexByte(cn, '/')
	if i <= 0 || i == len(cn)-1 {
		return "", "", false
	}
	return cn[:i], cn[i+1:], true
}

// allocateNodeID picks max(existing.NodeId)+1 from the current
// membership view. Starts at 1 when the table is empty (node_id=0 is
// the reserved unspecified sentinel). Mirrors the convention used by
// reflowd run --bootstrap for the first node's id.
func (s *Server) allocateNodeID(ctx context.Context) (uint64, error) {
	mems, err := s.host.Membership(ctx)
	if err != nil {
		return 0, err
	}
	var maxID uint64
	for _, m := range mems {
		if m.GetNodeId() > maxID {
			maxID = m.GetNodeId()
		}
	}
	return maxID + 1, nil
}

func caFingerprint(certPEM []byte) string {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return ""
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	const hextab = "0123456789abcdef"
	out := make([]byte, 0, 7+2*len(sum))
	out = append(out, "sha256:"...)
	for _, b := range sum {
		out = append(out, hextab[b>>4], hextab[b&0x0F])
	}
	return string(out)
}
