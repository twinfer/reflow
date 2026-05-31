package config

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base32"
	"encoding/pem"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/certmgr"
	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/storage/keys"
	configv1 "github.com/twinfer/reflow/proto/configv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// operatorMaxValidity caps IssueOperator leaf lifetimes. Mirrors the
// historic `reflowd pki issue-operator --validity` default.
const operatorMaxValidity = 30 * 24 * time.Hour

// joinTokenPlaintextEntropy is the byte length of the random portion of
// a token plaintext. 24 bytes = 192 bits, well past the brute-force
// horizon for a single-use credential.
const joinTokenPlaintextEntropy = 24

// CreateJoinToken mints a one-time bootstrap credential. Returns the
// plaintext to the operator exactly once; only sha256(plaintext) is
// persisted.
func (s *Server) CreateJoinToken(
	ctx context.Context,
	req *connect.Request[configv1.CreateJoinTokenRequest],
) (*connect.Response[configv1.CreateJoinTokenResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	kind := req.Msg.GetKind()
	if kind == enginev1.JoinTokenKind_JOIN_TOKEN_KIND_UNSPECIFIED {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: kind is required"))
	}
	name := strings.TrimSpace(req.Msg.GetRequestedName())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: requested_name is required"))
	}
	if kind == enginev1.JoinTokenKind_JOIN_TOKEN_KIND_OPERATOR && name == "auto" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: operator tokens require a fixed requested_name (not auto)"))
	}

	plaintext, err := generateTokenPlaintext()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: generate token: %w", err))
	}
	hash := sha256.Sum256([]byte(plaintext))

	nowMs := time.Now().UnixMilli()
	var expiry uint64
	if ttl := req.Msg.GetTtlSeconds(); ttl > 0 {
		expiry = uint64(nowMs) + ttl*1000
	}
	rec := &enginev1.JoinTokenRecord{
		TokenHash:     hash[:],
		Kind:          kind,
		RequestedName: name,
		ExpiryMs:      expiry,
		SingleUse:     req.Msg.GetSingleUse(),
		CreatedBy:     creatorPrincipal(ctx),
		CreatedAtMs:   uint64(nowMs),
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertJoinToken{
			UpsertJoinToken: &enginev1.UpsertJoinToken{Record: rec},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, 0); err != nil {
		return nil, err
	}
	newRev, err := s.readJoinTokenRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.CreateJoinTokenResponse{
		Token:         plaintext,
		TokenHash:     hash[:],
		TableRevision: newRev,
	}), nil
}

// DeleteJoinToken removes the row identified by token_hash.
func (s *Server) DeleteJoinToken(
	ctx context.Context,
	req *connect.Request[configv1.DeleteJoinTokenRequest],
) (*connect.Response[configv1.DeleteJoinTokenResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	hash := req.Msg.GetTokenHash()
	if len(hash) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: token_hash required"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteJoinToken{
			DeleteJoinToken: &enginev1.DeleteJoinToken{TokenHash: hash},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readJoinTokenRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.DeleteJoinTokenResponse{TableRevision: newRev}), nil
}

// ListJoinTokens returns every row plus the table's CAS revision.
func (s *Server) ListJoinTokens(
	ctx context.Context,
	_ *connect.Request[configv1.ListJoinTokensRequest],
) (*connect.Response[configv1.ListJoinTokensResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.JoinTokens(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read join tokens: %w", err))
	}
	return connect.NewResponse(&configv1.ListJoinTokensResponse{
		Records:       list.Records,
		TableRevision: list.TableRevision,
	}), nil
}

// IssueOperator signs an operator-supplied CSR against the active
// cluster CA. The operator generates the keypair locally — only the
// public key is signed.
func (s *Server) IssueOperator(
	ctx context.Context,
	req *connect.Request[configv1.IssueOperatorRequest],
) (*connect.Response[configv1.IssueOperatorResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if s.operatorIssuer == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("config: cluster CA not yet active; run `reflowd config ca init` first"))
	}
	csrDER := req.Msg.GetCsrDer()
	if len(csrDER) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: csr_der is required"))
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: parse CSR: %w", err))
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: CSR signature invalid: %w", err))
	}
	cn := csr.Subject.CommonName
	if !strings.HasPrefix(cn, "operator/") {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: CSR CN %q must be operator/<name>", cn))
	}

	validity := time.Duration(req.Msg.GetValiditySeconds()) * time.Second
	if validity <= 0 || validity > operatorMaxValidity {
		validity = operatorMaxValidity
	}
	leafPEM, err := s.operatorIssuer.IssueForPrincipal(csr, cn, certmgr.LeafOperator, nil, validity)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: sign operator leaf: %w", err))
	}
	caPEM := s.operatorIssuer.ActiveCertPEM()
	if len(caPEM) == 0 {
		return nil, connect.NewError(connect.CodeInternal,
			errors.New("config: active CA snapshot missing"))
	}
	return connect.NewResponse(&configv1.IssueOperatorResponse{
		CertPem:       leafPEM,
		CaChainPem:    caPEM,
		CaFingerprint: spkiFingerprintPEM(caPEM),
	}), nil
}

// IssueTenant signs a tenant-supplied CSR against the active cluster CA. The
// CN must be "tenant/<n>" with n a decimal band id in [1, MaxTenantBand); band
// 0 is reserved for untenanted traffic and is never issued as a leaf. Only the
// public key is signed — the requester generates the keypair locally.
func (s *Server) IssueTenant(
	ctx context.Context,
	req *connect.Request[configv1.IssueTenantRequest],
) (*connect.Response[configv1.IssueTenantResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if s.operatorIssuer == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("config: cluster CA not yet active; run `reflowd config ca init` first"))
	}
	csrDER := req.Msg.GetCsrDer()
	if len(csrDER) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: csr_der is required"))
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: parse CSR: %w", err))
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: CSR signature invalid: %w", err))
	}
	cn := csr.Subject.CommonName
	band, ok := strings.CutPrefix(cn, "tenant/")
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: CSR CN %q must be tenant/<n>", cn))
	}
	n, perr := strconv.ParseUint(band, 10, 32)
	if perr != nil || n == 0 || uint32(n) >= keys.MaxTenantBand {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("config: tenant band %q must be a decimal integer in [1,%d)", band, keys.MaxTenantBand))
	}

	validity := time.Duration(req.Msg.GetValiditySeconds()) * time.Second
	if validity <= 0 || validity > operatorMaxValidity {
		validity = operatorMaxValidity
	}
	leafPEM, err := s.operatorIssuer.IssueForPrincipal(csr, cn, certmgr.LeafTenant, nil, validity)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: sign tenant leaf: %w", err))
	}
	caPEM := s.operatorIssuer.ActiveCertPEM()
	if len(caPEM) == 0 {
		return nil, connect.NewError(connect.CodeInternal,
			errors.New("config: active CA snapshot missing"))
	}
	return connect.NewResponse(&configv1.IssueTenantResponse{
		CertPem:       leafPEM,
		CaChainPem:    caPEM,
		CaFingerprint: spkiFingerprintPEM(caPEM),
	}), nil
}

// readJoinTokenRevision is a SyncRead helper used by Create/Delete.
func (s *Server) readJoinTokenRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.JoinTokens(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// generateTokenPlaintext returns a base32-encoded random string, no
// padding. The encoding stays URL-safe and shell-safe so operators can
// paste the token into a `--join-token=` flag without quoting.
func generateTokenPlaintext() (string, error) {
	var buf [joinTokenPlaintextEntropy]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(buf[:])), nil
}

func creatorPrincipal(ctx context.Context) string {
	if p, ok := auth.PrincipalFromContext(ctx); ok {
		return p.Raw
	}
	return "anonymous"
}

func spkiFingerprintPEM(caPEM []byte) string {
	block, _ := pem.Decode(caPEM)
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

// _ silences unused-import warnings when this file is the only one
// referencing cluster (the imports must stay stable across editor
// rewrites of the table set).
var _ = cluster.RevisionTableJoinToken
