package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/apimap"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// UpsertSecret rebuilds a SecretRecord from the flat request fields, validates
// it, then proposes Command_UpsertSecret with the operator-supplied CAS guard.
// Returns the post-apply revision.
func (s *Server) UpsertSecret(ctx context.Context, req *connect.Request[adminv1.UpsertSecretRequest]) (*connect.Response[adminv1.UpsertSecretResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	rec := &enginev1.SecretRecord{
		Name: req.Msg.GetName(),
		Source: &enginev1.SecretRecord_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: req.Msg.GetBlobUri(),
				KekUri:  req.Msg.GetKekUri(),
			},
		},
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

// DeleteSecret removes the named row. CAS via if_table_revision_eq. Does NOT
// validate consumer references — they fail to resolve on next reconcile and
// preserve-prev.
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

// ListSecrets returns every SecretRecord (as views — never plaintext) plus the
// table's current CAS revision. No leader gate.
func (s *Server) ListSecrets(ctx context.Context, _ *connect.Request[adminv1.ListSecretsRequest]) (*connect.Response[adminv1.ListSecretsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Secrets(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read secrets: %w", err))
	}
	return connect.NewResponse(&adminv1.ListSecretsResponse{
		Secrets:       apimap.SecretViews(list.Records),
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

// validateSecretRecord enforces shape rules on a SecretRecord. No decrypt
// attempt — the SecretStore reconciler surfaces resolve errors via metrics.
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

// hasKnownBlobScheme returns true when uri starts with a gocloud.dev/blob scheme
// reflw links by default.
func hasKnownBlobScheme(uri string) bool {
	for _, p := range []string{"s3://", "gs://", "azblob://", "file://", "mem://"} {
		if strings.HasPrefix(uri, p) {
			return true
		}
	}
	return false
}
