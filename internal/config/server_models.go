package config

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"

	connect "connectrpc.com/connect"

	configv1 "github.com/twinfer/reflw/proto/configv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// UpsertModel validates a BPMN/CMMN model definition and proposes
// Command_UpsertModel to shard 0's ModelTable. Validation runs through the
// injected s.validateModel: when the process plane is enabled pkg/reflw wires
// processengine.ValidateModel (real parse + static validation), so a
// structurally-broken model is rejected here with InvalidArgument instead of
// being committed to Raft and failing silently per-node at reconcile. Without
// the process plane it falls back to a shallow well-formed-XML check (config
// must not depend on reflwos). The per-node TableResolver parse stays as
// defense-in-depth for version skew. CAS via if_table_revision_eq.
func (s *Server) UpsertModel(ctx context.Context, req *connect.Request[configv1.UpsertModelRequest]) (*connect.Response[configv1.UpsertModelResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	ref := req.Msg.GetModelRef()
	if ref.GetKind() == "" || ref.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: model_ref kind and name required"))
	}
	if err := s.validateModel(ref.GetKind(), req.Msg.GetXml()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := validateBundle(req.Msg.GetBundle()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertModel{
			UpsertModel: &enginev1.UpsertModel{Record: &enginev1.ModelRecord{
				ModelRef: ref,
				Xml:      req.Msg.GetXml(),
				Bundle:   req.Msg.GetBundle(),
				// registered_at_ms is stamped deterministically in the apply arm.
			}},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readModelRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.UpsertModelResponse{TableRevision: newRev}), nil
}

// ListModels returns every ModelRecord plus the table's current CAS revision.
// No leader gate (SyncRead). Operators use the revision as if_table_revision_eq
// on subsequent Upsert/Delete calls.
func (s *Server) ListModels(ctx context.Context, _ *connect.Request[configv1.ListModelsRequest]) (*connect.Response[configv1.ListModelsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Models(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read models: %w", err))
	}
	return connect.NewResponse(&configv1.ListModelsResponse{
		Records:       list.Records,
		TableRevision: list.TableRevision,
	}), nil
}

// DescribeModel returns one ModelRecord by model_ref, or CodeNotFound. Reads
// via SyncRead and filters the (small) cluster-config list in the server, so no
// single-row Lookup is needed in the FSM.
func (s *Server) DescribeModel(ctx context.Context, req *connect.Request[configv1.DescribeModelRequest]) (*connect.Response[configv1.DescribeModelResponse], error) {
	ref := req.Msg.GetModelRef()
	if ref.GetKind() == "" || ref.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: model_ref kind and name required"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Models(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read models: %w", err))
	}
	for _, rec := range list.Records {
		r := rec.GetModelRef()
		if r.GetKind() == ref.GetKind() && r.GetName() == ref.GetName() && r.GetVersion() == ref.GetVersion() {
			return connect.NewResponse(&configv1.DescribeModelResponse{Record: rec}), nil
		}
	}
	return nil, connect.NewError(connect.CodeNotFound,
		fmt.Errorf("config: model %s/%s/%s not found", ref.GetKind(), ref.GetName(), ref.GetVersion()))
}

// DeleteModel removes one ModelRecord. CAS via if_table_revision_eq. No force
// gate: an in-flight instance pins its model_ref and fails on its next turn if
// the model is gone, rather than corrupting other instances.
func (s *Server) DeleteModel(ctx context.Context, req *connect.Request[configv1.DeleteModelRequest]) (*connect.Response[configv1.DeleteModelResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	ref := req.Msg.GetModelRef()
	if ref.GetKind() == "" || ref.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: model_ref kind and name required"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteModel{
			DeleteModel: &enginev1.DeleteModel{ModelRef: ref},
		},
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	if err := s.proposeCAS(callCtx, cmd, req.Msg.GetIfTableRevisionEq()); err != nil {
		return nil, err
	}
	newRev, err := s.readModelRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.DeleteModelResponse{TableRevision: newRev}), nil
}

// readModelRevision is a SyncRead helper used by Upsert/Delete.
func (s *Server) readModelRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.Models(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// validateModelXML is the fallback gate when no reflwos-backed validator is
// injected (process plane disabled): kind must be bpmn or cmmn and the bytes
// must be well-formed XML. It deliberately does NOT semantically parse BPMN/CMMN
// (that would couple config to reflwos). With the process plane on, pkg/reflw
// injects processengine.ValidateModel instead, which catches structural defects
// this check cannot. Signature matches that injected func so the two are
// interchangeable in NewServer.
// validateBundle structurally checks a model's ref-resolution bundle: decision
// refs must name a dmn model, child refs a bpmn/cmmn model, and every ref must
// carry a name. It does NOT consult the table (cross-row existence is resolved
// per-node by the TableResolver) nor parse the model — pure proto checks, so
// config stays reflwos-free. A nil/empty bundle is valid.
func validateBundle(b *enginev1.ModelBundle) error {
	if b == nil {
		return nil
	}
	for decisionRef, ref := range b.GetDecisions() {
		if ref.GetName() == "" {
			return fmt.Errorf("config: bundle decision %q: ref name required", decisionRef)
		}
		if ref.GetKind() != "dmn" {
			return fmt.Errorf("config: bundle decision %q: ref kind must be dmn, got %q", decisionRef, ref.GetKind())
		}
	}
	for elem, ref := range b.GetChildren() {
		if ref.GetName() == "" {
			return fmt.Errorf("config: bundle child %q: ref name required", elem)
		}
		switch ref.GetKind() {
		case "bpmn", "cmmn":
		default:
			return fmt.Errorf("config: bundle child %q: ref kind must be bpmn or cmmn, got %q", elem, ref.GetKind())
		}
	}
	return nil
}

func validateModelXML(kind string, x []byte) error {
	switch kind {
	case "bpmn", "cmmn":
	default:
		return fmt.Errorf("config: model_ref.kind must be %q or %q, got %q", "bpmn", "cmmn", kind)
	}
	if len(x) == 0 {
		return errors.New("config: model xml required")
	}
	dec := xml.NewDecoder(bytes.NewReader(x))
	for {
		_, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("config: model xml not well-formed: %w", err)
		}
	}
}
