package admin

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/apimap"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// PlanModelSetFunc validates a set of proposed models against the existing
// ModelTable and returns the ModelRecords to write, each with its bundle
// (decisions / children / imports) computed. Input records carry only model_ref +
// xml (any input bundle is ignored — the planner derives it). It must reject a
// set that isn't dependency-closed or that contains an invalid/cyclic model.
// pkg/reflw injects processengine.PlanModelSet when the process plane is enabled;
// nil falls back to shallowPlanModelSet. Using *enginev1.ModelRecord keeps the
// seam proto-only so the reflwos-backed planner needn't import this package.
type PlanModelSetFunc func(entries []*enginev1.ModelRecord, existing []*enginev1.ModelRecord) ([]*enginev1.ModelRecord, error)

// RegisterModelSet atomically registers a model plus its dependency closure. It
// reads the existing ModelTable once (the closure snapshot + the CAS revision),
// runs the injected planner to validate the set ∪ existing-table is dependency-
// closed and to derive each model's bundle, then proposes a single
// Command_UpsertModelSet CAS'd on the revision the closure was computed against.
func (s *Server) RegisterModelSet(ctx context.Context, req *connect.Request[adminv1.RegisterModelSetRequest]) (*connect.Response[adminv1.RegisterModelSetResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	entries := req.Msg.GetEntries()
	if len(entries) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: at least one entry required"))
	}
	planEntries := make([]*enginev1.ModelRecord, 0, len(entries))
	for _, e := range entries {
		if e.GetKind() == "" || e.GetName() == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				errors.New("admin: each entry's kind and name required"))
		}
		planEntries = append(planEntries, &enginev1.ModelRecord{
			ModelRef: &enginev1.ModelRef{Kind: e.GetKind(), Name: e.GetName(), Version: e.GetVersion()},
			Xml:      e.GetXml(),
		})
	}

	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()

	// Read the existing ModelTable once: the snapshot the planner resolves the
	// dependency closure against, and the revision the proposal guards on.
	list, err := s.host.Models(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read models: %w", err))
	}
	readRev := list.TableRevision

	if want := req.Msg.GetIfTableRevisionEq(); want != 0 && want != readRev {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("admin: table revision changed (have %d, want %d); re-read and retry", readRev, want))
	}

	records, err := s.planModelSet(planEntries, list.Records)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpsertModelSet{
			UpsertModelSet: &enginev1.UpsertModelSet{Records: records},
		},
	}
	if err := s.proposeCAS(callCtx, cmd, readRev); err != nil {
		return nil, err
	}
	newRev, err := s.readModelRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.RegisterModelSetResponse{TableRevision: newRev}), nil
}

// ListModels returns every ModelRecord (as views) plus the table's current CAS
// revision. No leader gate (SyncRead).
func (s *Server) ListModels(ctx context.Context, _ *connect.Request[adminv1.ListModelsRequest]) (*connect.Response[adminv1.ListModelsResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Models(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read models: %w", err))
	}
	return connect.NewResponse(&adminv1.ListModelsResponse{
		Models:        apimap.ModelViews(list.Records),
		TableRevision: list.TableRevision,
	}), nil
}

// DescribeModel returns one ModelRecord by (kind, name, version) as a view, or
// CodeNotFound. Filters the (small) cluster-config list in the server.
func (s *Server) DescribeModel(ctx context.Context, req *connect.Request[adminv1.DescribeModelRequest]) (*connect.Response[adminv1.DescribeModelResponse], error) {
	kind, name, version := req.Msg.GetKind(), req.Msg.GetName(), req.Msg.GetVersion()
	if kind == "" || name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: kind and name required"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.Models(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("admin: read models: %w", err))
	}
	for _, rec := range list.Records {
		r := rec.GetModelRef()
		if r.GetKind() == kind && r.GetName() == name && r.GetVersion() == version {
			return connect.NewResponse(&adminv1.DescribeModelResponse{Model: apimap.ModelView(rec)}), nil
		}
	}
	return nil, connect.NewError(connect.CodeNotFound,
		fmt.Errorf("admin: model %s/%s/%s not found", kind, name, version))
}

// DeleteModel removes one ModelRecord. CAS via if_table_revision_eq. No force
// gate: an in-flight instance pins its model_ref and fails on its next turn.
func (s *Server) DeleteModel(ctx context.Context, req *connect.Request[adminv1.DeleteModelRequest]) (*connect.Response[adminv1.DeleteModelResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	kind, name, version := req.Msg.GetKind(), req.Msg.GetName(), req.Msg.GetVersion()
	if kind == "" || name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("admin: kind and name required"))
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_DeleteModel{
			DeleteModel: &enginev1.DeleteModel{ModelRef: &enginev1.ModelRef{Kind: kind, Name: name, Version: version}},
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
			fmt.Errorf("admin: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&adminv1.DeleteModelResponse{TableRevision: newRev}), nil
}

// readModelRevision is a SyncRead helper used by Register/Delete.
func (s *Server) readModelRevision(ctx context.Context) (uint64, error) {
	list, err := s.host.Models(ctx)
	if err != nil {
		return 0, err
	}
	return list.TableRevision, nil
}

// shallowPlanModelSet is the fallback planner when no reflwos-backed planner is
// injected (process plane disabled): each entry must be a known kind and
// well-formed XML; bundles are left empty and no dependency-closure check runs.
// It rejects a DMN that declares an <import> (the runtime resolves imports
// strictly from bundle.imports pins, which the shallow path can't produce).
func shallowPlanModelSet(entries []*enginev1.ModelRecord, _ []*enginev1.ModelRecord) ([]*enginev1.ModelRecord, error) {
	records := make([]*enginev1.ModelRecord, 0, len(entries))
	for _, e := range entries {
		kind := e.GetModelRef().GetKind()
		if err := validateModelXML(kind, e.GetXml()); err != nil {
			return nil, err
		}
		if kind == "dmn" && dmnDeclaresImport(e.GetXml()) {
			return nil, fmt.Errorf("admin: dmn %q declares <import>; cross-file DMN imports require "+
				"the process plane (the shallow registration path cannot validate the dependency closure)",
				e.GetModelRef().GetName())
		}
		records = append(records, &enginev1.ModelRecord{
			ModelRef: e.GetModelRef(),
			Xml:      e.GetXml(),
		})
	}
	return records, nil
}

// dmnDeclaresImport reports whether a DMN document contains an <import> element.
func dmnDeclaresImport(x []byte) bool {
	dec := xml.NewDecoder(bytes.NewReader(x))
	for {
		tok, err := dec.Token()
		if err != nil {
			return false
		}
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "import" {
			return true
		}
	}
}

// validateModelXML is the shallow check: kind must be bpmn, cmmn or dmn and the
// bytes must be well-formed XML. It deliberately does NOT semantically parse.
func validateModelXML(kind string, x []byte) error {
	switch kind {
	case "bpmn", "cmmn", "dmn":
	default:
		return fmt.Errorf("admin: model kind must be %q, %q or %q, got %q", "bpmn", "cmmn", "dmn", kind)
	}
	if len(x) == 0 {
		return errors.New("admin: model xml required")
	}
	dec := xml.NewDecoder(bytes.NewReader(x))
	for {
		_, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("admin: model xml not well-formed: %w", err)
		}
	}
}
