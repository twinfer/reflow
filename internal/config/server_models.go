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

// RegisterModelSet atomically registers a model plus its dependency closure. It
// reads the existing ModelTable once (the closure snapshot + the CAS revision),
// runs the injected planner to validate the set ∪ existing-table is dependency-
// closed and cycle-free and to derive each model's bundle (decisions / children /
// imports), then proposes a single Command_UpsertModelSet. The proposal is CAS'd
// on the revision the closure was computed against, so a concurrent ModelTable
// change between the read and the apply rejects it — the committed set is exactly
// the one the planner validated. A one-entry set is a single-model register.
func (s *Server) RegisterModelSet(ctx context.Context, req *connect.Request[configv1.RegisterModelSetRequest]) (*connect.Response[configv1.RegisterModelSetResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	entries := req.Msg.GetEntries()
	if len(entries) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("config: at least one entry required"))
	}
	planEntries := make([]*enginev1.ModelRecord, 0, len(entries))
	for _, e := range entries {
		ref := e.GetModelRef()
		if ref.GetKind() == "" || ref.GetName() == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				errors.New("config: each entry's model_ref kind and name required"))
		}
		planEntries = append(planEntries, &enginev1.ModelRecord{ModelRef: ref, Xml: e.GetXml()})
	}

	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()

	// Read the existing ModelTable once: the snapshot the planner resolves the
	// dependency closure against, and the revision the proposal guards on.
	list, err := s.host.Models(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("config: read models: %w", err))
	}
	readRev := list.TableRevision

	// If the caller pinned a CAS revision, it must match what we just read;
	// otherwise the table already moved and the closure they expect is stale.
	if want := req.Msg.GetIfTableRevisionEq(); want != 0 && want != readRev {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("config: table revision changed (have %d, want %d); re-read and retry", readRev, want))
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
	// CAS on the revision the closure was computed against (readRev). On a fresh
	// table readRev is 0 → CAS disabled, which is safe: an empty existing set
	// forces the request to be self-contained, so a concurrent add can't
	// invalidate it.
	if err := s.proposeCAS(callCtx, cmd, readRev); err != nil {
		return nil, err
	}
	newRev, err := s.readModelRevision(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("config: read post-apply revision: %w", err))
	}
	return connect.NewResponse(&configv1.RegisterModelSetResponse{TableRevision: newRev}), nil
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

// shallowPlanModelSet is the fallback planner when no reflwos-backed planner is
// injected (process plane disabled): each entry must be a known kind and
// well-formed XML; bundles are left empty and no dependency-closure check runs
// (config cannot import reflwos to derive refs). With the process plane on,
// pkg/reflw injects processengine.PlanModelSet, which derives bundles and
// validates the closure. The per-node TableResolver parse stays as
// defense-in-depth for version skew.
//
// It rejects a DMN that declares an <import>. The runtime resolves DMN imports
// strictly from each model's bundle.imports pins (no namespace-index fallback),
// and the shallow path produces empty bundles — so an import-bearing DMN
// registered here would silently fail to compile on every node if the process
// plane were later enabled. Rejecting it upfront turns that latent fail-closed
// into a clear registration error: register import-bearing DMNs with the process
// plane on, where the planner can pin the closure.
func shallowPlanModelSet(entries []*enginev1.ModelRecord, _ []*enginev1.ModelRecord) ([]*enginev1.ModelRecord, error) {
	records := make([]*enginev1.ModelRecord, 0, len(entries))
	for _, e := range entries {
		kind := e.GetModelRef().GetKind()
		if err := validateModelXML(kind, e.GetXml()); err != nil {
			return nil, err
		}
		if kind == "dmn" && dmnDeclaresImport(e.GetXml()) {
			return nil, fmt.Errorf("config: dmn %q declares <import>; cross-file DMN imports require "+
				"the process plane (the shallow registration path cannot validate the dependency closure)",
				e.GetModelRef().GetName())
		}
		records = append(records, &enginev1.ModelRecord{
			ModelRef: e.GetModelRef(),
			Xml:      e.GetXml(),
			// registered_at_ms stamped in the apply arm; bundle left empty.
		})
	}
	return records, nil
}

// dmnDeclaresImport reports whether a DMN document contains an <import> element.
// A token scan keeps config reflwos-free; well-formedness is already gated by
// validateModelXML, so a decode error here just ends the scan.
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

// validateModelXML is the shallow check used by shallowPlanModelSet: kind must be
// bpmn, cmmn or dmn and the bytes must be well-formed XML. It deliberately does
// NOT semantically parse the model (that would couple config to reflwos).
func validateModelXML(kind string, x []byte) error {
	switch kind {
	case "bpmn", "cmmn", "dmn":
	default:
		return fmt.Errorf("config: model_ref.kind must be %q, %q or %q, got %q", "bpmn", "cmmn", "dmn", kind)
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
