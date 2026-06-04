package iflowengine

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twinfer/iflow/bpmn"
	"github.com/twinfer/iflow/cmmn"
	"github.com/twinfer/iflow/dmn"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// modelReconcileInterval is the ticker backstop for the TableResolver; the
// ModelTable notifier wakes it sooner on a change, this bounds staleness if a
// notify is coalesced away. Mirrors internal/secretstore's 5s cadence.
const modelReconcileInterval = 5 * time.Second

// ModelTableReader reads the shard-0 ModelTable snapshot. pkg/reflow adapts
// engine.Host.Models to this so the resolver stays storage-agnostic.
type ModelTableReader interface {
	ListModels(ctx context.Context) ([]*enginev1.ModelRecord, uint64, error)
}

// parsedModel is one model materialized from a ModelRecord: the parsed graph or
// case definition, the bundle-resolved decision runtimes + child-ref overrides,
// and the resolved historyTimeToLive window. Immutable once built; the resolver
// swaps whole maps, never mutates a published entry.
type parsedModel struct {
	graph       *bpmn.ProcessGraph
	caseDef     *cmmn.CaseDefinition
	decisions   map[string]*dmn.Runtime       // decisionRef → runtime (from bundle.decisions)
	children    map[string]*enginev1.ModelRef // calledElement → child ref override (bundle.children)
	retentionMs uint64
}

// TableResolver is a ModelResolver fed by shard 0's ModelTable. A reconciler
// goroutine reparses the table on each notifier wake (and a 5s backstop) and
// atomically swaps an in-memory parsed-model cache; the read methods serve that
// cache with a single atomic load and no per-turn I/O — the no-per-turn-I/O
// contract the determinism rules require. It is the production counterpart to
// the boot-time MapResolver. Each model's bundle (ModelRecord.bundle) pins its
// DMN decision runtimes and child-ref overrides; ChildRef falls back to the
// name convention when the bundle declares no override.
type TableResolver struct {
	log         *slog.Logger
	live        atomic.Pointer[map[modelKey]*parsedModel]
	reconcileMu sync.Mutex // serializes concurrent reconciles
}

var (
	_ ModelResolver     = (*TableResolver)(nil)
	_ retentionResolver = (*TableResolver)(nil)
)

// NewTableResolver returns an empty resolver. Start RunReconciler to populate
// it from the ModelTable; reads return ErrModelNotFound until the first
// reconcile lands.
func NewTableResolver(log *slog.Logger) *TableResolver {
	if log == nil {
		log = slog.Default()
	}
	return &TableResolver{log: log}
}

func (r *TableResolver) lookup(ref *enginev1.ModelRef) *parsedModel {
	snap := r.live.Load()
	if snap == nil {
		return nil
	}
	return (*snap)[keyOf(ref)]
}

// BPMN implements ModelResolver.
func (r *TableResolver) BPMN(ref *enginev1.ModelRef) (*bpmn.ProcessGraph, error) {
	m := r.lookup(ref)
	if m == nil || m.graph == nil {
		return nil, fmt.Errorf("%w: bpmn %q/%q", ErrModelNotFound, ref.GetName(), ref.GetVersion())
	}
	return m.graph, nil
}

// CMMN implements ModelResolver.
func (r *TableResolver) CMMN(ref *enginev1.ModelRef) (*cmmn.CaseDefinition, error) {
	m := r.lookup(ref)
	if m == nil || m.caseDef == nil {
		return nil, fmt.Errorf("%w: cmmn %q/%q", ErrModelNotFound, ref.GetName(), ref.GetVersion())
	}
	return m.caseDef, nil
}

// BPMNDecisions implements ModelResolver: a resolver over the runtimes the
// model's bundle.decisions pinned at reconcile. A decisionRef absent from the
// bundle errors at resolve time (same as MapResolver without AddDecision).
func (r *TableResolver) BPMNDecisions(ref *enginev1.ModelRef) bpmn.DecisionResolver {
	m := r.lookup(ref)
	if m == nil {
		return decisionResolver(nil)
	}
	return decisionResolver(m.decisions)
}

// ChildRef implements ModelResolver: a bundle.children override if the model
// declared one for ref, else the name=ref convention under the parent's version
// (identical to MapResolver's fallback).
func (r *TableResolver) ChildRef(parent *enginev1.ModelRef, childKind, ref string) (*enginev1.ModelRef, error) {
	if m := r.lookup(parent); m != nil {
		if child, ok := m.children[ref]; ok {
			return child, nil
		}
	}
	return &enginev1.ModelRef{Kind: childKind, Name: ref, Version: parent.GetVersion()}, nil
}

// RetentionMs implements the optional retentionResolver capability: the model's
// historyTimeToLive in ms (0 = immediate delete).
func (r *TableResolver) RetentionMs(ref *enginev1.ModelRef) uint64 {
	m := r.lookup(ref)
	if m == nil {
		return 0
	}
	return m.retentionMs
}

// RunReconciler reparses the ModelTable on each notifier wake and on a 5s
// backstop, swapping the cache atomically. Blocks until ctx is cancelled.
func (r *TableResolver) RunReconciler(ctx context.Context, sub <-chan struct{}, reader ModelTableReader) error {
	ticker := time.NewTicker(modelReconcileInterval)
	defer ticker.Stop()
	r.reconcileFromReader(ctx, reader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			r.reconcileFromReader(ctx, reader)
		case <-ticker.C:
			r.reconcileFromReader(ctx, reader)
		}
	}
}

func (r *TableResolver) reconcileFromReader(ctx context.Context, reader ModelTableReader) {
	records, _, err := reader.ListModels(ctx)
	if err != nil {
		r.log.Warn("iflowengine: model reconcile read failed; keeping previous cache", "err", err)
		return
	}
	r.Reconcile(records)
}

// Reconcile parses each ModelRecord and atomically swaps the cache. DMN rows are
// compiled first so BPMN/CMMN models can bind their bundle.decisions; a row that
// fails to parse (or whose referenced DMN is missing) preserves its
// previously-cached entry — transient or build-time-bad models don't knock a
// working model offline.
func (r *TableResolver) Reconcile(records []*enginev1.ModelRecord) {
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	prev := r.live.Load()

	// First pass: compile every DMN row into a runtime. A DMN that fails to
	// compile is skipped here; a model referencing it then fails its second-pass
	// parse and preserves-prev.
	dmnRuntimes := make(map[modelKey]*dmn.Runtime)
	for _, rec := range records {
		ref := rec.GetModelRef()
		if ref.GetKind() != "dmn" || ref.GetName() == "" {
			continue
		}
		rt, err := dmn.NewRuntime(rec.GetXml())
		if err != nil {
			r.log.Warn("iflowengine: dmn compile failed; dependent models will preserve-prev",
				"name", ref.GetName(), "version", ref.GetVersion(), "err", err)
			continue
		}
		dmnRuntimes[keyOf(ref)] = rt
	}

	// Second pass: materialize BPMN/CMMN models (DMN rows are consumed above, not
	// served directly through BPMN/CMMN).
	next := make(map[modelKey]*parsedModel, len(records))
	for _, rec := range records {
		ref := rec.GetModelRef()
		if ref.GetName() == "" || ref.GetKind() == "" || ref.GetKind() == "dmn" {
			continue
		}
		key := keyOf(ref)
		pm, err := parseModelRecord(rec, dmnRuntimes)
		if err != nil {
			r.log.Warn("iflowengine: model parse failed; preserving previous",
				"kind", ref.GetKind(), "name", ref.GetName(), "version", ref.GetVersion(), "err", err)
			if prev != nil {
				if old, ok := (*prev)[key]; ok {
					next[key] = old
				}
			}
			continue
		}
		next[key] = pm
	}
	r.live.Store(&next)
}

func parseModelRecord(rec *enginev1.ModelRecord, dmnRuntimes map[modelKey]*dmn.Runtime) (*parsedModel, error) {
	ref := rec.GetModelRef()
	decisions, err := resolveDecisions(rec.GetBundle(), dmnRuntimes)
	if err != nil {
		return nil, err
	}
	children := resolveChildren(rec.GetBundle())
	switch ref.GetKind() {
	case "bpmn":
		g, err := bpmn.Parse(rec.GetXml(), "")
		if err != nil {
			return nil, fmt.Errorf("parse bpmn: %w", err)
		}
		return &parsedModel{graph: g, decisions: decisions, children: children, retentionMs: historyTTLFromBPMN(rec.GetXml())}, nil
	case "cmmn":
		def, err := cmmn.Parse(rec.GetXml(), "")
		if err != nil {
			return nil, fmt.Errorf("parse cmmn: %w", err)
		}
		return &parsedModel{caseDef: def, decisions: decisions, children: children, retentionMs: historyTTLFromCMMN(rec.GetXml())}, nil
	default:
		return nil, fmt.Errorf("unknown model kind %q", ref.GetKind())
	}
}

// resolveDecisions binds each bundle.decisions entry (decisionRef → dmn ref) to
// its compiled runtime. An entry pointing at a missing/uncompilable DMN row is
// an error so the referencing model preserves-prev rather than running with a
// half-resolved decision set.
func resolveDecisions(b *enginev1.ModelBundle, dmnRuntimes map[modelKey]*dmn.Runtime) (map[string]*dmn.Runtime, error) {
	if b == nil || len(b.GetDecisions()) == 0 {
		return nil, nil
	}
	out := make(map[string]*dmn.Runtime, len(b.GetDecisions()))
	for decisionRef, dmnRef := range b.GetDecisions() {
		rt, ok := dmnRuntimes[keyOf(dmnRef)]
		if !ok {
			return nil, fmt.Errorf("decision %q: unresolved dmn %s/%s", decisionRef, dmnRef.GetName(), dmnRef.GetVersion())
		}
		out[decisionRef] = rt
	}
	return out, nil
}

// resolveChildren copies the bundle.children overrides (calledElement → child
// ref) into a plain map for ChildRef.
func resolveChildren(b *enginev1.ModelBundle) map[string]*enginev1.ModelRef {
	if b == nil || len(b.GetChildren()) == 0 {
		return nil
	}
	out := make(map[string]*enginev1.ModelRef, len(b.GetChildren()))
	maps.Copy(out, b.GetChildren())
	return out
}
