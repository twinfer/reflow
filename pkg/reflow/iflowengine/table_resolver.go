package iflowengine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twinfer/iflow/bpmn"
	"github.com/twinfer/iflow/cmmn"
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
// case definition plus the resolved historyTimeToLive window. Immutable once
// built; the resolver swaps whole maps, never mutates a published entry.
type parsedModel struct {
	graph       *bpmn.ProcessGraph
	caseDef     *cmmn.CaseDefinition
	retentionMs uint64
}

// TableResolver is a ModelResolver fed by shard 0's ModelTable. A reconciler
// goroutine reparses the table on each notifier wake (and a 5s backstop) and
// atomically swaps an in-memory parsed-model cache; the read methods serve that
// cache with a single atomic load and no per-turn I/O — the no-per-turn-I/O
// contract the determinism rules require. It is the production counterpart to
// the boot-time MapResolver. v1 scope: BPMN + CMMN models, no DMN decision
// runtimes and no child-ref overrides (ChildRef falls back to the name
// convention, as MapResolver does); those are follow-ups.
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

// BPMNDecisions implements ModelResolver. v1 stores no DMN runtimes: a
// decision-free model never invokes the resolver; a BusinessRuleTask model
// errors at decision-resolve time (same as MapResolver without AddDecision).
func (r *TableResolver) BPMNDecisions(*enginev1.ModelRef) bpmn.DecisionResolver {
	return decisionResolver(nil)
}

// ChildRef implements ModelResolver. v1 has no per-model override rows, so it
// uses the name=ref convention under the parent's version — identical to
// MapResolver's fallback.
func (r *TableResolver) ChildRef(parent *enginev1.ModelRef, childKind, ref string) (*enginev1.ModelRef, error) {
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

// Reconcile parses each ModelRecord and atomically swaps the cache. A row that
// fails to parse preserves its previously-cached entry (transient or
// build-time-bad models don't knock a working model offline).
func (r *TableResolver) Reconcile(records []*enginev1.ModelRecord) {
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	prev := r.live.Load()
	next := make(map[modelKey]*parsedModel, len(records))
	for _, rec := range records {
		ref := rec.GetModelRef()
		if ref.GetKind() == "" || ref.GetName() == "" {
			continue
		}
		key := keyOf(ref)
		pm, err := parseModelRecord(rec)
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

func parseModelRecord(rec *enginev1.ModelRecord) (*parsedModel, error) {
	ref := rec.GetModelRef()
	switch ref.GetKind() {
	case "bpmn":
		g, err := bpmn.Parse(rec.GetXml(), "")
		if err != nil {
			return nil, fmt.Errorf("parse bpmn: %w", err)
		}
		return &parsedModel{graph: g, retentionMs: historyTTLFromBPMN(rec.GetXml())}, nil
	case "cmmn":
		def, err := cmmn.Parse(rec.GetXml(), "")
		if err != nil {
			return nil, fmt.Errorf("parse cmmn: %w", err)
		}
		return &parsedModel{caseDef: def, retentionMs: historyTTLFromCMMN(rec.GetXml())}, nil
	default:
		return nil, fmt.Errorf("unknown model kind %q", ref.GetKind())
	}
}
