// Package iflowengine adapts iflow's pure BPMN/CMMN engines to reflow's
// process-execution turn machine. It implements invoker.ProcessEngine: one
// Advance call decodes a single inbox event, runs exactly one deterministic
// iflow engine turn, and translates the emitted Commands into a reflow
// ProcessAdvanced. reflow's partition actuates the instructions and feeds
// results back as the next turn.
//
// internal/engine never imports iflow; the binding lives here and is injected
// via HostConfig.ProcessEngine, the same dependency inversion InProcDialer uses.
package iflowengine

import (
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/twinfer/reflwos/bpmn"
	"github.com/twinfer/reflwos/cmmn"
	"github.com/twinfer/reflwos/dmn"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ErrModelNotFound is returned by a ModelResolver when no model is registered for
// the requested ModelRef. Advance surfaces it as an error, which reflow's
// processSession converts into a failed ProcessTerminal.
var ErrModelNotFound = errors.New("iflowengine: model not found")

// ModelResolver hands the adapter the parsed iflow model for a ModelRef. It MUST
// be pure with respect to a given ModelRef: a ModelRef (kind+name+version) names
// an immutable snapshot, so serving a cached parse is replay-safe. Implementations
// must do no per-turn I/O — parse at boot, serve from cache — so Advance stays
// byte-for-byte reproducible.
type ModelResolver interface {
	// BPMN returns the parsed process graph for a BPMN ModelRef.
	BPMN(ref *enginev1.ModelRef) (*bpmn.ProcessGraph, error)

	// BPMNDecisions returns the DMN decision resolver for a BPMN model's
	// BusinessRuleTasks (evaluated inline by the engine via WithDecisionResolver).
	// For a model with no decisions the resolver errors on any ref — harmless,
	// since a decision-free model never invokes it.
	BPMNDecisions(ref *enginev1.ModelRef) bpmn.DecisionResolver

	// ChildRef resolves a child reference (a BPMN CallActivity calledElement, or a
	// CMMN process/case task ref) relative to the parent model to the child
	// instance's ModelRef. childKind is the child's engine kind ("bpmn"/"cmmn"),
	// which may differ from the parent's — a CMMN case can call a BPMN process.
	ChildRef(parent *enginev1.ModelRef, childKind, ref string) (*enginev1.ModelRef, error)

	// CMMN returns the parsed case definition for a CMMN ModelRef.
	CMMN(ref *enginev1.ModelRef) (*cmmn.CaseDefinition, error)
}

// modelKey is the comparable value form of a ModelRef, used as a cache map key.
type modelKey struct{ kind, name, version string }

func keyOf(ref *enginev1.ModelRef) modelKey {
	return modelKey{kind: ref.GetKind(), name: ref.GetName(), version: ref.GetVersion()}
}

// bpmnBundle is the parsed form of one BPMN model plus what its tasks reach:
// DMN runtimes for BusinessRuleTasks and child ModelRef overrides for
// CallActivities. All immutable after boot.
type bpmnBundle struct {
	graph     *bpmn.ProcessGraph
	decisions map[string]*dmn.Runtime       // decision ref → runtime
	children  map[string]*enginev1.ModelRef // calledElement → child ref override
}

// MapResolver is an in-memory ModelResolver populated at boot. It is the
// first-cut provisioning: models are parsed once and served from the map. A
// production resolver would back this with a reflow modelstore plus a pre-warm
// hook, but must preserve the no-per-turn-I/O contract.
//
// Safe for concurrent use. A sync.RWMutex guards the maps, so the read methods
// (BPMN/CMMN/ChildRef/BPMNDecisions) run concurrently across every shard's apply
// goroutine while AddBPMN/Parse*/AddDecision/AddChildRef may register a model at
// any time — registration need not all precede the first Advance. The returned
// graph / case-definition / child-ref values are immutable once parsed (mutators
// only replace map entries, never mutate a published model), so reading them
// after the lock is released is race-free; BPMNDecisions additionally snapshots
// the decision set under the lock so the resolver closure the engine invokes
// mid-turn never touches the live map.
type MapResolver struct {
	mu        sync.RWMutex
	bpmn      map[modelKey]*bpmnBundle
	cmmnDefs  map[modelKey]*cmmn.CaseDefinition
	retention map[modelKey]uint64 // parsed historyTimeToLive (ms) per model
}

var _ retentionResolver = (*MapResolver)(nil)

// NewMapResolver returns an empty resolver. Register models with AddBPMN /
// ParseBPMN / ParseCMMN before serving turns.
func NewMapResolver() *MapResolver {
	return &MapResolver{
		bpmn:      make(map[modelKey]*bpmnBundle),
		cmmnDefs:  make(map[modelKey]*cmmn.CaseDefinition),
		retention: make(map[modelKey]uint64),
	}
}

// bundle returns the (name, version) BPMN bundle, creating it if absent. The
// caller must hold mu for writing.
func (r *MapResolver) bundle(name, version string) *bpmnBundle {
	k := modelKey{kind: "bpmn", name: name, version: version}
	b, ok := r.bpmn[k]
	if !ok {
		b = &bpmnBundle{decisions: make(map[string]*dmn.Runtime), children: make(map[string]*enginev1.ModelRef)}
		r.bpmn[k] = b
	}
	return b
}

// AddBPMN registers a pre-parsed process graph under (name, version).
func (r *MapResolver) AddBPMN(name, version string, g *bpmn.ProcessGraph) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bundle(name, version).graph = g
}

// ParseBPMN parses BPMN XML (selecting the first executable process) and
// registers it under (name, version). Call at boot, never inside Advance.
func (r *MapResolver) ParseBPMN(name, version string, xml []byte) error {
	g, err := bpmn.Parse(xml, "")
	if err != nil {
		return fmt.Errorf("iflowengine: parse bpmn %q/%q: %w", name, version, err)
	}
	ttl := historyTTLFromBPMN(xml)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bundle(name, version).graph = g
	r.retention[modelKey{kind: "bpmn", name: name, version: version}] = ttl
	return nil
}

// AddDecision registers a DMN runtime under decisionRef for the BusinessRuleTasks
// of the (name, version) BPMN model.
func (r *MapResolver) AddDecision(name, version, decisionRef string, rt *dmn.Runtime) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bundle(name, version).decisions[decisionRef] = rt
}

// AddChildRef overrides the child ModelRef a CallActivity's calledElement
// resolves to within the (name, version) parent model. Without an override,
// ChildRef falls back to a name=calledElement convention.
func (r *MapResolver) AddChildRef(name, version, calledElement string, child *enginev1.ModelRef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bundle(name, version).children[calledElement] = child
}

// BPMN implements ModelResolver.
func (r *MapResolver) BPMN(ref *enginev1.ModelRef) (*bpmn.ProcessGraph, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.bpmn[keyOf(ref)]
	if !ok || b.graph == nil {
		return nil, fmt.Errorf("%w: bpmn %q/%q", ErrModelNotFound, ref.GetName(), ref.GetVersion())
	}
	return b.graph, nil
}

// BPMNDecisions implements ModelResolver.
func (r *MapResolver) BPMNDecisions(ref *enginev1.ModelRef) bpmn.DecisionResolver {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.bpmn[keyOf(ref)]
	if !ok {
		return decisionResolver(nil)
	}
	// Snapshot under the lock: the engine invokes the returned closure later in
	// the turn, so hand it a private copy rather than the live map a concurrent
	// AddDecision could be writing.
	snap := make(map[string]*dmn.Runtime, len(b.decisions))
	maps.Copy(snap, b.decisions)
	return decisionResolver(snap)
}

// ChildRef implements ModelResolver: a registered override, else a convention
// where ref names a model of childKind under the parent's version.
func (r *MapResolver) ChildRef(parent *enginev1.ModelRef, childKind, ref string) (*enginev1.ModelRef, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if b, ok := r.bpmn[keyOf(parent)]; ok {
		if child, ok := b.children[ref]; ok {
			return child, nil
		}
	}
	return &enginev1.ModelRef{Kind: childKind, Name: ref, Version: parent.GetVersion()}, nil
}

// ParseCMMN parses CMMN XML (selecting the first case) and registers it under
// (name, version). Call at boot, never inside Advance.
func (r *MapResolver) ParseCMMN(name, version string, xml []byte) error {
	def, err := cmmn.Parse(xml, "")
	if err != nil {
		return fmt.Errorf("iflowengine: parse cmmn %q/%q: %w", name, version, err)
	}
	ttl := historyTTLFromCMMN(xml)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmmnDefs[modelKey{kind: "cmmn", name: name, version: version}] = def
	r.retention[modelKey{kind: "cmmn", name: name, version: version}] = ttl
	return nil
}

// CMMN implements ModelResolver.
func (r *MapResolver) CMMN(ref *enginev1.ModelRef) (*cmmn.CaseDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.cmmnDefs[keyOf(ref)]
	if !ok {
		return nil, fmt.Errorf("%w: cmmn %q/%q", ErrModelNotFound, ref.GetName(), ref.GetVersion())
	}
	return def, nil
}

// RetentionMs implements the optional retentionResolver capability (see
// adapter.go): the model's parsed historyTimeToLive in ms, or 0 when the model
// declares none. Models registered via AddBPMN (pre-parsed, no XML) report 0.
func (r *MapResolver) RetentionMs(ref *enginev1.ModelRef) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.retention[keyOf(ref)]
}

// decisionResolver mirrors dboshost's makeBPMNDecisionResolver: a closure over
// the parsed DMN runtimes that the engine invokes for each BusinessRuleTask.
func decisionResolver(decisions map[string]*dmn.Runtime) bpmn.DecisionResolver {
	return func(ref string) (*dmn.Runtime, string, error) {
		rt, ok := decisions[ref]
		if !ok {
			return nil, "", fmt.Errorf("iflowengine: decision %q not registered", ref)
		}
		return rt, ref, nil
	}
}
