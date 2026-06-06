package processengine

import (
	"fmt"
	"log/slog"
	"strings"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflwos/bpmn"
	"github.com/twinfer/reflwos/cmmn"
	"github.com/twinfer/reflwos/dmn"
)

// PlanModelSet is the reflwos-backed planner pkg/reflw injects into config.Server
// (matching config.PlanModelSetFunc). Given a set of proposed models (model_ref +
// xml) and the existing ModelTable, it parses every entry, derives each model's
// bundle, and returns the ModelRecords to write. Bundle derivation:
//
//   - imports  (DMN <import> namespace → dmn ref): STRICT. A DMN whose import
//     can't be resolved in the set ∪ existing table is rejected — it cannot
//     compile. This is the dependency-closure guarantee the atomic register-set
//     exists to provide.
//   - decisions (BusinessRuleTask / DecisionTask ref → dmn ref) and
//     children (CallActivity / Process- / CaseTask ref → bpmn|cmmn ref):
//     PIN-IF-RESOLVABLE. Pinned to an exact ref when the target is in the set or
//     table; otherwise left unpinned, so the per-node TableResolver's existing
//     name-convention fallback (or an eval-time error, as today) applies. This
//     keeps incremental deployment working.
//
// Each model is then statically validated (BPMN/CMMN static checks; DMN compiled
// with a closure-scoped resolver so cycles and unresolved imports surface). Pure
// and off the apply path — runs on the leader before proposing.
func PlanModelSet(entries []*enginev1.ModelRecord, existing []*enginev1.ModelRecord) ([]*enginev1.ModelRecord, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("processengine: empty model set")
	}

	// Parse every set entry up front — a parse failure rejects the whole set.
	type parsedModel struct {
		ref   *enginev1.ModelRef
		xml   []byte
		kind  string
		graph *bpmn.ProcessGraph
		caseD *cmmn.CaseDefinition
		defs  *dmn.Definitions
	}
	setModels := make([]*parsedModel, 0, len(entries))
	for _, e := range entries {
		ref := e.GetModelRef()
		pm := &parsedModel{ref: ref, xml: e.GetXml(), kind: ref.GetKind()}
		switch ref.GetKind() {
		case "bpmn":
			g, err := bpmn.Parse(e.GetXml(), "")
			if err != nil {
				return nil, fmt.Errorf("processengine: parse bpmn %s/%s: %w", ref.GetName(), ref.GetVersion(), err)
			}
			pm.graph = g
		case "cmmn":
			def, err := cmmn.Parse(e.GetXml(), "")
			if err != nil {
				return nil, fmt.Errorf("processengine: parse cmmn %s/%s: %w", ref.GetName(), ref.GetVersion(), err)
			}
			pm.caseD = def
		case "dmn":
			d, err := dmn.Parse(e.GetXml())
			if err != nil {
				return nil, fmt.Errorf("processengine: parse dmn %s/%s: %w", ref.GetName(), ref.GetVersion(), err)
			}
			pm.defs = d
		default:
			return nil, fmt.Errorf("processengine: model %s/%s has unknown kind %q",
				ref.GetName(), ref.GetVersion(), ref.GetKind())
		}
		setModels = append(setModels, pm)
	}

	// Build the closure indices over existing ∪ set. Existing rows are added
	// first so a set entry takes precedence on any key collision.
	idx := newClosureIndex()
	for _, rec := range existing {
		idx.addExisting(rec)
	}
	for _, pm := range setModels {
		if err := idx.addSet(pm.ref, pm.kind, pm.graph, pm.caseD, pm.defs); err != nil {
			return nil, err
		}
	}

	// Closure-scoped resolver for DMN compilation: namespace → parsed defs over
	// every DMN in the closure. scanModels uses it to pull imported models, so a
	// cycle (ErrCyclicDependency) or an unresolved import surfaces at compile.
	resolver := func(namespace, _ /*locationURI*/, _ /*baseDir*/ string) (*dmn.Definitions, error) {
		if d := idx.dmnDefsByNamespace[namespace]; d != nil {
			return d, nil
		}
		return nil, fmt.Errorf("unresolved dmn import namespace %q", namespace)
	}

	out := make([]*enginev1.ModelRecord, 0, len(setModels))
	for _, pm := range setModels {
		bundle, err := idx.deriveBundle(pm.ref, pm.kind, pm.graph, pm.caseD, pm.defs)
		if err != nil {
			return nil, err
		}
		switch pm.kind {
		case "bpmn":
			res, verr := bpmn.ValidateStatic(pm.graph)
			if verr != nil {
				return nil, fmt.Errorf("processengine: validate bpmn %s/%s: %w", pm.ref.GetName(), pm.ref.GetVersion(), verr)
			}
			if res.HasErrors() {
				return nil, fmt.Errorf("processengine: bpmn %s/%s static validation: %s",
					pm.ref.GetName(), pm.ref.GetVersion(), joinIssues(res.Errors()))
			}
		case "cmmn":
			if verr := cmmn.Validate(pm.caseD); verr != nil {
				return nil, fmt.Errorf("processengine: validate cmmn %s/%s: %w", pm.ref.GetName(), pm.ref.GetVersion(), verr)
			}
		case "dmn":
			if _, verr := dmn.NewRuntimeFromModel(pm.defs, dmn.WithModelResolver(resolver)); verr != nil {
				return nil, fmt.Errorf("processengine: compile dmn %s/%s: %w", pm.ref.GetName(), pm.ref.GetVersion(), verr)
			}
			// Mangle validation over the import closure — parity with the bpmn /
			// cmmn cases above. The resolver registers imported elements so a
			// cross-model requirement resolves instead of false-positiving
			// REF001-003; only error-severity findings block registration.
			vr, verr := dmn.ValidateWithResolver(pm.defs, resolver)
			if verr != nil {
				return nil, fmt.Errorf("processengine: validate dmn %s/%s: %w", pm.ref.GetName(), pm.ref.GetVersion(), verr)
			}
			if vr.HasErrors() {
				return nil, fmt.Errorf("processengine: dmn %s/%s validation: %s",
					pm.ref.GetName(), pm.ref.GetVersion(), joinDMNIssues(vr.Errors()))
			}
		}
		out = append(out, &enginev1.ModelRecord{ModelRef: pm.ref, Xml: pm.xml, Bundle: bundle})
	}
	return out, nil
}

// closureIndex resolves a model's references to ModelRefs over the set ∪ existing
// table. Each index is keyed by what the engine matches on: DMN imports by
// namespace, decisions by decision/decision-service name, children by the child
// model's name and (for set entries) its parsed process/case id.
type closureIndex struct {
	dmnDefsByNamespace map[string]*dmn.Definitions   // for the validation resolver
	dmnByNamespace     map[string]*enginev1.ModelRef // import namespace → dmn ref
	dmnByDecision      map[string]*enginev1.ModelRef // decision / service name → dmn ref
	bpmnByKey          map[string]*enginev1.ModelRef // bpmn model name or process id → ref
	cmmnByKey          map[string]*enginev1.ModelRef // cmmn model name or case id → ref

	setNS       map[string]bool // namespaces claimed by a set entry (within-set dup detection)
	setDecision map[string]bool
	setBpmn     map[string]bool
	setCmmn     map[string]bool
}

func newClosureIndex() *closureIndex {
	return &closureIndex{
		dmnDefsByNamespace: map[string]*dmn.Definitions{},
		dmnByNamespace:     map[string]*enginev1.ModelRef{},
		dmnByDecision:      map[string]*enginev1.ModelRef{},
		bpmnByKey:          map[string]*enginev1.ModelRef{},
		cmmnByKey:          map[string]*enginev1.ModelRef{},
		setNS:              map[string]bool{},
		setDecision:        map[string]bool{},
		setBpmn:            map[string]bool{},
		setCmmn:            map[string]bool{},
	}
}

// put inserts key→ref. A set entry overwrites an existing one (set precedence) and
// errors on a second set entry for the same key (ambiguous within the set). An
// existing entry only fills a gap, never overwrites a set claim.
func put(idx map[string]*enginev1.ModelRef, setKeys map[string]bool, key string, ref *enginev1.ModelRef, fromSet bool, what string) error {
	if key == "" {
		return nil
	}
	if fromSet {
		if setKeys[key] {
			return fmt.Errorf("processengine: ambiguous %s %q: claimed by two models in the set", what, key)
		}
		setKeys[key] = true
		idx[key] = ref
		return nil
	}
	if !setKeys[key] {
		if _, ok := idx[key]; !ok {
			idx[key] = ref
		}
	}
	return nil
}

// addExisting indexes one already-registered row. DMN rows are parsed for their
// namespace + decision names; BPMN/CMMN rows are indexed by model name only (an
// existing child matches a parent's ref by the established name convention — no
// re-parse needed).
func (c *closureIndex) addExisting(rec *enginev1.ModelRecord) {
	ref := rec.GetModelRef()
	switch ref.GetKind() {
	case "dmn":
		d, err := dmn.Parse(rec.GetXml())
		if err != nil {
			slog.Default().Warn("processengine: skipping unparseable existing dmn row",
				"name", ref.GetName(), "version", ref.GetVersion(), "err", err)
			return
		}
		_ = c.indexDMN(ref, d, false)
	case "bpmn":
		_ = put(c.bpmnByKey, c.setBpmn, ref.GetName(), ref, false, "bpmn model name")
	case "cmmn":
		_ = put(c.cmmnByKey, c.setCmmn, ref.GetName(), ref, false, "cmmn model name")
	}
}

// addSet indexes one set entry. DMN rows contribute namespace + decision names;
// BPMN/CMMN rows are indexed by both model name and parsed process/case id so a
// CallActivity calledElement (a process id) or a name-convention reference both
// resolve.
func (c *closureIndex) addSet(ref *enginev1.ModelRef, kind string, graph *bpmn.ProcessGraph, caseD *cmmn.CaseDefinition, defs *dmn.Definitions) error {
	switch kind {
	case "dmn":
		return c.indexDMN(ref, defs, true)
	case "bpmn":
		if err := put(c.bpmnByKey, c.setBpmn, ref.GetName(), ref, true, "bpmn model name"); err != nil {
			return err
		}
		if graph != nil && graph.ID != ref.GetName() {
			if err := put(c.bpmnByKey, c.setBpmn, graph.ID, ref, true, "bpmn process id"); err != nil {
				return err
			}
		}
	case "cmmn":
		if err := put(c.cmmnByKey, c.setCmmn, ref.GetName(), ref, true, "cmmn model name"); err != nil {
			return err
		}
		if caseD != nil && caseD.ID != ref.GetName() {
			if err := put(c.cmmnByKey, c.setCmmn, caseD.ID, ref, true, "cmmn case id"); err != nil {
				return err
			}
		}
	}
	return nil
}

// indexDMN registers a DMN's namespace and decision/decision-service names.
func (c *closureIndex) indexDMN(ref *enginev1.ModelRef, defs *dmn.Definitions, fromSet bool) error {
	if ns := defs.Namespace; ns != "" {
		c.dmnDefsByNamespace[ns] = defs // last writer wins; set runs after existing
		if err := put(c.dmnByNamespace, c.setNS, ns, ref, fromSet, "dmn namespace"); err != nil {
			return err
		}
	}
	for i := range defs.Decisions {
		if err := put(c.dmnByDecision, c.setDecision, defs.Decisions[i].Name, ref, fromSet, "dmn decision name"); err != nil {
			return err
		}
	}
	for i := range defs.DecisionServices {
		if err := put(c.dmnByDecision, c.setDecision, defs.DecisionServices[i].Name, ref, fromSet, "dmn decision-service name"); err != nil {
			return err
		}
	}
	return nil
}

// deriveBundle walks a model's graph and resolves each reference to a ModelRef.
// imports are strict (unresolved → error); decisions/children are pinned only
// when resolvable.
func (c *closureIndex) deriveBundle(ref *enginev1.ModelRef, kind string, graph *bpmn.ProcessGraph, caseD *cmmn.CaseDefinition, defs *dmn.Definitions) (*enginev1.ModelBundle, error) {
	b := &enginev1.ModelBundle{}
	switch kind {
	case "bpmn":
		for _, node := range graph.Nodes {
			switch n := node.Node.(type) {
			case *bpmn.BusinessRuleTask:
				pinDecision(b, c.dmnByDecision, n.Name)
			case *bpmn.CallActivity:
				pinChild(b, c.bpmnByKey, n.CalledElement)
			}
		}
	case "cmmn":
		for _, def := range caseD.DefinitionByID {
			switch d := def.(type) {
			case cmmn.DecisionTaskDef:
				pinDecision(b, c.dmnByDecision, d.DecisionRef)
			case cmmn.ProcessTaskDef:
				pinChild(b, c.bpmnByKey, d.ProcessRef)
			case cmmn.CaseTaskDef:
				pinChild(b, c.cmmnByKey, d.CaseRef)
			}
		}
	case "dmn":
		for i := range defs.Imports {
			ns := defs.Imports[i].Namespace
			if ns == "" {
				continue
			}
			target := c.dmnByNamespace[ns]
			if target == nil {
				return nil, fmt.Errorf("processengine: dmn %s/%s imports namespace %q, "+
					"which no model in the set or table provides", ref.GetName(), ref.GetVersion(), ns)
			}
			if b.Imports == nil {
				b.Imports = map[string]*enginev1.ModelRef{}
			}
			b.Imports[ns] = target
		}
	}
	if len(b.GetDecisions()) == 0 && len(b.GetChildren()) == 0 && len(b.GetImports()) == 0 {
		return nil, nil
	}
	return b, nil
}

// pinDecision records a decision edge when the ref resolves to a DMN; an
// unresolvable (or empty) ref is left for the runtime resolver / eval to handle.
func pinDecision(b *enginev1.ModelBundle, dmnByDecision map[string]*enginev1.ModelRef, decisionRef string) {
	if decisionRef == "" {
		return
	}
	target := dmnByDecision[decisionRef]
	if target == nil {
		return
	}
	if b.Decisions == nil {
		b.Decisions = map[string]*enginev1.ModelRef{}
	}
	b.Decisions[decisionRef] = target
}

// pinChild records a child edge when the ref resolves; an unresolvable (or empty)
// ref is left for the runtime resolver's name-convention fallback.
func pinChild(b *enginev1.ModelBundle, childIndex map[string]*enginev1.ModelRef, childRef string) {
	if childRef == "" {
		return
	}
	target := childIndex[childRef]
	if target == nil {
		return
	}
	if b.Children == nil {
		b.Children = map[string]*enginev1.ModelRef{}
	}
	b.Children[childRef] = target
}

// joinIssues renders a model's static-validation findings as a single string.
func joinIssues(issues []bpmn.StaticValidationIssue) string {
	parts := make([]string, len(issues))
	for i, is := range issues {
		parts[i] = is.String()
	}
	return strings.Join(parts, "; ")
}

// joinDMNIssues renders a DMN model's validation findings as a single string.
func joinDMNIssues(issues []dmn.ValidationIssue) string {
	parts := make([]string, len(issues))
	for i, is := range issues {
		parts[i] = is.String()
	}
	return strings.Join(parts, "; ")
}
