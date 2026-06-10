package processengine

import (
	"context"
	"encoding/json"
	"fmt"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	"github.com/twinfer/reflwos/bpmn"
	"github.com/twinfer/reflwos/cmmn"
	"github.com/twinfer/reflwos/schemagen"
)

// TaskSchema derives the submission JSON Schema for a parked task (BPMN user task
// / CMMN human task) addressed by (modelRef, nodeID) — the model-derived schema
// the ingress GET /v1/tasks/{token} surface returns alongside the task descriptor.
// It runs off the apply path (a human-initiated read), so it parses the cached
// model document lazily here rather than at every reconcile; the engine turn
// machine never calls it, so the no-per-turn-I/O determinism contract is unaffected.
//
// Returns (nil, nil) when the model resolves but the task declares no typed
// completion contract (no ioSpecification / output parameters) — the caller then
// surfaces the descriptor without a schema. Returns an error only when the model
// itself is unresolvable or unparseable.
//
// The bytes are a self-contained JSON Schema: the task's input object plus the
// model's $defs, so a structureRef-resolved property ($ref into $defs) stays
// dereferenceable standalone. Reflw forwards these bytes opaquely — schema
// *generation* is a reflwos (gateway) concern, surfaced here through the resolver
// the engine already injects.
func (r *TableResolver) TaskSchema(_ context.Context, modelRef *enginev1.ModelRef, nodeID string) ([]byte, error) {
	pm := r.lookup(modelRef)
	if pm == nil || len(pm.xml) == 0 {
		return nil, fmt.Errorf("%w: %s %q/%q", ErrModelNotFound, modelRef.GetKind(), modelRef.GetName(), modelRef.GetVersion())
	}
	doc, err := schemaDocForModel(modelRef.GetKind(), pm)
	if err != nil {
		return nil, err
	}
	for i := range doc.Operations {
		op := &doc.Operations[i]
		if op.Kind == schemagen.KindCompleteTask && op.OperationID == nodeID {
			return marshalTaskSchema(op.Input, doc.Defs)
		}
	}
	return nil, nil // model resolved, but this task declares no typed completion contract
}

// schemaDocForModel parses the cached model document and runs the plane-specific
// schemagen pass. payload is nil: an itemDefinition / caseFileItem structureRef
// pointing at an imported XSD type resolves to an open schema tagged
// x-structure-ref (the property name + required-ness are still emitted), which is
// sufficient for the resume-point contract; full XSD payload resolution is a
// gateway elaboration. prefixes (best-effort) sharpen QName resolution.
func schemaDocForModel(kind string, pm *parsedModel) (*schemagen.Document, error) {
	switch kind {
	case "bpmn":
		defs, err := bpmn.ParseDocument(pm.xml)
		if err != nil {
			return nil, fmt.Errorf("parse bpmn document: %w", err)
		}
		var prefixes map[string]string
		if env, eerr := bpmn.ParseEnvelope(pm.xml); eerr == nil {
			prefixes = env.Prefixes
		}
		return schemagen.FromBPMN(defs, nil, prefixes), nil
	case "cmmn":
		defs, err := cmmn.ParseDocument(pm.xml)
		if err != nil {
			return nil, fmt.Errorf("parse cmmn document: %w", err)
		}
		var (
			prefixes map[string]string
			imports  []cmmn.Import
		)
		if env, eerr := cmmn.ParseEnvelope(pm.xml); eerr == nil {
			prefixes, imports = env.Prefixes, env.Imports
		}
		return schemagen.FromCMMN(defs, nil, prefixes, imports), nil
	default:
		return nil, fmt.Errorf("unsupported model kind %q for task schema", kind)
	}
}

// marshalTaskSchema renders a completion operation's input schema as a
// self-contained JSON Schema document, grafting the model's $defs so any $ref the
// input carries stays resolvable. Returns nil when the input is empty.
func marshalTaskSchema(input *schemagen.Schema, defs map[string]*schemagen.Schema) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	inBytes, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal task input schema: %w", err)
	}
	if len(defs) == 0 {
		return inBytes, nil
	}
	// A schemagen.Schema always marshals to a JSON object, so splicing "$defs"
	// onto it yields a valid standalone schema.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(inBytes, &obj); err != nil {
		return nil, fmt.Errorf("splice $defs: %w", err)
	}
	defsBytes, err := json.Marshal(defs)
	if err != nil {
		return nil, fmt.Errorf("marshal $defs: %w", err)
	}
	obj["$defs"] = defsBytes
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal task schema: %w", err)
	}
	return out, nil
}
