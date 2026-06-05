package processengine

import (
	"encoding/json"
	"fmt"

	"github.com/twinfer/reflwos/bpmn"
)

// decodeVars decodes a JSON object of variables into a map. Empty/nil bytes or a
// JSON null normalize to an empty (non-nil) map so the engine's merge logic never
// sees a nil it must guard.
func decodeVars(b []byte) (map[string]any, error) {
	if len(b) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// encodeVars marshals a variable map to a JSON object. Deterministic: Go's
// encoding/json emits map keys in sorted order.
func encodeVars(m map[string]any) ([]byte, error) {
	return json.Marshal(m)
}

// externalEvent is the typed envelope for a non-start external event delivered to
// a running instance — the future message/signal-delivery path, reachable once
// reflw actuates SignalSubscribe. It mirrors dboshost/wire's BPMNEventEnvelope:
// a stable Kind discriminator plus the JSON-marshaled event. The START path does
// NOT use this envelope — a start (root or child) carries a bare vars object
// (see decodeVars), distinguished by an empty StateBlob.
type externalEvent struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// decodeBPMNExternalEvent reconstructs a typed bpmn.EngineEvent from an external
// event envelope via the engine's own codec (bpmn.UnmarshalEvent), keeping
// interface round-tripping correct across reflw's opaque External bytes.
func decodeBPMNExternalEvent(b []byte) (bpmn.EngineEvent, error) {
	var env externalEvent
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("processengine: decode external event envelope: %w", err)
	}
	ev, err := bpmn.UnmarshalEvent(env.Kind, env.Payload)
	if err != nil {
		return nil, fmt.Errorf("processengine: external event: %w", err)
	}
	return ev, nil
}
