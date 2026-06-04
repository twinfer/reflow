package iflowengine

import (
	"fmt"
	"strings"

	"github.com/twinfer/iflow/bpmn"
	"github.com/twinfer/iflow/cmmn"
)

// ValidateModel parses and statically validates a model definition, returning a
// human-readable error if it is not a runnable BPMN/CMMN model. It is the
// registration-time gate injected into the config UpsertModel RPC: internal/config
// cannot import iflow, so it inverts to this func (the same dependency inversion
// HostConfig.ProcessEngine uses). Without it, registration only checks well-formed
// XML and a structurally-broken model is committed to Raft, then fails silently on
// every node at reconcile (TableResolver preserve-prev) — the operator sees a 200
// while the model never materializes.
//
// Strictness: reject on a parse failure AND on any error-severity static finding
// (bpmn.ValidateStatic / cmmn.Validate). Warnings pass. Pure and off the apply
// path — safe on the leader before proposing the command.
func ValidateModel(kind string, xml []byte) error {
	if len(xml) == 0 {
		return fmt.Errorf("model xml required")
	}
	switch kind {
	case "bpmn":
		g, err := bpmn.Parse(xml, "")
		if err != nil {
			return fmt.Errorf("parse bpmn: %w", err)
		}
		res, err := bpmn.ValidateStatic(g)
		if err != nil {
			return fmt.Errorf("validate bpmn: %w", err)
		}
		if res.HasErrors() {
			return fmt.Errorf("bpmn static validation: %s", joinIssues(res.Errors()))
		}
		return nil
	case "cmmn":
		def, err := cmmn.Parse(xml, "")
		if err != nil {
			return fmt.Errorf("parse cmmn: %w", err)
		}
		if err := cmmn.Validate(def); err != nil {
			return fmt.Errorf("validate cmmn: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("model kind must be %q or %q, got %q", "bpmn", "cmmn", kind)
	}
}

func joinIssues(issues []bpmn.StaticValidationIssue) string {
	parts := make([]string, len(issues))
	for i, is := range issues {
		parts[i] = is.String()
	}
	return strings.Join(parts, "; ")
}
