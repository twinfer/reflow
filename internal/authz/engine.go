// Package authz is Reflow's Cedar-based authorization engine. It resolves
// the embedded schema once, compiles policy text into a Cedar PolicySet,
// validates every policy against the schema at compile time (layer 1), and
// evaluates authorization requests against an atomically-swappable snapshot.
//
// Authn (who the caller is) stays in internal/auth; this package is authz
// (what the caller may do). The enforcement seam is a Connect interceptor
// (PR2) — it needs the decoded request body to extract resource attributes,
// which the HTTP-layer policy handler never saw.
package authz

import (
	_ "embed"
	"fmt"
	"sync/atomic"

	"github.com/cedar-policy/cedar-go"
	cedarast "github.com/cedar-policy/cedar-go/ast"
	"github.com/cedar-policy/cedar-go/types"
	xexpast "github.com/cedar-policy/cedar-go/x/exp/ast"
	"github.com/cedar-policy/cedar-go/x/exp/schema"
	"github.com/cedar-policy/cedar-go/x/exp/schema/resolved"
	"github.com/cedar-policy/cedar-go/x/exp/schema/validate"
)

//go:embed schema.cedar
var schemaText []byte

// Engine holds the resolved schema, a schema-bound validator, and the live
// policy set. Authorize is the hot path (one atomic load + one evaluation);
// SetPolicies swaps the compiled bundle on a reconcile.
type Engine struct {
	resolved  *resolved.Schema
	validator *validate.Validator
	policies  atomic.Pointer[cedar.PolicySet]
}

// NewEngine resolves the embedded schema and installs an initial policy set,
// validating it against the schema. A validation failure here is a
// programming error (the foundational policies must conform), so callers
// treat the error as fatal at startup.
func NewEngine(policyText []byte) (*Engine, error) {
	rs, err := resolveSchema()
	if err != nil {
		return nil, err
	}
	e := &Engine{resolved: rs, validator: validate.New(rs)}
	ps, err := e.CompileAndValidate(policyText)
	if err != nil {
		return nil, err
	}
	e.policies.Store(ps)
	return e, nil
}

// resolveSchema parses and resolves the embedded schema.cedar.
func resolveSchema() (*resolved.Schema, error) {
	var s schema.Schema
	if err := s.UnmarshalCedar(schemaText); err != nil {
		return nil, fmt.Errorf("authz: parse embedded schema: %w", err)
	}
	rs, err := s.Resolve()
	if err != nil {
		return nil, fmt.Errorf("authz: resolve embedded schema: %w", err)
	}
	return rs, nil
}

// CompileAndValidate parses policyText into a PolicySet and runs layer-1
// schema validation on every policy — type errors and appliesTo violations
// are caught here at compile/upload time, never at evaluation. The returned
// set is not installed; callers pass it to SetPolicies once accepted.
func (e *Engine) CompileAndValidate(policyText []byte) (*cedar.PolicySet, error) {
	ps, err := cedar.NewPolicySetFromBytes("authz.cedar", policyText)
	if err != nil {
		return nil, fmt.Errorf("authz: parse policies: %w", err)
	}
	for id, p := range ps.All() {
		// cedar/ast.Policy has the same underlying type as x/exp/ast.Policy
		// (it is declared `type Policy ast.Policy`), so this pointer
		// conversion is the supported bridge into the x/exp validator.
		xp := (*xexpast.Policy)((*cedarast.Policy)(p.AST()))
		if err := e.validator.Policy(string(id), xp); err != nil {
			return nil, fmt.Errorf("authz: policy %q fails schema validation: %w", id, err)
		}
	}
	return ps, nil
}

// SetPolicies atomically swaps the live policy set. Used by the per-node
// reconciler (PR3) when shard-0 policy text changes.
func (e *Engine) SetPolicies(ps *cedar.PolicySet) { e.policies.Store(ps) }

// Authorize evaluates req against the live policy set. Cedar is default-deny:
// a nil policy set or no matching permit yields Deny.
func (e *Engine) Authorize(req cedar.Request, entities types.EntityGetter) (cedar.Decision, cedar.Diagnostic) {
	ps := e.policies.Load()
	if ps == nil {
		return cedar.Deny, cedar.Diagnostic{}
	}
	return cedar.Authorize(ps, entities, req)
}
