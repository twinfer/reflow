package auth

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	optionsv1 "github.com/twinfer/reflow/proto/optionsv1"
)

// RoleMatcher decides whether the caller's claims satisfy the required
// role string declared in proto. The default exact-Kind matcher is
// enough for today's "node" vs "operator" world; sub-role semantics
// (path-prefix match) plug in by passing a different matcher to
// ProtoPolicyAuthorizer.WithMatcher.
type RoleMatcher func(claims *Claims, required string) bool

// DefaultRoleMatcher is strict equality on Claims.Kind. Nil claims
// always fail.
func DefaultRoleMatcher(claims *Claims, required string) bool {
	if claims == nil {
		return false
	}
	return claims.Kind == required
}

// BuildMethodPolicy walks svc and returns a map keyed by gRPC
// FullMethod ("/<package>.<service>/<method>") with the required SPIFFE
// role for each RPC. Method-level required_spiffe_role wins; otherwise
// the service-level default_required_spiffe_role applies. Returns an
// error if any method ends up without a role — drift between the proto
// and the authz layer is caught at startup, not at first call.
func BuildMethodPolicy(svc protoreflect.ServiceDescriptor) (map[string]string, error) {
	if svc == nil {
		return nil, fmt.Errorf("auth: BuildMethodPolicy called with nil service descriptor")
	}
	serviceDefault, _ := proto.GetExtension(
		svc.Options(), optionsv1.E_DefaultRequiredSpiffeRole,
	).(string)

	methods := svc.Methods()
	out := make(map[string]string, methods.Len())
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		methodRole, _ := proto.GetExtension(
			m.Options(), optionsv1.E_RequiredSpiffeRole,
		).(string)
		role := methodRole
		if role == "" {
			role = serviceDefault
		}
		if role == "" {
			return nil, fmt.Errorf(
				"auth: method %s has no required_spiffe_role and service has no default",
				m.FullName(),
			)
		}
		out["/"+string(svc.FullName())+"/"+string(m.Name())] = role
	}
	return out, nil
}

// ProtoPolicyAuthorizer enforces a FullMethod->role map produced by
// BuildMethodPolicy. Combine with the default matcher (exact Kind) or
// inject a custom one via WithMatcher.
type ProtoPolicyAuthorizer struct {
	policy  map[string]string
	matcher RoleMatcher
}

// NewProtoPolicyAuthorizer wraps policy with the default exact-Kind
// matcher.
func NewProtoPolicyAuthorizer(policy map[string]string) *ProtoPolicyAuthorizer {
	return &ProtoPolicyAuthorizer{policy: policy, matcher: DefaultRoleMatcher}
}

// WithMatcher swaps the matcher. Nil is a no-op (retains the default).
// Returns a so callers can chain construction.
func (a *ProtoPolicyAuthorizer) WithMatcher(m RoleMatcher) *ProtoPolicyAuthorizer {
	if m != nil {
		a.matcher = m
	}
	return a
}

// Authorize implements Authorizer. A target with no policy entry fails
// closed (Deny + "no policy") — this surfaces proto/authz-table drift
// at runtime instead of silently allowing.
func (a *ProtoPolicyAuthorizer) Authorize(_ context.Context, claims *Claims, target *CallTarget) (Result, error) {
	required, ok := a.policy[target.APIName]
	if !ok {
		return Result{Decision: DecisionDeny,
			Reason: fmt.Sprintf("no policy for %s", target.APIName)}, nil
	}
	if claims == nil {
		return Result{Decision: DecisionDeny, Reason: "no claims"}, nil
	}
	if !a.matcher(claims, required) {
		return Result{Decision: DecisionDeny,
			Reason: fmt.Sprintf("requires role %q; caller is %q", required, claims.Kind)}, nil
	}
	return Result{Decision: DecisionAllow}, nil
}
