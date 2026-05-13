package admin

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	optionsv1 "github.com/twinfer/reflow/proto/optionsv1"
)

// RoleMatcher reports whether the caller's identity satisfies a
// required role declared in the proto. The default exact-Kind matcher
// is enough for the single-role world; sub-role semantics (path-prefix
// match) plug in by passing a different matcher to AuthzInterceptor.
type RoleMatcher func(actual PeerIdentity, required string) bool

// DefaultRoleMatcher is a strict equality check against PeerIdentity.Kind.
func DefaultRoleMatcher(actual PeerIdentity, required string) bool {
	return actual.Kind == required
}

// BuildMethodPolicy walks svc and returns a map keyed by gRPC
// FullMethod ("/<package>.<service>/<method>") with the required SPIFFE
// role for each RPC. A method-level required_spiffe_role wins; otherwise
// the service-level default_required_spiffe_role applies. Returns an
// error if any method ends up without a role — drift between the proto
// and the authz layer is caught at startup, not at first call.
func BuildMethodPolicy(svc protoreflect.ServiceDescriptor) (map[string]string, error) {
	if svc == nil {
		return nil, fmt.Errorf("admin: BuildMethodPolicy called with nil service descriptor")
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
				"admin: method %s has no required_spiffe_role and service has no default",
				m.FullName(),
			)
		}
		out["/"+string(svc.FullName())+"/"+string(m.Name())] = role
	}
	return out, nil
}

// AdminMethodPolicy returns the policy map for the Admin service,
// computed from the compiled-in proto descriptor.
func AdminMethodPolicy() (map[string]string, error) {
	svc := adminv1.File_adminv1_admin_proto.Services().ByName("Admin")
	if svc == nil {
		return nil, fmt.Errorf("admin: Admin service descriptor missing")
	}
	return BuildMethodPolicy(svc)
}

// AuthzInterceptor returns a unary interceptor that rejects calls whose
// PeerIdentity does not satisfy the policy entry for info.FullMethod.
// match defaults to DefaultRoleMatcher when nil. The caller's
// PeerIdentity is expected on the context — AuditInterceptor must run
// first in a chain.
func AuthzInterceptor(policy map[string]string, match RoleMatcher) grpc.UnaryServerInterceptor {
	if match == nil {
		match = DefaultRoleMatcher
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		required, ok := policy[info.FullMethod]
		if !ok {
			return nil, status.Errorf(codes.Internal,
				"admin: no authz policy for %s", info.FullMethod)
		}
		id, ok := PeerIdentityFromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated,
				"admin: missing peer identity")
		}
		if !match(id, required) {
			return nil, status.Errorf(codes.PermissionDenied,
				"admin: %s requires role %q; caller is %q",
				info.FullMethod, required, id.Kind)
		}
		return h(ctx, req)
	}
}
