package authz

import (
	"context"
	"errors"
	"log/slog"

	connect "connectrpc.com/connect"

	"github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"

	"github.com/twinfer/reflow/internal/auth"
)

// Sentinel denial messages. Opaque on the wire so authz does not leak which
// procedure or principal was rejected.
var (
	errUnauthenticated = errors.New("unauthenticated")
	errForbidden       = errors.New("forbidden")
)

// Interceptor is the Cedar authorization enforcement point, shared by every
// Connect service (ingress, config, clusterctl, delivery). It implements the
// full connect.Interceptor interface so streaming RPCs (Delivery/Deliver) are
// gated too — connect.UnaryInterceptorFunc silently skips streaming handlers.
//
// Authentication already happened in the HTTP middleware (internal/auth),
// which attached the verified principal to the context; this interceptor only
// authorizes.
type Interceptor struct {
	engine        *Engine
	log           *slog.Logger
	bearerEnabled bool // emit WWW-Authenticate: Bearer on anonymous denials
	// resourceTenant, when set, recovers the tenant band of the resource an
	// ingress request acts on (for the Invocation resource's tenant_id). Only
	// needed for by-id RPCs, where the id can name another tenant's resource;
	// by-target RPCs route into the caller's own band, so a nil resolver (and
	// the false return) falls back to the principal's band. Injected by the
	// run wiring (which knows the ingress message shapes); nil in unit tests.
	resourceTenant ResourceTenantFunc
}

// ResourceTenantFunc recovers the tenant band of the resource an ingress
// request targets. msg is the unary request message. ok=false means "not
// determinable from the message" — the interceptor then uses the principal's
// own band, which is correct for by-target ingress (it routes there anyway).
type ResourceTenantFunc func(procedure string, msg any) (tenant uint32, ok bool)

// SetResourceResolver attaches the ingress resource-tenant resolver. Called
// once by the run wiring after construction; safe before the interceptor is
// serving. A nil resolver leaves by-id tenant isolation to the principal-band
// default (sufficient for unit tests; production wires the real resolver).
func (i *Interceptor) SetResourceResolver(f ResourceTenantFunc) { i.resourceTenant = f }

// NewInterceptor builds the enforcement interceptor against engine. bearerEnabled
// should mirror "an OIDC issuer is configured" so anonymous 401s advertise the
// bearer scheme (RFC 7235) only when a token path actually exists.
func NewInterceptor(engine *Engine, log *slog.Logger, bearerEnabled bool) *Interceptor {
	if log == nil {
		log = slog.Default()
	}
	return &Interceptor{engine: engine, log: log, bearerEnabled: bearerEnabled}
}

// NewFoundationalInterceptor builds an Interceptor over a fresh engine loaded
// with the in-binary FoundationalClusterPolicies — the default authorization
// until cluster-managed policy lands (PR3). The foundational policies are
// compile-time valid, so this only errors on an internal regression. Handy
// for tests that stand up a single Connect service without the full run.go
// wiring.
func NewFoundationalInterceptor(log *slog.Logger, bearerEnabled bool) (*Interceptor, error) {
	eng, err := NewEngine([]byte(FoundationalClusterPolicies))
	if err != nil {
		return nil, err
	}
	return NewInterceptor(eng, log, bearerEnabled), nil
}

// authorize evaluates one procedure call against the live policy set. The
// action is the procedure's Cedar action id plus its plane-group parents
// (procmap.actionEntity); the principal comes from the context. An unmapped
// procedure is default-denied — a new RPC must be classified in procMap before
// it is reachable (procmap_test enforces coverage).
//
// Resource: non-ingress procedures evaluate against the PlatformConfig
// sentinel (config/clusterctl/mesh planes don't carry a tenant). Ingress
// procedures build a tenant-scoped Invocation resource so the isolation
// when-clause can compare resource.tenant_id to the principal's band. The
// tenant defaults to the principal's own band (correct for by-target ingress,
// which routes there); the injected resolver overrides it for by-id RPCs,
// where the request id can name another tenant's resource. msg is the unary
// request message (nil on the streaming path, which is mesh-only).
func (i *Interceptor) authorize(ctx context.Context, procedure string, msg any) error {
	principal, _ := auth.PrincipalFromContext(ctx)
	pUID, pEnt := PrincipalEntity(principal)
	aUID, aEnt, ok := actionEntity(procedure)
	if !ok {
		i.log.Warn("authz: denied unmapped procedure", "procedure", procedure, "principal", principal.String())
		return i.deny(principal)
	}

	resUID := PlatformConfigUID
	resEnt := types.Entity{UID: PlatformConfigUID}
	if isIngressProcedure(procedure) {
		tenant := auth.TenantIDFromPrincipal(principal)
		if i.resourceTenant != nil {
			if rt, rok := i.resourceTenant(procedure, msg); rok {
				tenant = rt
			}
		}
		resUID = InvocationResourceUID
		resEnt = types.Entity{UID: resUID, Attributes: types.NewRecord(types.RecordMap{
			"tenant_id": types.Long(int64(tenant)),
			"service":   types.String(""),
		})}
	}

	decision, _ := i.engine.Authorize(cedar.Request{
		Principal: pUID,
		Action:    aUID,
		Resource:  resUID,
	}, types.EntityMap{pUID: pEnt, aUID: aEnt, resUID: resEnt})
	if decision == cedar.Allow {
		return nil
	}
	i.log.Warn("authz: denied", "procedure", procedure, "principal", principal.String())
	return i.deny(principal)
}

// deny maps a denial to the right Connect code: anonymous callers get
// Unauthenticated (plus a WWW-Authenticate: Bearer challenge when an OIDC path
// exists), known principals get PermissionDenied. Messages are opaque so authz
// leaks neither which procedure nor which principal was rejected.
func (i *Interceptor) deny(principal auth.Principal) error {
	if principal.IsAnonymous() {
		err := connect.NewError(connect.CodeUnauthenticated, errUnauthenticated)
		if i.bearerEnabled {
			err.Meta().Set("WWW-Authenticate", "Bearer")
		}
		return err
	}
	return connect.NewError(connect.CodePermissionDenied, errForbidden)
}

func (i *Interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := i.authorize(ctx, req.Spec().Procedure, req.Any()); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// WrapStreamingClient is a pass-through: this interceptor enforces inbound
// authorization on the handler side only.
func (i *Interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i *Interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		// Streaming RPCs are mesh-only (Delivery/Deliver) — no per-message
		// resource; nil msg falls to the PlatformConfig sentinel.
		if err := i.authorize(ctx, conn.Spec().Procedure, nil); err != nil {
			return err
		}
		return next(ctx, conn)
	}
}
