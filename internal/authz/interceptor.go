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
}

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
// action is the full Connect procedure path; the principal comes from the
// context. For now the resource is a single PlatformConfig sentinel —
// foundational policies leave resource unconstrained and cedar.Authorize never
// consults the schema at eval. Per-record resource extraction (with tenant_id)
// arrives in PR4 via a procedure->resource map.
func (i *Interceptor) authorize(ctx context.Context, procedure string) error {
	principal, _ := auth.PrincipalFromContext(ctx)
	pUID, pEnt := PrincipalEntity(principal)
	entities := types.EntityMap{
		pUID:              pEnt,
		PlatformConfigUID: {UID: PlatformConfigUID},
	}
	decision, _ := i.engine.Authorize(cedar.Request{
		Principal: pUID,
		Action:    cedar.NewEntityUID("Action", cedar.String(procedure)),
		Resource:  PlatformConfigUID,
	}, entities)
	if decision == cedar.Allow {
		return nil
	}
	i.log.Warn("authz: denied", "procedure", procedure, "principal", principal.String())
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
		if err := i.authorize(ctx, req.Spec().Procedure); err != nil {
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
		if err := i.authorize(ctx, conn.Spec().Procedure); err != nil {
			return err
		}
		return next(ctx, conn)
	}
}
