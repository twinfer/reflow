package reflow

import (
	"context"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/engine"
)

// proposalPrincipalInterceptor bridges auth.Principal (attached to the
// inbound ctx by the Connect authn middleware) into the engine
// proposer's WithProposalPrincipal ctx key. With it installed on the
// admin + config Connect listeners, every Raft proposal originating
// from an operator (or tenant) Connect call carries
// Envelope.Header.principal automatically — no per-handler change
// required.
//
// Engine self-proposals (TimerService, Invoker, lpMover, audit GC)
// never traverse this interceptor and so the proposer sees an empty
// principal; the audit emitter substitutes "engine" for those rows.
//
// Implements the full connect.Interceptor interface (not the
// UnaryInterceptorFunc shorthand) so future streaming admin RPCs
// inherit the same principal-stamping guarantee — see the project
// CLAUDE.md note on Connect interceptors.
type proposalPrincipalInterceptor struct{}

func (proposalPrincipalInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if p, ok := auth.PrincipalFromContext(ctx); ok && !p.IsAnonymous() {
			ctx = engine.WithProposalPrincipal(ctx, p.String())
		}
		return next(ctx, req)
	}
}

func (proposalPrincipalInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	// Server-side concern only; client calls are a no-op.
	return next
}

func (proposalPrincipalInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if p, ok := auth.PrincipalFromContext(ctx); ok && !p.IsAnonymous() {
			ctx = engine.WithProposalPrincipal(ctx, p.String())
		}
		return next(ctx, conn)
	}
}
