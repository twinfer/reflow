package auth

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// authInfoFromContext extracts the AuthInfo material the interceptors
// hand to the ClaimMapper. Only the TLS half is populated today.
func authInfoFromContext(ctx context.Context) AuthInfo {
	info := AuthInfo{}
	if p, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			info.TLSConnection = &tlsInfo
		}
	}
	// TODO(jwt): pull the Authorization header off the gRPC metadata
	// and stash it as info.AuthToken when a JWTClaimMapper lands.
	return info
}

// UnaryInterceptor wires the mapper → audit log → authorizer chain
// onto a unary gRPC server. Successful calls see Claims on the
// downstream context via ClaimsFromContext.
//
// Mapper or Authorizer errors surface as Unauthenticated /
// PermissionDenied. The audit line carries the FullMethod and the
// caller (or the reason for rejection) so log streams remain useful in
// both happy and rejection paths.
func UnaryInterceptor(mapper ClaimMapper, authz Authorizer, log *slog.Logger) grpc.UnaryServerInterceptor {
	if log == nil {
		log = slog.Default()
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		_, mapped, err := runAuthChain(ctx, mapper, authz, info.FullMethod, log)
		if err != nil {
			return nil, err
		}
		return h(mapped, req)
	}
}

// wrappedServerStream is a tiny shim so handlers calling
// stream.Context() see the claims-augmented context. Wrapping is the
// stdlib pattern for stream interceptors that need to mutate context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context { return w.ctx }

// StreamInterceptor is the streaming analogue of UnaryInterceptor.
// Authorization runs once at stream open; the handler never runs on
// rejection.
func StreamInterceptor(mapper ClaimMapper, authz Authorizer, log *slog.Logger) grpc.StreamServerInterceptor {
	if log == nil {
		log = slog.Default()
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		_, mapped, err := runAuthChain(ss.Context(), mapper, authz, info.FullMethod, log)
		if err != nil {
			return err
		}
		return h(srv, &wrappedServerStream{ServerStream: ss, ctx: mapped})
	}
}

// runAuthChain centralises the mapper+log+authorize sequence shared by
// the unary and stream interceptors. It returns the parsed claims, the
// context augmented with those claims (so handlers can call
// ClaimsFromContext), and any rejection error already wrapped in a
// gRPC status.
func runAuthChain(
	ctx context.Context,
	mapper ClaimMapper,
	authz Authorizer,
	fullMethod string,
	log *slog.Logger,
) (*Claims, context.Context, error) {
	claims, mErr := mapper.GetClaims(ctx, authInfoFromContext(ctx))
	if mErr != nil {
		log.Warn("auth: claim mapping failed",
			"method", fullMethod, "err", mErr)
		return nil, ctx, status.Errorf(codes.Unauthenticated,
			"auth: claim mapping failed: %v", mErr)
	}
	if claims == nil {
		log.Warn("auth: no identity", "method", fullMethod)
		return nil, ctx, status.Error(codes.Unauthenticated,
			"auth: missing peer identity")
	}
	res, aErr := authz.Authorize(ctx, claims, &CallTarget{APIName: fullMethod})
	if aErr != nil {
		log.Error("auth: authorizer error",
			"method", fullMethod, "err", aErr, "caller", claims)
		return nil, ctx, status.Errorf(codes.Internal,
			"auth: authorizer error: %v", aErr)
	}
	if res.Decision != DecisionAllow {
		log.Warn("auth: rpc denied",
			"method", fullMethod, "caller", claims, "reason", res.Reason)
		return nil, ctx, status.Errorf(codes.PermissionDenied,
			"auth: %s denied: %s", fullMethod, res.Reason)
	}
	log.Info("auth: rpc",
		"method", fullMethod, "kind", claims.Kind, "subject", claims.Subject)
	return claims, ContextWithClaims(ctx, claims), nil
}
