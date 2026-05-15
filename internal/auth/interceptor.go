package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/authz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// PrincipalHeader is the metadata key the shim interceptor stamps with
// the server-computed Principal.Raw before the authz interceptor runs.
// The embedded starter policy matches on this header; custom policies
// SHOULD do the same so authn and authz are decoupled.
const PrincipalHeader = "x-reflow-principal"

// FileWatcherReload is how often grpc-go's authz.FileWatcher re-reads
// the policy file. 30 seconds is the operator-facing default; not
// configurable today.
const FileWatcherReload = 30 * time.Second

// Config drives NewServerInterceptors. Extractor is the principal
// source; PolicyFile, when set, points at a JSON authz policy that
// grpc-go reloads on a 30s cadence. Empty PolicyFile installs the
// embedded starter policy from starter_policy.json.
type Config struct {
	Extractor  Extractor
	PolicyFile string
	Log        *slog.Logger
}

// NewServerInterceptors returns the chained (unary, stream)
// interceptor pair installed onto every reflow gRPC server. The chain
// is:
//
//  1. Principal-stamping shim — strips any forged x-reflow-principal
//     header, runs the Extractor, then stamps the server-computed
//     Principal.Raw onto the incoming metadata.
//  2. grpc-go authz interceptor — matches the stamped header against
//     the JSON policy.
//
// closer releases the FileWatcher goroutine when one is in use; safe
// to call when nil (Static policies have no resources to free).
func NewServerInterceptors(cfg Config) (unary grpc.UnaryServerInterceptor, stream grpc.StreamServerInterceptor, closer func() error, err error) {
	if cfg.Extractor == nil {
		return nil, nil, nil, errors.New("auth: Config.Extractor is required")
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	var (
		authzUnary  grpc.UnaryServerInterceptor
		authzStream grpc.StreamServerInterceptor
		closeFn     func() error
	)
	switch {
	case cfg.PolicyFile != "":
		fw, ferr := authz.NewFileWatcher(cfg.PolicyFile, FileWatcherReload)
		if ferr != nil {
			return nil, nil, nil, fmt.Errorf("auth: load policy %s: %w", cfg.PolicyFile, ferr)
		}
		authzUnary = fw.UnaryInterceptor
		authzStream = fw.StreamInterceptor
		closeFn = func() error { fw.Close(); return nil }
	default:
		si, serr := authz.NewStatic(StarterPolicyJSON)
		if serr != nil {
			return nil, nil, nil, fmt.Errorf("auth: load embedded policy: %w", serr)
		}
		authzUnary = si.UnaryInterceptor
		authzStream = si.StreamInterceptor
	}

	shimUnary, shimStream := newPrincipalShim(cfg.Extractor, log)

	unary = chainUnary(shimUnary, authzUnary)
	stream = chainStream(shimStream, authzStream)
	return unary, stream, closeFn, nil
}

// newPrincipalShim builds the unary+stream shim pair that strips any
// inbound x-reflow-principal header (forgery defense), runs the
// Extractor, and stamps Principal.Raw into the metadata so the
// downstream authz interceptor matches the server-computed identity.
func newPrincipalShim(e Extractor, log *slog.Logger) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	if log == nil {
		log = slog.Default()
	}
	stamp := func(ctx context.Context, fullMethod string) (context.Context, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		md = md.Copy()
		// Forgery defense: discard whatever the client supplied —
		// the policy MUST see only what this server extracted.
		md.Delete(PrincipalHeader)
		principal, perr := e.Extract(ctx)
		if perr != nil {
			log.Warn("auth: extractor rejected request",
				"method", fullMethod, "err", perr)
			return ctx, status.Errorf(codes.Unauthenticated,
				"auth: %v", perr)
		}
		if !principal.IsAnonymous() {
			md.Set(PrincipalHeader, principal.Raw)
		}
		ctx = metadata.NewIncomingContext(ctx, md)
		ctx = ContextWithPrincipal(ctx, principal)
		return ctx, nil
	}
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		stamped, err := stamp(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return h(stamped, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		stamped, err := stamp(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return h(srv, &wrappedServerStream{ServerStream: ss, ctx: stamped})
	}
	return unary, stream
}

// wrappedServerStream re-points stream.Context() at the shim-stamped
// context so the downstream authz interceptor and the handler both see
// PrincipalHeader on the incoming metadata.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context { return w.ctx }

// chainUnary composes two unary interceptors so first runs before
// second. Mirrors grpc.ChainUnaryInterceptor's order semantics.
func chainUnary(first, second grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	if second == nil {
		return first
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return first(ctx, req, info, func(ctx context.Context, req any) (any, error) {
			return second(ctx, req, info, h)
		})
	}
}

// chainStream composes two stream interceptors so first runs before
// second.
func chainStream(first, second grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	if second == nil {
		return first
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		return first(srv, ss, info, func(srv any, inner grpc.ServerStream) error {
			return second(srv, inner, info, h)
		})
	}
}
