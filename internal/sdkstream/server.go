// Package sdkstream hosts the gRPC bidi-streaming entrypoint for
// out-of-process SDKs (TypeScript, Python, future Rust, etc.).
//
// Phase 2 ships the wire contract (proto/sdkv1.SessionService) and a
// server-side stub registered alongside the ingress gRPC server. The stub
// returns Unimplemented on Invoke so external clients can discover the
// service descriptor via reflection without crashing the engine.
//
// Production wiring lands when a non-Go SDK exists:
//
//   - SDK dials Invoke and emits a registration frame (TBD; likely
//     reuses StartInvocation with an empty InvocationId).
//   - Engine adds the stream to a per-service routing pool.
//   - When the Invoker would normally spawn an in-process handler, it
//     instead picks a connected SDK stream and forwards the
//     StartInvocation downstream.
//   - SDK runs the handler, frames ProposeEntry/Completion/EndInvocation
//     back over the stream.
//
// Until then this package is intentionally tiny — just enough to lock the
// proto and prove the gRPC wire is reachable.
package sdkstream

import (
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/internal/engine"
	sdkv1 "github.com/twinfer/reflow/proto/sdkv1"
)

// Server implements sdkv1.SessionServiceServer. Phase 2 returns
// Unimplemented; later phases route the stream into an out-of-process
// handler pool. Stateless, safe for concurrent use.
type Server struct {
	sdkv1.UnimplementedSessionServiceServer

	host *engine.Host
	log  *slog.Logger
}

// New builds a stub Server bound to the given host. Log defaults to
// slog.Default if nil.
func New(host *engine.Host, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{host: host, log: log}
}

// Invoke is the bidi-streaming session protocol. Phase 2 stub: rejects
// all attempts with Unimplemented and logs the connection. Clients can
// still discover the method via gRPC reflection.
func (s *Server) Invoke(stream sdkv1.SessionService_InvokeServer) error {
	peer := "unknown"
	if p, ok := peerFromCtx(stream); ok {
		peer = p
	}
	s.log.Info("sdkstream: Invoke stream opened (stub, returning Unimplemented)", "peer", peer)
	return status.Error(codes.Unimplemented,
		"sdkstream: SessionService.Invoke is a Phase 2 stub; out-of-process SDK routing is not yet wired")
}

// Register adds the stub to an existing gRPC server. Hook from
// ingress.Config.ExtraGRPC or any other gRPC server entry point.
func Register(gs *grpc.Server, host *engine.Host, log *slog.Logger) {
	sdkv1.RegisterSessionServiceServer(gs, New(host, log))
}
