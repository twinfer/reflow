package server

import (
	"errors"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// GRPCServer hosts protocolv1.SessionService + DiscoveryService over
// real gRPC. Construct with NewGRPC, then Serve on a listener. One
// Server may host both services because the underlying *grpc.Server is
// shared.
type GRPCServer struct {
	cfg    Config
	gs     *grpc.Server
	mu     sync.Mutex
	closed bool
}

// NewGRPC builds a gRPC handler-side server backed by cfg.Registry. The
// returned *GRPCServer registers protocolv1.SessionService and
// DiscoveryService on its internal *grpc.Server; callers can extract
// that via Server() if they want to add their own interceptors or
// services.
func NewGRPC(cfg Config) (*GRPCServer, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	s := &GRPCServer{cfg: cfg, gs: grpc.NewServer()}
	protocolv1.RegisterSessionServiceServer(s.gs, &grpcSessionServer{cfg: cfg})
	protocolv1.RegisterDiscoveryServiceServer(s.gs, &discoveryServer{registry: cfg.Registry})
	return s, nil
}

// Server exposes the underlying *grpc.Server. Useful when the caller
// wants to register additional services (auth interceptors, health
// check, etc.) on the same listener.
func (s *GRPCServer) Server() *grpc.Server { return s.gs }

// Serve runs the gRPC server on ln until Shutdown or ln is closed.
func (s *GRPCServer) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("sdk/server: GRPCServer closed")
	}
	s.mu.Unlock()
	return s.gs.Serve(ln)
}

// Shutdown gracefully stops the gRPC server, waiting for in-flight RPCs
// to finish. Idempotent.
func (s *GRPCServer) Shutdown() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.gs.GracefulStop()
	return nil
}

// grpcSessionServer implements protocolv1.SessionServiceServer by
// delegating each Invoke RPC to runSession over a stream adapter.
type grpcSessionServer struct {
	protocolv1.UnimplementedSessionServiceServer
	cfg Config
}

// Invoke handles one bidi stream as one session. gRPC's transport
// already framed the protobuf envelopes; we hand the stream directly
// to runSession.
func (s *grpcSessionServer) Invoke(stream grpc.BidiStreamingServer[protocolv1.Frame, protocolv1.Frame]) error {
	// gRPC carries no URL path, so route is empty — runSession reads
	// StartMessage.service_name/handler_name instead.
	return runSession(stream.Context(), grpcStream{stream}, s.cfg.Registry, s.cfg.Codec, handlerclient.Route{})
}

// grpcStream adapts grpc.BidiStreamingServer onto frameStream.
type grpcStream struct {
	s grpc.BidiStreamingServer[protocolv1.Frame, protocolv1.Frame]
}

func (g grpcStream) Send(f *protocolv1.Frame) error   { return g.s.Send(f) }
func (g grpcStream) Recv() (*protocolv1.Frame, error) { return g.s.Recv() }
