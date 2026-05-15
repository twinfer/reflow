package ingress

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"

	"github.com/twinfer/reflow/internal/engine"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// Config is the minimum the ingress runtime needs. Mirrors the public
// pkg/reflow.IngressConfig but kept internal so engine packages don't
// pull in the public surface.
type Config struct {
	// GRPCAddr is the listen address for the native gRPC server.
	// Empty disables the gRPC listener.
	GRPCAddr string
	// HTTPAddr is the listen address for the grpc-gateway HTTP/JSON server.
	// Empty disables the HTTP listener.
	HTTPAddr string
	// Log is the structured logger; defaults to slog.Default.
	Log *slog.Logger
	// ExtraGRPC registers additional services on the ingress gRPC server
	// before Serve is called. Operators wire optional services here so
	// the ingress port can multiplex without ingress depending on
	// downstream packages.
	ExtraGRPC func(*grpc.Server)
}

// Runtime is a started ingress server. Close it to stop both transports
// gracefully (or via the parent context). Safe to call Close multiple times.
type Runtime struct {
	cfg      Config
	server   *Server
	grpcSrv  *grpc.Server
	httpSrv  *http.Server
	grpcLn   net.Listener
	httpLn   net.Listener
	wg       sync.WaitGroup
	closeMu  sync.Mutex
	closed   bool
	grpcAddr string
	httpAddr string
}

// Start binds the listeners and serves gRPC + HTTP/JSON in background
// goroutines. Returns once both listeners are accepting (or one is disabled);
// the caller should defer rt.Close().
//
// The HTTP/JSON gateway registers the Server directly (no in-process gRPC
// dial) so the two transports share method dispatch without an extra
// loopback hop.
func Start(ctx context.Context, host *engine.Host, cfg Config) (*Runtime, error) {
	if host == nil {
		return nil, errors.New("ingress: host is required")
	}
	if cfg.GRPCAddr == "" && cfg.HTTPAddr == "" {
		return nil, errors.New("ingress: at least one of GRPCAddr or HTTPAddr must be set")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	srv := NewServer(host, cfg.Log)
	rt := &Runtime{cfg: cfg, server: srv}

	if cfg.GRPCAddr != "" {
		ln, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			return nil, fmt.Errorf("ingress: listen grpc %s: %w", cfg.GRPCAddr, err)
		}
		gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
			withDefaultDeadline(defaultLookupTimeout),
		))
		ingressv1.RegisterIngressServer(gs, srv)
		if cfg.ExtraGRPC != nil {
			cfg.ExtraGRPC(gs)
		}
		rt.grpcSrv = gs
		rt.grpcLn = ln
		rt.grpcAddr = ln.Addr().String()
		rt.wg.Go(func() {
			if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				cfg.Log.Error("ingress: grpc Serve exited", "err", err)
			}
		})
	}

	if cfg.HTTPAddr != "" {
		ln, err := net.Listen("tcp", cfg.HTTPAddr)
		if err != nil {
			rt.stopGRPC()
			return nil, fmt.Errorf("ingress: listen http %s: %w", cfg.HTTPAddr, err)
		}
		mux := runtime.NewServeMux()
		if err := ingressv1.RegisterIngressHandlerServer(ctx, mux, srv); err != nil {
			_ = ln.Close()
			rt.stopGRPC()
			return nil, fmt.Errorf("ingress: register http handler: %w", err)
		}
		hs := &http.Server{
			Handler:           withHTTPDefaultDeadline(mux, defaultLookupTimeout),
			ReadHeaderTimeout: 5 * time.Second,
		}
		rt.httpSrv = hs
		rt.httpLn = ln
		rt.httpAddr = ln.Addr().String()
		rt.wg.Go(func() {
			if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				cfg.Log.Error("ingress: http Serve exited", "err", err)
			}
		})
	}

	cfg.Log.Info("ingress: started",
		"grpc", rt.grpcAddr,
		"http", rt.httpAddr,
	)
	return rt, nil
}

// GRPCAddr returns the bound gRPC address (useful when the caller passed
// ":0" to let the kernel pick a port). Empty when the gRPC transport is
// disabled.
func (r *Runtime) GRPCAddr() string { return r.grpcAddr }

// HTTPAddr returns the bound HTTP address (useful when the caller passed
// ":0" to let the kernel pick a port). Empty when the HTTP transport is
// disabled.
func (r *Runtime) HTTPAddr() string { return r.httpAddr }

// Close stops both transports. Idempotent.
func (r *Runtime) Close() error {
	r.closeMu.Lock()
	if r.closed {
		r.closeMu.Unlock()
		return nil
	}
	r.closed = true
	r.closeMu.Unlock()

	if r.httpSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = r.httpSrv.Shutdown(shutdownCtx)
		cancel()
	}
	r.stopGRPC()
	r.wg.Wait()
	return nil
}

func (r *Runtime) stopGRPC() {
	if r.grpcSrv != nil {
		r.grpcSrv.GracefulStop()
	}
}
