// remote-handler is the canonical minimal example of a Go reflow
// handler running out-of-process — i.e. dispatched over the wire by
// the reflow engine rather than embedded in the engine binary.
//
// Build it, point a reflow.Config at the resulting URL via
// Config.Handlers.Endpoints, and the engine will discover its handlers
// and route invocations to them over gRPC (or HTTP/2 — pass -http to
// switch transports).
//
// Usage:
//
//	go run ./examples/remote-handler                 # gRPC on :50051
//	go run ./examples/remote-handler -addr :50052    # gRPC on :50052
//	go run ./examples/remote-handler -http           # raw HTTP/2 on :50051
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twinfer/reflow/pkg/sdk"
	"github.com/twinfer/reflow/pkg/sdk/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "host:port to listen on")
	useHTTP := flag.Bool("http", false, "serve via raw HTTP/2 (h2c) instead of gRPC")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Greeter", "hello", greet); err != nil {
		log.Error("register Greeter/hello", "err", err)
		os.Exit(1)
	}
	if err := reg.RegisterService("Echo", "echo", echo); err != nil {
		log.Error("register Echo/echo", "err", err)
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen", "addr", *addr, "err", err)
		os.Exit(1)
	}

	var srv server.Server
	if *useHTTP {
		s, err := server.NewHTTP2(server.Config{Registry: reg})
		if err != nil {
			log.Error("NewHTTP2", "err", err)
			os.Exit(1)
		}
		srv = s
		log.Info("remote-handler listening (HTTP/2 h2c)",
			"addr", ln.Addr().String(),
			"register_url", "http://"+ln.Addr().String())
	} else {
		s, err := server.NewGRPC(server.Config{Registry: reg})
		if err != nil {
			log.Error("NewGRPC", "err", err)
			os.Exit(1)
		}
		srv = s
		log.Info("remote-handler listening (gRPC)",
			"addr", ln.Addr().String(),
			"register_url", "grpc://"+ln.Addr().String())
	}

	// Trap SIGINT / SIGTERM and Shutdown gracefully so in-flight
	// invocations get a chance to finish.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info("remote-handler shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = shutdownCtx // honoured by the server's internal Shutdown timeout
		_ = srv.Shutdown()
	}()

	if err := srv.Serve(ln); err != nil {
		log.Error("Serve exited", "err", err)
		os.Exit(1)
	}
}

// greet returns "hello, <input>" — the smoke handler for asserting
// engine→handler routing.
func greet(_ sdk.Context, in []byte) ([]byte, error) {
	return fmt.Appendf(nil, "hello, %s", in), nil
}

// echo returns its input unchanged. Used by tests that round-trip a
// byte payload through the wire path.
func echo(_ sdk.Context, in []byte) ([]byte, error) { return in, nil }
