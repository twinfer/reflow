// remote-handler is the canonical minimal example of a Go reflow
// handler running out-of-process — i.e. dispatched over the wire by
// the reflow engine rather than embedded in the engine binary.
//
// Build it, point a reflow.Config at the resulting URL via
// Config.Handlers.Endpoints, and the engine will discover its handlers
// over GET /discover and route invocations to them over raw HTTP/2.
//
// Usage:
//
//	go run ./examples/remote-handler                 # h2c on :50051
//	go run ./examples/remote-handler -addr :50052    # h2c on :50052
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

	"github.com/twinfer/reflow/pkg/handler"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "host:port to listen on")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	reg := handler.NewRegistry()
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

	srv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		log.Error("NewServer", "err", err)
		os.Exit(1)
	}
	log.Info("remote-handler listening (HTTP/2 h2c)",
		"addr", ln.Addr().String(),
		"register_url", "http://"+ln.Addr().String())

	// Trap SIGINT / SIGTERM and Shutdown gracefully so in-flight
	// invocations get a chance to finish.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info("remote-handler shutting down")
		_ = srv.Shutdown()
	}()

	if err := srv.Serve(ln); err != nil {
		log.Error("Serve exited", "err", err)
		os.Exit(1)
	}
}

// greet returns "hello, <input>" — the smoke handler for asserting
// engine→handler routing.
func greet(_ handler.Context, in []byte) ([]byte, error) {
	return fmt.Appendf(nil, "hello, %s", in), nil
}

// echo returns its input unchanged. Used by tests that round-trip a
// byte payload through the wire path.
func echo(_ handler.Context, in []byte) ([]byte, error) { return in, nil }
