// Command loadhandler is a test-only HTTP/2 (h2c) handler endpoint
// used by the e2e chaos harness. It runs as a sidecar container on the
// same docker network as a reflowd cluster and is registered as a
// deployment via Config.RegisterDeployment after cluster bring-up.
//
// Hosts a single service `e2e.Echo` with a single handler `echo` that
// returns its input verbatim — enough to assert end-to-end engine →
// handler routing while keeping the binary small enough to live in a
// distroless image.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/twinfer/reflow/pkg/handler"
)

func main() {
	addr := flag.String("addr", ":9100", "listen addr (host:port)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	reg := handler.NewRegistry()
	if err := reg.RegisterService("e2e.Echo", "echo", echo); err != nil {
		log.Error("register e2e.Echo/echo", "err", err)
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
	log.Info("loadhandler listening (HTTP/2 h2c)", "addr", ln.Addr().String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown()
	}()
	if err := srv.Serve(ln); err != nil {
		log.Error("serve exited", "err", err)
		os.Exit(1)
	}
}

func echo(_ handler.Context, in []byte) ([]byte, error) {
	return in, nil
}
