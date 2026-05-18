// embedded is the canonical "single-binary dev" example: one main()
// runs both the reflow engine and the Go handler. The engine still talks
// to the handler over HTTP/2 (there is no in-process fast path); we just
// host both on localhost in the same process so a developer can iterate
// without orchestrating two binaries.
//
// Architecture:
//
//	┌─────────────────────────────────────────────┐
//	│ embedded (this main)                        │
//	│  ┌────────────────────┐  http://127.0.0.1:N │
//	│  │ pkg/handler     │ ◄────────────────┐  │
//	│  └────────────────────┘                  │  │
//	│  ┌────────────────────┐                  │  │
//	│  │ reflow.Run engine  │ ─ Handlers.Endpoints
//	│  │  + ingress HTTP    │                     │
//	│  └────────────────────┘                     │
//	└─────────────────────────────────────────────┘
//	                       │
//	                       │ HTTP POST /invocation/Greeter/hello
//	                       ▼
//	                  curl / your client
//
// Usage:
//
//	go run ./examples/embedded
//	# in another shell:
//	curl -X POST \
//	  -H 'content-type: application/json' \
//	  -d '{"input":"d29ybGQ="}' \  # base64("world")
//	  http://127.0.0.1:8080/invocation/Greeter/hello
//
// Production deployments should use cmd/reflowd for the engine and
// examples/remote-handler (or a real handler binary) for the handlers.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/twinfer/reflow/pkg/handler"
	"github.com/twinfer/reflow/pkg/reflow"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1. Build a registry and register the handlers in this process.
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Greeter", "hello", greet); err != nil {
		log.Error("register Greeter/hello", "err", err)
		os.Exit(1)
	}
	if err := reg.RegisterService("Echo", "echo", echo); err != nil {
		log.Error("register Echo/echo", "err", err)
		os.Exit(1)
	}

	// 2. Start the SDK server on a free port. The engine talks to this
	// listener over HTTP/2 — same wire path a remote handler uses.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Error("listen sdk server", "err", err)
		os.Exit(1)
	}
	handlerURL := "http://" + ln.Addr().String()
	srv, err := handler.NewHTTP2(handler.Config{Registry: reg})
	if err != nil {
		log.Error("sdk server NewHTTP2", "err", err)
		os.Exit(1)
	}
	go func() {
		if err := srv.Serve(ln); err != nil {
			log.Error("sdk Serve exited", "err", err)
		}
	}()
	log.Info("embedded: sdk handlers listening", "url", handlerURL)

	// 3. Start the engine. cfg.Handlers.Endpoints auto-registers the
	// in-process SDK server as a deployment at metadata-leader bootstrap,
	// so invocations submitted via ingress resolve to it.
	dataDir, err := os.MkdirTemp("", "reflow-embedded-")
	if err != nil {
		log.Error("mkdir tmp", "err", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dataDir)

	cfg := reflow.Config{
		Node: reflow.NodeConfig{
			ID:       1,
			RaftAddr: "127.0.0.1:5410",
		},
		Storage: reflow.StorageConfig{
			DataDir: filepath.Join(dataDir, "node1"),
		},
		Ingress: reflow.IngressConfig{
			GRPCAddr: "127.0.0.1:8081",
			HTTPAddr: "127.0.0.1:8080",
		},
		Metrics: reflow.MetricsConfig{Disabled: true},
		Handlers: reflow.HandlersConfig{
			Endpoints: []reflow.HandlerEndpoint{{URL: handlerURL}},
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		log.Error("reflow.Run", "err", err)
		_ = srv.Shutdown()
		os.Exit(1)
	}
	log.Info("embedded: engine + ingress live; submit via POST http://127.0.0.1:8080/invocation/Greeter/hello")

	// 4. Wait for SIGINT/SIGTERM. Shutdown order: engine first (drains
	// ingress + partitions), then the SDK server. Reversed order would
	// leave in-flight invocations targeting a dead handler.
	<-ctx.Done()
	log.Info("embedded: shutting down")
	if err := host.Close(); err != nil {
		log.Warn("engine close", "err", err)
	}
	if err := srv.Shutdown(); err != nil {
		log.Warn("sdk Shutdown", "err", err)
	}
	_ = ln.Close()
}

// greet returns "hello, <input>" — useful for a quick smoke test:
//
//	curl -d '{"input":"d29ybGQ="}' http://127.0.0.1:8080/invocation/Greeter/hello
//
// (the JSON `input` field is base64-encoded raw bytes).
func greet(_ handler.Context, in []byte) ([]byte, error) {
	return fmt.Appendf(nil, "hello, %s", in), nil
}

// echo returns its input unchanged.
func echo(_ handler.Context, in []byte) ([]byte, error) { return in, nil }
