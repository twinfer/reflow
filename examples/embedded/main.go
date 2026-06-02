// embedded is the canonical "single-binary" example: one main() runs both
// the reflow engine and the Go handlers in the same process, with NO HTTP
// hop between them. The handlers are registered in a handler.Registry handed
// to reflow.Run via Config.Handlers.InProcess; the engine dispatches to them
// directly over an in-process transport.
//
// Architecture:
//
//	┌────────────────────────────────────────────────┐
//	│ embedded (this main)                             │
//	│  ┌────────────────────┐                          │
//	│  │ handler.Registry   │ ◄── in-process call ───┐ │
//	│  └────────────────────┘                        │ │
//	│  ┌────────────────────┐                        │ │
//	│  │ reflow.Run engine  │ ── Handlers.InProcess ──┘ │
//	│  │  + ingress HTTP    │                           │
//	│  └────────────────────┘                           │
//	└────────────────────────────────────────────────┘
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
// To run handlers as a separate process instead (the remote model), host a
// handler.NewServer on a listener and register its URL via
// Config.Handlers.Endpoints. cmd/reflowd is the production engine binary.
package main

import (
	"context"
	"fmt"
	"log/slog"
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

	// 2. Start the engine with the handlers running in-process. Run
	// registers the Registry as a single inproc:// deployment at
	// metadata-leader bootstrap and dispatches to it directly — no HTTP,
	// no second listener.
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
			Addr: "127.0.0.1:8080",
		},
		Metrics: reflow.MetricsConfig{Disabled: true},
		Handlers: reflow.HandlersConfig{
			InProcess: reg,
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		log.Error("reflow.Run", "err", err)
		os.Exit(1)
	}
	log.Info("embedded: engine + in-process handlers live; submit via POST http://127.0.0.1:8080/invocation/Greeter/hello")

	// 3. Wait for SIGINT/SIGTERM, then drain the engine.
	<-ctx.Done()
	log.Info("embedded: shutting down")
	if err := host.Close(); err != nil {
		log.Warn("engine close", "err", err)
	}
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
