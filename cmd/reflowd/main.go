// Command reflowd starts a reflow node using layered configuration from
// environment variables and optional config files. For custom deployments
// that link the engine in their own binary, call reflow.Run(ctx, cfg)
// directly with a programmatically constructed Config.
//
// Configuration sources (later overrides earlier):
//
//  1. Built-in defaults (single-node, shard 1, sensible ports).
//  2. Optional config file from $REFLOW_CONFIG (YAML or JSON).
//  3. REFLOW_* environment variables.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twinfer/reflow/pkg/reflow"
	"github.com/twinfer/reflow/pkg/reflow/config"
	"github.com/twinfer/reflow/pkg/sdk"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "reflowd: %v\n", err)
		os.Exit(2)
	}
	cfg.Handlers = sdk.NewRegistry()
	// User binaries register handlers here before reflow.Run; reflowd
	// ships with an empty registry — useful for smoke-testing the
	// ingress + admin endpoints without a real workload.

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reflowd: %v\n", err)
		os.Exit(1)
	}
	<-ctx.Done()
	slog.Default().Info("reflowd: shutting down")
	_ = host.Close()
}

// loadConfig layers built-in defaults, an optional config file, and
// REFLOW_* env vars (in that order — later sources win). Returns the
// merged Config or any error from a source.
func loadConfig() (reflow.Config, error) {
	sources := []config.Source{
		config.FromMap(defaultValues()),
	}
	if path := os.Getenv("REFLOW_CONFIG"); path != "" {
		sources = append(sources, config.FromFile(path))
	}
	sources = append(sources, config.FromEnv())

	cfg, _, err := config.Load(sources...)
	return cfg, err
}

// defaultValues are the baked-in defaults. Picked to make `go run
// ./cmd/reflowd` work out of the box on a developer machine. Phase 4.1
// multi-node fields (node.gossip_bind_addr, node.delivery_addr,
// cluster.peers) are left empty by default — single-node bootstrap when
// they are unset.
func defaultValues() map[string]any {
	return map[string]any{
		"node.id":           uint64(1),
		"node.raft_addr":    "127.0.0.1:9091",
		"storage.data_dir":  "./data",
		"cluster.shards":    []uint64{1},
		"ingress.grpc_addr": ":8081",
		"ingress.http_addr": ":8080",
		"metrics.addr":      ":9090",
		"logging.level":     "INFO",
	}
}
