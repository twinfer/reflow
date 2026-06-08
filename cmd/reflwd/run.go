package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twinfer/reflw/pkg/reflw"
	"github.com/twinfer/reflw/pkg/reflw/config"
)

// cmdRun is the "reflwd run" subcommand: load layered config and start
// the engine until SIGINT/SIGTERM. Configuration sources (later overrides
// earlier):
//
//  1. Built-in defaults (single-node, shard 1, sensible ports).
//  2. Optional config file from $REFLW_CONFIG (YAML or JSON).
//  3. REFLW_* environment variables.
//
// Joining an existing cluster needs no special flags: configure
// cluster_ca.* (so the node self-issues its mesh leaf from the cluster
// CA), set cluster.join_existing=true, and point node.gossip_* at a
// seed. The node then self-joins over mTLS — there is no join token and
// no separate bootstrap port.
func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	host, err := reflw.Run(ctx, cfg)
	if err != nil {
		return err
	}

	<-ctx.Done()
	slog.Default().Info("reflwd: shutting down")
	return host.Close()
}

// loadConfig layers built-in defaults, an optional config file, and
// REFLW_* env vars (in that order — later sources win).
func loadConfig() (reflw.Config, error) {
	sources := []config.Source{
		config.FromMap(defaultValues()),
	}
	if path := os.Getenv("REFLW_CONFIG"); path != "" {
		sources = append(sources, config.FromFile(path))
	}
	sources = append(sources, config.FromEnv())

	cfg, _, err := config.Load(sources...)
	return cfg, err
}

// defaultValues are the baked-in defaults. Picked so `reflwd run`
// works out of the box on a developer machine. Multi-node fields
// (node.gossip_bind_addr, node.delivery_addr, cluster.peers) are
// left empty by default — single-node bootstrap when they are unset.
func defaultValues() map[string]any {
	return map[string]any{
		"node.id":          uint64(1),
		"node.raft_addr":   "127.0.0.1:9091",
		"storage.data_dir": "./data",
		// Ingress is the user-facing API; reflw.Run starts it
		// unconditionally and applies this same default if the operator
		// leaves Addr empty. Surfaced here so users can see the canonical
		// port without reading library code.
		"ingress.addr":  ":8080",
		"metrics.addr":  ":9090",
		"logging.level": "INFO",
		// Admin + snapshot defaults. The admin server starts when
		// Admin.Addr is set, so leaving it populated is safe for
		// single-node out of the box. The snapshot producer is disabled
		// by default (Interval=0); operators opt in via REFLW_SNAPSHOT_
		// INTERVAL once they have a sustained DR plan.
		"admin.addr":           ":8082",
		"snapshot.retain":      24,
		"snapshot.interval":    "0s",
		"snapshot.scratch_dir": "",
	}
}
