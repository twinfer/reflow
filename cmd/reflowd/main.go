// Command reflowd starts a single-node reflow Host. Phase 1 only — no
// cluster manager, no SDK gateway.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/observability"
)

func main() {
	var (
		nodeID      uint64
		raftAddr    string
		dataDir     string
		metricsAddr string
		level       string
		shards      uint64Slice
	)
	flag.Uint64Var(&nodeID, "node-id", 1, "Replica ID for this node (must be > 0)")
	flag.StringVar(&raftAddr, "raft-addr", "127.0.0.1:9091", "Raft RPC address (host:port)")
	flag.StringVar(&dataDir, "data-dir", "./data", "Directory for raft and partition state")
	flag.StringVar(&metricsAddr, "metrics-addr", ":9090", "HTTP listen address for /metrics")
	flag.StringVar(&level, "log-level", "info", "slog level: debug|info|warn|error")
	flag.Var(&shards, "shard", "Shard ID to start (repeatable; default: 1)")
	flag.Parse()

	logger := observability.NewLogger(parseLevel(level))
	slog.SetDefault(logger)

	_ = observability.NewMetrics(nil) // registers against the default registry

	if len(shards) == 0 {
		shards = append(shards, 1)
	}

	host, err := engine.NewHost(engine.HostConfig{
		NodeID:        nodeID,
		RaftAddr:      raftAddr,
		DataDir:       dataDir,
		Log:           logger,
		EnableMetrics: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "reflowd: NewHost: %v\n", err)
		os.Exit(1)
	}

	for _, sh := range shards {
		if _, err := host.StartPartition(sh); err != nil {
			fmt.Fprintf(os.Stderr, "reflowd: StartPartition(%d): %v\n", sh, err)
			os.Exit(1)
		}
		logger.Info("started partition", "shard", sh)
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintln(w, "ok")
		})
		logger.Info("metrics listening", "addr", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server exited", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	logger.Info("shutting down")
	_ = host.Close()
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// uint64Slice supports a repeatable --shard flag.
type uint64Slice []uint64

func (s *uint64Slice) String() string {
	return fmt.Sprintf("%v", []uint64(*s))
}

func (s *uint64Slice) Set(v string) error {
	var n uint64
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return err
	}
	*s = append(*s, n)
	return nil
}
