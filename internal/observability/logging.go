package observability

import (
	"log/slog"
	"os"
)

// NewLogger returns a slog.Logger writing JSON to stderr at the requested
// level. The "reflow" service name is bundled in via WithAttrs.
func NewLogger(level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", "reflow")
}

// PartitionLogger returns a logger pre-tagged with the given partition
// metadata; use it for per-partition components.
func PartitionLogger(base *slog.Logger, shardID, nodeID uint64) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	return base.With("shard_id", shardID, "node_id", nodeID)
}
