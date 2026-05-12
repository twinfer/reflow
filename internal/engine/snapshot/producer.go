package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Source abstracts the dragonboat-backed "make me an exported snapshot
// at this path" capability so the snapshot package does not depend on
// dragonboat directly. The engine package supplies a wrapper around
// nh.SyncRequestSnapshot at wire-up time.
type Source interface {
	// SnapshotToDir invokes dragonboat's RequestSnapshot{Exported=true,
	// ExportPath=dir} and blocks until the export is complete. Returns
	// the Raft index of the created snapshot.
	SnapshotToDir(ctx context.Context, shardID uint64, dir string) (index uint64, err error)
}

// ProducerConfig drives a single per-shard archive goroutine.
type ProducerConfig struct {
	ShardID  uint64
	Interval time.Duration
	Source   Source
	Repo     Repository
	// ScratchDir is where Exported snapshots land before being archived.
	// Sub-directories (one per snapshot attempt) are created inside it
	// and removed after Put completes.
	ScratchDir string
	Log        *slog.Logger
}

// RunProducer blocks until ctx is cancelled. On every Interval tick it
// requests an Exported snapshot from Source, archives it into Repo, and
// cleans up the scratch directory. Errors are logged; no retry-with-
// backoff yet — the next tick is the retry.
func RunProducer(ctx context.Context, cfg ProducerConfig) {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := SnapshotOnce(ctx, cfg); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				cfg.Log.Warn("snapshot: producer cycle failed",
					"shard", cfg.ShardID, "err", err)
			}
		}
	}
}

// SnapshotOnce performs a single export+archive cycle. Exposed so admin
// RPCs can trigger an immediate snapshot outside the producer's cadence.
func SnapshotOnce(ctx context.Context, cfg ProducerConfig) error {
	if cfg.Source == nil || cfg.Repo == nil {
		return errors.New("snapshot: Source and Repo are required")
	}
	scratch, err := os.MkdirTemp(cfg.ScratchDir, fmt.Sprintf("export-%d-", cfg.ShardID))
	if err != nil {
		return fmt.Errorf("snapshot: mkdir scratch: %w", err)
	}
	defer os.RemoveAll(scratch)

	// Dragonboat's RequestSnapshot{Exported=true} writes a single
	// subdirectory under ExportPath; the scratch dir must exist + be
	// empty going in. The wrapper inside Source.SnapshotToDir is
	// responsible for translating any per-driver path conventions.
	idx, err := cfg.Source.SnapshotToDir(ctx, cfg.ShardID, scratch)
	if err != nil {
		return fmt.Errorf("snapshot: export: %w", err)
	}
	if idx == 0 {
		return errors.New("snapshot: source returned zero index")
	}
	// The exported snapshot is the only thing inside scratch; find it.
	entries, err := os.ReadDir(scratch)
	if err != nil {
		return fmt.Errorf("snapshot: scan scratch: %w", err)
	}
	if len(entries) == 0 {
		return errors.New("snapshot: source produced no files")
	}
	// If dragonboat wrote a single subdirectory (the typical case),
	// archive that subdirectory's contents. If it wrote files at the
	// top level, archive scratch itself.
	src := scratch
	if len(entries) == 1 && entries[0].IsDir() {
		src = filepath.Join(scratch, entries[0].Name())
	}
	if err := cfg.Repo.Put(ctx, cfg.ShardID, idx, src); err != nil {
		return fmt.Errorf("snapshot: archive: %w", err)
	}
	cfg.Log.Info("snapshot: archived",
		"shard", cfg.ShardID, "index", idx)
	return nil
}
