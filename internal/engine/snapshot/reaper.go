package snapshot

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// ReaperConfig drives the per-shard retention reaper goroutine.
//
// The reaper is intentionally run on every node, not just the metadata
// leader: Repository.Delete is idempotent against missing keys, so
// duplicate work is benign. Revisit only if Delete RPS to the object
// store becomes a billing concern.
type ReaperConfig struct {
	ShardID uint64
	// Interval between reap cycles. Defaults to 1h when zero.
	Interval time.Duration
	Repo     Repository
	// Retain mirrors the count policy enforced inline by BlobRepository.Put.
	// 0 disables count-based reaping.
	Retain int
	// RetentionAge drops anything whose CreatedAt is older than now - this.
	// 0 disables age-based reaping.
	RetentionAge time.Duration
	Log          *slog.Logger
	// Now is an injection seam for tests; defaults to time.Now.
	Now func() time.Time
}

// RunReaper blocks until ctx is cancelled. On every Interval tick it
// lists archives for the shard and deletes any that violate the
// configured Retain / RetentionAge policy. Errors are logged; no
// retry-with-backoff — the next tick is the retry.
func RunReaper(ctx context.Context, cfg ReaperConfig) {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Retain <= 0 && cfg.RetentionAge <= 0 {
		// No-op configuration; don't waste a goroutine.
		return
	}
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := ReapOnce(ctx, cfg); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				cfg.Log.Warn("snapshot: reaper cycle failed",
					"shard", cfg.ShardID, "err", err)
			}
		}
	}
}

// ReapOnce performs a single reap pass. Exposed so tests can drive the
// reaper without spawning a goroutine.
func ReapOnce(ctx context.Context, cfg ReaperConfig) error {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	refs, err := cfg.Repo.List(ctx, cfg.ShardID)
	if err != nil {
		return err
	}
	// Age policy first: anything older than the cutoff is gone regardless
	// of count. Then count policy on whatever survives.
	survivors := refs
	if cfg.RetentionAge > 0 {
		cutoff := cfg.Now().Add(-cfg.RetentionAge)
		kept := survivors[:0]
		for _, r := range refs {
			if r.CreatedAt.Before(cutoff) {
				if err := cfg.Repo.Delete(ctx, cfg.ShardID, r.Index); err != nil {
					return err
				}
				continue
			}
			kept = append(kept, r)
		}
		survivors = kept
	}
	if cfg.Retain > 0 && len(survivors) > cfg.Retain {
		// refs are sorted ascending; oldest at the front.
		drop := survivors[:len(survivors)-cfg.Retain]
		for _, r := range drop {
			if err := cfg.Repo.Delete(ctx, cfg.ShardID, r.Index); err != nil {
				return err
			}
		}
	}
	return nil
}
