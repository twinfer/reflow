package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"
)

// ReaperConfig drives the per-shard retention reaper goroutine.
//
// Three policies cohabit:
//
//   - Retain        — count: keep the newest N refs.
//   - RetentionAge  — age: drop anything older than now-D.
//   - Tiered*       — GFS-style: keep one ref per recent day/week/month.
//
// Retain + RetentionAge compose (each runs in turn). Tiered is exclusive:
// when any Tiered* knob is set it replaces the count and age policies
// entirely. All boundaries are evaluated in UTC.
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
	// TieredDaily / TieredWeekly / TieredMonthly express GFS-style
	// retention. The reaper walks refs newest-first and assigns each to
	// the most-granular bucket whose slot is still open and whose
	// day/week/month key has not yet been claimed. Anything that does
	// not land in a bucket is deleted.
	//
	// Any non-zero value puts the reaper in tiered mode; Retain and
	// RetentionAge are ignored on the same shard.
	TieredDaily   int
	TieredWeekly  int
	TieredMonthly int
	Log           *slog.Logger
	// Now is an injection seam for tests; defaults to time.Now.
	Now func() time.Time
}

// hasTieredPolicy reports whether any tiered slot is configured.
func (c ReaperConfig) hasTieredPolicy() bool {
	return c.TieredDaily > 0 || c.TieredWeekly > 0 || c.TieredMonthly > 0
}

// hasAnyPolicy reports whether the reaper has any work to do.
func (c ReaperConfig) hasAnyPolicy() bool {
	return c.Retain > 0 || c.RetentionAge > 0 || c.hasTieredPolicy()
}

// RunReaper blocks until ctx is cancelled. On every Interval tick it
// lists archives for the shard and deletes any that violate the
// configured policy. Errors are logged; no retry-with-backoff — the
// next tick is the retry.
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
	if !cfg.hasAnyPolicy() {
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
	if cfg.hasTieredPolicy() {
		return reapTiered(ctx, cfg, refs)
	}
	return reapCountAndAge(ctx, cfg, refs)
}

func reapCountAndAge(ctx context.Context, cfg ReaperConfig, refs []SnapshotRef) error {
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

// reapTiered keeps refs under GFS-style tier coverage. Walking
// newest-first, each ref tries to claim daily → weekly → monthly. Tiers
// cascade: a ref claiming a daily slot also marks its week and month as
// "covered" so the same span isn't double-counted at a coarser tier. A
// ref whose day/week/month is already covered (or whose target tier is
// at capacity) is dropped.
//
// Capacity slots are independent: TieredDaily=N means N distinct days
// kept at the daily tier, regardless of how many weeks/months those
// days happen to span.
func reapTiered(ctx context.Context, cfg ReaperConfig, refs []SnapshotRef) error {
	sorted := make([]SnapshotRef, len(refs))
	copy(sorted, refs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})

	dailySlots := map[string]struct{}{}
	weeklySlots := map[string]struct{}{}
	monthlySlots := map[string]struct{}{}
	// "Covered" tracks any key claimed by *or implied by* a kept ref:
	// a daily ref implies coverage of its week and month; a weekly ref
	// implies coverage of its month. Coverage blocks coarser-tier
	// claims for the same span so a daily snapshot of Thursday doesn't
	// also get re-kept as "this week's weekly" or "this month's monthly".
	weeklyCovered := map[string]struct{}{}
	monthlyCovered := map[string]struct{}{}
	survivors := map[uint64]struct{}{}

	for _, ref := range sorted {
		day, week, month := bucketKeys(ref.CreatedAt)
		switch {
		case cfg.TieredDaily > 0 && len(dailySlots) < cfg.TieredDaily &&
			!contains(dailySlots, day):
			dailySlots[day] = struct{}{}
			weeklyCovered[week] = struct{}{}
			monthlyCovered[month] = struct{}{}
			survivors[ref.Index] = struct{}{}
		case cfg.TieredWeekly > 0 && len(weeklySlots) < cfg.TieredWeekly &&
			!contains(weeklyCovered, week):
			weeklySlots[week] = struct{}{}
			weeklyCovered[week] = struct{}{}
			monthlyCovered[month] = struct{}{}
			survivors[ref.Index] = struct{}{}
		case cfg.TieredMonthly > 0 && len(monthlySlots) < cfg.TieredMonthly &&
			!contains(monthlyCovered, month):
			monthlySlots[month] = struct{}{}
			monthlyCovered[month] = struct{}{}
			survivors[ref.Index] = struct{}{}
		}
	}

	for _, ref := range refs {
		if _, keep := survivors[ref.Index]; keep {
			continue
		}
		if err := cfg.Repo.Delete(ctx, cfg.ShardID, ref.Index); err != nil {
			return err
		}
	}
	return nil
}

func contains(set map[string]struct{}, key string) bool {
	_, ok := set[key]
	return ok
}

// bucketKeys returns (day, week, month) keys for a timestamp in UTC.
// Week key is ISO-8601 (year-week).
func bucketKeys(t time.Time) (day, week, month string) {
	u := t.UTC()
	day = u.Format("2006-01-02")
	y, w := u.ISOWeek()
	week = fmt.Sprintf("%d-W%02d", y, w)
	month = u.Format("2006-01")
	return
}
