package snapshot

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// fakeRepo is an in-memory Repository stub the reaper tests drive
// directly — no filesystem, no goroutines, deterministic timestamps.
type fakeRepo struct {
	mu   sync.Mutex
	rows map[uint64][]SnapshotRef // by shardID, sorted ascending by Index
}

func newFakeRepo() *fakeRepo { return &fakeRepo{rows: map[uint64][]SnapshotRef{}} }

func (r *fakeRepo) insert(shardID, index uint64, createdAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[shardID] = append(r.rows[shardID], SnapshotRef{
		ShardID: shardID, Index: index, SizeBytes: 1, CreatedAt: createdAt,
	})
}

func (r *fakeRepo) NewWriter(_ context.Context, _, _ uint64) (io.WriteCloser, error) {
	return nil, errors.New("fakeRepo: NewWriter not used by reaper tests")
}
func (r *fakeRepo) NewReader(_ context.Context, _, _ uint64) (io.ReadCloser, error) {
	return nil, errors.New("fakeRepo: NewReader not used by reaper tests")
}
func (r *fakeRepo) List(_ context.Context, shardID uint64) ([]SnapshotRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SnapshotRef, len(r.rows[shardID]))
	copy(out, r.rows[shardID])
	return out, nil
}
func (r *fakeRepo) Delete(_ context.Context, shardID, index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	in := r.rows[shardID]
	kept := in[:0]
	for _, ref := range in {
		if ref.Index != index {
			kept = append(kept, ref)
		}
	}
	r.rows[shardID] = kept
	return nil
}

func TestReaper_AgeBased(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	repo := newFakeRepo()
	repo.insert(1, 10, now.Add(-72*time.Hour))
	repo.insert(1, 20, now.Add(-48*time.Hour))
	repo.insert(1, 30, now.Add(-1*time.Hour))

	cfg := ReaperConfig{
		ShardID:      1,
		Repo:         repo,
		RetentionAge: 24 * time.Hour,
		Now:          func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	if len(got) != 1 || got[0].Index != 30 {
		t.Fatalf("after age reap: %+v; want only index=30", got)
	}
}

func TestReaper_CountBased(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	repo := newFakeRepo()
	for i, idx := range []uint64{10, 20, 30, 40, 50} {
		repo.insert(1, idx, now.Add(-time.Duration(i)*time.Minute))
	}
	cfg := ReaperConfig{
		ShardID: 1,
		Repo:    repo,
		Retain:  2,
		Now:     func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	if len(got) != 2 {
		t.Fatalf("after count reap: len=%d; want 2", len(got))
	}
	if got[0].Index != 40 || got[1].Index != 50 {
		t.Fatalf("after count reap: %+v; want 40,50", got)
	}
}

func TestReaper_AgeAndCountCombined(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	repo := newFakeRepo()
	// 100 is ancient (age-dropped).
	repo.insert(1, 100, now.Add(-96*time.Hour))
	// 200/300/400/500 are recent; count=2 keeps only the newest two.
	repo.insert(1, 200, now.Add(-4*time.Hour))
	repo.insert(1, 300, now.Add(-3*time.Hour))
	repo.insert(1, 400, now.Add(-2*time.Hour))
	repo.insert(1, 500, now.Add(-1*time.Hour))

	cfg := ReaperConfig{
		ShardID:      1,
		Repo:         repo,
		Retain:       2,
		RetentionAge: 48 * time.Hour,
		Now:          func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	if len(got) != 2 || got[0].Index != 400 || got[1].Index != 500 {
		t.Fatalf("combined reap: %+v; want 400,500", got)
	}
}

func TestReaper_NoopWhenBothPoliciesDisabled(t *testing.T) {
	repo := newFakeRepo()
	repo.insert(1, 1, time.Now().Add(-1000*time.Hour))
	cfg := ReaperConfig{ShardID: 1, Repo: repo}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	if len(got) != 1 {
		t.Fatalf("expected no-op when both policies are zero; got len=%d", len(got))
	}
}

func TestReaper_TieredKeepsNewestPerBucket(t *testing.T) {
	// "now" is a Wednesday at noon UTC. ISO week boundary is Mon-Sun.
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	repo := newFakeRepo()
	// Three refs the same UTC day. Tiered should keep the newest, drop
	// the older two — they don't promote to weekly/monthly because the
	// day, week, and month keys all match the survivor.
	repo.insert(1, 100, now.Add(-6*time.Hour))
	repo.insert(1, 101, now.Add(-3*time.Hour))
	repo.insert(1, 102, now.Add(-1*time.Hour))

	cfg := ReaperConfig{
		ShardID:       1,
		Repo:          repo,
		TieredDaily:   1,
		TieredWeekly:  1,
		TieredMonthly: 1,
		Now:           func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	if len(got) != 1 || got[0].Index != 102 {
		t.Fatalf("tiered same-day: %+v; want only newest (102)", got)
	}
}

func TestReaper_TieredFillsDayWeekMonth(t *testing.T) {
	// Strict cascade: daily refs cover their week+month; weekly refs
	// cover their month. Data is laid out so each tier is exercised
	// without the cascade incidentally swallowing the next tier's slot.
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC) // Thursday
	repo := newFakeRepo()
	mk := func(idx uint64, t time.Time) { repo.insert(1, idx, t) }

	// Daily tier — TieredDaily=2.
	mk(900, now.Add(-1*time.Hour)) // Thu 5-14: daily slot 1
	mk(800, now.Add(-6*time.Hour)) // Thu 5-14: same day → drop
	mk(700, now.AddDate(0, 0, -1)) // Wed 5-13: daily slot 2
	// Weekly tier — TieredWeekly=2 (May still has weeks left).
	mk(500, time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC))  // Mon 5-04 (W19): weekly slot 1
	mk(400, time.Date(2026, 4, 27, 12, 0, 0, 0, time.UTC)) // Mon 4-27 (W18, April): weekly slot 2 (April now covered)
	// Monthly tier — TieredMonthly=2 (months Mar + Feb available).
	mk(300, time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC)) // Sat 3-21 (March): monthly slot 1
	mk(200, time.Date(2026, 2, 13, 12, 0, 0, 0, time.UTC)) // Fri 2-13 (February): monthly slot 2
	// Ancient — exceeds every slot, drop.
	mk(100, time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)) // Jan 2026

	cfg := ReaperConfig{
		ShardID:       1,
		Repo:          repo,
		TieredDaily:   2,
		TieredWeekly:  2,
		TieredMonthly: 2,
		Now:           func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	gotIdx := map[uint64]bool{}
	for _, r := range got {
		gotIdx[r.Index] = true
	}
	want := []uint64{900, 700, 500, 400, 300, 200}
	for _, w := range want {
		if !gotIdx[w] {
			t.Errorf("missing expected survivor %d (got %+v)", w, got)
		}
	}
	for _, drop := range []uint64{800, 100} {
		if gotIdx[drop] {
			t.Errorf("expected %d dropped; still present", drop)
		}
	}
	if len(got) != len(want) {
		t.Errorf("survivor count = %d; want %d (refs: %+v)", len(got), len(want), got)
	}
}

func TestReaper_TieredCascadeBlocksCoarserTiers(t *testing.T) {
	// A daily ref's week+month are implicitly "covered" — sibling refs
	// in the same week or month don't get promoted to weekly or
	// monthly. Three refs same week: only one survives (the daily slot).
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC) // Thursday W20
	repo := newFakeRepo()
	repo.insert(1, 10, now.AddDate(0, 0, -2)) // Tue 5-12 (same W20, same May)
	repo.insert(1, 20, now.AddDate(0, 0, -1)) // Wed 5-13 (same W20, same May)
	repo.insert(1, 30, now)                   // Thu 5-14 (newest; claims daily)

	cfg := ReaperConfig{
		ShardID:       1,
		Repo:          repo,
		TieredDaily:   1,
		TieredWeekly:  1,
		TieredMonthly: 1,
		Now:           func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	if len(got) != 1 || got[0].Index != 30 {
		t.Fatalf("cascade: %+v; want only index=30 (daily; week+month covered)", got)
	}
}

func TestReaper_TieredIgnoresRetainAndAge(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	repo := newFakeRepo()
	repo.insert(1, 10, now.Add(-1*time.Hour))
	repo.insert(1, 20, now.Add(-2*time.Hour))
	// Tiered set; Retain=0 / RetentionAge=0 would normally be no-op, but
	// the test verifies that the tiered path runs even when count/age
	// are off. With same-day refs and Daily=1, we keep only the newest.
	cfg := ReaperConfig{
		ShardID:     1,
		Repo:        repo,
		Retain:      0,
		TieredDaily: 1,
		Now:         func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	if len(got) != 1 || got[0].Index != 10 {
		t.Fatalf("after tiered: %+v; want only index=10", got)
	}
}

func TestReaper_TieredPolicyTakesPrecedenceOverCountAge(t *testing.T) {
	// When both tiered and count/age are configured, tiered wins —
	// Retain and RetentionAge are ignored.
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	repo := newFakeRepo()
	// 100 days of one-per-day refs. Count says "keep 1", Age says "keep
	// last 24h" (i.e. 2 days). Tiered says "1 daily, 4 weekly".
	for i := range 100 {
		repo.insert(1, uint64(1000-i), now.AddDate(0, 0, -i))
	}
	cfg := ReaperConfig{
		ShardID:      1,
		Repo:         repo,
		Retain:       1,
		RetentionAge: 24 * time.Hour,
		TieredDaily:  1,
		TieredWeekly: 4,
		Now:          func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	// Tiered: 1 daily + 4 weekly = at most 5 refs. If count/age had run
	// the result would be 1 or 2 refs respectively.
	if len(got) < 4 || len(got) > 5 {
		t.Fatalf("tiered precedence: got len=%d; want 4-5 (1 daily + up to 4 weekly)", len(got))
	}
}

func TestReaper_TieredBucketBoundariesUTC(t *testing.T) {
	// Midnight UTC: refs straddling 00:00 should fall in different day
	// buckets. With TieredDaily=2, both are kept.
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	dayStart := time.Date(2026, 5, 14, 0, 5, 0, 0, time.UTC)
	prevDayEnd := time.Date(2026, 5, 13, 23, 55, 0, 0, time.UTC)
	repo := newFakeRepo()
	repo.insert(1, 1, prevDayEnd)
	repo.insert(1, 2, dayStart)

	cfg := ReaperConfig{
		ShardID:     1,
		Repo:        repo,
		TieredDaily: 2,
		Now:         func() time.Time { return now },
	}
	if err := ReapOnce(context.Background(), cfg); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	got, _ := repo.List(context.Background(), 1)
	if len(got) != 2 {
		t.Fatalf("boundary refs: got %d; want both kept (different UTC days)", len(got))
	}
}
