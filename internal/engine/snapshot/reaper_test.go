package snapshot

import (
	"context"
	"errors"
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

func (r *fakeRepo) Put(_ context.Context, _ uint64, _ uint64, _ string) error {
	return errors.New("fakeRepo: Put not used by reaper tests")
}
func (r *fakeRepo) Fetch(_ context.Context, _ uint64, _ uint64, _ string) error {
	return errors.New("fakeRepo: Fetch not used by reaper tests")
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
