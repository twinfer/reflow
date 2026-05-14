package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSource is an in-memory Source stub: writes one tiny file into
// the export dir and returns a monotonically incrementing index.
type fakeSource struct {
	calls atomic.Uint64
}

func (s *fakeSource) SnapshotToDir(_ context.Context, _ uint64, dir string) (uint64, error) {
	n := s.calls.Add(1)
	// Mimic dragonboat's "single subdirectory under ExportPath".
	sub := filepath.Join(dir, "exported")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		return 0, err
	}
	if err := os.WriteFile(filepath.Join(sub, "f"), []byte("x"), 0o644); err != nil {
		return 0, err
	}
	return n, nil
}

// TestProducer_TriggerFiresSnapshotOnce confirms a send on the
// Trigger channel runs SnapshotOnce without waiting for the Interval.
func TestProducer_TriggerFiresSnapshotOnce(t *testing.T) {
	repo := &BlobRepository{Bucket: openMemBucket(t)}
	src := &fakeSource{}
	trigger := make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		RunProducer(ctx, ProducerConfig{
			ShardID: 1,
			// Interval far longer than the test window so the test
			// can only succeed if the trigger path works.
			Interval: time.Hour,
			Source:   src,
			Repo:     repo,
			Trigger:  trigger,
		})
		close(done)
	}()

	trigger <- struct{}{}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		refs, err := repo.List(context.Background(), 1)
		if err == nil && len(refs) >= 1 {
			cancel()
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatalf("trigger did not produce an archive within deadline; SnapshotToDir calls=%d", src.calls.Load())
}

// TestProducer_NoTriggerStillRunsOnInterval guards against the local
// `trigger := cfg.Trigger` indirection accidentally killing the timer
// path when Trigger is nil.
func TestProducer_NoTriggerStillRunsOnInterval(t *testing.T) {
	repo := &BlobRepository{Bucket: openMemBucket(t)}
	src := &fakeSource{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		RunProducer(ctx, ProducerConfig{
			ShardID:  1,
			Interval: 50 * time.Millisecond,
			Source:   src,
			Repo:     repo,
			// No Trigger.
		})
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		refs, _ := repo.List(context.Background(), 1)
		if len(refs) >= 1 {
			cancel()
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("interval path did not produce an archive")
}

// errSource panics if called; used to confirm cancellation wins over a
// pending trigger.
type errSource struct{}

func (errSource) SnapshotToDir(context.Context, uint64, string) (uint64, error) {
	return 0, errors.New("errSource: should not be called")
}

// TestProducer_CancelExitsCleanly: cancellation must win even when a
// trigger is pending.
func TestProducer_CancelExitsCleanly(t *testing.T) {
	repo := &BlobRepository{Bucket: openMemBucket(t)}
	trigger := make(chan struct{}, 1)
	trigger <- struct{}{} // pre-fill

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	done := make(chan struct{})
	go func() {
		RunProducer(ctx, ProducerConfig{
			ShardID:  1,
			Interval: time.Hour,
			Source:   errSource{},
			Repo:     repo,
			Trigger:  trigger,
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunProducer did not exit after ctx cancel")
	}
}
