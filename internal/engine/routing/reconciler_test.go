package routing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeReader struct {
	mu    sync.Mutex
	snap  map[uint32]uint64
	rev   uint64
	err   error
	calls atomic.Uint32
}

func (f *fakeReader) SnapshotLPOwners(ctx context.Context) (map[uint32]uint64, uint64, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, 0, f.err
	}
	out := make(map[uint32]uint64, len(f.snap))
	maps.Copy(out, f.snap)
	return out, f.rev, nil
}

func (f *fakeReader) set(snap map[uint32]uint64, rev uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snap = snap
	f.rev = rev
	f.err = nil
}

func (f *fakeReader) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestReconciler_WakeOnNotifyInstallsSnapshot covers the primary
// signal path: a TableNotifier wake triggers SyncRead and atomic-swaps
// the new snapshot.
func TestReconciler_WakeOnNotifyInstallsSnapshot(t *testing.T) {
	p := NewPartitioner(3)
	reader := &fakeReader{snap: map[uint32]uint64{1: 7, 2: 9}, rev: 5}
	notify := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- RunReconciler(ctx, notify, reader, p, discardLogger()) }()

	// First reconcileOnce fires immediately on loop entry; wait for it.
	waitFor(t, time.Second, func() bool {
		return p.LPOwnersSnapshot() != nil
	})
	if got := p.LPOwnersSnapshot()[1]; got != 7 {
		t.Fatalf("initial snapshot lp=1 = %d; want 7", got)
	}

	// Mutate the desired state and bump the notifier — Partitioner
	// should see the new value.
	reader.set(map[uint32]uint64{1: 11}, 6)
	notify <- struct{}{}
	waitFor(t, time.Second, func() bool {
		snap := p.LPOwnersSnapshot()
		return snap != nil && snap[1] == 11
	})
	if snap := p.LPOwnersSnapshot(); snap[1] != 11 {
		t.Fatalf("post-notify lp=1 = %d; want 11", snap[1])
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunReconciler exit = %v; want context.Canceled", err)
	}
}

// TestReconciler_EmptySnapshotPreservesPrevious confirms that a
// SnapshotLPOwners returning no rows (pre-bootstrap-seed or transient
// SyncRead glitch) does NOT clobber a previously installed snapshot —
// the Partitioner would otherwise drop to modulo fallback on every
// routing decision.
func TestReconciler_EmptySnapshotPreservesPrevious(t *testing.T) {
	p := NewPartitioner(3)
	reader := &fakeReader{snap: map[uint32]uint64{42: 2}}
	notify := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- RunReconciler(ctx, notify, reader, p, discardLogger()) }()

	waitFor(t, time.Second, func() bool { return p.LPOwnersSnapshot() != nil })
	if snap := p.LPOwnersSnapshot(); snap[42] != 2 {
		t.Fatalf("initial install lp=42 = %d; want 2", snap[42])
	}

	reader.set(map[uint32]uint64{}, 7) // empty snapshot
	notify <- struct{}{}
	// Wait long enough for one reconcile pass.
	time.Sleep(50 * time.Millisecond)
	if snap := p.LPOwnersSnapshot(); snap == nil || snap[42] != 2 {
		t.Fatalf("after empty snapshot wake, lp=42 should still be 2; got snap=%v", snap)
	}

	cancel()
	<-done
}

// TestReconciler_ReadErrorPreservesPrevious confirms that a transient
// SnapshotLPOwners error leaves the existing snapshot in place. The
// reconciler logs the error but keeps the loop alive for the next
// wake-up.
func TestReconciler_ReadErrorPreservesPrevious(t *testing.T) {
	p := NewPartitioner(3)
	reader := &fakeReader{snap: map[uint32]uint64{1: 5}}
	notify := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- RunReconciler(ctx, notify, reader, p, discardLogger()) }()
	waitFor(t, time.Second, func() bool { return p.LPOwnersSnapshot() != nil })

	reader.setErr(errors.New("transient"))
	notify <- struct{}{}
	time.Sleep(50 * time.Millisecond)
	if snap := p.LPOwnersSnapshot(); snap == nil || snap[1] != 5 {
		t.Fatalf("after error wake, lp=1 should still be 5; got snap=%v", snap)
	}

	cancel()
	<-done
}

// TestReconciler_NilDepsReturnError documents the constructor's two
// required arguments. Both nils surface as plain errors rather than
// nil-deref panics inside the goroutine.
func TestReconciler_NilDepsReturnError(t *testing.T) {
	if err := RunReconciler(context.Background(), nil, nil, NewPartitioner(1), discardLogger()); err == nil {
		t.Error("nil reader: err = nil; want non-nil")
	}
	r := &fakeReader{}
	if err := RunReconciler(context.Background(), nil, r, nil, discardLogger()); err == nil {
		t.Error("nil partitioner: err = nil; want non-nil")
	}
}

func waitFor(t *testing.T, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waitFor: predicate did not become true within %s", timeout)
}
