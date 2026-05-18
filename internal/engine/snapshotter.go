package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/engine/snapshot"
	"github.com/twinfer/reflow/internal/storage"
)

// StoreFactory opens a fresh storage.Store rooted at the given path. It is
// used by Snapshotter to reopen the underlying DB after a swap.
type StoreFactory func(path string) (storage.Store, error)

// Snapshotter implements dragonboat's SaveSnapshot / RecoverFromSnapshot for
// a single partition's state-machine store. It does NOT own the DB lifecycle
// outside of the swap; callers obtain the current Store via Store() (for
// short-lived synchronous use on the apply goroutine) or via Acquire (for
// long-lived leases that must outlive Close).
//
// Close holds a sync.WaitGroup of outstanding Acquire leases. Once any
// Acquire has happened, Close blocks until every release fires before
// closing the underlying store — so a leader-scoped goroutine that ran
// onBecomeLeader and captured the store doesn't get a panic when the
// owning host shuts down.
//
// RecoverFromSnapshot performs the close → swap → reopen dance that was
// missing from the original reflow code (local_snapshotter.go:88 removed the
// data dir while the DB was still open). On any failure mid-recover the
// original dataDir is restored from the side-saved .old copy.
type Snapshotter struct {
	dataDir string
	open    StoreFactory

	mu      sync.Mutex
	current storage.Store
	closing bool
	leases  sync.WaitGroup
}

// NewSnapshotter opens the initial store at dataDir using the given factory.
// The caller is responsible for ensuring dataDir's parent exists.
func NewSnapshotter(dataDir string, open StoreFactory) (*Snapshotter, error) {
	if open == nil {
		return nil, errors.New("snapshotter: nil StoreFactory")
	}
	if err := os.MkdirAll(filepath.Dir(dataDir), 0o755); err != nil {
		return nil, err
	}
	s, err := open(dataDir)
	if err != nil {
		return nil, err
	}
	return &Snapshotter{
		dataDir: dataDir,
		open:    open,
		current: s,
	}, nil
}

// Store returns the currently bound storage.Store. The reference is replaced
// atomically on RecoverFromSnapshot, so callers should re-Read it after any
// recovery. Returns nil after Close.
//
// Store is safe for short-lived synchronous use on the apply goroutine
// where dragonboat serializes Update vs Close. Goroutines that outlive a
// single apply (timer Run loop, leader-scope rebuild) MUST use Acquire
// instead so Close waits for them to release before tearing down the
// underlying pebble.DB.
func (s *Snapshotter) Store() storage.Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

// Acquire returns a lease on the current store. The release function must
// be called exactly once when the caller is done with the store; Close
// blocks on outstanding leases before closing the underlying pebble.DB.
// Returns (nil, nil, false) when the snapshotter is already closing — the
// caller must treat this as a benign shutdown signal and skip work.
func (s *Snapshotter) Acquire() (storage.Store, func(), bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.current == nil {
		return nil, nil, false
	}
	s.leases.Add(1)
	cur := s.current
	return cur, s.leases.Done, true
}

// DataDir returns the underlying data directory.
func (s *Snapshotter) DataDir() string { return s.dataDir }

// SaveSnapshot writes a tar of a fresh Pebble checkpoint to w. The current
// DB stays open throughout; Pebble checkpoints are online.
func (s *Snapshotter) SaveSnapshot(w io.Writer) error {
	s.mu.Lock()
	store := s.current
	s.mu.Unlock()
	if store == nil {
		return errors.New("snapshotter: store is closed")
	}

	// Pebble Checkpoint requires the dest dir NOT to exist
	// (pebble v1.1.5 checkpoint.go:145-154). Use a timestamped sibling of
	// the data dir so there is no chance of collision.
	ckpDir := fmt.Sprintf("%s.ck-%d", s.dataDir, time.Now().UnixNano())
	if err := store.Checkpoint(ckpDir); err != nil {
		return fmt.Errorf("snapshotter: checkpoint: %w", err)
	}
	defer os.RemoveAll(ckpDir)

	return snapshot.TarDir(context.Background(), w, ckpDir)
}

// RecoverFromSnapshot replaces the current data dir with the contents of the
// tar stream. On success the bound Store reference is replaced with one
// opened against the recovered files.
func (s *Snapshotter) RecoverFromSnapshot(r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stagingDir := s.dataDir + ".staging"
	oldDir := s.dataDir + ".old"

	// Clean any leftover sidecar dirs from a previous failed recover.
	_ = os.RemoveAll(stagingDir)
	_ = os.RemoveAll(oldDir)

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return err
	}
	if err := snapshot.UntarDir(context.Background(), r, stagingDir); err != nil {
		_ = os.RemoveAll(stagingDir)
		return fmt.Errorf("snapshotter: untar: %w", err)
	}

	// Close the current DB so the data dir can be moved out from under it.
	if s.current != nil {
		if err := s.current.Close(); err != nil {
			_ = os.RemoveAll(stagingDir)
			return fmt.Errorf("snapshotter: close current store: %w", err)
		}
		s.current = nil
	}

	// Move existing data aside (so it can be restored on failure).
	if _, err := os.Stat(s.dataDir); err == nil {
		if err := os.Rename(s.dataDir, oldDir); err != nil {
			_ = os.RemoveAll(stagingDir)
			s.tryReopen() // best-effort
			return fmt.Errorf("snapshotter: rename current -> old: %w", err)
		}
	}

	// Promote staging to live.
	if err := os.Rename(stagingDir, s.dataDir); err != nil {
		// Roll back to .old if we have one.
		if _, statErr := os.Stat(oldDir); statErr == nil {
			_ = os.Rename(oldDir, s.dataDir)
		}
		s.tryReopen()
		return fmt.Errorf("snapshotter: rename staging -> current: %w", err)
	}

	// Open the recovered store.
	newStore, err := s.open(s.dataDir)
	if err != nil {
		// Open failed — try to restore .old.
		_ = os.RemoveAll(s.dataDir)
		if _, statErr := os.Stat(oldDir); statErr == nil {
			_ = os.Rename(oldDir, s.dataDir)
			s.tryReopen()
		}
		return fmt.Errorf("snapshotter: open recovered store: %w", err)
	}
	s.current = newStore

	_ = os.RemoveAll(oldDir)
	return nil
}

// Close marks the snapshotter closing, waits for every outstanding
// Acquire lease to release, and closes the bound store. Idempotent.
//
// Acquire returns false after Close has started, so no new leases can
// extend the wait. Any goroutine that already holds a lease must release
// it (typically via the function returned by Acquire) for Close to make
// progress — Close blocks otherwise.
func (s *Snapshotter) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	s.mu.Unlock()

	s.leases.Wait()

	s.mu.Lock()
	cur := s.current
	s.current = nil
	s.mu.Unlock()
	if cur == nil {
		return nil
	}
	return cur.Close()
}

// tryReopen is a best-effort attempt to rebind a store at dataDir; ignores
// errors because it is only called from a failure path.
func (s *Snapshotter) tryReopen() {
	if s.current != nil {
		return
	}
	if st, err := s.open(s.dataDir); err == nil {
		s.current = st
	}
}
