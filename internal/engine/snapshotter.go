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
// outside of the swap; callers obtain the current Store via Store().
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
// recovery.
func (s *Snapshotter) Store() storage.Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
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

// Close closes the bound store. Idempotent.
func (s *Snapshotter) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil
	}
	err := s.current.Close()
	s.current = nil
	return err
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
