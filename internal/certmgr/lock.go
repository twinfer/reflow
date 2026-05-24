package certmgr

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// acquireLock writes "<nodeID>\n" to <dir>/lock and refuses to start
// when an existing file holds a different node id. Mirrors Pebble's
// data-dir lock: the CertMagic FileStorage is per-node-private, and a
// second node accidentally pointed at the same path would corrupt the
// renewal state. Returns a release callback that removes the lock; safe
// to call on a nil receiver after a failed New.
func acquireLock(dir, nodeID string) (release func() error, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("certmgr: create %s: %w", dir, err)
	}
	path := filepath.Join(dir, "lock")
	if existing, rerr := os.ReadFile(path); rerr == nil {
		got := strings.TrimSpace(string(existing))
		if got != "" && got != nodeID {
			return nil, fmt.Errorf("certmgr: cert cache %s is locked by node %q; refusing to start as %q", dir, got, nodeID)
		}
	} else if !errors.Is(rerr, os.ErrNotExist) {
		return nil, fmt.Errorf("certmgr: read lock: %w", rerr)
	}
	if err := os.WriteFile(path, []byte(nodeID+"\n"), 0o600); err != nil {
		return nil, fmt.Errorf("certmgr: write lock: %w", err)
	}
	return func() error { return os.Remove(path) }, nil
}
