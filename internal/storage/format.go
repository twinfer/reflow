package storage

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/twinfer/reflow/internal/storage/keys"
)

// StorageFormatVersion is the on-disk format version this binary writes and
// understands. Bumped by hand whenever a non-additive on-disk change lands
// (new key layout, value-encoding change). Additive proto field changes do
// not require a bump.
const StorageFormatVersion uint32 = 1

// ErrFormatVersionTooNew means the DB was written by a newer binary than this
// one. The operator must run a binary >= the persisted version.
var ErrFormatVersionTooNew = errors.New("storage: format version too new for this binary")

// ErrFormatVersionStale means the DB was written by an older binary and no
// migration is registered for the gap. Run an intermediate binary first, or
// register the migration.
var ErrFormatVersionStale = errors.New("storage: format version older than this binary")

// VerifyAndStampFormatVersion enforces format-version compatibility on an
// open Store. Behaviour:
//   - marker absent: writes `current` and returns nil.
//   - persisted == current: returns nil.
//   - persisted >  current: returns %w of ErrFormatVersionTooNew.
//   - persisted <  current: returns %w of ErrFormatVersionStale (no migrations
//     registered yet).
func VerifyAndStampFormatVersion(s Store, current uint32) error {
	raw, closer, err := s.Get(keys.FormatVersionKey())
	if errors.Is(err, ErrNotFound) {
		return stampFormatVersion(s, current)
	}
	if err != nil {
		return fmt.Errorf("read format version: %w", err)
	}
	defer closer.Close()
	if len(raw) != 4 {
		return fmt.Errorf("storage: format version key has %d bytes; want 4", len(raw))
	}
	persisted := binary.BigEndian.Uint32(raw)
	switch {
	case persisted == current:
		return nil
	case persisted > current:
		return fmt.Errorf("%w: persisted=%d current=%d", ErrFormatVersionTooNew, persisted, current)
	default:
		return fmt.Errorf("%w: persisted=%d current=%d", ErrFormatVersionStale, persisted, current)
	}
}

func stampFormatVersion(s Store, v uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	b := s.NewBatch()
	defer b.Close()
	if err := b.Set(keys.FormatVersionKey(), buf[:]); err != nil {
		return fmt.Errorf("stamp format version: %w", err)
	}
	if err := b.Commit(true); err != nil {
		return fmt.Errorf("commit format version: %w", err)
	}
	return nil
}

// OpenPebbleWithFormatGuard opens a pebble DB at dir and verifies/stamps the
// storage format version against `current` before returning. Engine call
// sites should prefer this over OpenPebble; tests that don't care about
// format versioning may still use OpenPebble directly.
//
// If verification fails the underlying *PebbleStore is closed before the
// error is returned, so callers do not need to worry about leaked handles.
func OpenPebbleWithFormatGuard(dir string, opts *pebble.Options, current uint32) (*PebbleStore, error) {
	s, err := OpenPebble(dir, opts)
	if err != nil {
		return nil, err
	}
	if err := VerifyAndStampFormatVersion(s, current); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}
