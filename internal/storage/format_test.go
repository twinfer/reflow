package storage_test

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
)

func writeFormatVersion(t *testing.T, s storage.Store, v uint32) {
	t.Helper()
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	b := s.NewBatch()
	defer b.Close()
	if err := b.Set(keys.FormatVersionKey(), buf[:]); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func writeRaw(t *testing.T, s storage.Store, key, val []byte) {
	t.Helper()
	b := s.NewBatch()
	defer b.Close()
	if err := b.Set(key, val); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func readFormatVersion(t *testing.T, s storage.Store) (uint32, bool) {
	t.Helper()
	raw, closer, err := s.Get(keys.FormatVersionKey())
	if errors.Is(err, storage.ErrNotFound) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer closer.Close()
	if len(raw) != 4 {
		t.Fatalf("format key has %d bytes; want 4", len(raw))
	}
	return binary.BigEndian.Uint32(raw), true
}

func TestFormatVersion_StampsOnFreshOpen(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()

	if _, present := readFormatVersion(t, s); present {
		t.Fatalf("expected key absent on fresh store")
	}
	if err := storage.VerifyAndStampFormatVersion(s, 1); err != nil {
		t.Fatalf("verify: %v", err)
	}
	v, present := readFormatVersion(t, s)
	if !present || v != 1 {
		t.Fatalf("after stamp: present=%v v=%d; want present=true v=1", present, v)
	}
	// Second call is a no-op.
	if err := storage.VerifyAndStampFormatVersion(s, 1); err != nil {
		t.Fatalf("verify (second call): %v", err)
	}
}

func TestFormatVersion_NoOpWhenEqual(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()

	writeFormatVersion(t, s, 1)
	if err := storage.VerifyAndStampFormatVersion(s, 1); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestFormatVersion_RejectsTooNew(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()

	writeFormatVersion(t, s, 2)
	err := storage.VerifyAndStampFormatVersion(s, 1)
	if !errors.Is(err, storage.ErrFormatVersionTooNew) {
		t.Fatalf("got %v; want ErrFormatVersionTooNew", err)
	}
}

func TestFormatVersion_RejectsStale(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()

	writeFormatVersion(t, s, 1)
	err := storage.VerifyAndStampFormatVersion(s, 2)
	if !errors.Is(err, storage.ErrFormatVersionStale) {
		t.Fatalf("got %v; want ErrFormatVersionStale", err)
	}
}

func TestFormatVersion_RejectsMalformedValue(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()

	writeRaw(t, s, keys.FormatVersionKey(), []byte{0, 0, 0}) // 3 bytes
	err := storage.VerifyAndStampFormatVersion(s, 1)
	if err == nil {
		t.Fatalf("expected error for malformed value")
	}
	// Must not match either typed sentinel (it's a corruption, not a version skew).
	if errors.Is(err, storage.ErrFormatVersionTooNew) || errors.Is(err, storage.ErrFormatVersionStale) {
		t.Fatalf("got typed sentinel %v; want generic malformed error", err)
	}
}

func TestOpenPebbleWithFormatGuard_RoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	// First open: stamps version 1.
	s1, err := storage.OpenPebbleWithFormatGuard(dir, nil, 1)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	v, present := readFormatVersion(t, s1)
	if !present || v != 1 {
		t.Fatalf("after first open: present=%v v=%d; want present=true v=1", present, v)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Second open: clean no-op.
	s2, err := storage.OpenPebbleWithFormatGuard(dir, nil, 1)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestOpenPebbleWithFormatGuard_RejectsTooNew_OnDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	// Manually stamp version 2 via OpenPebble (bypassing the guard).
	s, err := storage.OpenPebble(dir, nil)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	writeFormatVersion(t, s, 2)
	if err := s.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	// Guarded open with current=1 should refuse and not leak a handle.
	got, err := storage.OpenPebbleWithFormatGuard(dir, nil, 1)
	if got != nil {
		t.Fatalf("expected nil store on rejection; got %v", got)
	}
	if !errors.Is(err, storage.ErrFormatVersionTooNew) {
		t.Fatalf("got %v; want ErrFormatVersionTooNew", err)
	}

	// Re-opening at the matching version should succeed (proves the prior
	// rejection closed the DB cleanly — pebble would refuse a double-open).
	s2, err := storage.OpenPebbleWithFormatGuard(dir, nil, 2)
	if err != nil {
		t.Fatalf("reopen at v2: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close v2: %v", err)
	}
}
