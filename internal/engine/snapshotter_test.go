package engine_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/storage"
)

func newPebbleFactory(t *testing.T) engine.StoreFactory {
	t.Helper()
	return func(path string) (storage.Store, error) {
		return storage.OpenPebble(path, nil)
	}
}

func TestSnapshotter_RoundTrip(t *testing.T) {
	open := newPebbleFactory(t)

	dirA := filepath.Join(t.TempDir(), "p1", "state")
	sA, err := engine.NewSnapshotter(dirA, open)
	if err != nil {
		t.Fatal(err)
	}

	bA := sA.Store().NewBatch()
	for i := range 32 {
		key := []byte{byte(i), 0xAA}
		val := []byte{byte(i), byte(i)}
		if err := bA.Set(key, val); err != nil {
			t.Fatal(err)
		}
	}
	if err := bA.Commit(true); err != nil {
		t.Fatal(err)
	}
	bA.Close()

	var buf bytes.Buffer
	if err := sA.SaveSnapshot(&buf); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := sA.Close(); err != nil {
		t.Fatal(err)
	}

	dirB := filepath.Join(t.TempDir(), "p1", "state")
	sB, err := engine.NewSnapshotter(dirB, open)
	if err != nil {
		t.Fatal(err)
	}
	defer sB.Close()

	// Pre-load some unrelated state to verify it's replaced wholesale.
	bB := sB.Store().NewBatch()
	if err := bB.Set([]byte("stale"), []byte("must-be-gone")); err != nil {
		t.Fatal(err)
	}
	_ = bB.Commit(true)
	bB.Close()

	if err := sB.RecoverFromSnapshot(&buf); err != nil {
		t.Fatalf("RecoverFromSnapshot: %v", err)
	}

	// New store should hold A's data and NOT B's prior state.
	for i := range 32 {
		key := []byte{byte(i), 0xAA}
		val, closer, err := sB.Store().Get(key)
		if err != nil {
			t.Errorf("Get(%x) after recover: %v", key, err)
			continue
		}
		want := []byte{byte(i), byte(i)}
		if !bytes.Equal(val, want) {
			t.Errorf("Get(%x) = %x; want %x", key, val, want)
		}
		closer.Close()
	}
	if _, closer, err := sB.Store().Get([]byte("stale")); err == nil {
		closer.Close()
		t.Errorf("stale row should be gone after recover")
	}
}

func TestSnapshotter_NewWithMissingParentCreatesIt(t *testing.T) {
	open := newPebbleFactory(t)
	dir := filepath.Join(t.TempDir(), "nested", "deep", "p1", "state")
	s, err := engine.NewSnapshotter(dir, open)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Store() == nil {
		t.Error("expected non-nil store")
	}
}
