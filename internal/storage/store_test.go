package storage_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/twinfer/reflw/internal/storage"
)

// runConformanceSuite exercises both the MemStore and a Pebble-backed Store
// on top of vfs.NewMem(). Any behavior divergence indicates an interface
// implementation bug.
func runConformanceSuite(t *testing.T, name string, open func(t *testing.T) storage.Store) {
	t.Helper()
	t.Run(name+"/PutGetDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()

		b := s.NewBatch()
		if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
			t.Fatal(err)
		}
		if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
			t.Fatal(err)
		}
		if err := b.Commit(true); err != nil {
			t.Fatal(err)
		}
		b.Close()

		mustGet(t, s, "k1", "v1")
		mustGet(t, s, "k2", "v2")
		mustNotFound(t, s, "k3")

		b2 := s.NewBatch()
		if err := b2.Delete([]byte("k1")); err != nil {
			t.Fatal(err)
		}
		if err := b2.Commit(true); err != nil {
			t.Fatal(err)
		}
		b2.Close()

		mustNotFound(t, s, "k1")
		mustGet(t, s, "k2", "v2")
	})

	t.Run(name+"/IterBoundsAndOrder", func(t *testing.T) {
		s := open(t)
		defer s.Close()

		b := s.NewBatch()
		for _, k := range []string{"a", "ab", "b", "c", "d"} {
			if err := b.Set([]byte(k), []byte(k+"-v")); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Commit(true); err != nil {
			t.Fatal(err)
		}
		b.Close()

		// Range [ab, d) should yield ab, b, c — in order.
		iter, err := s.NewIter([]byte("ab"), []byte("d"))
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		var got []string
		for ok := iter.First(); ok; ok = iter.Next() {
			got = append(got, string(iter.Key()))
		}
		want := []string{"ab", "b", "c"}
		if len(got) != len(want) {
			t.Fatalf("len(got)=%d want %d (got=%v)", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("entry %d: got %q want %q", i, got[i], want[i])
			}
		}
	})

	t.Run(name+"/DeleteRange", func(t *testing.T) {
		s := open(t)
		defer s.Close()

		b := s.NewBatch()
		for _, k := range []string{"a", "b", "c", "d", "e"} {
			if err := b.Set([]byte(k), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Commit(true); err != nil {
			t.Fatal(err)
		}
		b.Close()

		b2 := s.NewBatch()
		// Half-open: deletes b, c, d but not e.
		if err := b2.DeleteRange([]byte("b"), []byte("e")); err != nil {
			t.Fatal(err)
		}
		if err := b2.Commit(true); err != nil {
			t.Fatal(err)
		}
		b2.Close()

		mustGet(t, s, "a", "v")
		mustNotFound(t, s, "b")
		mustNotFound(t, s, "c")
		mustNotFound(t, s, "d")
		mustGet(t, s, "e", "v")
	})

	t.Run(name+"/SeekGE", func(t *testing.T) {
		s := open(t)
		defer s.Close()

		b := s.NewBatch()
		for _, k := range []string{"a", "c", "e"} {
			if err := b.Set([]byte(k), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Commit(true); err != nil {
			t.Fatal(err)
		}
		b.Close()

		iter, err := s.NewIter(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		if !iter.SeekGE([]byte("b")) {
			t.Fatalf("SeekGE(b) returned false")
		}
		if string(iter.Key()) != "c" {
			t.Errorf("SeekGE(b) key = %q want c", iter.Key())
		}
		if !iter.Next() {
			t.Fatalf("Next returned false")
		}
		if string(iter.Key()) != "e" {
			t.Errorf("Next key = %q want e", iter.Key())
		}
		if iter.Next() {
			t.Errorf("Next past end returned true")
		}
	})
}

func TestConformance(t *testing.T) {
	runConformanceSuite(t, "Mem", func(t *testing.T) storage.Store {
		return storage.NewMemStore()
	})
	runConformanceSuite(t, "PebbleMemFS", func(t *testing.T) storage.Store {
		s, err := storage.OpenPebble("/p", &pebble.Options{FS: vfs.NewMem()})
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestPebbleCheckpoint_RoundTrip(t *testing.T) {
	fs := vfs.NewMem()
	src, err := storage.OpenPebble("/src", &pebble.Options{FS: fs})
	if err != nil {
		t.Fatal(err)
	}

	b := src.NewBatch()
	for i := byte('a'); i <= byte('z'); i++ {
		if err := b.Set([]byte{i}, []byte{i, i, i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	// Source DB MUST stay open while checkpoint runs (pebble Checkpoint is
	// online).
	if err := src.Checkpoint("/ckp"); err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}

	// Pebble Checkpoint into an existing dir must fail.
	if err := src.Checkpoint("/ckp"); err == nil {
		t.Errorf("expected error when checkpoint dir exists")
	}

	// Open the checkpoint; same vfs.MemFS so the dir is visible.
	dst, err := storage.OpenPebble("/ckp", &pebble.Options{FS: fs})
	if err != nil {
		t.Fatalf("open checkpoint failed: %v", err)
	}
	defer dst.Close()

	for i := byte('a'); i <= byte('z'); i++ {
		val, closer, err := dst.Get([]byte{i})
		if err != nil {
			t.Fatalf("Get(%q) on checkpoint: %v", string(i), err)
		}
		if !bytes.Equal(val, []byte{i, i, i}) {
			t.Errorf("checkpoint Get(%q) = %x want %x", string(i), val, []byte{i, i, i})
		}
		closer.Close()
	}

	src.Close()
}

func TestPebbleCheckpoint_DestMustNotExist_OSFS(t *testing.T) {
	// Pebble v1.1.5 returns *os.PathError{ErrExist} when destDir exists.
	// Verify under the real OS filesystem (vfs.MemFS exhibits the same
	// behaviour but this test pins the production contract).
	dir := t.TempDir()
	src, err := storage.OpenPebble(filepath.Join(dir, "src"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	existing := filepath.Join(dir, "ckp")
	if err := os.Mkdir(existing, 0o755); err != nil {
		t.Fatal(err)
	}
	err = src.Checkpoint(existing)
	if err == nil {
		t.Fatal("expected error when checkpoint dir exists")
	}
	if !errors.Is(err, os.ErrExist) && !isPathExistError(err) {
		t.Errorf("unexpected error type: %v", err)
	}
}

func TestMemStore_CheckpointUnsupported(t *testing.T) {
	s := storage.NewMemStore()
	if err := s.Checkpoint("/anywhere"); err == nil {
		t.Fatal("expected checkpoint to fail on MemStore")
	}
}

func TestBatchClosedRejects(t *testing.T) {
	s := storage.NewMemStore()
	b := s.NewBatch()
	b.Close()
	if err := b.Set([]byte("k"), []byte("v")); err == nil {
		t.Errorf("Set after Close should fail")
	}
	if err := b.Delete([]byte("k")); err == nil {
		t.Errorf("Delete after Close should fail")
	}
	if err := b.DeleteRange([]byte("a"), []byte("b")); err == nil {
		t.Errorf("DeleteRange after Close should fail")
	}
	if err := b.Commit(true); err == nil {
		t.Errorf("Commit after Close should fail")
	}
}

func mustGet(t *testing.T, s storage.Store, key, want string) {
	t.Helper()
	v, closer, err := s.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	defer closer.Close()
	if !bytes.Equal(v, []byte(want)) {
		t.Errorf("Get(%q) = %q want %q", key, v, want)
	}
}

func mustNotFound(t *testing.T, s storage.Store, key string) {
	t.Helper()
	_, closer, err := s.Get([]byte(key))
	if !errors.Is(err, storage.ErrNotFound) {
		if closer != nil {
			closer.Close()
		}
		t.Fatalf("Get(%q): err=%v want ErrNotFound", key, err)
	}
}

func isPathExistError(err error) bool {
	// pebble may return its own wrapping; fall back to checking for the
	// stdlib *os.PathError with ErrExist.
	var perr *os.PathError
	if errors.As(err, &perr) {
		return errors.Is(perr.Err, os.ErrExist)
	}
	// io.ErrShortWrite style sentinel — not relevant; check inner Is.
	return errors.Is(err, io.ErrUnexpectedEOF) == false && false
}
