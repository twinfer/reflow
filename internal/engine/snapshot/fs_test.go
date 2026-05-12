package snapshot

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// writeFile is a small helper used by the round-trip and retention tests.
func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

// readFile is a small helper for assertions.
func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPhase4_2_FSRepository_PutFetchRoundTrip(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(t.TempDir(), "export")
	// Layout an exported-snapshot-shaped directory.
	writeFile(t, filepath.Join(src, "snapshot-metadata"), []byte("epoch=1"))
	writeFile(t, filepath.Join(src, "data", "000001.sst"), bytes.Repeat([]byte{0xA}, 4096))
	writeFile(t, filepath.Join(src, "data", "000002.sst"), bytes.Repeat([]byte{0xB}, 8192))

	repo := &FSRepository{Root: root}
	if err := repo.Put(context.Background(), 7, 100, src); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Refusing to overwrite is a deliberate guard.
	if err := repo.Put(context.Background(), 7, 100, src); err == nil {
		t.Fatal("expected Put to fail on duplicate (shard,index); got nil")
	}

	dst := filepath.Join(t.TempDir(), "restored")
	if err := repo.Fetch(context.Background(), 7, 100, dst); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got, want := readFile(t, filepath.Join(dst, "snapshot-metadata")), []byte("epoch=1"); !bytes.Equal(got, want) {
		t.Fatalf("snapshot-metadata mismatch: got %q want %q", got, want)
	}
	if got := readFile(t, filepath.Join(dst, "data", "000001.sst")); len(got) != 4096 || got[0] != 0xA {
		t.Fatalf("000001.sst payload mismatch")
	}
	if got := readFile(t, filepath.Join(dst, "data", "000002.sst")); len(got) != 8192 || got[0] != 0xB {
		t.Fatalf("000002.sst payload mismatch")
	}
}

func TestPhase4_2_FSRepository_ListSortedByIndex(t *testing.T) {
	root := t.TempDir()
	repo := &FSRepository{Root: root}
	src := filepath.Join(t.TempDir(), "src")
	writeFile(t, filepath.Join(src, "f"), []byte("x"))

	for _, idx := range []uint64{200, 50, 100} {
		if err := repo.Put(context.Background(), 1, idx, src); err != nil {
			t.Fatalf("Put %d: %v", idx, err)
		}
	}
	refs, err := repo.List(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Fatalf("len = %d; want 3", len(refs))
	}
	got := make([]uint64, len(refs))
	for i, r := range refs {
		got[i] = r.Index
	}
	want := []uint64{50, 100, 200}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i] < got[j] }) {
		t.Fatalf("not sorted ascending: %v", got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("[%d] = %d; want %d", i, got[i], want[i])
		}
	}
}

func TestPhase4_2_FSRepository_RetentionDeletesOldest(t *testing.T) {
	root := t.TempDir()
	repo := &FSRepository{Root: root, Retain: 2}
	src := filepath.Join(t.TempDir(), "src")
	writeFile(t, filepath.Join(src, "f"), []byte("x"))

	for _, idx := range []uint64{10, 20, 30, 40} {
		if err := repo.Put(context.Background(), 1, idx, src); err != nil {
			t.Fatalf("Put %d: %v", idx, err)
		}
	}
	refs, _ := repo.List(context.Background(), 1)
	if len(refs) != 2 {
		t.Fatalf("retention=2; got %d refs", len(refs))
	}
	if refs[0].Index != 30 || refs[1].Index != 40 {
		t.Fatalf("retained wrong: got %d,%d want 30,40", refs[0].Index, refs[1].Index)
	}
}

func TestPhase4_2_FSRepository_DeleteIdempotent(t *testing.T) {
	root := t.TempDir()
	repo := &FSRepository{Root: root}
	// Delete on absent shard should be a no-op.
	if err := repo.Delete(context.Background(), 99, 99); err != nil {
		t.Fatalf("Delete on absent: %v", err)
	}
}
