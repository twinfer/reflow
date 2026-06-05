package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// openMemBucket returns a fresh in-memory bucket scoped to this test.
// memblob is per-OpenBucket — every call yields its own backing map.
func openMemBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	b, err := blob.OpenBucket(context.Background(), "mem://")
	if err != nil {
		t.Fatalf("open mem bucket: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func writeExport(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "snapshot-metadata"), []byte("epoch=1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "data", "000001.sst"), bytes.Repeat([]byte{0xA}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "data", "000002.sst"), bytes.Repeat([]byte{0xB}, 8192), 0o644); err != nil {
		t.Fatal(err)
	}
	return src
}

func TestBlob_WriterReaderRoundTrip(t *testing.T) {
	ctx := context.Background()
	repo := &BlobRepository{Bucket: openMemBucket(t)}
	src := writeExport(t)

	if err := SaveDir(ctx, repo, 7, 100, src); err != nil {
		t.Fatalf("SaveDir: %v", err)
	}

	dst := filepath.Join(t.TempDir(), "restored")
	if err := RestoreDir(ctx, repo, 7, 100, dst); err != nil {
		t.Fatalf("RestoreDir: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "snapshot-metadata"))
	if err != nil || !bytes.Equal(got, []byte("epoch=1")) {
		t.Fatalf("snapshot-metadata mismatch: got %q err=%v", got, err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "data", "000001.sst")); len(got) != 4096 || got[0] != 0xA {
		t.Fatalf("000001.sst payload mismatch")
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "data", "000002.sst")); len(got) != 8192 || got[0] != 0xB {
		t.Fatalf("000002.sst payload mismatch")
	}

	// Sidecar carries shard_id, raft_index, and a hex sha256 of the
	// archive body. Decode as opaque JSON — protojson uses camelCase
	// field names so plain encoding/json works fine for assertions.
	metaRaw, err := repo.Bucket.ReadAll(ctx, metaKey(7, 100))
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	var meta map[string]any
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		t.Fatalf("meta json: %v", err)
	}
	if got, _ := meta["shardId"].(string); got != "7" {
		// protojson emits uint64 as string; verify either int or string form.
		if v, _ := meta["shardId"].(float64); v != 7 {
			t.Fatalf("meta shard_id = %v", meta["shardId"])
		}
	}
	if cs, _ := meta["checksumSha256"].(string); len(cs) != 64 {
		t.Fatalf("meta checksum_sha256 length = %d (want 64 hex chars)", len(cs))
	}

	refs, err := repo.List(ctx, 7)
	if err != nil || len(refs) != 1 {
		t.Fatalf("List: refs=%+v err=%v", refs, err)
	}
	if refs[0].SizeBytes == 0 {
		t.Fatal("SizeBytes = 0 after SaveDir")
	}
	if refs[0].CreatedAt.IsZero() {
		t.Fatal("CreatedAt is zero after SaveDir")
	}
}

func TestBlob_NewWriterRefusesOverwrite(t *testing.T) {
	ctx := context.Background()
	repo := &BlobRepository{Bucket: openMemBucket(t)}
	src := writeExport(t)

	if err := SaveDir(ctx, repo, 1, 1, src); err != nil {
		t.Fatalf("first SaveDir: %v", err)
	}
	if _, err := repo.NewWriter(ctx, 1, 1); err == nil {
		t.Fatal("expected duplicate NewWriter to fail; got nil")
	} else if !strings.Contains(err.Error(), "already archived") {
		t.Fatalf("error %q lacks 'already archived'", err)
	}
}

func TestBlob_ListSortedByIndex(t *testing.T) {
	ctx := context.Background()
	repo := &BlobRepository{Bucket: openMemBucket(t)}
	src := writeExport(t)

	for _, idx := range []uint64{200, 50, 100} {
		if err := SaveDir(ctx, repo, 1, idx, src); err != nil {
			t.Fatalf("SaveDir %d: %v", idx, err)
		}
	}
	refs, err := repo.List(ctx, 1)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("len = %d; want 3", len(refs))
	}
	got := []uint64{refs[0].Index, refs[1].Index, refs[2].Index}
	want := []uint64{50, 100, 200}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i] < got[j] }) {
		t.Fatalf("not sorted ascending: %v", got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("[%d] = %d; want %d", i, got[i], want[i])
		}
		if refs[i].SizeBytes == 0 {
			t.Fatalf("ref[%d] SizeBytes = 0", i)
		}
		if refs[i].CreatedAt.IsZero() {
			t.Fatalf("ref[%d] CreatedAt is zero", i)
		}
	}
}

func TestBlob_DeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	repo := &BlobRepository{Bucket: openMemBucket(t)}
	// Delete on absent (shard, index) is a no-op.
	if err := repo.Delete(ctx, 99, 99); err != nil {
		t.Fatalf("Delete on absent: %v", err)
	}
	// Delete after a SaveDir removes both the archive and the sidecar.
	src := writeExport(t)
	if err := SaveDir(ctx, repo, 5, 5, src); err != nil {
		t.Fatalf("SaveDir: %v", err)
	}
	if err := repo.Delete(ctx, 5, 5); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, _ := repo.Bucket.Exists(ctx, archiveKey(5, 5)); ok {
		t.Fatal("archive still present after Delete")
	}
	if ok, _ := repo.Bucket.Exists(ctx, metaKey(5, 5)); ok {
		t.Fatal("meta sidecar still present after Delete")
	}
	// Second Delete is also a no-op.
	if err := repo.Delete(ctx, 5, 5); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
}

func TestBlob_NewReaderNotFound(t *testing.T) {
	ctx := context.Background()
	repo := &BlobRepository{Bucket: openMemBucket(t)}
	_, err := repo.NewReader(ctx, 1, 1)
	if err == nil {
		t.Fatal("expected error reading missing snapshot; got nil")
	}
	if gcerrors.Code(err) != gcerrors.NotFound {
		t.Fatalf("error code = %v; want NotFound (so callers can detect)", gcerrors.Code(err))
	}
}

func TestBlob_WriterAbortLeavesNoArtifact(t *testing.T) {
	ctx := context.Background()
	repo := &BlobRepository{Bucket: openMemBucket(t)}

	// Abandoned-without-Close: caller "crashes" mid-stream. gocloud's
	// blob.Writer only finalizes the blob on Close, so abandoning
	// leaves no observable artifact and the sidecar is never written.
	w, err := repo.NewWriter(ctx, 1, 1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := io.WriteString(w, "partial bytes, never a valid tar"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Deliberately do not call w.Close().

	if ok, _ := repo.Bucket.Exists(ctx, archiveKey(1, 1)); ok {
		t.Fatal("archive present after abandoned writer")
	}
	if ok, _ := repo.Bucket.Exists(ctx, metaKey(1, 1)); ok {
		t.Fatal("sidecar present after abandoned writer")
	}
	refs, err := repo.List(ctx, 1)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("List after abandon = %+v; want empty", refs)
	}
}

func TestBlob_RetentionDeletesOldest(t *testing.T) {
	ctx := context.Background()
	repo := &BlobRepository{Bucket: openMemBucket(t), Retain: 2}
	src := writeExport(t)

	for _, idx := range []uint64{10, 20, 30, 40} {
		if err := SaveDir(ctx, repo, 1, idx, src); err != nil {
			t.Fatalf("SaveDir %d: %v", idx, err)
		}
	}
	refs, _ := repo.List(ctx, 1)
	if len(refs) != 2 {
		t.Fatalf("retention=2; got %d refs", len(refs))
	}
	if refs[0].Index != 30 || refs[1].Index != 40 {
		t.Fatalf("retained wrong: got %d,%d want 30,40", refs[0].Index, refs[1].Index)
	}
	// Sidecars for the evicted indexes should also be gone.
	for _, idx := range []uint64{10, 20} {
		if ok, _ := repo.Bucket.Exists(ctx, metaKey(1, idx)); ok {
			t.Fatalf("sidecar for evicted idx %d still present", idx)
		}
	}
}

func TestBlob_FileBlobRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// fileblob URL: scheme + absolute path. create_dir=true so missing
	// nested folders are auto-created at write time.
	bucket, err := blob.OpenBucket(ctx, "file://"+dir+"?create_dir=true")
	if err != nil {
		t.Fatalf("open fileblob: %v", err)
	}
	t.Cleanup(func() { _ = bucket.Close() })

	repo := &BlobRepository{Bucket: bucket}
	src := writeExport(t)
	if err := SaveDir(ctx, repo, 3, 9, src); err != nil {
		t.Fatalf("SaveDir: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "restored")
	if err := RestoreDir(ctx, repo, 3, 9, dst); err != nil {
		t.Fatalf("RestoreDir: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "snapshot-metadata")); !bytes.Equal(got, []byte("epoch=1")) {
		t.Fatalf("metadata roundtrip mismatch: %q", got)
	}
}

func TestBlob_PrefixedBucket(t *testing.T) {
	ctx := context.Background()
	// gocloud's native ?prefix= URL parameter wraps the bucket with
	// PrefixedBucket — keys land under the configured sub-folder.
	bucket, err := blob.OpenBucket(ctx, "mem://?prefix=reflw/snaps/")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	repo := &BlobRepository{Bucket: bucket}
	src := writeExport(t)
	if err := SaveDir(ctx, repo, 2, 7, src); err != nil {
		t.Fatalf("SaveDir: %v", err)
	}
	// The List should still see the shard's archive — PrefixedBucket
	// hides the prefix from the caller.
	refs, err := repo.List(ctx, 2)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(refs) != 1 || refs[0].Index != 7 {
		t.Fatalf("List after prefix = %+v", refs)
	}
}
