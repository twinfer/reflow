package snapshot

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/protobuf/encoding/protojson"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"

	// Register every supported scheme. Including all drivers in one
	// binary is a deliberate trade-off (~10MB binary growth) so reflowd
	// can switch between local-fs and cloud storage by config alone.
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
)

// BlobRepository archives snapshots to any gocloud.dev/blob-backed
// bucket — s3, gs, azblob, file, mem. The directory-based Repository
// contract is preserved: Put tars + gzips the source directory into a
// single blob; Fetch streams + untars on the receive side.
//
// Object layout (relative to the bucket's prefix):
//
//	p{shardID:08d}/snapshot-{raftIndex:020d}.tar.gz
//	p{shardID:08d}/snapshot-{raftIndex:020d}.meta.json
//
// The .meta.json sidecar is purely operator-facing — it lets `aws s3 ls`
// + `jq` identify a snapshot without unpacking the tar. The driver
// itself never reads it back; List populates SnapshotRef from the
// .tar.gz blob's Attributes.
type BlobRepository struct {
	// Bucket is owned by the repository — Close closes it.
	Bucket *blob.Bucket
	// Retain is the per-shard count retention enforced inline after each
	// successful Put. 0 means "retain all"; the reaper goroutine handles
	// time-based retention separately.
	Retain int
}

var _ Repository = (*BlobRepository)(nil)

const (
	blobArchiveSuffix = ".tar.gz"
	blobMetaSuffix    = ".meta.json"
)

func shardKeyPrefix(shardID uint64) string {
	return fmt.Sprintf("p%08d/", shardID)
}

func archiveKey(shardID, index uint64) string {
	return fmt.Sprintf("p%08d/snapshot-%020d%s", shardID, index, blobArchiveSuffix)
}

func metaKey(shardID, index uint64) string {
	return fmt.Sprintf("p%08d/snapshot-%020d%s", shardID, index, blobMetaSuffix)
}

// Close releases the underlying bucket. Safe to call multiple times.
func (r *BlobRepository) Close() error {
	if r.Bucket == nil {
		return nil
	}
	return r.Bucket.Close()
}

// Put streams srcDir as a gzipped tarball into the bucket and writes a
// sidecar .meta.json with the archive's size and SHA-256. Refuses to
// overwrite an existing (shardID, index) — callers must Delete first.
func (r *BlobRepository) Put(ctx context.Context, shardID, index uint64, srcDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if shardID == 0 || index == 0 {
		return fmt.Errorf("snapshot: shardID and index must be non-zero")
	}
	key := archiveKey(shardID, index)
	exists, err := r.Bucket.Exists(ctx, key)
	if err != nil {
		return fmt.Errorf("snapshot: probe %s: %w", key, err)
	}
	if exists {
		return fmt.Errorf("snapshot: (%d, %d) already archived", shardID, index)
	}

	w, err := r.Bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("snapshot: open writer %s: %w", key, err)
	}
	hash := sha256.New()
	mw := io.MultiWriter(w, hash)
	gz := gzip.NewWriter(mw)
	tarErr := TarDir(ctx, gz, srcDir)
	closeGzErr := gz.Close()
	closeWErr := w.Close()
	if tarErr != nil {
		// Best-effort cleanup of the partial blob.
		_ = r.Bucket.Delete(context.Background(), key)
		return fmt.Errorf("snapshot: tar: %w", tarErr)
	}
	if closeGzErr != nil {
		_ = r.Bucket.Delete(context.Background(), key)
		return fmt.Errorf("snapshot: close gzip: %w", closeGzErr)
	}
	if closeWErr != nil {
		_ = r.Bucket.Delete(context.Background(), key)
		return fmt.Errorf("snapshot: close blob writer: %w", closeWErr)
	}

	// Fetch the archive's persisted size from the bucket — local byte
	// counters miss any provider-side rewriting (e.g. chunked encoding).
	attrs, err := r.Bucket.Attributes(ctx, key)
	if err != nil {
		_ = r.Bucket.Delete(context.Background(), key)
		return fmt.Errorf("snapshot: attrs after upload: %w", err)
	}

	meta := &enginev1.SnapshotMeta{
		ShardId:        shardID,
		RaftIndex:      index,
		ChecksumSha256: hex.EncodeToString(hash.Sum(nil)),
		CreatedAtMs:    uint64(time.Now().UnixMilli()),
	}
	metaBytes, err := protojson.Marshal(meta)
	if err != nil {
		_ = r.Bucket.Delete(context.Background(), key)
		return fmt.Errorf("snapshot: marshal meta: %w", err)
	}
	if err := r.Bucket.WriteAll(ctx, metaKey(shardID, index), metaBytes, nil); err != nil {
		_ = r.Bucket.Delete(context.Background(), key)
		return fmt.Errorf("snapshot: write meta: %w", err)
	}
	_ = attrs // attrs.Size is observable via List; we don't need it stored locally

	if r.Retain > 0 {
		if err := r.enforceRetention(ctx, shardID); err != nil {
			return fmt.Errorf("snapshot: enforce retention: %w", err)
		}
	}
	return nil
}

// Fetch streams the archive for (shardID, index) into dstDir.
func (r *BlobRepository) Fetch(ctx context.Context, shardID, index uint64, dstDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rd, err := r.Bucket.NewReader(ctx, archiveKey(shardID, index), nil)
	if err != nil {
		return fmt.Errorf("snapshot: open reader: %w", err)
	}
	defer rd.Close()
	gz, err := gzip.NewReader(rd)
	if err != nil {
		return fmt.Errorf("snapshot: open gzip: %w", err)
	}
	defer gz.Close()
	return UntarDir(ctx, gz, dstDir)
}

// List enumerates archived snapshots for a shard, sorted by Index ascending.
func (r *BlobRepository) List(ctx context.Context, shardID uint64) ([]SnapshotRef, error) {
	prefix := shardKeyPrefix(shardID)
	iter := r.Bucket.List(&blob.ListOptions{Prefix: prefix})
	var out []SnapshotRef
	for {
		obj, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("snapshot: list %s: %w", prefix, err)
		}
		if obj.IsDir {
			continue
		}
		name := strings.TrimPrefix(obj.Key, prefix)
		if !strings.HasSuffix(name, blobArchiveSuffix) {
			continue
		}
		base := strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), blobArchiveSuffix)
		idx, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}
		out = append(out, SnapshotRef{
			ShardID:   shardID,
			Index:     idx,
			SizeBytes: obj.Size,
			CreatedAt: obj.ModTime,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

// Delete removes the archive and its sidecar. Idempotent against
// missing keys (NotFound is swallowed for each leg).
func (r *BlobRepository) Delete(ctx context.Context, shardID, index uint64) error {
	if err := r.deleteIfPresent(ctx, archiveKey(shardID, index)); err != nil {
		return err
	}
	return r.deleteIfPresent(ctx, metaKey(shardID, index))
}

func (r *BlobRepository) deleteIfPresent(ctx context.Context, key string) error {
	err := r.Bucket.Delete(ctx, key)
	if err == nil || gcerrors.Code(err) == gcerrors.NotFound {
		return nil
	}
	return fmt.Errorf("snapshot: delete %s: %w", key, err)
}

func (r *BlobRepository) enforceRetention(ctx context.Context, shardID uint64) error {
	refs, err := r.List(ctx, shardID)
	if err != nil {
		return err
	}
	if len(refs) <= r.Retain {
		return nil
	}
	drop := refs[:len(refs)-r.Retain]
	for _, ref := range drop {
		if err := r.Delete(ctx, shardID, ref.Index); err != nil {
			return err
		}
	}
	return nil
}

// OpenBucket opens a gocloud.dev/blob bucket from a URL string. Thin
// wrapper around blob.OpenBucket; kept as a seam in case future schemes
// want pre-open normalization. Supports gocloud's native `?prefix=…`
// query parameter for sub-folder layouts (e.g. `s3://mybucket?prefix=reflow/`).
func OpenBucket(ctx context.Context, urlStr string) (*blob.Bucket, error) {
	b, err := blob.OpenBucket(ctx, urlStr)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open bucket %q: %w", urlStr, err)
	}
	return b, nil
}
