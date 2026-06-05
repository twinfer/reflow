package snapshot

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"google.golang.org/protobuf/encoding/protojson"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"

	// Register every supported scheme. Including all drivers in one
	// binary is a deliberate trade-off (~10MB binary growth) so reflwd
	// can switch between local-fs and cloud storage by config alone.
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
)

// BlobRepository archives snapshots to any gocloud.dev/blob-backed
// bucket — s3, gs, azblob, file, mem. The streaming Repository
// contract is implemented by archiveWriter / archiveReader, which
// own the gzip + sha256 + sidecar plumbing internally.
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
	// Retain is the per-shard count retention enforced inline on each
	// successful NewWriter.Close. 0 means "retain all"; the reaper
	// goroutine handles time-based and tiered retention separately.
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

// NewWriter opens an upload stream for (shardID, raftIndex). The caller
// writes raw bytes (typically a tar stream); the returned WriteCloser
// gzips on the fly, accumulates a sha256, and on Close persists the
// .meta.json sidecar and enforces inline retention.
//
// The archive is durable only after a successful Close. A caller that
// returns without calling Close (or whose Close errors mid-way) leaves
// no observable artifact — gocloud's NewWriter only finalizes on
// successful Close, and the sidecar is written after the archive
// commits.
func (r *BlobRepository) NewWriter(ctx context.Context, shardID, raftIndex uint64) (io.WriteCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if shardID == 0 || raftIndex == 0 {
		return nil, fmt.Errorf("snapshot: shardID and raftIndex must be non-zero")
	}
	key := archiveKey(shardID, raftIndex)
	exists, err := r.Bucket.Exists(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("snapshot: probe %s: %w", key, err)
	}
	if exists {
		return nil, fmt.Errorf("snapshot: (%d, %d) already archived", shardID, raftIndex)
	}
	bw, err := r.Bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open writer %s: %w", key, err)
	}
	h := sha256.New()
	// gzip writes into a MultiWriter so the hash sees the same
	// compressed bytes the bucket stores. Operators verifying the
	// sidecar against `sha256sum snapshot-*.tar.gz` get a match.
	gz := gzip.NewWriter(io.MultiWriter(bw, h))
	return &archiveWriter{
		ctx:     ctx,
		repo:    r,
		shard:   shardID,
		index:   raftIndex,
		bucketW: bw,
		gz:      gz,
		hash:    h,
	}, nil
}

// NewReader opens a download stream for (shardID, raftIndex). gzip is
// stripped automatically — callers see the raw tar bytes that were
// originally written. The returned error satisfies
// gcerrors.Code(err) == gcerrors.NotFound when the snapshot is absent.
func (r *BlobRepository) NewReader(ctx context.Context, shardID, raftIndex uint64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bucketR, err := r.Bucket.NewReader(ctx, archiveKey(shardID, raftIndex), nil)
	if err != nil {
		// Surface gcerrors.NotFound unwrapped so callers can detect.
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, err
		}
		return nil, fmt.Errorf("snapshot: open reader: %w", err)
	}
	gz, err := gzip.NewReader(bucketR)
	if err != nil {
		_ = bucketR.Close()
		return nil, fmt.Errorf("snapshot: open gzip: %w", err)
	}
	return &archiveReader{rd: bucketR, gz: gz}, nil
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
func (r *BlobRepository) Delete(ctx context.Context, shardID, raftIndex uint64) error {
	if err := r.deleteIfPresent(ctx, archiveKey(shardID, raftIndex)); err != nil {
		return err
	}
	return r.deleteIfPresent(ctx, metaKey(shardID, raftIndex))
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

// archiveWriter is the streaming WriteCloser returned by
// BlobRepository.NewWriter. Bytes flow caller → gzip → (bucket | sha256).
type archiveWriter struct {
	ctx     context.Context
	repo    *BlobRepository
	shard   uint64
	index   uint64
	bucketW *blob.Writer
	gz      *gzip.Writer
	hash    hash.Hash
	closed  bool
	// writeErr latches the first Write error; subsequent calls are
	// short-circuited and Close performs cleanup.
	writeErr error
}

func (w *archiveWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("snapshot: write after close")
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	n, err := w.gz.Write(p)
	if err != nil {
		w.writeErr = err
	}
	return n, err
}

// Close finalizes the archive. On any failure path the partial blob
// and any sidecar are best-effort removed so abandoned writers leave
// no observable artifact.
func (w *archiveWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	abort := func(cause error) error {
		_ = w.repo.Bucket.Delete(context.Background(), archiveKey(w.shard, w.index))
		_ = w.repo.Bucket.Delete(context.Background(), metaKey(w.shard, w.index))
		return cause
	}

	if w.writeErr != nil {
		_ = w.gz.Close()
		_ = w.bucketW.Close()
		return abort(fmt.Errorf("snapshot: write: %w", w.writeErr))
	}
	if err := w.gz.Close(); err != nil {
		_ = w.bucketW.Close()
		return abort(fmt.Errorf("snapshot: close gzip: %w", err))
	}
	if err := w.bucketW.Close(); err != nil {
		return abort(fmt.Errorf("snapshot: close blob writer: %w", err))
	}

	// Fetch the archive's persisted size from the bucket — local byte
	// counters miss any provider-side rewriting (e.g. chunked encoding).
	attrs, err := w.repo.Bucket.Attributes(w.ctx, archiveKey(w.shard, w.index))
	if err != nil {
		return abort(fmt.Errorf("snapshot: attrs after upload: %w", err))
	}

	meta := &enginev1.SnapshotMeta{
		ShardId:        w.shard,
		RaftIndex:      w.index,
		ChecksumSha256: hex.EncodeToString(w.hash.Sum(nil)),
		CreatedAtMs:    uint64(time.Now().UnixMilli()),
	}
	metaBytes, err := protojson.Marshal(meta)
	if err != nil {
		return abort(fmt.Errorf("snapshot: marshal meta: %w", err))
	}
	if err := w.repo.Bucket.WriteAll(w.ctx, metaKey(w.shard, w.index), metaBytes, nil); err != nil {
		return abort(fmt.Errorf("snapshot: write meta: %w", err))
	}
	_ = attrs // attrs.Size is observable via List; we don't need it stored locally.

	if w.repo.Retain > 0 {
		if err := w.repo.enforceRetention(w.ctx, w.shard); err != nil {
			return fmt.Errorf("snapshot: enforce retention: %w", err)
		}
	}
	return nil
}

// archiveReader is the streaming ReadCloser returned by
// BlobRepository.NewReader. Closes both the gzip wrapper and the
// underlying bucket reader, joining any errors.
type archiveReader struct {
	rd     *blob.Reader
	gz     *gzip.Reader
	closed bool
}

func (r *archiveReader) Read(p []byte) (int, error) { return r.gz.Read(p) }

func (r *archiveReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return errors.Join(r.gz.Close(), r.rd.Close())
}

// OpenBucket opens a gocloud.dev/blob bucket from a URL string. Thin
// wrapper around blob.OpenBucket; kept as a seam in case future schemes
// want pre-open normalization. Supports gocloud's native `?prefix=…`
// query parameter for sub-folder layouts (e.g. `s3://mybucket?prefix=reflw/`).
func OpenBucket(ctx context.Context, urlStr string) (*blob.Bucket, error) {
	b, err := blob.OpenBucket(ctx, urlStr)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open bucket %q: %w", urlStr, err)
	}
	return b, nil
}
