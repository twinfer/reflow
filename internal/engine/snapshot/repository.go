// Package snapshot is the off-host snapshot archive for reflw's
// partition shards.
//
// Dragonboat already streams snapshots between live replicas
// internally; this package is purely for disaster recovery — archive a
// point-in-time Exported snapshot somewhere durable so that a
// totally-cold cluster can be reseeded via tools.ImportSnapshot.
//
// The Repository contract is stream-oriented (NewWriter/NewReader)
// and mirrors gocloud.dev/blob.Bucket so the tee path into
// dragonboat's SaveSnapshot (PR2) needs no extra plumbing — callers
// can hand a Repository writer to io.MultiWriter directly. Callers
// archiving or restoring a directory should use the SaveDir /
// RestoreDir helpers, which wrap TarDir / UntarDir around the
// stream. BlobRepository is the single concrete implementation and
// covers every gocloud.dev/blob scheme: s3, gs, azblob, file, mem.
package snapshot

import (
	"context"
	"io"
	"time"
)

// SnapshotRef identifies a single archived snapshot.
type SnapshotRef struct {
	ShardID   uint64
	Index     uint64
	SizeBytes int64
	CreatedAt time.Time
}

// Repository is the abstract archive. BlobRepository is the only
// concrete implementation; it covers every gocloud.dev/blob scheme
// (s3, gs, azblob, file, mem).
type Repository interface {
	// NewWriter opens an upload stream for (shardID, raftIndex). The
	// returned WriteCloser frames bytes through gzip + sha256
	// internally; Close finalizes the archive, writes the .meta.json
	// sidecar, and enforces inline retention. The archive is durable
	// only after a successful Close — abandoned writers leave no
	// observable blob. Refuses overwrite: callers must Delete first
	// when (shardID, raftIndex) already exists.
	NewWriter(ctx context.Context, shardID, raftIndex uint64) (io.WriteCloser, error)
	// NewReader opens a download stream for (shardID, raftIndex). The
	// returned ReadCloser already has gzip stripped — callers see the
	// raw tar bytes that were originally written. Returns an error
	// satisfying gcerrors.Code(err) == gcerrors.NotFound when the
	// snapshot is absent.
	NewReader(ctx context.Context, shardID, raftIndex uint64) (io.ReadCloser, error)
	// List returns refs sorted by index ascending.
	List(ctx context.Context, shardID uint64) ([]SnapshotRef, error)
	// Delete removes (shardID, raftIndex) from the archive. No-op when
	// the snapshot is absent.
	Delete(ctx context.Context, shardID, raftIndex uint64) error
}
