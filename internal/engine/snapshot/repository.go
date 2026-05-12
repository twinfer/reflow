// Package snapshot is the off-host snapshot archive for reflow's
// partition shards. Phase 4.2.
//
// Dragonboat already streams snapshots between live replicas
// internally; this package is purely for disaster recovery — archive a
// point-in-time Exported snapshot somewhere durable (Phase 4.2 ships a
// filesystem driver) so that a totally-cold cluster can be reseeded
// via tools.ImportSnapshot.
package snapshot

import (
	"context"
	"time"
)

// SnapshotRef identifies a single archived snapshot.
type SnapshotRef struct {
	ShardID   uint64
	Index     uint64
	SizeBytes int64
	CreatedAt time.Time
}

// Repository is the abstract archive. Phase 4.2 ships only the
// filesystem driver (FSRepository); S3 / GCS land later behind the
// same surface.
type Repository interface {
	// Put archives the contents of srcDir as the snapshot for
	// (shardID, index). Returns an error if a snapshot with the same
	// (shardID, index) already exists (no overwrite — callers must
	// Delete first).
	Put(ctx context.Context, shardID, index uint64, srcDir string) error
	// Fetch restores the snapshot for (shardID, index) into dstDir,
	// which must be an existing empty directory. The resulting layout
	// matches what dragonboat's RequestSnapshot{Exported=true} wrote.
	Fetch(ctx context.Context, shardID, index uint64, dstDir string) error
	// List returns refs sorted by index ascending.
	List(ctx context.Context, shardID uint64) ([]SnapshotRef, error)
	// Delete removes (shardID, index) from the archive. No-op when the
	// snapshot is absent.
	Delete(ctx context.Context, shardID, index uint64) error
}
