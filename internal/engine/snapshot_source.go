package engine

import (
	"context"

	"github.com/twinfer/reflow/internal/engine/snapshot"
)

// HostSnapshotSource adapts *Host to the snapshot.Source interface.
// Used by the snapshot producer + Admin.CreateSnapshot RPC.
type HostSnapshotSource struct{ Host *Host }

var _ snapshot.Source = (*HostSnapshotSource)(nil)

// SnapshotToDir delegates to Host.SnapshotPartitionToDir.
func (s *HostSnapshotSource) SnapshotToDir(ctx context.Context, shardID uint64, dir string) (uint64, error) {
	return s.Host.SnapshotPartitionToDir(ctx, shardID, dir)
}
