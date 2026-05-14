package snapshot

import "context"

// SaveDir streams srcDir as a tar through repo.NewWriter for
// (shardID, raftIndex). The archive is durable only after a successful
// Close; failures from tar generation tear down the partial blob.
func SaveDir(ctx context.Context, repo Repository, shardID, raftIndex uint64, srcDir string) error {
	w, err := repo.NewWriter(ctx, shardID, raftIndex)
	if err != nil {
		return err
	}
	if tarErr := TarDir(ctx, w, srcDir); tarErr != nil {
		_ = w.Close()
		return tarErr
	}
	return w.Close()
}

// RestoreDir streams the archive for (shardID, raftIndex) and untars
// into dstDir, which must be an existing empty directory. Returns the
// underlying Repository error (e.g. gcerrors.NotFound) when the
// snapshot is absent.
func RestoreDir(ctx context.Context, repo Repository, shardID, raftIndex uint64, dstDir string) error {
	r, err := repo.NewReader(ctx, shardID, raftIndex)
	if err != nil {
		return err
	}
	defer r.Close()
	return UntarDir(ctx, r, dstDir)
}
