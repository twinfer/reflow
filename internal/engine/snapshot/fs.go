package snapshot

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// FSRepository persists archived snapshots to a local filesystem rooted
// at Root. Layout:
//
//	<Root>/
//	  <shardID>/
//	    <index>.tar.gz
//
// Phase 4.2 uses gzip rather than zstd to avoid promoting an indirect
// compress dependency. Snapshots are not on a hot path; the gzip ratio
// is fine for DR archives.
type FSRepository struct {
	Root string
	// Retain is the number of newest snapshots to keep per shard.
	// 0 means "retain all" (no garbage-collection on Put). The cleanup
	// happens during Put after the new snapshot is fully written so a
	// failure mid-tar can't leave the shard with zero archives.
	Retain int
}

var _ Repository = (*FSRepository)(nil)

const fsArchiveSuffix = ".tar.gz"

func (r *FSRepository) shardDir(shardID uint64) string {
	return filepath.Join(r.Root, strconv.FormatUint(shardID, 10))
}

func (r *FSRepository) archivePath(shardID, index uint64) string {
	return filepath.Join(r.shardDir(shardID), strconv.FormatUint(index, 10)+fsArchiveSuffix)
}

// Put tarballs srcDir into <Root>/<shardID>/<index>.tar.gz. Writes to a
// .tmp sibling and renames on success so partial writes do not leave a
// corrupt archive in place.
func (r *FSRepository) Put(ctx context.Context, shardID, index uint64, srcDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if shardID == 0 || index == 0 {
		return fmt.Errorf("snapshot: shardID and index must be non-zero")
	}
	if err := os.MkdirAll(r.shardDir(shardID), 0o755); err != nil {
		return fmt.Errorf("snapshot: mkdir shard dir: %w", err)
	}
	final := r.archivePath(shardID, index)
	if _, err := os.Stat(final); err == nil {
		return fmt.Errorf("snapshot: (%d, %d) already archived", shardID, index)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("snapshot: stat existing archive: %w", err)
	}
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("snapshot: open tmp: %w", err)
	}
	gz := gzip.NewWriter(f)
	if err := TarDir(ctx, gz, srcDir); err != nil {
		_ = gz.Close()
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: close gzip: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: close file: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	if r.Retain > 0 {
		if err := r.enforceRetention(shardID); err != nil {
			// Retention failures should not roll back the successful
			// archive — log via the returned error and let callers
			// decide whether to escalate.
			return fmt.Errorf("snapshot: enforce retention: %w", err)
		}
	}
	return nil
}

// Fetch untars the archive into dstDir.
func (r *FSRepository) Fetch(ctx context.Context, shardID, index uint64, dstDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	src := r.archivePath(shardID, index)
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("snapshot: open archive: %w", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("snapshot: open gzip: %w", err)
	}
	defer gz.Close()
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("snapshot: mkdir dst: %w", err)
	}
	return UntarDir(ctx, gz, dstDir)
}

// List enumerates archived snapshots for a shard, sorted by index ascending.
func (r *FSRepository) List(_ context.Context, shardID uint64) ([]SnapshotRef, error) {
	dir := r.shardDir(shardID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]SnapshotRef, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, fsArchiveSuffix) {
			continue
		}
		base := strings.TrimSuffix(name, fsArchiveSuffix)
		idx, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, SnapshotRef{
			ShardID:   shardID,
			Index:     idx,
			SizeBytes: info.Size(),
			CreatedAt: info.ModTime(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

// Delete removes the archive for (shardID, index). No-op when absent.
func (r *FSRepository) Delete(_ context.Context, shardID, index uint64) error {
	path := r.archivePath(shardID, index)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// enforceRetention deletes oldest archives until at most r.Retain remain.
func (r *FSRepository) enforceRetention(shardID uint64) error {
	refs, err := r.List(context.Background(), shardID)
	if err != nil {
		return err
	}
	if len(refs) <= r.Retain {
		return nil
	}
	// refs is sorted ascending; oldest indices are at the front.
	drop := refs[:len(refs)-r.Retain]
	for _, ref := range drop {
		if err := r.Delete(context.Background(), shardID, ref.Index); err != nil {
			return err
		}
	}
	return nil
}

// TarDir writes the contents of srcDir into w as a tar stream (relative
// paths, no leading "."). Exported so internal/engine's Snapshotter can
// share the implementation; ctx cancellation is honored between files.
func TarDir(ctx context.Context, w io.Writer, srcDir string) error {
	tw := tar.NewWriter(w)
	defer tw.Close()
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		h, err := tar.FileInfoHeader(info, rel)
		if err != nil {
			return err
		}
		h.Name = rel
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, f)
		_ = f.Close()
		return copyErr
	})
}

// UntarDir extracts r into dstDir. Exported so internal/engine's
// Snapshotter can share the implementation.
func UntarDir(ctx context.Context, r io.Reader, dstDir string) error {
	tr := tar.NewReader(r)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		target := filepath.Join(dstDir, h.Name)
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(h.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		default:
			// Skip symlinks etc. Dragonboat exports are regular files.
		}
	}
}
