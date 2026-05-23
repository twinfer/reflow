package storage

import (
	"context"
	"errors"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// PebbleStore is a Store backed by cockroachdb/pebble. Open a fresh DB with
// OpenPebble; close it with Close.
type PebbleStore struct {
	db  *pebble.DB
	dir string
}

// OpenPebble opens a Pebble DB at dir with the given options. If opts is nil
// a default Options is used (production OS filesystem). Pass
// &pebble.Options{FS: vfs.NewMem()} for in-memory tests.
func OpenPebble(dir string, opts *pebble.Options) (*PebbleStore, error) {
	if opts == nil {
		opts = &pebble.Options{}
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleStore{db: db, dir: dir}, nil
}

// DB returns the underlying *pebble.DB. Use only when an operation is not
// expressible through the Store interface (e.g. building a custom snapshot).
func (s *PebbleStore) DB() *pebble.DB { return s.db }

// DataDir returns the on-disk directory passed to OpenPebble. Sibling
// directories rooted at dataDir.<suffix> are the agreed convention for
// non-Pebble state (snapshot staging, LP transfer SSTs) — putting
// state inside dataDir would violate Pebble's sole-ownership of its
// directory.
func (s *PebbleStore) DataDir() string { return s.dir }

func (s *PebbleStore) Get(key []byte) ([]byte, io.Closer, error) {
	v, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	return v, closer, nil
}

func (s *PebbleStore) NewBatch() Batch {
	// Indexed batch so reads against the batch see in-batch writes plus
	// the committed DB. Required by partition.go's apply loop: a single
	// dragonboat Update call may carry multiple entries that read each
	// other's writes (e.g. Ingress → JEInput → Complete for one
	// invocation under partition-heal catch-up). A non-indexed batch
	// would surface those reads as "not found" and corrupt FSM
	// transitions.
	return &pebbleBatch{batch: s.db.NewIndexedBatch()}
}

func (s *PebbleStore) NewIter(lower, upper []byte) (Iter, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	return &pebbleIter{iter: iter}, nil
}

// Checkpoint delegates to pebble.DB.Checkpoint (pebble v1.1.5
// checkpoint.go:135). The destDir must not exist; pebble returns
// *os.PathError{ErrExist} otherwise.
func (s *PebbleStore) Checkpoint(destDir string) error {
	return s.db.Checkpoint(destDir)
}

func (s *PebbleStore) Flush() error { return s.db.Flush() }
func (s *PebbleStore) Close() error { return s.db.Close() }

// Metrics returns the live pebble.Metrics snapshot. Used by the load
// harness to sample write amplification, L0 file count, compaction
// stats, and block-cache hit rate during sustained workload.
func (s *PebbleStore) Metrics() *pebble.Metrics { return s.db.Metrics() }

// SSTWriter is the narrow writer surface callers use to build SSTs.
// It is a strict subset of *sstable.Writer — Set adds one key/value
// (keys MUST be strictly increasing), Close finalizes the file and
// closes the underlying handle.
type SSTWriter interface {
	Set(key, value []byte) error
	Close() error
}

// IngestSSTs atomically adds the SSTs at the given paths into the
// store's LSM. Wraps pebble.DB.Ingest. The dest LP keyspace is empty
// before the LPOwnersTable flip so plain Ingest (no excise) is
// sufficient. Paths must point to SSTs whose key ranges are pairwise
// non-overlapping; the LP transfer protocol builds one SST per
// LP-prefixed namespace (disjoint by top-level prefix) so this holds
// by construction.
//
// Ingest hardlinks (or moves) the files into the DB's owned space;
// callers must not delete the source paths after a successful return.
func (s *PebbleStore) IngestSSTs(ctx context.Context, paths []string) error {
	return s.db.Ingest(ctx, paths)
}

// OpenSSTFile creates a new SST writer at path, configured to match
// this store's on-disk format (Comparer + TableFormat). The caller
// writes strictly-increasing keys via SSTWriter.Set and calls
// SSTWriter.Close to finalize; Close also fsyncs and closes the
// underlying file. The destination store must be opened by the same
// reflow binary (so the format matches) — Ingest fails otherwise.
//
// Lives on PebbleStore so all sstable/objstorage type references stay
// confined to this file; callers see only the narrow SSTWriter
// interface.
func (s *PebbleStore) OpenSSTFile(path string) (SSTWriter, error) {
	f, err := vfs.Default.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	opts := sstable.WriterOptions{
		Comparer:    pebble.DefaultComparer,
		TableFormat: s.db.FormatMajorVersion().MaxTableFormat(),
	}
	return &sstFileWriter{w: sstable.NewWriter(objstorageprovider.NewFileWritable(f), opts)}, nil
}

// sstFileWriter narrows *sstable.Writer to the two methods reflow
// uses. Keeping the wrapper at this layer means lp_transfer_sst.go
// does not import pebble/sstable.
type sstFileWriter struct {
	w *sstable.Writer
}

func (w *sstFileWriter) Set(key, value []byte) error { return w.w.Set(key, value) }
func (w *sstFileWriter) Close() error                { return w.w.Close() }

type pebbleBatch struct {
	batch *pebble.Batch
}

func (b *pebbleBatch) Get(key []byte) ([]byte, io.Closer, error) {
	v, closer, err := b.batch.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	return v, closer, nil
}

func (b *pebbleBatch) NewIter(lower, upper []byte) (Iter, error) {
	iter, err := b.batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	return &pebbleIter{iter: iter}, nil
}

func (b *pebbleBatch) Set(key, value []byte) error {
	return b.batch.Set(key, value, nil)
}

func (b *pebbleBatch) Delete(key []byte) error {
	return b.batch.Delete(key, nil)
}

func (b *pebbleBatch) DeleteRange(start, end []byte) error {
	return b.batch.DeleteRange(start, end, nil)
}

func (b *pebbleBatch) Commit(sync bool) error {
	if sync {
		return b.batch.Commit(pebble.Sync)
	}
	return b.batch.Commit(pebble.NoSync)
}

func (b *pebbleBatch) Close() error { return b.batch.Close() }

type pebbleIter struct {
	iter *pebble.Iterator
}

func (it *pebbleIter) First() bool            { return it.iter.First() }
func (it *pebbleIter) SeekGE(key []byte) bool { return it.iter.SeekGE(key) }
func (it *pebbleIter) Next() bool             { return it.iter.Next() }
func (it *pebbleIter) Valid() bool            { return it.iter.Valid() }
func (it *pebbleIter) Key() []byte            { return it.iter.Key() }
func (it *pebbleIter) Value() []byte          { return it.iter.Value() }
func (it *pebbleIter) Error() error           { return it.iter.Error() }
func (it *pebbleIter) Close() error           { return it.iter.Close() }
