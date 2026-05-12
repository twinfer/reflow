package storage

import (
	"errors"
	"io"

	"github.com/cockroachdb/pebble/v2"
)

// PebbleStore is a Store backed by cockroachdb/pebble. Open a fresh DB with
// OpenPebble; close it with Close.
type PebbleStore struct {
	db *pebble.DB
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
	return &PebbleStore{db: db}, nil
}

// DB returns the underlying *pebble.DB. Use only when an operation is not
// expressible through the Store interface (e.g. building a custom snapshot).
func (s *PebbleStore) DB() *pebble.DB { return s.db }

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
	return &pebbleBatch{batch: s.db.NewBatch()}
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

type pebbleBatch struct {
	batch *pebble.Batch
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
