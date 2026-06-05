// Package storage defines the byte-level key/value abstraction backing a
// reflw partition. Each partition owns its own Store; isolation is at the DB
// level, so keys do NOT carry a partition_id prefix (see internal/storage/keys).
package storage

import (
	"errors"
	"io"
)

// ErrNotFound is returned by Store.Get when the key is absent.
var ErrNotFound = errors.New("storage: key not found")

// Reader is the read-only surface every storage layer exposes. Both Store
// and Batch satisfy Reader so tables can be bound to either: production
// code binds tables to a Store for general reads, and partition.go's
// apply loop binds tables to the in-flight Batch so within-batch writes
// are visible to subsequent reads in the same Update call.
//
// Without this within-batch read coherence, a multi-entry apply batch
// where entry-K writes a row and entry-(K+M) reads it would observe
// `not found` for entry-(K+M)'s read — the bug that stranded ~3% of
// invocations in Scheduled/Invoked under partition heal, where catch-up
// produced large multi-entry apply batches.
type Reader interface {
	// Get returns the value for key. The returned slice is only valid until
	// closer.Close() is called; the caller MUST close it. Returns ErrNotFound
	// if the key is absent.
	Get(key []byte) (value []byte, closer io.Closer, err error)

	// NewIter returns an Iter over [lower, upper). A nil bound means
	// unbounded on that side. The caller MUST call Close on the iterator.
	// After construction the iterator is unpositioned — call First or SeekGE
	// before reading.
	NewIter(lower, upper []byte) (Iter, error)
}

// Store is the partition-local K/V interface. Implementations exist for Pebble
// (production) and in-memory map (tests).
type Store interface {
	Reader

	// NewBatch returns an empty Batch. The Batch is not safe for concurrent use.
	// The returned Batch is a Reader: reads against it see the Store's
	// committed state plus any in-batch writes (pebble.IndexedBatch semantics).
	NewBatch() Batch

	// Checkpoint writes a consistent snapshot of the Store to destDir.
	// destDir MUST NOT exist (Pebble v1.1.5 contract; checkpoint.go:145-154).
	Checkpoint(destDir string) error

	// Flush forces in-memory state to durable storage (no-op for in-memory
	// implementations).
	Flush() error

	// Close releases all resources. After Close, all further operations
	// return an error.
	Close() error
}

// Batch accumulates writes to be applied atomically AND exposes Reader so
// callers can observe their own in-batch writes within a single Update.
// See Reader's doc for why within-batch read coherence matters.
type Batch interface {
	Reader

	// Set, Delete and DeleteRange buffer the operation; nothing is durable
	// until Commit returns.
	Set(key, value []byte) error
	Delete(key []byte) error
	// DeleteRange deletes every point key in [start, end). Half-open;
	// matches pebble v1.1.5 batch.go:885 semantics.
	DeleteRange(start, end []byte) error

	// Commit atomically applies the batch. If sync is true, the durability
	// promise is "the write has been fsync'd"; otherwise the write may be
	// lost on crash. Implementations should map sync to pebble.Sync /
	// pebble.NoSync.
	Commit(sync bool) error

	// Close releases the batch. After Close, calls to Set/Delete/DeleteRange/
	// Commit return an error. Idempotent.
	Close() error
}

// Iter is a forward-only iterator over a key range.
type Iter interface {
	// First positions the iterator at the first key in the range. Returns
	// false if the range is empty.
	First() bool
	// SeekGE positions the iterator at the first key >= the given key
	// (within the configured bounds). Returns false if no such key exists.
	SeekGE(key []byte) bool
	// Next advances. Returns false past the end of the range.
	Next() bool
	// Valid reports whether the iterator is currently positioned on a key.
	Valid() bool
	// Key and Value return the current entry. The returned slices are only
	// valid until the next Next/SeekGE/First/Close call — copy if needed.
	Key() []byte
	Value() []byte
	// Error returns any deferred error encountered during iteration.
	Error() error
	// Close releases the iterator. Idempotent.
	Close() error
}
