// Package tables defines typed accessors over a partition's storage.Store.
// Each table is a thin struct that owns its key encoding and proto marshaling.
//
// Writes take a storage.Batch so different tables compose atomically inside a
// single Raft apply. Reads happen directly against the Store.
//
// Mirrors the fat Transaction trait in restate
// crates/storage-api/src/lib.rs:112-137, except we model composability through
// shared Batch rather than a single trait — there is no trait-object overhead
// in Go and explicit batches make the apply path easier to read.
//
// "Row absent" conventions across the package:
//
//   - "Default" tables — InvocationTable, MetaTable — return a
//     semantic zero-value (Free status / empty PartitionMeta) and nil err.
//     Callers can use the result unconditionally.
//   - "Required" tables — AwakeableTable, JournalTable, OutboxTable,
//     DedupTable — return (nil, storage.ErrNotFound). Callers know the
//     id should exist and treat absent as an error.
//   - "Optional" tables — IdempotencyTable, KeyLeaseTable, StateTable —
//     return (nil, nil) for absent. Callers nil-check the result.
//
// Marshaling boilerplate lives in protoio.go (getProto / putProto).
package tables

import (
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// MetaTable holds the partition-level singleton (applied index, leader epoch).
type MetaTable struct{ S storage.Reader }

// Get returns the PartitionMeta; an empty record when the row is absent
// ("default" convention — see package doc).
func (t MetaTable) Get() (*enginev1.PartitionMeta, error) {
	var m enginev1.PartitionMeta
	if err := getProto(t.S, keys.MetaKey(), &m); err != nil {
		if isNotFound(err) {
			return &m, nil
		}
		return nil, err
	}
	return &m, nil
}

func (t MetaTable) Put(b storage.Batch, m *enginev1.PartitionMeta) error {
	return putProto(b, keys.MetaKey(), m)
}
