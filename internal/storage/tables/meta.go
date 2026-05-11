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
package tables

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// MetaTable holds the partition-level singleton (applied index, leader epoch).
type MetaTable struct{ S storage.Store }

// Get returns the PartitionMeta; a zero-value record if the row is absent.
func (t MetaTable) Get() (*enginev1.PartitionMeta, error) {
	val, closer, err := t.S.Get(keys.MetaKey())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &enginev1.PartitionMeta{}, nil
		}
		return nil, err
	}
	defer closer.Close()
	var m enginev1.PartitionMeta
	if err := proto.Unmarshal(val, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (t MetaTable) Put(b storage.Batch, m *enginev1.PartitionMeta) error {
	buf, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return b.Set(keys.MetaKey(), buf)
}
