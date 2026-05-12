package tables

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// AwakeableTable is the directory mapping awakeable_id → owning invocation
// + journal entry index. Populated when a handler mints an awakeable
// (JEAwakeable), consulted by ingress when an external caller resolves
// one, and deleted after the resolution is journaled. Phase 2.
type AwakeableTable struct{ S storage.Store }

// Put records the directory row. id must already be validated via
// keys.ValidateAwakeableID; the table itself does not re-check.
func (t AwakeableTable) Put(b storage.Batch, id string, entry *enginev1.AwakeableEntry) error {
	buf, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	return b.Set(keys.AwakeableKey(id), buf)
}

// Get loads the directory row. Returns (nil, ErrNotFound) when absent.
func (t AwakeableTable) Get(id string) (*enginev1.AwakeableEntry, error) {
	val, closer, err := t.S.Get(keys.AwakeableKey(id))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	var entry enginev1.AwakeableEntry
	if err := proto.Unmarshal(val, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// Delete removes the directory row.
func (t AwakeableTable) Delete(b storage.Batch, id string) error {
	return b.Delete(keys.AwakeableKey(id))
}
