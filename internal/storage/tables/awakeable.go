package tables

import (
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
	return putProto(b, keys.AwakeableKey(id), entry)
}

// Get loads the directory row. Returns (nil, ErrNotFound) when absent
// (this is a "required-id" lookup; caller is expected to have minted
// id earlier).
func (t AwakeableTable) Get(id string) (*enginev1.AwakeableEntry, error) {
	var entry enginev1.AwakeableEntry
	if err := getProto(t.S, keys.AwakeableKey(id), &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// Delete removes the directory row.
func (t AwakeableTable) Delete(b storage.Batch, id string) error {
	return b.Delete(keys.AwakeableKey(id))
}
