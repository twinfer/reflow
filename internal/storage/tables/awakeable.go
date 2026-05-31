package tables

import (
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// AwakeableTable is the directory mapping awakeable_id → owning invocation
// + journal entry index. Populated when a handler mints an awakeable
// (JEAwakeable), consulted by ingress when an external caller resolves
// one, and deleted after the resolution is journaled.
//
// The owning invocation's partition_key is embedded in the awakeable id
// body — the table derives the LP via keys.AwakeableOwnerPartitionKey so
// callers don't have to thread it. A malformed id surfaces as an error from
// each method.
type AwakeableTable struct{ S storage.Reader }

func awakeableLoc(id string) (lp uint32, err error) {
	pk, err := keys.AwakeableOwnerPartitionKey(id)
	if err != nil {
		return 0, err
	}
	return keys.LPFromPartitionKey(pk), nil
}

// Put records the directory row. id must already be validated via
// keys.ValidateAwakeableID; the table itself does not re-check beyond what
// AwakeableOwnerPartitionKey enforces.
func (t AwakeableTable) Put(b storage.Batch, id string, entry *enginev1.AwakeableEntry) error {
	lp, err := awakeableLoc(id)
	if err != nil {
		return err
	}
	return putProto(b, keys.AwakeableKey(lp, id), entry)
}

// Get loads the directory row. Returns (nil, ErrNotFound) when absent
// (this is a "required-id" lookup; caller is expected to have minted
// id earlier).
func (t AwakeableTable) Get(id string) (*enginev1.AwakeableEntry, error) {
	lp, err := awakeableLoc(id)
	if err != nil {
		return nil, err
	}
	var entry enginev1.AwakeableEntry
	if err := getProto(t.S, keys.AwakeableKey(lp, id), &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// Delete removes the directory row.
func (t AwakeableTable) Delete(b storage.Batch, id string) error {
	lp, err := awakeableLoc(id)
	if err != nil {
		return err
	}
	return b.Delete(keys.AwakeableKey(lp, id))
}
