package tables

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// JournalTable stores per-invocation journal entries.
//
// Keys are journal/<inv_id>/<u32 BE command_index>, so a prefix scan yields
// entries in index order. Mirrors restate
// crates/storage-api/src/journal_table_v2.
type JournalTable struct{ S storage.Reader }

func (t JournalTable) Append(b storage.Batch, tenant uint32, id *enginev1.InvocationId, e *enginev1.JournalEntry) error {
	k, err := keys.JournalKey(tenant, id, e.GetIndex())
	if err != nil {
		return err
	}
	return putProto(b, k, e)
}

// Read returns the entry at (id, index). Returns (nil, ErrNotFound) when
// the entry does not exist — "required" convention.
func (t JournalTable) Read(tenant uint32, id *enginev1.InvocationId, index uint32) (*enginev1.JournalEntry, error) {
	k, err := keys.JournalKey(tenant, id, index)
	if err != nil {
		return nil, err
	}
	var e enginev1.JournalEntry
	if err := getProto(t.S, k, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// Scan iterates every entry for an invocation in index order. fn returning
// non-nil aborts and is returned.
func (t JournalTable) Scan(tenant uint32, id *enginev1.InvocationId, fn func(*enginev1.JournalEntry) error) error {
	prefix, err := keys.JournalPrefix(tenant, id)
	if err != nil {
		return err
	}
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		var e enginev1.JournalEntry
		if err := proto.Unmarshal(iter.Value(), &e); err != nil {
			return err
		}
		if err := fn(&e); err != nil {
			return err
		}
	}
	return iter.Error()
}

// DeletePrefix removes every entry for an invocation. Used when purging a
// completed invocation.
func (t JournalTable) DeletePrefix(b storage.Batch, tenant uint32, id *enginev1.InvocationId) error {
	prefix, err := keys.JournalPrefix(tenant, id)
	if err != nil {
		return err
	}
	upper := keys.PrefixUpperBound(prefix)
	// PrefixUpperBound returns nil only if prefix is all-0xFF, which our
	// journal prefix never is.
	if upper == nil {
		return errors.New("journal prefix has no upper bound (should not happen)")
	}
	return b.DeleteRange(prefix, upper)
}
