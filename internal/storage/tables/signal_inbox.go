package tables

import (
	"errors"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// SignalInboxTable buffers signals delivered to an invocation that has
// not yet called ctx.WaitSignal(name) for them. Keyed by (inv_id, name)
// at signal_inbox/<inv_id>/<name>. Last-writer-wins per name: a second
// signal with the same name overwrites the first while it sits
// unconsumed. Cleared on consumption and on invocation purge.
type SignalInboxTable struct{ S storage.Reader }

// Put writes a buffered signal entry. The name must not contain "/".
func (t SignalInboxTable) Put(b storage.Batch, id *enginev1.InvocationId, name string, entry *enginev1.SignalInboxEntry) error {
	k, err := keys.SignalInboxKey(id, name)
	if err != nil {
		return err
	}
	return putProto(b, k, entry)
}

// Get returns the buffered signal for (id, name), or (nil, nil) when
// absent. Distinguishing "absent" from "error" lets the apply arm
// branch on whether to stitch synchronously vs write an awaiter row.
func (t SignalInboxTable) Get(id *enginev1.InvocationId, name string) (*enginev1.SignalInboxEntry, error) {
	k, err := keys.SignalInboxKey(id, name)
	if err != nil {
		return nil, err
	}
	var entry enginev1.SignalInboxEntry
	if err := getProto(t.S, k, &entry); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &entry, nil
}

// Delete removes the buffered entry.
func (t SignalInboxTable) Delete(b storage.Batch, id *enginev1.InvocationId, name string) error {
	k, err := keys.SignalInboxKey(id, name)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// DeleteAllForInvocation range-deletes every buffered signal under
// (inv_id). Called by onPurge so a completed invocation's inbox doesn't
// leak. Cost is independent of the number of rows.
func (t SignalInboxTable) DeleteAllForInvocation(b storage.Batch, id *enginev1.InvocationId) error {
	prefix, err := keys.SignalInboxPrefixForInvocation(id)
	if err != nil {
		return err
	}
	upper := keys.PrefixUpperBound(prefix)
	if upper == nil {
		return nil
	}
	return b.DeleteRange(prefix, upper)
}
