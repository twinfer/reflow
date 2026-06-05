package tables

import (
	"errors"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// SignalAwaiterTable maps (inv_id, signal_name) → the journal slot of
// the JEAwaitSignal that's blocked waiting for it. Written when a
// handler calls ctx.WaitSignal(name) and no buffered signal is present;
// consulted on InvokerEffect.SignalDelivered to stitch the result into
// the awaiting invocation's journal at the recorded slot.
//
// At most one row per (inv_id, name) — the SDK enforces single-active-
// awaiter-per-name. A second WaitSignal with the same name while one is
// pending would overwrite this row; that behaviour is documented as a
// known MVP limitation.
type SignalAwaiterTable struct{ S storage.Reader }

// Put writes the directory row. The name must not contain "/".
func (t SignalAwaiterTable) Put(b storage.Batch, id *enginev1.InvocationId, name string, entry *enginev1.SignalAwaiter) error {
	k, err := keys.SignalAwaiterKey(id, name)
	if err != nil {
		return err
	}
	return putProto(b, k, entry)
}

// Get returns the awaiter for (id, name), or (nil, nil) when absent.
func (t SignalAwaiterTable) Get(id *enginev1.InvocationId, name string) (*enginev1.SignalAwaiter, error) {
	k, err := keys.SignalAwaiterKey(id, name)
	if err != nil {
		return nil, err
	}
	var entry enginev1.SignalAwaiter
	if err := getProto(t.S, k, &entry); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &entry, nil
}

// Delete removes the directory row.
func (t SignalAwaiterTable) Delete(b storage.Batch, id *enginev1.InvocationId, name string) error {
	k, err := keys.SignalAwaiterKey(id, name)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// DeleteAllForInvocation range-deletes every awaiter row under
// (inv_id). Called by onPurge.
func (t SignalAwaiterTable) DeleteAllForInvocation(b storage.Batch, id *enginev1.InvocationId) error {
	prefix, err := keys.SignalAwaiterPrefixForInvocation(id)
	if err != nil {
		return err
	}
	upper := keys.PrefixUpperBound(prefix)
	if upper == nil {
		return nil
	}
	return b.DeleteRange(prefix, upper)
}
