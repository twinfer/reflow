package tables

import (
	"errors"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// PromiseAwaiterTable maps (workflow_service, workflow_key, name) → the
// journal slot of the JEGetPromise that's blocked on it. Written when a
// handler calls Promise(name).Result() and the promise is still pending;
// consulted on JECompletePromise / InvokerEffect.PromiseCompleted to
// stitch the result into the awaiting invocation's journal at the
// recorded slot.
//
// At most one row per (svc, key, name) — a second Promise(name).Result()
// while one is pending overwrites this row. Same documented MVP
// limitation as SignalAwaiter.
type PromiseAwaiterTable struct{ S storage.Reader }

// Put writes the directory row.
func (t PromiseAwaiterTable) Put(b storage.Batch, service, workflowKey, name string, entry *enginev1.PromiseAwaiter) error {
	return putProto(b, keys.PromiseAwaiterKey(service, workflowKey, name), entry)
}

// Get returns the awaiter for (service, workflow_key, name) or
// (nil, nil) when absent.
func (t PromiseAwaiterTable) Get(service, workflowKey, name string) (*enginev1.PromiseAwaiter, error) {
	var entry enginev1.PromiseAwaiter
	if err := getProto(t.S, keys.PromiseAwaiterKey(service, workflowKey, name), &entry); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &entry, nil
}

// Delete removes the directory row.
func (t PromiseAwaiterTable) Delete(b storage.Batch, service, workflowKey, name string) error {
	return b.Delete(keys.PromiseAwaiterKey(service, workflowKey, name))
}

// DeleteAllForWorkflow range-deletes every awaiter row under
// (service, workflow_key). Used by the workflow retention reaper.
func (t PromiseAwaiterTable) DeleteAllForWorkflow(b storage.Batch, service, workflowKey string) error {
	prefix := keys.PromiseAwaiterPrefixForWorkflow(service, workflowKey)
	upper := keys.PrefixUpperBound(prefix)
	if upper == nil {
		return nil
	}
	return b.DeleteRange(prefix, upper)
}
