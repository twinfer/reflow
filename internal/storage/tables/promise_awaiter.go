package tables

import (
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// PromiseAwaiterTable maps (workflow_service, workflow_key, name, entry_index)
// → the journal slot of the JEGetPromise that's blocked on it. Multiple
// concurrent Promise(name).Result() calls from distinct invocations each
// get their own row keyed by their entry_index, so resolution prefix-scans
// (svc, key, name) and stitches every awaiter in the same batch.
//
// Callers compute lp at the apply-path boundary via
// keys.LPFromPartitionKey(routing.PartitionKey(svc, workflow_key)).
type PromiseAwaiterTable struct{ S storage.Reader }

// PutForSlot writes the directory row at the awaiter's entry_index. The
// entry_index appears in both the key (so resolution can prefix-scan to
// find all slots) and the value (so the apply arm has the journal slot
// without re-parsing the key).
func (t PromiseAwaiterTable) PutForSlot(b storage.Batch, lp, tenant uint32, service, workflowKey, name string, entry *enginev1.PromiseAwaiter) error {
	return putProto(b, keys.PromiseAwaiterKey(lp, tenant, service, workflowKey, name, entry.GetEntryIndex()), entry)
}

// ScanForName invokes fn for every awaiter row at
// (service, workflow_key, name) in entry_index order. Returning a non-nil
// error from fn aborts the scan and is returned.
func (t PromiseAwaiterTable) ScanForName(lp, tenant uint32, service, workflowKey, name string, fn func(*enginev1.PromiseAwaiter) error) error {
	prefix := keys.PromiseAwaiterPrefixForName(lp, tenant, service, workflowKey, name)
	upper := keys.PrefixUpperBound(prefix)
	if upper == nil {
		return nil
	}
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		var entry enginev1.PromiseAwaiter
		if err := proto.Unmarshal(iter.Value(), &entry); err != nil {
			return err
		}
		if err := fn(&entry); err != nil {
			return err
		}
	}
	return iter.Error()
}

// DeleteForSlot removes the directory row at (svc, key, name, entry_index).
func (t PromiseAwaiterTable) DeleteForSlot(b storage.Batch, lp, tenant uint32, service, workflowKey, name string, entryIndex uint32) error {
	return b.Delete(keys.PromiseAwaiterKey(lp, tenant, service, workflowKey, name, entryIndex))
}

// DeleteAllForWorkflow range-deletes every awaiter row under
// (service, workflow_key). Used by the workflow retention reaper.
func (t PromiseAwaiterTable) DeleteAllForWorkflow(b storage.Batch, lp, tenant uint32, service, workflowKey string) error {
	prefix := keys.PromiseAwaiterPrefixForWorkflow(lp, tenant, service, workflowKey)
	upper := keys.PrefixUpperBound(prefix)
	if upper == nil {
		return nil
	}
	return b.DeleteRange(prefix, upper)
}
