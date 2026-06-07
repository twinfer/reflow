package tables

import (
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ProcessInvokeIndexTable is the per-instance reverse index of in-flight
// service-task invocations, one row per (instance, invocation id) at
// proc_invoke_idx/<lp:4><24B instance root><24B invocation id>, value = the
// dispatching node id. Written when a turn dispatches a task
// (actuateProcessInstructions' adv.Invoke loop → mintProcessTaskID) and
// deleted when the task feeds back (ProcessTaskCompleted carries the id), so a
// terminating or cancelled instance can find and force-cancel every task still
// in flight (by-id cancel routed to the callee's shard) instead of leaving each
// to complete and self-drop. The node id in the value lets a mid-case CancelTask
// cancel only the task(s) under the exited node. The proc_child_idx analog for
// the service-task plane.
type ProcessInvokeIndexTable struct{ S storage.Reader }

// Put records that root has the task invocation invID (dispatched at nodeID) in
// flight. nodeID is the value so ScanByInstance can match a CancelTask to its
// node without a second index.
func (t ProcessInvokeIndexTable) Put(b storage.Batch, root, invID *enginev1.InvocationId, nodeID string) error {
	k, err := keys.ProcessInvokeIndexKey(root, invID)
	if err != nil {
		return err
	}
	return b.Set(k, []byte(nodeID))
}

// Delete removes the index row for (root, invID) — paired with the
// delete-on-feedback when the task's ProcessTaskCompleted lands. Deleting an
// absent row is a no-op.
func (t ProcessInvokeIndexTable) Delete(b storage.Batch, root, invID *enginev1.InvocationId) error {
	k, err := keys.ProcessInvokeIndexKey(root, invID)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// DeleteByInstance range-deletes every task-index row for one instance — the
// terminal / cancel cleanup after teardownInstanceInvocations has dispatched
// the cancels.
func (t ProcessInvokeIndexTable) DeleteByInstance(b storage.Batch, root *enginev1.InvocationId) error {
	prefix, err := keys.ProcessInvokeIndexInstancePrefix(root)
	if err != nil {
		return err
	}
	return b.DeleteRange(prefix, keys.PrefixUpperBound(prefix))
}

// ScanByInstance visits every in-flight task invocation for one instance,
// decoding the id from the trailing bytes of each key and the dispatching node
// id from the value. Bounded by the instance's in-flight-task count. Read-only;
// the caller collects then mutates so it never writes the batch while iterating.
func (t ProcessInvokeIndexTable) ScanByInstance(root *enginev1.InvocationId, fn func(invID *enginev1.InvocationId, nodeID string) error) error {
	prefix, err := keys.ProcessInvokeIndexInstancePrefix(root)
	if err != nil {
		return err
	}
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		invID, err := keys.DecodeInvocationID(iter.Key()[len(prefix):])
		if err != nil {
			return err
		}
		if err := fn(invID, string(iter.Value())); err != nil {
			return err
		}
	}
	return iter.Error()
}
