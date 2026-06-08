package tables

import (
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ProcessHeldTable buffers a plan item's completion while it is Suspended (CMMN
// §7.6.1: the host must defer events until ManualResume). One row per
// (instance, node) at proc_held/<lp:4><24B instance root><node>, value = the
// verbatim ProcessInboxEntry the adapter declined to advance. Written when a
// turn would fire a completion against a suspended node (onProcessAdvanced sees
// ProcessAdvanced.hold_event_node) and replayed into the inbox when the engine
// emits ResumeTask (actuateProcessInstructions sees release_held_node). A plan
// item has at most one Run*Task in flight, so a node buffers at most one
// completion. Rides the LP-transfer scan via keys.ProcessHeldLPPrefix.
type ProcessHeldTable struct{ S storage.Reader }

// Put buffers entry for (root, node), overwriting any prior buffer for that node
// (a node holds at most one completion, so this is a no-op replace in practice).
func (t ProcessHeldTable) Put(b storage.Batch, root *enginev1.InvocationId, node string, entry *enginev1.ProcessInboxEntry) error {
	k, err := keys.ProcessHeldKey(root, node)
	if err != nil {
		return err
	}
	return putProto(b, k, entry)
}

// Get loads the buffered entry for (root, node). ok is false (err nil) when no
// completion is buffered (e.g. the item resumed before its task completed).
func (t ProcessHeldTable) Get(root *enginev1.InvocationId, node string) (*enginev1.ProcessInboxEntry, bool, error) {
	k, err := keys.ProcessHeldKey(root, node)
	if err != nil {
		return nil, false, err
	}
	var e enginev1.ProcessInboxEntry
	if err := getProto(t.S, k, &e); err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &e, true, nil
}

// Delete removes the buffer for (root, node) — paired with the replay on resume.
// Deleting an absent row is a no-op.
func (t ProcessHeldTable) Delete(b storage.Batch, root *enginev1.InvocationId, node string) error {
	k, err := keys.ProcessHeldKey(root, node)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// DeleteByInstance range-deletes every buffered completion for one instance —
// the terminal cleanup when a suspended item (or its case) is exited/terminated
// with a completion still buffered.
func (t ProcessHeldTable) DeleteByInstance(b storage.Batch, root *enginev1.InvocationId) error {
	prefix, err := keys.ProcessHeldInstancePrefix(root)
	if err != nil {
		return err
	}
	return b.DeleteRange(prefix, keys.PrefixUpperBound(prefix))
}
