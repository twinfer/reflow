package tables

import (
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ProcessChildIndexTable is the per-parent reverse index of live child
// instances, one row per started child at
// proc_child_idx/<lp:4><24-byte parent root><24-byte child root>, value = the
// child's ProcessCancel address. It lives on the parent's own shard (the child
// record lives on the child's LP, generally a different shard), so
// finishProcessInstance can find every child a terminating parent still owns
// with one bounded prefix scan and ship a ProcessCancel to each. Bounded to live
// children by delete-on-complete: a child's terminal delivers ChildCompleted
// (carrying child_root), whose apply drops the matching row. The proc_sub_idx
// analog for the parent→child plane.
type ProcessChildIndexTable struct{ S storage.Reader }

// Put records that parentRoot started the child addressed by cancel; the value is
// the ProcessCancel so a later terminal sweep ships it verbatim.
func (t ProcessChildIndexTable) Put(b storage.Batch, parentRoot, childRoot *enginev1.InvocationId, cancel *enginev1.ProcessCancel) error {
	k, err := keys.ProcessChildIndexKey(parentRoot, childRoot)
	if err != nil {
		return err
	}
	return putProto(b, k, cancel)
}

// Delete removes the index row for one (parentRoot, childRoot) — the
// delete-on-complete arm when a child terminates. Deleting an absent row is a
// no-op.
func (t ProcessChildIndexTable) Delete(b storage.Batch, parentRoot, childRoot *enginev1.InvocationId) error {
	k, err := keys.ProcessChildIndexKey(parentRoot, childRoot)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// DeleteByParent range-deletes every child-index row for one parent — the
// terminal/cancel cleanup after the cascade has shipped its ProcessCancels.
// Half-open [prefix, PrefixUpperBound(prefix)).
func (t ProcessChildIndexTable) DeleteByParent(b storage.Batch, parentRoot *enginev1.InvocationId) error {
	prefix, err := keys.ProcessChildIndexInstancePrefix(parentRoot)
	if err != nil {
		return err
	}
	return b.DeleteRange(prefix, keys.PrefixUpperBound(prefix))
}

// ScanByParent visits every live child started by parentRoot, decoding each
// ProcessCancel in key order. Bounded by the parent's live-child count. Used by
// finishProcessInstance / cancelInstanceTree. Read-only; the caller collects then
// mutates so it never writes the batch while iterating.
func (t ProcessChildIndexTable) ScanByParent(parentRoot *enginev1.InvocationId, fn func(*enginev1.ProcessCancel) error) error {
	prefix, err := keys.ProcessChildIndexInstancePrefix(parentRoot)
	if err != nil {
		return err
	}
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		c := &enginev1.ProcessCancel{}
		if err := proto.Unmarshal(iter.Value(), c); err != nil {
			return err
		}
		if err := fn(c); err != nil {
			return err
		}
	}
	return iter.Error()
}
