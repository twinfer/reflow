package tables

import (
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ProcessTimerIndexTable is the per-instance reverse index of armed process
// timers, one row per (instance, timer id) at
// proc_timer_idx/<lp:4><24B instance root><24B timer id> (empty value — the
// timer id is in the key). The timer id (processTimerID) encodes node+slot, so
// each armed timer is a distinct row. Pair-maintained with TimerTable at every
// process-timer arm / cancel / fire, so a terminating or cancelled instance can
// find and delete every timer it still has armed (via the existing TimerTable
// per-id scan) instead of leaving each to self-reclaim on fire. The
// proc_sub_idx / proc_child_idx analog for the timer plane.
type ProcessTimerIndexTable struct{ S storage.Reader }

// Put records that root has the timer timerID armed.
func (t ProcessTimerIndexTable) Put(b storage.Batch, root, timerID *enginev1.InvocationId) error {
	k, err := keys.ProcessTimerIndexKey(root, timerID)
	if err != nil {
		return err
	}
	return b.Set(k, nil)
}

// Delete removes the index row for (root, timerID) — paired with the TimerTable
// delete on cancel / fire. Deleting an absent row is a no-op.
func (t ProcessTimerIndexTable) Delete(b storage.Batch, root, timerID *enginev1.InvocationId) error {
	k, err := keys.ProcessTimerIndexKey(root, timerID)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// DeleteByInstance range-deletes every timer-index row for one instance — the
// terminal / cancel cleanup after teardownInstanceTimers has dropped the timers.
func (t ProcessTimerIndexTable) DeleteByInstance(b storage.Batch, root *enginev1.InvocationId) error {
	prefix, err := keys.ProcessTimerIndexInstancePrefix(root)
	if err != nil {
		return err
	}
	return b.DeleteRange(prefix, keys.PrefixUpperBound(prefix))
}

// ScanByInstance visits every armed timer id for one instance, decoding the id
// from the trailing bytes of each key. Bounded by the instance's armed-timer
// count. Read-only; the caller collects then mutates so it never writes the batch
// while iterating it.
func (t ProcessTimerIndexTable) ScanByInstance(root *enginev1.InvocationId, fn func(timerID *enginev1.InvocationId) error) error {
	prefix, err := keys.ProcessTimerIndexInstancePrefix(root)
	if err != nil {
		return err
	}
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		tid, err := keys.DecodeInvocationID(iter.Key()[len(prefix):])
		if err != nil {
			return err
		}
		if err := fn(tid); err != nil {
			return err
		}
	}
	return iter.Error()
}
