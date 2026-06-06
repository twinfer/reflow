package tables

import (
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ProcessHistoryTable is the per-instance, append-only activity timeline, keyed
// proc_hist/<lp:4><24-byte root id><8-byte seq>. The cursor (hist_seq) lives on
// ProcessInstanceRecord; this table stores one ProcessHistoryEvent per turn-level
// effect (start, inbound event, task dispatched/completed, timer armed/fired,
// child started/completed, subscribe/unsubscribe, terminal). Unlike
// ProcessInboxTable rows it is NOT deleted per turn — it is bounded live by the
// keep-last-N cap (limits.DefaultMaxProcessHistoryEvents) and range-deleted on
// instance reap. Rides the LP-transfer scan via keys.ProcessHistoryLPPrefix.
type ProcessHistoryTable struct{ S storage.Reader }

// Append writes one timeline event at seq into the batch.
func (t ProcessHistoryTable) Append(b storage.Batch, root *enginev1.InvocationId, seq uint64, ev *enginev1.ProcessHistoryEvent) error {
	k, err := keys.ProcessHistoryKey(root, seq)
	if err != nil {
		return err
	}
	return putProto(b, k, ev)
}

// DeleteAt removes the single row at seq — the keep-last-N eviction (delete the
// row that just fell out of the window). Deleting an absent row is a no-op.
func (t ProcessHistoryTable) DeleteAt(b storage.Batch, root *enginev1.InvocationId, seq uint64) error {
	k, err := keys.ProcessHistoryKey(root, seq)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// DeleteInstance range-deletes every timeline row for one instance — the terminal
// cleanup (the immediate-delete branch and the windowed reap), mirroring the
// LP-transfer source range-delete. Half-open [prefix, PrefixUpperBound(prefix)).
func (t ProcessHistoryTable) DeleteInstance(b storage.Batch, root *enginev1.InvocationId) error {
	prefix, err := keys.ProcessHistoryInstancePrefix(root)
	if err != nil {
		return err
	}
	return b.DeleteRange(prefix, keys.PrefixUpperBound(prefix))
}

// ScanByInstance visits an instance's timeline in append (seq) order, resuming
// strictly past the after seq (after==0 starts at the beginning). Backs the
// Phase-1 history query RPC and the apply-path tests. Read-only.
func (t ProcessHistoryTable) ScanByInstance(root *enginev1.InvocationId, after uint64, fn func(*enginev1.ProcessHistoryEvent) error) error {
	prefix, err := keys.ProcessHistoryInstancePrefix(root)
	if err != nil {
		return err
	}
	var afterKey []byte
	if after > 0 {
		afterKey, err = keys.ProcessHistoryKey(root, after)
		if err != nil {
			return err
		}
	}
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := scanStart(iter, afterKey); ok; ok = iter.Next() {
		ev := &enginev1.ProcessHistoryEvent{}
		if err := proto.Unmarshal(iter.Value(), ev); err != nil {
			return err
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return iter.Error()
}
