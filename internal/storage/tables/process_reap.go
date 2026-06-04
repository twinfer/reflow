package tables

import (
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// ProcessReapTable stores due-times for terminal process-instance history
// cleanup. One row per retained terminal instance at
// proc_reap/<8-byte BE fire_at_ms>/<24-byte process root id>, value = the
// marshaled ReapProcessInstance (pk / service / instance_key / fire_at — the
// key's id digest can't recover the strings). The leader's process reaper (a
// generalized ReapService instance) scans the namespace in fire_at_ms order and
// proposes Command.ReapProcessInstance when the head row is due.
//
// Same shape as ReapTable but addressed by the process root id and valued with
// the reap command, so retention is opt-in — only ProcessTerminal.retention_ms
// > 0 writes a row — and shares the reap mechanism with invocation retention.
type ProcessReapTable struct{ S storage.Reader }

// Put writes the reap row for cmd at (cmd.fire_at_ms, root).
func (t ProcessReapTable) Put(b storage.Batch, root *enginev1.InvocationId, cmd *enginev1.ReapProcessInstance) error {
	k, err := keys.ProcessReapKey(cmd.GetFireAtMs(), root)
	if err != nil {
		return err
	}
	return putProto(b, k, cmd)
}

// Exists reports whether the reap row at (fireAtMs, root) is present. The
// reaper's apply arm gates record deletion on this so a duplicate / stale fire
// (whose row was already consumed atomically with the record) is a no-op and
// can never delete a re-created instance's record.
func (t ProcessReapTable) Exists(fireAtMs uint64, root *enginev1.InvocationId) (bool, error) {
	k, err := keys.ProcessReapKey(fireAtMs, root)
	if err != nil {
		return false, err
	}
	_, closer, err := t.S.Get(k)
	if isNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// Delete removes the reap row at (fireAtMs, root).
func (t ProcessReapTable) Delete(b storage.Batch, fireAtMs uint64, root *enginev1.InvocationId) error {
	k, err := keys.ProcessReapKey(fireAtMs, root)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// ScanAll iterates every reap row in fire_at_ms order, decoding the stored
// ReapProcessInstance. Used by the process reaper's Rebuild on leader gain.
func (t ProcessReapTable) ScanAll(fn func(*enginev1.ReapProcessInstance) error) error {
	prefix := keys.ProcessReapPrefix()
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
		cmd := &enginev1.ReapProcessInstance{}
		if err := proto.Unmarshal(iter.Value(), cmd); err != nil {
			return err
		}
		if err := fn(cmd); err != nil {
			return err
		}
	}
	return iter.Error()
}
