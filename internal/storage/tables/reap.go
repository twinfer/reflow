package tables

import (
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ReapTable stores due-times for invocation retention cleanup. One row
// per Completed invocation at reap/<8-byte BE fire_at_ms>/<24-byte
// inv_id>, value empty. The leader's ReapService scans the namespace
// ordered by fire_at_ms and proposes Command.ReapInvocation when the
// head row's fire_at_ms <= nowMs.
//
// Same shape as TimerTable but addressed by invocation id. Every
// Completed invocation — plain service, virtual object, or workflow run
// — schedules exactly one row; the apply arm decides whether to also
// sweep entity-scoped (state/promise/workflow_run) rows based on whether
// the invocation is the current workflow run for its key.
type ReapTable struct{ S storage.Reader }

// Put writes a reap row at (fireAtMs, id).
func (t ReapTable) Put(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId) error {
	k, err := keys.ReapKey(fireAtMs, id)
	if err != nil {
		return err
	}
	return b.Set(k, nil)
}

// Delete removes the reap row at (fireAtMs, id).
func (t ReapTable) Delete(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId) error {
	k, err := keys.ReapKey(fireAtMs, id)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// ReapRow is the decoded (fireAtMs, invocation_id) of a single scanned
// reap directory row.
type ReapRow struct {
	FireAtMs uint64
	ID       *enginev1.InvocationId
}

// ScanAll iterates every reap row in fire_at_ms order. fn returning
// non-nil aborts and is returned. Used by ReapService.Rebuild on leader
// gain; the live path uses Push to wake on new rows.
func (t ReapTable) ScanAll(fn func(ReapRow) error) error {
	prefix := keys.ReapPrefix()
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
		fireAt, id, derr := keys.DecodeReapKey(iter.Key())
		if derr != nil {
			return derr
		}
		if err := fn(ReapRow{FireAtMs: fireAt, ID: id}); err != nil {
			return err
		}
	}
	return iter.Error()
}
