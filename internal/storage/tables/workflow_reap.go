package tables

import (
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
)

// WorkflowReapTable stores due-times for workflow retention cleanup.
// One row per Completed workflow run at
// workflow_reap/<8-byte BE fire_at_ms>/<service>/<workflow_key>, value
// empty. The leader's WorkflowReapService scans the namespace ordered
// by fire_at_ms and proposes Command.ReapWorkflow when the head row's
// fire_at_ms <= nowMs.
//
// Same shape as TimerTable but with (service, workflow_key) addressing
// instead of (inv_id, sleep_index). Single row per (service,
// workflow_key) — replays that try to insert at a different fire_at_ms
// for the same key are tolerated; the apply path may write only one
// row per Completed transition.
type WorkflowReapTable struct{ S storage.Reader }

// Put writes a reap row at (fireAtMs, service, workflowKey).
func (t WorkflowReapTable) Put(b storage.Batch, fireAtMs uint64, service, workflowKey string) error {
	return b.Set(keys.WorkflowReapKey(fireAtMs, service, workflowKey), nil)
}

// Delete removes the reap row at (fireAtMs, service, workflowKey).
func (t WorkflowReapTable) Delete(b storage.Batch, fireAtMs uint64, service, workflowKey string) error {
	return b.Delete(keys.WorkflowReapKey(fireAtMs, service, workflowKey))
}

// ReapRow is the decoded (fireAtMs, service, workflow_key) of a single
// scanned reap directory row.
type ReapRow struct {
	FireAtMs    uint64
	Service     string
	WorkflowKey string
}

// ScanAll iterates every reap row in fire_at_ms order. fn returning
// non-nil aborts and is returned. Used by WorkflowReapService.Rebuild
// on leader gain; the live path uses Push to wake on new rows.
func (t WorkflowReapTable) ScanAll(fn func(ReapRow) error) error {
	prefix := keys.WorkflowReapPrefix()
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
		fireAt, svc, wfKey, derr := keys.DecodeWorkflowReapKey(iter.Key())
		if derr != nil {
			return derr
		}
		if err := fn(ReapRow{FireAtMs: fireAt, Service: svc, WorkflowKey: wfKey}); err != nil {
			return err
		}
	}
	return iter.Error()
}
