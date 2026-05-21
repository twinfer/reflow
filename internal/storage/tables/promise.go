package tables

import (
	"errors"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// PromiseTable persists workflow-scoped durable promises keyed by
// (workflow_service, workflow_key, name). A row is created lazily by the
// first JECompletePromise (or InvokerEffect.PromiseCompleted) that touches
// the name; readers (JEGetPromise / JEPeekPromise) treat absent and
// Pending as "not yet completed".
//
// Callers compute lp at the apply-path boundary via
// keys.LPFromPartitionKey(routing.PartitionKey(svc, workflow_key)).
//
// Lifetime is the owning workflow run's retention window — rows survive
// the run's Completed transition and are reaped together with state +
// workflow_run rows when the workflow retention reaper sweeps the key
// (see Step 3.3).
type PromiseTable struct{ S storage.Reader }

// Get returns the PromiseValue at (service, workflow_key, name) or
// (nil, nil) when absent.
func (t PromiseTable) Get(lp uint32, service, workflowKey, name string) (*enginev1.PromiseValue, error) {
	var v enginev1.PromiseValue
	if err := getProto(t.S, keys.PromiseKey(lp, service, workflowKey, name), &v); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &v, nil
}

// Put writes the PromiseValue at (service, workflow_key, name).
func (t PromiseTable) Put(b storage.Batch, lp uint32, service, workflowKey, name string, v *enginev1.PromiseValue) error {
	return putProto(b, keys.PromiseKey(lp, service, workflowKey, name), v)
}

// Delete removes the PromiseValue row. Used by the workflow retention reaper.
func (t PromiseTable) Delete(b storage.Batch, lp uint32, service, workflowKey, name string) error {
	return b.Delete(keys.PromiseKey(lp, service, workflowKey, name))
}

// DeleteAllForWorkflow range-deletes every promise row under
// (service, workflow_key). Used by the workflow retention reaper.
func (t PromiseTable) DeleteAllForWorkflow(b storage.Batch, lp uint32, service, workflowKey string) error {
	prefix := keys.PromisePrefixForWorkflow(lp, service, workflowKey)
	upper := keys.PrefixUpperBound(prefix)
	if upper == nil {
		return nil
	}
	return b.DeleteRange(prefix, upper)
}
