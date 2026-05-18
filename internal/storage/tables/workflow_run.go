package tables

import (
	"errors"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// WorkflowRunTable maps (workflow_service, workflow_key) → the InvocationId
// of the currently-active or most-recently-completed run for that key. Used
// by the apply path to enforce single-run-per-key submit dedup on
// KIND_WORKFLOW Run handlers: a hit means a prior run already claimed the
// key; later submissions for the same (service, key) are dropped and the
// caller's optimistic ingress lookup returns the existing id.
//
// The row outlives the run's Completed status until the workflow retention
// reaper sweeps both rows together.
type WorkflowRunTable struct{ S storage.Reader }

// Get returns the prior InvocationId for the (service, workflow_key)
// pair. Returns (nil, nil) when no run claimed this key.
func (t WorkflowRunTable) Get(service, workflowKey string) (*enginev1.InvocationId, error) {
	var id enginev1.InvocationId
	err := getProto(t.S, keys.WorkflowRunKey(service, workflowKey), &id)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// Put records the InvocationId for (service, workflow_key). Called from
// the apply path's onInvoke when a fresh workflow run is accepted.
func (t WorkflowRunTable) Put(b storage.Batch, service, workflowKey string, id *enginev1.InvocationId) error {
	return putProto(b, keys.WorkflowRunKey(service, workflowKey), id)
}

// Delete removes the (service, workflow_key) → id mapping. Called by the
// workflow retention reaper after a Completed run ages past its TTL.
func (t WorkflowRunTable) Delete(b storage.Batch, service, workflowKey string) error {
	return b.Delete(keys.WorkflowRunKey(service, workflowKey))
}
