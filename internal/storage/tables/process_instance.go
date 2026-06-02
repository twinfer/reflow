package tables

import (
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// ProcessInstanceTable stores one iflow process/case instance per
// (service, instance_key) at proc/<lp:4><service>/<instance_key>. The value is
// a ProcessInstanceRecord whose state_blob is the opaque iflow
// ExecutionState/CaseState — reflow never parses it; only the partition
// leader's procSession (which links iflow) decodes it to run Advance.
//
// Single-writer per instance is the key-lease FSM in
// internal/engine/object_fsm.go: each inbound ProcessEvent is one serialized
// turn. The <lp:4> prefix means an instance rides the LP-transfer scan
// (keys.ProcessInstanceLPPrefix) and is range-deleted by FinishLPTransfer
// alongside the state/dedup rows for the same logical partition.
type ProcessInstanceTable struct{ S storage.Reader }

// Get loads an instance record. ok is false (err nil) when the row is absent.
func (t ProcessInstanceTable) Get(lp uint32, service, instanceKey string) (*enginev1.ProcessInstanceRecord, bool, error) {
	var rec enginev1.ProcessInstanceRecord
	if err := getProto(t.S, keys.ProcessInstanceKey(lp, service, instanceKey), &rec); err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &rec, true, nil
}

// Put writes the record into the batch; visible after Commit.
func (t ProcessInstanceTable) Put(b storage.Batch, lp uint32, service, instanceKey string, rec *enginev1.ProcessInstanceRecord) error {
	return putProto(b, keys.ProcessInstanceKey(lp, service, instanceKey), rec)
}

// Delete removes the instance row (terminal reap).
func (t ProcessInstanceTable) Delete(b storage.Batch, lp uint32, service, instanceKey string) error {
	return b.Delete(keys.ProcessInstanceKey(lp, service, instanceKey))
}
