package tables

import (
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ProcessInboxTable is the per-instance durable FIFO of pending engine events,
// keyed proc_inbox/<lp:4><service>/<instance_key>/<seq:8>. The cursor lives on
// ProcessInstanceRecord (next_seq/active_seq); this table stores only the event
// payloads so an in-flight turn survives a leader change (the new leader reads
// the active_seq row and re-drives the turn). Rides the LP-transfer scan via
// keys.ProcessInboxLPPrefix.
type ProcessInboxTable struct{ S storage.Reader }

// Append writes the entry at seq into the batch.
func (t ProcessInboxTable) Append(b storage.Batch, lp uint32, service, instanceKey string, seq uint64, entry *enginev1.ProcessInboxEntry) error {
	return putProto(b, keys.ProcessInboxKey(lp, service, instanceKey, seq), entry)
}

// Get loads the entry at seq. ok is false (err nil) when the row is absent.
func (t ProcessInboxTable) Get(lp uint32, service, instanceKey string, seq uint64) (*enginev1.ProcessInboxEntry, bool, error) {
	var e enginev1.ProcessInboxEntry
	if err := getProto(t.S, keys.ProcessInboxKey(lp, service, instanceKey, seq), &e); err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &e, true, nil
}

// Delete removes the entry at seq (called when its turn completes).
func (t ProcessInboxTable) Delete(b storage.Batch, lp uint32, service, instanceKey string, seq uint64) error {
	return b.Delete(keys.ProcessInboxKey(lp, service, instanceKey, seq))
}
