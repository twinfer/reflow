package tables

import (
	"errors"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// KeyLeaseTable is the per-(service, object_key) gate enforcing the Virtual
// Object single-writer invariant. The apply path reads the current
// KeyLeaseStatus, fires the per-key FSM in internal/engine/object_fsm.go,
// and writes the new status back into the same Pebble batch as the
// invocation status transition that triggered the fire.
//
// Callers compute lp at the apply-path boundary via
// keys.LPFromPartitionKey(routing.PartitionKey(svc, obj)).
type KeyLeaseTable struct{ S storage.Reader }

// Get loads the lease row. Returns (nil, nil) when absent — callers
// treat that as IDLE with an empty queue. "Optional lookup" convention.
func (t KeyLeaseTable) Get(lp uint32, service, objectKey string) (*enginev1.KeyLeaseStatus, error) {
	var s enginev1.KeyLeaseStatus
	err := getProto(t.S, keys.KeyLeaseKey(lp, service, objectKey), &s)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// Put writes the lease row into the batch.
func (t KeyLeaseTable) Put(b storage.Batch, lp uint32, service, objectKey string, s *enginev1.KeyLeaseStatus) error {
	return putProto(b, keys.KeyLeaseKey(lp, service, objectKey), s)
}
