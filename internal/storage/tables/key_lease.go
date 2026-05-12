package tables

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// KeyLeaseTable is the per-(service, object_key) gate enforcing the Virtual
// Object single-writer invariant. The apply path reads the current
// KeyLeaseStatus, fires the per-key FSM in internal/engine/object_fsm.go,
// and writes the new status back into the same Pebble batch as the
// invocation status transition that triggered the fire. Phase 3.
type KeyLeaseTable struct{ S storage.Store }

// Get loads the lease row. Returns (nil, nil) when absent — callers should
// treat that as IDLE with an empty queue.
func (t KeyLeaseTable) Get(service, objectKey string) (*enginev1.KeyLeaseStatus, error) {
	val, closer, err := t.S.Get(keys.KeyLeaseKey(service, objectKey))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var s enginev1.KeyLeaseStatus
	if err := proto.Unmarshal(val, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Put writes the lease row into the batch.
func (t KeyLeaseTable) Put(b storage.Batch, service, objectKey string, s *enginev1.KeyLeaseStatus) error {
	buf, err := proto.Marshal(s)
	if err != nil {
		return err
	}
	return b.Set(keys.KeyLeaseKey(service, objectKey), buf)
}

// Delete removes the lease row. Used when an object's queue drains to IDLE
// to avoid leaving empty rows around (optional — IDLE with empty queue is
// also a valid absent state).
func (t KeyLeaseTable) Delete(b storage.Batch, service, objectKey string) error {
	return b.Delete(keys.KeyLeaseKey(service, objectKey))
}
