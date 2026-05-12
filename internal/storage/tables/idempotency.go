package tables

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// IdempotencyTable maps a (service, handler, object_key, idempotency_key)
// tuple to the InvocationId of the first invocation that claimed it. The
// apply path's onInvoke consults this table on every InvokeCommand that
// carries a non-empty idempotency_key — a hit means a prior submission
// already created the invocation, and the new InvocationId is dropped.
// Phase 3.
type IdempotencyTable struct{ S storage.Store }

// Get returns the prior InvocationId for the tuple. Returns (nil, nil)
// when no prior invocation claimed this key.
func (t IdempotencyTable) Get(service, handler, objectKey, idempotencyKey string) (*enginev1.InvocationId, error) {
	val, closer, err := t.S.Get(keys.IdempotencyKey(service, handler, objectKey, idempotencyKey))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var id enginev1.InvocationId
	if err := proto.Unmarshal(val, &id); err != nil {
		return nil, err
	}
	return &id, nil
}

// Put records the InvocationId that claimed the tuple. Called from the
// apply path's onInvoke when a fresh idempotency_key is seen.
func (t IdempotencyTable) Put(b storage.Batch, service, handler, objectKey, idempotencyKey string, id *enginev1.InvocationId) error {
	buf, err := proto.Marshal(id)
	if err != nil {
		return err
	}
	return b.Set(keys.IdempotencyKey(service, handler, objectKey, idempotencyKey), buf)
}
