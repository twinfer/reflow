package tables

import (
	"bytes"
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// InvocationTable stores InvocationStatus rows keyed by invocation id.
//
// Mirrors restate crates/storage-api/src/invocation_status_table — each row
// is a single discriminated union (the InvocationStatus oneof).
type InvocationTable struct{ S storage.Reader }

// Get loads an invocation's status. Returns Free if the row is absent
// ("default" convention — matches restate's default at
// crates/storage-api/src/invocation_status_table/mod.rs:152-154).
func (t InvocationTable) Get(id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	k, err := keys.InvocationKey(id)
	if err != nil {
		return nil, err
	}
	var s enginev1.InvocationStatus
	if err := getProto(t.S, k, &s); err != nil {
		if isNotFound(err) {
			return &enginev1.InvocationStatus{
				Status: &enginev1.InvocationStatus_Free{Free: &enginev1.Free{}},
			}, nil
		}
		return nil, err
	}
	return &s, nil
}

func (t InvocationTable) Put(b storage.Batch, id *enginev1.InvocationId, s *enginev1.InvocationStatus) error {
	k, err := keys.InvocationKey(id)
	if err != nil {
		return err
	}
	return putProto(b, k, s)
}

func (t InvocationTable) Delete(b storage.Batch, id *enginev1.InvocationId) error {
	k, err := keys.InvocationKey(id)
	if err != nil {
		return err
	}
	return b.Delete(k)
}

// ScanAll iterates every persisted invocation status. The callback is called
// in key order; returning a non-nil error aborts iteration and is returned.
// Iteration aborts early when ctx is cancelled, returning ctx.Err().
//
// Rows whose Status is Free are skipped (they're equivalent to the row being
// absent; defensive against partial migrations).
func (t InvocationTable) ScanAll(ctx context.Context, fn func(id *enginev1.InvocationId, s *enginev1.InvocationStatus) error) error {
	prefix := []byte("inv/")
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		// Skip the namespace prefix + 4-byte LP to reach the 24-byte id body.
		id, err := keys.DecodeInvocationID(key[len(prefix)+keys.LPLen:])
		if err != nil {
			return err
		}
		var s enginev1.InvocationStatus
		if err := proto.Unmarshal(iter.Value(), &s); err != nil {
			return err
		}
		if _, free := s.GetStatus().(*enginev1.InvocationStatus_Free); free {
			continue
		}
		if err := fn(id, &s); err != nil {
			return err
		}
	}
	return iter.Error()
}

// ScanLP iterates every invocation status whose owner partition_key reduces
// to the given logical partition id. Bounded by a single Pebble range scan
// over [inv/<lp:4>, inv/<lp+1:4>). Used by the future cross-shard LP
// transfer protocol to extract one LP's invocations without touching the
// rest of the shard.
func (t InvocationTable) ScanLP(ctx context.Context, lp uint32, fn func(id *enginev1.InvocationId, s *enginev1.InvocationStatus) error) error {
	lower := keys.InvocationLPPrefix(lp)
	upper := keys.PrefixUpperBound(lower)
	iter, err := t.S.NewIter(lower, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		// key is inv/<lp:4><id:24>; slice off the prefix + lp to get the id body.
		id, err := keys.DecodeInvocationID(key[len(lower):])
		if err != nil {
			return err
		}
		var s enginev1.InvocationStatus
		if err := proto.Unmarshal(iter.Value(), &s); err != nil {
			return err
		}
		if _, free := s.GetStatus().(*enginev1.InvocationStatus_Free); free {
			continue
		}
		if err := fn(id, &s); err != nil {
			return err
		}
	}
	return iter.Error()
}
