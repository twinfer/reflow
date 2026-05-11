package tables

import (
	"bytes"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// InvocationTable stores InvocationStatus rows keyed by invocation id.
//
// Mirrors restate crates/storage-api/src/invocation_status_table — each row
// is a single discriminated union (the InvocationStatus oneof).
type InvocationTable struct{ S storage.Store }

// Get loads an invocation's status. Returns Free if the row is absent
// (matches restate's default value at
// crates/storage-api/src/invocation_status_table/mod.rs:152-154).
func (t InvocationTable) Get(id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	k, err := keys.InvocationKey(id)
	if err != nil {
		return nil, err
	}
	val, closer, err := t.S.Get(k)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &enginev1.InvocationStatus{
				Status: &enginev1.InvocationStatus_Free{Free: &enginev1.Free{}},
			}, nil
		}
		return nil, err
	}
	defer closer.Close()
	var s enginev1.InvocationStatus
	if err := proto.Unmarshal(val, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (t InvocationTable) Put(b storage.Batch, id *enginev1.InvocationId, s *enginev1.InvocationStatus) error {
	k, err := keys.InvocationKey(id)
	if err != nil {
		return err
	}
	buf, err := proto.Marshal(s)
	if err != nil {
		return err
	}
	return b.Set(k, buf)
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
//
// Rows whose Status is Free are skipped (they're equivalent to the row being
// absent; defensive against partial migrations).
func (t InvocationTable) ScanAll(fn func(id *enginev1.InvocationId, s *enginev1.InvocationStatus) error) error {
	prefix := []byte("inv/")
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		id, err := keys.DecodeInvocationID(key[len(prefix):])
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
