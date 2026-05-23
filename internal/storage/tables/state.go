package tables

import (
	"bytes"
	"errors"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// StateTable stores per-(service, object_key) state KV pairs. Keys are
// state/<lp:4><service>/<object_key>/<state_key>; unkeyed services pass
// object_key="".
//
// Callers compute lp at the apply-path boundary via
// keys.LPFromPartitionKey(routing.PartitionKey(svc, obj)) — the keys package
// is intentionally kept free of the routing import.
//
// Handler reads issue a JEGetState command; the FSM resolves the value here
// and writes a JEGetState completion at the next journal index. Single-writer
// enforcement across concurrent invocations on the same object is handled by
// the key-lease FSM in internal/engine/object_fsm.go.
type StateTable struct{ S storage.Reader }

// Get returns the raw value bytes for the (target, key) pair. The boolean
// is false (with err == nil) when the row is absent — handlers distinguish
// "present-but-empty" from "missing" via this flag.
func (t StateTable) Get(lp, tenant uint32, target *enginev1.InvocationTarget, key string) ([]byte, bool, error) {
	k := keys.StateKey(lp, tenant, target.GetServiceName(), target.GetObjectKey(), key)
	val, closer, err := t.S.Get(k)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer closer.Close()
	// Copy out: the closer invalidates the slice.
	out := append([]byte(nil), val...)
	return out, true, nil
}

// Set writes a value into the batch. The write is visible after Commit.
func (t StateTable) Set(b storage.Batch, lp, tenant uint32, target *enginev1.InvocationTarget, key string, value []byte) error {
	k := keys.StateKey(lp, tenant, target.GetServiceName(), target.GetObjectKey(), key)
	return b.Set(k, value)
}

// Clear deletes a single (target, key) row.
func (t StateTable) Clear(b storage.Batch, lp, tenant uint32, target *enginev1.InvocationTarget, key string) error {
	k := keys.StateKey(lp, tenant, target.GetServiceName(), target.GetObjectKey(), key)
	return b.Delete(k)
}

// ClearObject wipes every state row scoped to (service, object_key). Backed
// by Pebble's DeleteRange over [StatePrefixForObject, upper-bound) so the
// cost is independent of the number of rows. Invoked from the apply arm
// when a JEClearAllState journal entry lands.
func (t StateTable) ClearObject(b storage.Batch, lp, tenant uint32, target *enginev1.InvocationTarget) error {
	prefix := keys.StatePrefixForObject(lp, tenant, target.GetServiceName(), target.GetObjectKey())
	upper := keys.PrefixUpperBound(prefix)
	if upper == nil {
		// PrefixUpperBound returns nil only when prefix is all 0xff — never
		// the case for our "state/" namespace. Defensive: fall back to
		// point-deletes by scanning.
		return t.ScanObject(lp, tenant, target, func(key string, _ []byte) error {
			return t.Clear(b, lp, tenant, target, key)
		})
	}
	return b.DeleteRange(prefix, upper)
}

// ScanObject iterates every (key, value) tuple under (service, object_key)
// in lexicographic key order. Used for eager-state preload and debugging.
func (t StateTable) ScanObject(lp, tenant uint32, target *enginev1.InvocationTarget, fn func(key string, value []byte) error) error {
	prefix := keys.StatePrefixForObject(lp, tenant, target.GetServiceName(), target.GetObjectKey())
	upper := keys.PrefixUpperBound(prefix)
	iter, err := t.S.NewIter(prefix, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		k := iter.Key()
		if !bytes.HasPrefix(k, prefix) {
			continue
		}
		stateKey := string(k[len(prefix):])
		if err := fn(stateKey, iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}
