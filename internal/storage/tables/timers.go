package tables

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TimerTable stores durable sleep timers.
//
// Primary keys are timer/<8-byte BE fire_at_ms>/<24-byte inv_id>; values
// are the 4-byte BE journal index of the originating Sleep entry, so the
// timer service can refer back to it when constructing the SleepResult.
//
// A secondary index at timer_idx/<24-byte id>/<8-byte BE fire_at_ms>
// (empty value) is pair-written on every Insert/Delete so onPurge can
// find every pending timer for one invocation with a bounded range scan.
// The index is purely additive: pre-fix data without secondary entries
// is still correct — primary rows self-fire on schedule even when the
// secondary lookup turns up nothing.
//
// Mirrors restate crates/storage-api/src/timer_table.
type TimerTable struct{ S storage.Reader }

// Insert writes a new timer (primary + secondary index) to the batch.
func (t TimerTable) Insert(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId, sleepIdx uint32) error {
	pk, err := keys.TimerKey(fireAtMs, id)
	if err != nil {
		return err
	}
	ik, err := keys.TimerIdxKey(id, fireAtMs)
	if err != nil {
		return err
	}
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], sleepIdx)
	if err := b.Set(pk, v[:]); err != nil {
		return err
	}
	return b.Set(ik, nil)
}

// Delete removes a timer (primary + secondary index) from the batch.
func (t TimerTable) Delete(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId) error {
	pk, err := keys.TimerKey(fireAtMs, id)
	if err != nil {
		return err
	}
	ik, err := keys.TimerIdxKey(id, fireAtMs)
	if err != nil {
		return err
	}
	if err := b.Delete(pk); err != nil {
		return err
	}
	return b.Delete(ik)
}

// TimerEntry is the decoded form yielded by scans.
type TimerEntry struct {
	FireAtMs uint64
	ID       *enginev1.InvocationId
	SleepIdx uint32
}

// ScanAll iterates every timer in (fire_at, id) order. Used on leader gain to
// rebuild the in-memory heap.
func (t TimerTable) ScanAll(fn func(TimerEntry) error) error {
	prefix := keys.TimerPrefix()
	return t.scanRange(prefix, keys.PrefixUpperBound(prefix), fn)
}

// ScanAllIndex iterates every secondary-index row in the partition. Yields
// (id, fire_at_ms) pairs in encoded (id, fire_at_ms) order. Used by tests
// to assert the primary↔secondary invariant.
func (t TimerTable) ScanAllIndex(fn func(id *enginev1.InvocationId, fireAtMs uint64) error) error {
	lower := keys.TimerIdxPrefix()
	upper := keys.PrefixUpperBound(lower)
	iter, err := t.S.NewIter(lower, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		id, fireAt, err := keys.DecodeTimerIdxKey(iter.Key())
		if err != nil {
			return err
		}
		if err := fn(id, fireAt); err != nil {
			return err
		}
	}
	return iter.Error()
}

// ScanByInvocation iterates the fire_at_ms of every pending timer for one
// invocation via the secondary index. Bounded by the per-invocation timer
// count (typically 1-2), not the global timer table size. Used by onPurge.
func (t TimerTable) ScanByInvocation(id *enginev1.InvocationId, fn func(fireAtMs uint64) error) error {
	lower, err := keys.TimerIdxPrefixForID(id)
	if err != nil {
		return err
	}
	upper := keys.PrefixUpperBound(lower)
	iter, err := t.S.NewIter(lower, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, fireAt, err := keys.DecodeTimerIdxKey(iter.Key())
		if err != nil {
			return err
		}
		if err := fn(fireAt); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (t TimerTable) scanRange(lower, upper []byte, fn func(TimerEntry) error) error {
	iter, err := t.S.NewIter(lower, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, lower[:len(keys.TimerPrefix())]) {
			continue
		}
		fireAt, id, err := keys.DecodeTimerKey(key)
		if err != nil {
			return err
		}
		val := iter.Value()
		if len(val) != 4 {
			return errors.New("timer value has wrong length")
		}
		sleepIdx := binary.BigEndian.Uint32(val)
		if err := fn(TimerEntry{FireAtMs: fireAt, ID: id, SleepIdx: sleepIdx}); err != nil {
			return err
		}
	}
	return iter.Error()
}
