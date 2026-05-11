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
// Keys are timer/<8-byte BE fire_at_ms>/<24-byte inv_id>; values are the
// 4-byte BE journal index of the originating Sleep entry, so the timer
// service can refer back to it when constructing the SleepResult.
//
// Mirrors restate crates/storage-api/src/timer_table.
type TimerTable struct{ S storage.Store }

// Insert writes a new timer to the batch.
func (t TimerTable) Insert(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId, sleepIdx uint32) error {
	k, err := keys.TimerKey(fireAtMs, id)
	if err != nil {
		return err
	}
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], sleepIdx)
	return b.Set(k, v[:])
}

// Delete removes a timer.
func (t TimerTable) Delete(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId) error {
	k, err := keys.TimerKey(fireAtMs, id)
	if err != nil {
		return err
	}
	return b.Delete(k)
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

// ScanDue iterates timers whose fire_at_ms <= nowMs, in fire-time order.
func (t TimerTable) ScanDue(nowMs uint64, fn func(TimerEntry) error) error {
	prefix := keys.TimerPrefix()

	// Build the exclusive upper bound: timer/<nowMs+1><0...>.
	upper := make([]byte, 0, len(prefix)+8)
	upper = append(upper, prefix...)
	var fireBuf [8]byte
	// Inclusive of nowMs ⇒ exclusive of nowMs+1. Saturate at MaxUint64 to
	// avoid wraparound (then everything is due).
	switch {
	case nowMs == ^uint64(0):
		return t.ScanAll(fn)
	default:
		binary.BigEndian.PutUint64(fireBuf[:], nowMs+1)
	}
	upper = append(upper, fireBuf[:]...)

	return t.scanRange(prefix, upper, fn)
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
