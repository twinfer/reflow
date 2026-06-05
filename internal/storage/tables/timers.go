package tables

import (
	"bytes"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// TimerTable stores durable sleep timers.
//
// Primary keys are timer/<8-byte BE fire_at_ms>/<24-byte inv_id>; values are a
// marshaled TimerValue — either sleep_idx (the originating Sleep entry's
// journal index, so the timer service can construct the SleepResult) or a
// process descriptor (so the timer fires as a Command_ProcessEvent). The
// primary namespace is LP-agnostic — the live timer service drains in fire_at
// order, which an LP discriminator would fragment.
//
// A secondary per-invocation index at timer_idx/<lp:4><24-byte id>/<8-byte BE
// fire_at_ms> (empty value) is pair-written on every Insert/Delete so onPurge
// can find every pending timer for one invocation with a bounded range scan.
//
// A second secondary per-LP index at timer_lp/<lp:4><8-byte BE fire_at_ms>/
// <24-byte id> (value mirrors the primary) is pair-written too so the future
// cross-shard LP transfer protocol can extract every timer in an LP via a
// single bounded range scan.
//
// Both secondaries are purely additive: pre-existing data without secondary
// entries is still correct — primary rows self-fire on schedule even when
// secondary lookups turn up nothing.
//
// Mirrors restate crates/storage-api/src/timer_table.
type TimerTable struct{ S storage.Reader }

// Insert writes a new sleep / run-retry timer (primary + per-invocation + per-LP
// indexes). All three keys embed the 24-byte invocation id. The primary
// timer/<fire>/<id> namespace stays LP-agnostic so the timer service drains in
// fire_at order.
func (t TimerTable) Insert(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId, sleepIdx uint32) error {
	return t.insert(b, fireAtMs, id, &enginev1.TimerValue{SleepIdx: sleepIdx})
}

// InsertProcess writes a process timer keyed by the synthetic process-timer id
// (pk routes to the instance's shard). On fire the TimerService reads pt and
// proposes a Command_ProcessEvent{timer_fired} instead of a Command_TimerFired.
func (t TimerTable) InsertProcess(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId, pt *enginev1.ProcessTimer) error {
	return t.insert(b, fireAtMs, id, &enginev1.TimerValue{Process: pt})
}

func (t TimerTable) insert(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId, tv *enginev1.TimerValue) error {
	pk, err := keys.TimerKey(fireAtMs, id)
	if err != nil {
		return err
	}
	ik, err := keys.TimerIdxKey(id, fireAtMs)
	if err != nil {
		return err
	}
	lpk, err := keys.TimerLPKey(keys.LPFromPartitionKey(id.GetPartitionKey()), fireAtMs, id)
	if err != nil {
		return err
	}
	v, err := proto.Marshal(tv)
	if err != nil {
		return err
	}
	if err := b.Set(pk, v); err != nil {
		return err
	}
	if err := b.Set(ik, nil); err != nil {
		return err
	}
	return b.Set(lpk, v)
}

// Delete removes a timer (primary + per-invocation + per-LP indexes).
func (t TimerTable) Delete(b storage.Batch, fireAtMs uint64, id *enginev1.InvocationId) error {
	pk, err := keys.TimerKey(fireAtMs, id)
	if err != nil {
		return err
	}
	ik, err := keys.TimerIdxKey(id, fireAtMs)
	if err != nil {
		return err
	}
	lpk, err := keys.TimerLPKey(keys.LPFromPartitionKey(id.GetPartitionKey()), fireAtMs, id)
	if err != nil {
		return err
	}
	if err := b.Delete(pk); err != nil {
		return err
	}
	if err := b.Delete(ik); err != nil {
		return err
	}
	return b.Delete(lpk)
}

// TimerEntry is the decoded form yielded by scans. Process is non-nil for a
// process timer; SleepIdx applies to a plain sleep / run-retry timer.
type TimerEntry struct {
	FireAtMs uint64
	ID       *enginev1.InvocationId
	SleepIdx uint32
	Process  *enginev1.ProcessTimer
}

// decodeTimerValue parses a TimerTable row value (marshaled TimerValue). An
// empty value decodes to the zero TimerValue (sleep_idx 0, no process).
func decodeTimerValue(val []byte) (uint32, *enginev1.ProcessTimer, error) {
	var tv enginev1.TimerValue
	if err := proto.Unmarshal(val, &tv); err != nil {
		return 0, nil, err
	}
	return tv.GetSleepIdx(), tv.GetProcess(), nil
}

// ScanAll iterates every timer in (fire_at, id) order. Used on leader gain to
// rebuild the in-memory heap.
func (t TimerTable) ScanAll(fn func(TimerEntry) error) error {
	prefix := keys.TimerPrefix()
	return t.scanRange(prefix, keys.PrefixUpperBound(prefix), fn)
}

// ScanAllIndex iterates every per-invocation secondary-index row in the
// partition. Yields (id, fire_at_ms) tuples in encoded (lp, id, fire_at_ms)
// order. Used by tests to assert the primary↔secondary invariant.
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
// invocation via the per-invocation secondary index. Bounded by the
// per-invocation timer count (typically 1-2), not the global timer table
// size. Used by onPurge.
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

// ScanLP iterates every timer in one logical partition in (fire_at, id)
// order via the per-LP secondary index. Used by the cross-shard LP transfer
// protocol.
func (t TimerTable) ScanLP(lp uint32, fn func(TimerEntry) error) error {
	lower := keys.TimerLPPrefixForLP(lp)
	upper := keys.PrefixUpperBound(lower)
	iter, err := t.S.NewIter(lower, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, fireAt, id, err := keys.DecodeTimerLPKey(iter.Key())
		if err != nil {
			return err
		}
		sleepIdx, pt, derr := decodeTimerValue(iter.Value())
		if derr != nil {
			return derr
		}
		if err := fn(TimerEntry{FireAtMs: fireAt, ID: id, SleepIdx: sleepIdx, Process: pt}); err != nil {
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
		sleepIdx, pt, derr := decodeTimerValue(iter.Value())
		if derr != nil {
			return derr
		}
		if err := fn(TimerEntry{FireAtMs: fireAt, ID: id, SleepIdx: sleepIdx, Process: pt}); err != nil {
			return err
		}
	}
	return iter.Error()
}
