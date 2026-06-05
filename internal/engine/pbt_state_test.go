package engine

// State-surface PBT actions. Drives JESetState / JEClearState /
// JEClearAllState writes and JEGetState / JEGetStateKeys lazy reads
// against the partition FSM, mirroring the writes into m.state. The
// invariant block in Check (pbt_test.go) compares the SUT's StateTable
// against m.state at every step; the read actions additionally assert
// the inline result entry (JEGetStateResult / JEGetStateKeysResult)
// landed at result_completion_id with the right shape.

import (
	"bytes"
	"reflect"
	"sort"

	"pgregory.net/rapid"

	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// stateKeyPool is the small set of state keys the state actions sample
// from. Three keys is enough to drive set / clear / clear-all
// interleavings while keeping rapid's search space tractable.
var stateKeyPool = []string{"counter", "name", "ttl"}

// stateValuePool covers the present-but-empty edge case alongside two
// representative payloads. Empty bytes is a distinct encoding from
// "absent" — StateTable.Set with []byte{} writes a present row.
var stateValuePool = [][]byte{
	[]byte(""),
	[]byte("v0"),
	[]byte("hello"),
}

// keyedLiveSpec draws a spec whose target is keyed (object_key != "")
// and whose invocation is currently mInvoked or mSuspended. Returns
// (invSpec, true) on success; (zero, false) when no eligible spec was
// drawn — caller short-circuits the action.
//
// State writes / reads are only meaningful for keyed targets: an
// unkeyed service has no per-object state scope, and the SDK's
// wireContext.GetState bails out before emitting a fetch when
// stateCache == nil && !partialState.
func (m *engineMachine) keyedLiveSpec(t *rapid.T, label string) (invSpec, bool) {
	spec := rapid.SampledFrom(m.specPool).Draw(t, label)
	if spec.tgt.objectKey == "" {
		return invSpec{}, false
	}
	cur := m.getOrCreate(spec.id)
	if cur.status != mInvoked && cur.status != mSuspended {
		return invSpec{}, false
	}
	return spec, true
}

// wakeOnAppend mirrors transitionOnJournalAppend's Suspended → Invoked
// branch (invocation_fsm.go:97-108): every JournalAppended effect on a
// Suspended invocation transitions it to Invoked. State actions must
// reflect that on the model side or the Check invariant trips.
func wakeOnAppend(cur *modelInv) {
	if cur.status == mSuspended {
		cur.status = mInvoked
	}
}

// SetState appends a JESetState entry for a live keyed invocation. The
// apply path writes through StateTable; the model mirrors the write so
// the Check invariant catches any drift.
func (m *engineMachine) SetState(t *rapid.T) {
	spec, ok := m.keyedLiveSpec(t, "set_state_spec")
	if !ok {
		return
	}
	key := rapid.SampledFrom(stateKeyPool).Draw(t, "set_state_key")
	val := rapid.SampledFrom(stateValuePool).Draw(t, "set_state_val")

	cur := m.invs[idHex(spec.id)]
	idx := cur.journalLen
	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_SetState{
							SetState: &enginev1.JESetState{Key: key, Value: val},
						},
					},
				},
			},
		}},
	})
	cur.journalLen = idx + 1
	wakeOnAppend(cur)

	lk := leaseKey{service: spec.tgt.service, objectKey: spec.tgt.objectKey}
	if m.state[lk] == nil {
		m.state[lk] = map[string][]byte{}
	}
	m.state[lk][key] = append([]byte(nil), val...)
}

// ClearState appends a JEClearState entry, removing the key from both
// the SUT and the model. Idempotent on missing keys — the apply arm
// no-ops cleanly via StateTable.Clear → Batch.Delete.
func (m *engineMachine) ClearState(t *rapid.T) {
	spec, ok := m.keyedLiveSpec(t, "clear_state_spec")
	if !ok {
		return
	}
	key := rapid.SampledFrom(stateKeyPool).Draw(t, "clear_state_key")

	cur := m.invs[idHex(spec.id)]
	idx := cur.journalLen
	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_ClearState{
							ClearState: &enginev1.JEClearState{Key: key},
						},
					},
				},
			},
		}},
	})
	cur.journalLen = idx + 1
	wakeOnAppend(cur)

	lk := leaseKey{service: spec.tgt.service, objectKey: spec.tgt.objectKey}
	if inner, ok := m.state[lk]; ok {
		delete(inner, key)
		if len(inner) == 0 {
			delete(m.state, lk)
		}
	}
}

// ClearAllState wipes every state row scoped to the invocation's
// (service, object_key). Backed by Pebble DeleteRange on the SUT side.
func (m *engineMachine) ClearAllState(t *rapid.T) {
	spec, ok := m.keyedLiveSpec(t, "clear_all_state_spec")
	if !ok {
		return
	}

	cur := m.invs[idHex(spec.id)]
	idx := cur.journalLen
	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_ClearAllState{
							ClearAllState: &enginev1.JEClearAllState{},
						},
					},
				},
			},
		}},
	})
	cur.journalLen = idx + 1
	wakeOnAppend(cur)

	lk := leaseKey{service: spec.tgt.service, objectKey: spec.tgt.objectKey}
	delete(m.state, lk)
}

// GetLazyState journals a JEGetState command. The apply path reads the
// StateTable in the same batch and appends JEGetStateResult inline at
// result_completion_id; this action asserts the stamped result matches
// the model's view AND that an ActInvoke was emitted to wake the
// suspended session.
func (m *engineMachine) GetLazyState(t *rapid.T) {
	spec, ok := m.keyedLiveSpec(t, "get_state_spec")
	if !ok {
		return
	}
	key := rapid.SampledFrom(stateKeyPool).Draw(t, "get_state_key")

	cur := m.invs[idHex(spec.id)]
	idx := cur.journalLen
	resultIdx := idx + 1
	shard := m.shardOf(spec.id)

	actions := m.applyEnvelope(t, shard, &enginev1.Envelope{
		Command: &enginev1.Command{
			Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
				InvocationId: spec.id,
				Kind: &enginev1.InvokerEffect_JournalAppended{
					JournalAppended: &enginev1.JournalEntryAppended{
						Entry: &enginev1.JournalEntry{
							Index: idx,
							Entry: &enginev1.JournalEntry_GetState{
								GetState: &enginev1.JEGetState{
									Key:                key,
									ResultCompletionId: resultIdx,
								},
							},
						},
					},
				},
			}},
		},
	})
	// cmd + inline result.
	cur.journalLen = idx + 2
	wakeOnAppend(cur)

	// Assert the result entry was stamped at resultIdx with the right shape.
	jt := tables.JournalTable{S: m.snaps[m.sIdx(shard)].Store()}
	je, err := jt.Read(spec.id, resultIdx)
	if err != nil {
		t.Fatalf("GetLazyState: Journal.Read(idx=%d): %v", resultIdx, err)
	}
	gsr := je.GetGetStateResult()
	if gsr == nil {
		t.Fatalf("GetLazyState: entry at idx=%d has type %T; want JEGetStateResult",
			resultIdx, je.GetEntry())
	}

	lk := leaseKey{service: spec.tgt.service, objectKey: spec.tgt.objectKey}
	var wantVal []byte
	wantPresent := false
	if inner, ok := m.state[lk]; ok {
		if v, present := inner[key]; present {
			wantVal = v
			wantPresent = true
		}
	}
	if gsr.GetPresent() != wantPresent {
		t.Fatalf("GetLazyState %s[%q]: SUT present=%v, model present=%v",
			lk.service, key, gsr.GetPresent(), wantPresent)
	}
	// bytes.Equal treats nil and []byte{} as equal — matches the SDK's
	// wireContext, which carries the empty-byte present case verbatim.
	if !bytes.Equal(gsr.GetValue(), wantVal) {
		t.Fatalf("GetLazyState %s[%q]: SUT value=%q, model value=%q",
			lk.service, key, gsr.GetValue(), wantVal)
	}

	// Wake path: apply arm must push ActInvoke so the suspended session
	// gets a pendingRespawn. This is the load-bearing guarantee that the
	// inline-result branch wakes the SDK.
	wokeUp := false
	for _, a := range actions {
		if ai, ok := a.(ActInvoke); ok && idHex(ai.ID) == idHex(spec.id) {
			wokeUp = true
			break
		}
	}
	if !wokeUp {
		t.Fatalf("GetLazyState %s[%q]: no ActInvoke pushed; the wake path is broken",
			lk.service, key)
	}

	// Now route the actions so the wake-respawn re-feed (the JEInput
	// reseed on first-time spawn, no-op on already-Invoked) flows
	// through normally.
	m.routeActions(t, shard, actions)
}

// GetLazyStateKeys journals a JEGetStateKeys command; symmetric to
// GetLazyState. Asserts the JEGetStateKeysResult at result_completion_id
// contains the model's key set in lexicographic order.
func (m *engineMachine) GetLazyStateKeys(t *rapid.T) {
	spec, ok := m.keyedLiveSpec(t, "get_state_keys_spec")
	if !ok {
		return
	}

	cur := m.invs[idHex(spec.id)]
	idx := cur.journalLen
	resultIdx := idx + 1
	shard := m.shardOf(spec.id)

	actions := m.applyEnvelope(t, shard, &enginev1.Envelope{
		Command: &enginev1.Command{
			Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
				InvocationId: spec.id,
				Kind: &enginev1.InvokerEffect_JournalAppended{
					JournalAppended: &enginev1.JournalEntryAppended{
						Entry: &enginev1.JournalEntry{
							Index: idx,
							Entry: &enginev1.JournalEntry_GetStateKeys{
								GetStateKeys: &enginev1.JEGetStateKeys{
									ResultCompletionId: resultIdx,
								},
							},
						},
					},
				},
			}},
		},
	})
	cur.journalLen = idx + 2
	wakeOnAppend(cur)

	jt := tables.JournalTable{S: m.snaps[m.sIdx(shard)].Store()}
	je, err := jt.Read(spec.id, resultIdx)
	if err != nil {
		t.Fatalf("GetLazyStateKeys: Journal.Read(idx=%d): %v", resultIdx, err)
	}
	gskr := je.GetGetStateKeysResult()
	if gskr == nil {
		t.Fatalf("GetLazyStateKeys: entry at idx=%d has type %T; want JEGetStateKeysResult",
			resultIdx, je.GetEntry())
	}

	lk := leaseKey{service: spec.tgt.service, objectKey: spec.tgt.objectKey}
	var wantKeys []string
	if inner, ok := m.state[lk]; ok {
		for k := range inner {
			wantKeys = append(wantKeys, k)
		}
		sort.Strings(wantKeys)
	}
	gotKeys := gskr.GetKeys()
	// nil and []string{} are interchangeable for an empty key set.
	if len(gotKeys) == 0 && len(wantKeys) == 0 {
		// noop
	} else if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("GetLazyStateKeys %+v: SUT=%v, model=%v", lk, gotKeys, wantKeys)
	}

	wokeUp := false
	for _, a := range actions {
		if ai, ok := a.(ActInvoke); ok && idHex(ai.ID) == idHex(spec.id) {
			wokeUp = true
			break
		}
	}
	if !wokeUp {
		t.Fatalf("GetLazyStateKeys %+v: no ActInvoke pushed", lk)
	}

	m.routeActions(t, shard, actions)
}

// GetEagerStateKeys journals a single-slot JEGetEagerStateKeys with the
// model's current keys inline. Asserts:
//
//   - the SUT journal carries the entry at the right slot with the
//     payload the SDK shipped (the engine trusts the inline list — no
//     re-derivation, no extra result entry);
//   - no result slot was written at idx+1;
//   - ActInvoke is pushed iff the prior status was Suspended (the
//     FSM's Suspended → Invoked branch wakes on any JournalAppended;
//     eager's apply path adds no further push of its own).
func (m *engineMachine) GetEagerStateKeys(t *rapid.T) {
	spec, ok := m.keyedLiveSpec(t, "get_eager_state_keys_spec")
	if !ok {
		return
	}

	cur := m.invs[idHex(spec.id)]
	idx := cur.journalLen
	shard := m.shardOf(spec.id)

	lk := leaseKey{service: spec.tgt.service, objectKey: spec.tgt.objectKey}
	var keys []string
	if inner, ok := m.state[lk]; ok {
		for k := range inner {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	}

	wasSuspended := cur.status == mSuspended
	actions := m.applyEnvelope(t, shard, &enginev1.Envelope{
		Command: &enginev1.Command{
			Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
				InvocationId: spec.id,
				Kind: &enginev1.InvokerEffect_JournalAppended{
					JournalAppended: &enginev1.JournalEntryAppended{
						Entry: &enginev1.JournalEntry{
							Index: idx,
							Entry: &enginev1.JournalEntry_GetEagerStateKeys{
								GetEagerStateKeys: &enginev1.JEGetEagerStateKeys{
									Keys: keys,
								},
							},
						},
					},
				},
			}},
		},
	})
	cur.journalLen = idx + 1
	wakeOnAppend(cur)

	jt := tables.JournalTable{S: m.snaps[m.sIdx(shard)].Store()}
	je, err := jt.Read(spec.id, idx)
	if err != nil {
		t.Fatalf("GetEagerStateKeys: Journal.Read(idx=%d): %v", idx, err)
	}
	gesk := je.GetGetEagerStateKeys()
	if gesk == nil {
		t.Fatalf("GetEagerStateKeys: entry at idx=%d has type %T; want JEGetEagerStateKeys",
			idx, je.GetEntry())
	}
	gotKeys := gesk.GetKeys()
	if !(len(gotKeys) == 0 && len(keys) == 0) && !reflect.DeepEqual(gotKeys, keys) {
		t.Fatalf("GetEagerStateKeys %+v: SUT keys=%v, sent=%v", lk, gotKeys, keys)
	}
	// Single-slot — nothing must have been appended at idx+1 by the
	// apply path on our behalf.
	if next, err := jt.Read(spec.id, idx+1); err == nil && next != nil {
		t.Fatalf("GetEagerStateKeys %+v: unexpected entry at idx+1 (%T); eager is single-slot",
			lk, next.GetEntry())
	}

	sawInvoke := false
	for _, a := range actions {
		if ai, ok := a.(ActInvoke); ok && idHex(ai.ID) == idHex(spec.id) {
			sawInvoke = true
			break
		}
	}
	if wasSuspended && !sawInvoke {
		t.Fatalf("GetEagerStateKeys %+v: prior=Suspended; want ActInvoke (FSM wake)", lk)
	}
	if !wasSuspended && sawInvoke {
		t.Fatalf("GetEagerStateKeys %+v: prior=Invoked; want no ActInvoke (eager has no apply-path push)", lk)
	}

	m.routeActions(t, shard, actions)
}
