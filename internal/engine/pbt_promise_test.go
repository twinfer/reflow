package engine

// Promise-subsystem PBT actions and invariants. Carved out of pbt_test.go
// so the workflow-scoped DurablePromise surface (Get / Peek / Complete +
// the ingress-side PromiseCompleted resolver) lives next to the model
// types in pbt_helpers_test.go.
//
// Promise tracking state is owned by engineMachine in pbt_test.go:
//   - promises          map[promiseKey]*modelPromise
//   - promiseAwaiters   map[promiseKey][]*modelPromiseAwaiter
//   - wfSpecs           []invSpec (workflow + workflow-shared subset)
//   - promisePool       []string
//
// Generator pool init and ReapInvocation's promise cleanup remain in
// pbt_test.go (they touch other subsystems too). The Check skeleton
// calls checkPromiseInvariants below.

import (
	"bytes"

	"pgregory.net/rapid"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// PromiseGet journals a JEGetPromise for an mInvoked/mSuspended workflow or
// shared invocation. Mirrors the JournalEntry_GetPromise apply arm: if the
// promise is already terminal the result lands inline (journalLen += 2);
// otherwise a PromiseAwaiter row is recorded (journalLen += 1) so a
// subsequent PromiseComplete or PromiseCompletedExternal can stitch the
// result. A second pending Get on the same (svc,key,name) overwrites the
// prior awaiter — that's the MVP single-awaiter limitation documented in
// the design plan.
func (m *engineMachine) PromiseGet(t *rapid.T) {
	if len(m.wfSpecs) == 0 {
		return
	}
	spec := rapid.SampledFrom(m.wfSpecs).Draw(t, "wf_spec")
	name := rapid.SampledFrom(m.promisePool).Draw(t, "promise")
	cur := m.getOrCreate(spec.id)
	if cur.status != mInvoked && cur.status != mSuspended {
		return
	}
	idx := cur.journalLen
	resultIdx := idx + 1
	pk := promiseKey{spec.tgt.service, spec.tgt.objectKey, name}

	pv := m.promises[pk]
	inline := pv != nil && pv.state != promisePending

	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_GetPromise{
							GetPromise: &enginev1.JEGetPromise{
								Name:               name,
								ResultCompletionId: resultIdx,
								Service:            spec.tgt.service,
								WorkflowKey:        spec.tgt.objectKey,
							},
						},
					},
				},
			},
		}},
	})

	if cur.status == mSuspended {
		cur.status = mInvoked
	}
	if inline {
		cur.journalLen += 2
	} else {
		cur.journalLen += 1
		// PromiseAwaiterTable.PutForSlot keys by (svc, key, name, entry_index)
		// — owner is in the value, not the key. Two concurrent invocations
		// that both do Get(name) at the same entry_index (e.g. both at slot
		// 1 right after JEInput) collide and the second overwrites the
		// first; the originally-awaiting invocation is stranded with no row
		// in the SUT directory. Mirror that here: dedupe by entry_index
		// within a promiseKey, replacing on collision rather than appending.
		awaiters := m.promiseAwaiters[pk]
		replaced := false
		for i, a := range awaiters {
			if a.entryIndex == idx {
				awaiters[i] = &modelPromiseAwaiter{
					ownerIDHex: idHex(spec.id),
					entryIndex: idx,
				}
				replaced = true
				break
			}
		}
		if !replaced {
			awaiters = append(awaiters, &modelPromiseAwaiter{
				ownerIDHex: idHex(spec.id),
				entryIndex: idx,
			})
		}
		m.promiseAwaiters[pk] = awaiters
	}
}

// PromiseComplete journals a JECompletePromise. The apply arm writes a
// PromiseValue (if not already terminal) and appends JEPromiseCompleteResult
// at result_completion_id regardless of conflict; if an awaiter is pending
// it stitches a JEPromiseResult on the awaiter's journal at
// awaiter.entry_index+1 and clears the awaiter row. Note: stitching does
// NOT transition the awaiter's FSM (the only side effect is an ActInvoke
// the invoker filters via pendingRespawn) — InvokerEffect_PromiseCompleted
// is the only path that runs transitionOnPromiseResolved.
func (m *engineMachine) PromiseComplete(t *rapid.T) {
	if len(m.wfSpecs) == 0 {
		return
	}
	spec := rapid.SampledFrom(m.wfSpecs).Draw(t, "wf_spec")
	name := rapid.SampledFrom(m.promisePool).Draw(t, "promise")
	cur := m.getOrCreate(spec.id)
	if cur.status != mInvoked && cur.status != mSuspended {
		return
	}
	idx := cur.journalLen
	resultIdx := idx + 1
	pk := promiseKey{spec.tgt.service, spec.tgt.objectKey, name}
	value := []byte("v-" + name)

	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_CompletePromise{
							CompletePromise: &enginev1.JECompletePromise{
								Name:               name,
								Value:              value,
								ResultCompletionId: resultIdx,
								Service:            spec.tgt.service,
								WorkflowKey:        spec.tgt.objectKey,
							},
						},
					},
				},
			},
		}},
	})

	if cur.status == mSuspended {
		cur.status = mInvoked
	}
	// JECompletePromise (the cmd) + JEPromiseCompleteResult (the ack) — two
	// slots always, regardless of conflict.
	cur.journalLen += 2

	pv := m.promises[pk]
	if pv == nil || pv.state == promisePending {
		m.promises[pk] = &modelPromise{state: promiseResolved, value: value}
		// Stitch every co-pending awaiter: each owner's journal grows by
		// one (the JEPromiseResult at entry_index+1). FSM stays put — the
		// resolver path wakes owners via ActInvoke + pendingRespawn, not
		// transitionOnPromiseResolved.
		for _, awaiter := range m.promiseAwaiters[pk] {
			if owner := m.invs[awaiter.ownerIDHex]; owner != nil {
				owner.journalLen++
			}
		}
		delete(m.promiseAwaiters, pk)
	}
}

// PromisePeek journals a JEPeekPromise. The apply arm mutates the entry in
// place (stamping completed/value/failure_message from the current
// PromiseValue) and re-appends it. From the model's perspective the
// invocation just bumps its journalLen by 1 — peek has no FSM or state
// side effects.
func (m *engineMachine) PromisePeek(t *rapid.T) {
	if len(m.wfSpecs) == 0 {
		return
	}
	spec := rapid.SampledFrom(m.wfSpecs).Draw(t, "wf_spec")
	name := rapid.SampledFrom(m.promisePool).Draw(t, "promise")
	cur := m.getOrCreate(spec.id)
	if cur.status != mInvoked && cur.status != mSuspended {
		return
	}
	idx := cur.journalLen
	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_PeekPromise{
							PeekPromise: &enginev1.JEPeekPromise{Name: name, Service: spec.tgt.service, WorkflowKey: spec.tgt.objectKey},
						},
					},
				},
			},
		}},
	})
	if cur.status == mSuspended {
		cur.status = mInvoked
	}
	cur.journalLen++
}

// PromiseCompletedExternal proposes an InvokerEffect.PromiseCompleted
// envelope, mimicking ingress.ResolveWorkflowPromise. Unlike JECompletePromise
// the resolver isn't a real invocation — the envelope is addressed by
// (service, workflow_key, name) and the apply arm runs transitionOnPromiseResolved
// on the awaiter (Suspended → Invoked + ActInvoke). If no awaiter is pending
// only the PromiseValue row is written.
func (m *engineMachine) PromiseCompletedExternal(t *rapid.T) {
	if len(m.wfSpecs) == 0 {
		return
	}
	spec := rapid.SampledFrom(m.wfSpecs).Draw(t, "wf_spec")
	name := rapid.SampledFrom(m.promisePool).Draw(t, "promise")
	if spec.tgt.objectKey == "" {
		return
	}
	pk := promiseKey{spec.tgt.service, spec.tgt.objectKey, name}
	value := []byte("e-" + name)

	pv := m.promises[pk]
	alreadyTerminal := pv != nil && pv.state != promisePending

	shard := m.partitioner.ShardForTarget(0, &enginev1.InvocationTarget{
		ServiceName: spec.tgt.service,
		ObjectKey:   spec.tgt.objectKey,
	})
	m.apply(t, shard, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_PromiseCompleted{
				PromiseCompleted: &enginev1.PromiseCompleted{
					Service:     spec.tgt.service,
					WorkflowKey: spec.tgt.objectKey,
					PromiseName: name,
					Value:       value,
				},
			},
		}},
	})

	if alreadyTerminal {
		return // ingress is a silent no-op on conflict
	}
	m.promises[pk] = &modelPromise{state: promiseResolved, value: value}
	for _, awaiter := range m.promiseAwaiters[pk] {
		owner := m.invs[awaiter.ownerIDHex]
		if owner == nil {
			continue
		}
		owner.journalLen++
		if owner.status == mSuspended {
			owner.status = mInvoked
		}
	}
	delete(m.promiseAwaiters, pk)
}

// checkPromiseInvariants compares the SUT's PromiseTable and
// PromiseAwaiterTable against the model on every Check call. The
// terminal states (Resolved, Rejected) are compared by
// value/failure_message; absent rows in the model (promisePending) must
// mean either absent SUT row or a Pending PromiseValue.
//
// Each (svc, key, name) may have multiple awaiter rows (one per
// concurrent Promise(name).Result()), keyed by entry_index — so the
// awaiter table is scanned and compared as an unordered bag of
// (owner, entry_index) pairs.
func (m *engineMachine) checkPromiseInvariants(t *rapid.T) {
	for pk, want := range m.promises {
		shard := m.partitioner.ShardForTarget(0, &enginev1.InvocationTarget{
			ServiceName: pk.service, ObjectKey: pk.workflowKey,
		})
		lp := keys.LPFromPartitionKey(routing.PartitionKey(0, pk.service, pk.workflowKey))
		got, err := (tables.PromiseTable{S: m.snaps[m.sIdx(shard)].Store()}).Get(lp, pk.service, pk.workflowKey, pk.name)
		if err != nil {
			t.Fatalf("PromiseTable.Get shard=%d %+v: %v", shard, pk, err)
		}
		switch want.state {
		case promiseResolved:
			r := got.GetResolved()
			if r == nil {
				t.Fatalf("promise %+v shard=%d: SUT not Resolved (got %T), model Resolved", pk, shard, got.GetState())
			}
			if !bytes.Equal(r.GetValue(), want.value) {
				t.Fatalf("promise %+v shard=%d: SUT value=%q model value=%q", pk, shard, r.GetValue(), want.value)
			}
		case promiseRejected:
			rj := got.GetRejected()
			if rj == nil {
				t.Fatalf("promise %+v shard=%d: SUT not Rejected (got %T), model Rejected", pk, shard, got.GetState())
			}
			if rj.GetFailureMessage() != want.failureMessage {
				t.Fatalf("promise %+v shard=%d: SUT failure=%q model failure=%q",
					pk, shard, rj.GetFailureMessage(), want.failureMessage)
			}
		case promisePending:
			if got != nil && got.GetPending() == nil {
				t.Fatalf("promise %+v shard=%d: SUT terminal, model Pending", pk, shard)
			}
		}
	}

	for pk, want := range m.promiseAwaiters {
		shard := m.partitioner.ShardForTarget(0, &enginev1.InvocationTarget{
			ServiceName: pk.service, ObjectKey: pk.workflowKey,
		})
		lp := keys.LPFromPartitionKey(routing.PartitionKey(0, pk.service, pk.workflowKey))
		var got []*enginev1.PromiseAwaiter
		if err := (tables.PromiseAwaiterTable{S: m.snaps[m.sIdx(shard)].Store()}).ScanForName(lp, pk.service, pk.workflowKey, pk.name, func(a *enginev1.PromiseAwaiter) error {
			got = append(got, proto.Clone(a).(*enginev1.PromiseAwaiter))
			return nil
		}); err != nil {
			t.Fatalf("PromiseAwaiterTable.ScanForName shard=%d %+v: %v", shard, pk, err)
		}
		if len(got) != len(want) {
			t.Fatalf("promise_awaiter %+v shard=%d: SUT has %d rows, model has %d",
				pk, shard, len(got), len(want))
		}
		sutByIdx := map[uint32]*enginev1.PromiseAwaiter{}
		for _, a := range got {
			sutByIdx[a.GetEntryIndex()] = a
		}
		for _, w := range want {
			a, ok := sutByIdx[w.entryIndex]
			if !ok {
				t.Fatalf("promise_awaiter %+v shard=%d: model entry_index=%d absent in SUT",
					pk, shard, w.entryIndex)
			}
			if idHex(a.GetOwner()) != w.ownerIDHex {
				t.Fatalf("promise_awaiter %+v shard=%d entry_index=%d: SUT owner=%s, model owner=%s",
					pk, shard, w.entryIndex, idHex(a.GetOwner()), w.ownerIDHex)
			}
		}
	}
}
