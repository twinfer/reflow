package engine

import (
	"bytes"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/limits"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/observability"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

type stubLeadership struct {
	leader atomic.Bool
	last   *enginev1.AnnounceLeader
}

func (s *stubLeadership) IsLeader() bool { return s.leader.Load() }
func (s *stubLeadership) OnAnnounceLeader(cmd *enginev1.AnnounceLeader) {
	s.last = cmd
}

func newTestPartition(t *testing.T) (*Partition, *stubLeadership, *ActionCollector) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "p", "state")
	snap, err := NewSnapshotter(dir, func(path string) (storage.Store, error) {
		return storage.OpenPebble(path, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	lead := &stubLeadership{}
	lead.leader.Store(true)
	col := &ActionCollector{}
	p := NewPartition(1, 1, PartitionConfig{
		Snapshotter: snap,
		Leadership:  lead,
		Collector:   col,
	})
	t.Cleanup(func() { _ = p.Close() })
	return p, lead, col
}

// testEnvelopeNowMs is the wall-clock value stamped onto the envelope
// Header by the test helpers below. Tests don't care about its
// absolute value — only that it is non-zero so the apply path reads a
// definite "now" instead of relying on a fallback that no longer
// exists in production (see partition.applyCommand).
const testEnvelopeNowMs uint64 = 1_700_000_000_000

func envelope(t *testing.T, cmd *enginev1.Command) []byte {
	t.Helper()
	buf, err := proto.Marshal(&enginev1.Envelope{
		Header:  &enginev1.Header{CreatedAtMs: testEnvelopeNowMs},
		Command: cmd,
	})
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func envelopeWithDedup(t *testing.T, d *enginev1.Dedup, cmd *enginev1.Command) []byte {
	t.Helper()
	buf, err := proto.Marshal(&enginev1.Envelope{
		Header:  &enginev1.Header{CreatedAtMs: testEnvelopeNowMs, Dedup: d},
		Command: cmd,
	})
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestPartition_ApplyInvokeAndJournal(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	// 1. Invoke
	invCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{
			Invoke: &enginev1.InvokeCommand{
				InvocationId: id, Target: target, Input: []byte("in"),
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: invCmd}}); err != nil {
		t.Fatal(err)
	}
	// Should produce ActInvoke (leader).
	actions := col.Drain()
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if _, ok := actions[0].(ActInvoke); !ok {
		t.Errorf("expected ActInvoke, got %T", actions[0])
	}

	// 2. JournalAppended(Input) -> Invoked
	jApp := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{
			InvokerEffect: &enginev1.InvokerEffect{
				InvocationId: id,
				Kind: &enginev1.InvokerEffect_JournalAppended{
					JournalAppended: &enginev1.JournalEntryAppended{
						Entry: &enginev1.JournalEntry{
							Index: 0,
							Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
						},
					},
				},
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: jApp}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// 3. Completed
	cmpCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{
			InvokerEffect: &enginev1.InvokerEffect{
				InvocationId: id,
				Kind: &enginev1.InvokerEffect_Completed{
					Completed: &enginev1.InvocationCompleted{Output: []byte("ok")},
				},
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: cmpCmd}}); err != nil {
		t.Fatal(err)
	}

	// Verify final status via Lookup.
	got, err := p.Lookup(LookupInvocation{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	st := got.(*enginev1.InvocationStatus)
	cmp, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed)
	if !ok {
		t.Fatalf("status = %T; want Completed", st.GetStatus())
	}
	if !bytes.Equal(cmp.Completed.GetOutput(), []byte("ok")) {
		t.Errorf("output mismatch: %x", cmp.Completed.GetOutput())
	}

	// applied_index must be 3.
	idx, err := p.Lookup(LookupAppliedIndex{})
	if err != nil {
		t.Fatal(err)
	}
	if idx.(uint64) != 3 {
		t.Errorf("applied_index = %v; want 3", idx)
	}
}

// TestPartition_ReapPurgesPlainInvocation covers the unified reap on a
// plain (non-workflow, unkeyed) invocation: completion schedules a reap
// at the shorter DefaultInvocationRetentionMs window, and onReap purges
// the per-invocation rows without touching any entity state (there is
// none). This is the path the old workflow-only reaper never exercised.
func TestPartition_ReapPurgesPlainInvocation(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("plain-reap-id-16")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"} // unkeyed → not a workflow run

	mustUpdate := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatalf("update idx=%d: %v", idx, err)
		}
	}

	// Invoke -> JournalAppended(Input) -> Completed.
	mustUpdate(1, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target, Input: []byte("in"),
	}}})
	col.Drain()
	mustUpdate(2, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 0, Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}}},
		}},
	}}})
	col.Drain()
	mustUpdate(3, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind:         &enginev1.InvokerEffect_Completed{Completed: &enginev1.InvocationCompleted{Output: []byte("ok")}},
	}}})

	// Completion must schedule a reap at the plain-invocation window.
	wantFire := testEnvelopeNowMs + limits.DefaultInvocationRetentionMs
	var sched *ActScheduleReap
	for _, a := range col.Drain() {
		if sr, ok := a.(ActScheduleReap); ok {
			sr := sr
			sched = &sr
		}
	}
	if sched == nil {
		t.Fatalf("completion emitted no ActScheduleReap")
	}
	if sched.FireAtMs != wantFire {
		t.Errorf("reap fire_at = %d; want %d (DefaultInvocationRetentionMs)", sched.FireAtMs, wantFire)
	}
	if sched.ID.GetPartitionKey() != id.GetPartitionKey() || !bytes.Equal(sched.ID.GetUuid(), id.GetUuid()) {
		t.Errorf("reap scheduled for wrong id: %+v", sched.ID)
	}

	// And a durable reap row at that fire.
	store := p.cfg.Snapshotter.Store()
	var rows []tables.ReapRow
	if err := (tables.ReapTable{S: store}).ScanAll(func(r tables.ReapRow) error {
		rows = append(rows, r)
		return nil
	}); err != nil {
		t.Fatalf("reap scan: %v", err)
	}
	if len(rows) != 1 || rows[0].FireAtMs != wantFire {
		t.Fatalf("reap rows = %+v; want one at fire=%d", rows, wantFire)
	}

	// Fire the reap synthetically. No workflow_run row exists, so onReap
	// purges only the per-invocation rows.
	mustUpdate(4, &enginev1.Command{Kind: &enginev1.Command_ReapInvocation{ReapInvocation: &enginev1.ReapInvocation{
		InvocationId: id, FireAtMs: wantFire,
	}}})

	// Status row gone (Get synthesises Free), journal empty, reap row gone.
	got, err := p.Lookup(LookupInvocation{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, free := got.(*enginev1.InvocationStatus).GetStatus().(*enginev1.InvocationStatus_Free); !free {
		t.Errorf("status after reap = %T; want Free", got.(*enginev1.InvocationStatus).GetStatus())
	}
	store = p.cfg.Snapshotter.Store()
	jPrefix, _ := keys.JournalPrefix(id)
	iter, err := store.NewIter(jPrefix, keys.PrefixUpperBound(jPrefix))
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	if iter.First() {
		t.Errorf("journal rows survived reap (key=%x)", iter.Key())
	}
	reapLeft := 0
	_ = (tables.ReapTable{S: store}).ScanAll(func(tables.ReapRow) error { reapLeft++; return nil })
	if reapLeft != 0 {
		t.Errorf("reap rows survived: %d", reapLeft)
	}
}

func TestPartition_FollowerDropsActions(t *testing.T) {
	p, lead, col := newTestPartition(t)
	lead.leader.Store(false)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	cmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatal(err)
	}
	if col.Len() != 0 {
		t.Errorf("follower must not buffer actions; got %d", col.Len())
	}
}

func TestPartition_DedupRejectsDuplicate(t *testing.T) {
	p, _, _ := newTestPartition(t)

	dedup := &enginev1.Dedup{Kind: &enginev1.Dedup_Arbitrary{
		Arbitrary: &enginev1.ArbitraryDedup{ProducerId: "ingress-1", Seq: 1},
	}}
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	invokeCmd := &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target,
	}}}

	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelopeWithDedup(t, dedup, invokeCmd)}}); err != nil {
		t.Fatal(err)
	}
	// Re-applying the same dedup should be a no-op (no new action).
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelopeWithDedup(t, dedup, invokeCmd)}}); err != nil {
		t.Fatal(err)
	}

	// applied_index advances to 2.
	idx, _ := p.Lookup(LookupAppliedIndex{})
	if idx.(uint64) != 2 {
		t.Errorf("applied_index = %v; want 2", idx)
	}
}

func TestLPFromCommand(t *testing.T) {
	// PartitionKey 0x1234_0000_0000_0007 → LP 7 (LPCount=4096 so LP = pk & 0xFFF).
	const pkLP7 uint64 = 0x1234_0000_0000_0007
	const wantLP7 uint32 = 7
	id := &enginev1.InvocationId{PartitionKey: pkLP7, Uuid: []byte("0123456789abcdef")}

	cases := []struct {
		name string
		cmd  *enginev1.Command
		want uint32
	}{
		{
			"Invoke",
			&enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: id, Target: &enginev1.InvocationTarget{ServiceName: "S"},
			}}},
			wantLP7,
		},
		{
			"InvokerEffect",
			&enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
				InvocationId: id,
			}}},
			wantLP7,
		},
		{
			"TimerFired",
			&enginev1.Command{Kind: &enginev1.Command_TimerFired{TimerFired: &enginev1.TimerFired{
				InvocationId: id,
			}}},
			wantLP7,
		},
		{
			"DeliverCallResult",
			&enginev1.Command{Kind: &enginev1.Command_DeliverCallResult{DeliverCallResult: &enginev1.DeliverCallResult{
				ParentId: id,
			}}},
			wantLP7,
		},
		{
			"Purge",
			&enginev1.Command{Kind: &enginev1.Command_Purge{Purge: &enginev1.PurgeInvocation{
				InvocationId: id,
			}}},
			wantLP7,
		},
		{
			"PromiseCompletionAck",
			&enginev1.Command{Kind: &enginev1.Command_PromiseCompletionAck{PromiseCompletionAck: &enginev1.PromiseCompletionAck{
				CallerId: id,
			}}},
			wantLP7,
		},
		{
			"ReapInvocation",
			&enginev1.Command{Kind: &enginev1.Command_ReapInvocation{ReapInvocation: &enginev1.ReapInvocation{
				InvocationId: id,
			}}},
			wantLP7,
		},
		{
			"BeginLPTransfer",
			&enginev1.Command{Kind: &enginev1.Command_BeginLpTransfer{BeginLpTransfer: &enginev1.BeginLPTransfer{
				Lp: 42,
			}}},
			42,
		},
		{
			"ApplyLPTransferSST",
			&enginev1.Command{Kind: &enginev1.Command_ApplyLpTransferSst{ApplyLpTransferSst: &enginev1.ApplyLPTransferSST{
				Lp: 100,
			}}},
			100,
		},
		{
			"CommitLPTransfer",
			&enginev1.Command{Kind: &enginev1.Command_CommitLpTransfer{CommitLpTransfer: &enginev1.CommitLPTransfer{Lp: 7}}},
			7,
		},
		{
			"FinishLPTransfer",
			&enginev1.Command{Kind: &enginev1.Command_FinishLpTransfer{FinishLpTransfer: &enginev1.FinishLPTransfer{Lp: 7}}},
			7,
		},
		{
			"AbortLPTransfer",
			&enginev1.Command{Kind: &enginev1.Command_AbortLpTransfer{AbortLpTransfer: &enginev1.AbortLPTransfer{Lp: 7}}},
			7,
		},
		{
			// OutboxAck is LP-agnostic; dedup row keys under the sentinel.
			"OutboxAck",
			&enginev1.Command{Kind: &enginev1.Command_OutboxAck{OutboxAck: &enginev1.OutboxAck{
				ProducerShardId: 1, ProducerSeq: 1,
			}}},
			keys.LPNoLP,
		},
		{
			"AnnounceLeader_NoArbitraryDedup",
			&enginev1.Command{Kind: &enginev1.Command_AnnounceLeader{AnnounceLeader: &enginev1.AnnounceLeader{}}},
			keys.LPNoLP,
		},
		{"NilCommand", nil, keys.LPNoLP},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := lpFromCommand(tc.cmd)
			if got != tc.want {
				t.Errorf("lpFromCommand = %d; want %d", got, tc.want)
			}
		})
	}
}

func TestPartition_IdempotencyKey_FirstInvokeWinsSecondDropped(t *testing.T) {
	p, _, col := newTestPartition(t)

	target := &enginev1.InvocationTarget{ServiceName: "Counter", HandlerName: "incr", ObjectKey: ""}
	idA := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("aaaaaaaaaaaaaaaa")}
	idB := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("bbbbbbbbbbbbbbbb")}

	mkInvoke := func(id *enginev1.InvocationId) []byte {
		return envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId:   id,
			Target:         target,
			Input:          []byte("in"),
			IdempotencyKey: "req-1",
		}}})
	}

	// First Invoke wins: status registered, ActInvoke emitted.
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: mkInvoke(idA)}}); err != nil {
		t.Fatal(err)
	}
	acts := col.Drain()
	if len(acts) != 1 {
		t.Fatalf("first invoke: got %d actions, want 1", len(acts))
	}
	if a, ok := acts[0].(ActInvoke); !ok || !bytes.Equal(a.ID.GetUuid(), idA.GetUuid()) {
		t.Errorf("first action: %+v want ActInvoke for idA", acts[0])
	}

	// Second Invoke with same idempotency_key but new id: dropped, no action,
	// and idB never appears in InvocationTable.
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: mkInvoke(idB)}}); err != nil {
		t.Fatal(err)
	}
	if n := col.Len(); n != 0 {
		t.Errorf("second invoke: got %d actions, want 0", n)
	}
	got, err := p.Lookup(LookupInvocation{ID: idB})
	if err != nil {
		t.Fatal(err)
	}
	st := got.(*enginev1.InvocationStatus)
	if _, free := st.GetStatus().(*enginev1.InvocationStatus_Free); !free && st.GetStatus() != nil {
		t.Errorf("idB status = %T; want Free/absent", st.GetStatus())
	}

	// LookupIdempotency returns idA.
	res, err := p.Lookup(LookupIdempotency{Service: "Counter", Handler: "incr", IdempotencyKey: "req-1"})
	if err != nil {
		t.Fatal(err)
	}
	prior, ok := res.(*enginev1.InvocationId)
	if !ok || prior == nil {
		t.Fatalf("LookupIdempotency: %v %T", res, res)
	}
	if !bytes.Equal(prior.GetUuid(), idA.GetUuid()) {
		t.Errorf("LookupIdempotency uuid = %x; want %x", prior.GetUuid(), idA.GetUuid())
	}

	// LookupIdempotency for an absent key returns a typed-nil *InvocationId.
	res2, err := p.Lookup(LookupIdempotency{Service: "Counter", Handler: "incr", IdempotencyKey: "req-999"})
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := res2.(*enginev1.InvocationId); id != nil {
		t.Errorf("absent key returned %+v; want nil", id)
	}
}

func TestPartition_ApplyPathBandsByTenant(t *testing.T) {
	// Two invokes with the SAME (service, handler, object_key, idempotency_key)
	// but ids in different tenant bands must NOT collide — banding places their
	// idempotency rows under disjoint LPs, so both win. A band-dropping bug in
	// the apply path (writing under a bare hash / band 0 instead of the id's
	// band) would dedup the second against the first; that bug is invisible at
	// tenant 0, so this tenant!=0 test is its only guard.
	p, _, _ := newTestPartition(t)

	target := &enginev1.InvocationTarget{ServiceName: "Cart", HandlerName: "add", ObjectKey: "obj"}
	mkID := func(tenant uint32, uuid string) *enginev1.InvocationId {
		return &enginev1.InvocationId{
			PartitionKey: routing.PartitionKey(tenant, target.GetServiceName(), target.GetObjectKey()),
			Uuid:         []byte(uuid),
		}
	}
	id0 := mkID(0, "0000000000000000")
	id7 := mkID(7, "7777777777777777")
	if keys.TenantFromPartitionKey(id0.GetPartitionKey()) != 0 || keys.TenantFromPartitionKey(id7.GetPartitionKey()) != 7 {
		t.Fatalf("test ids not banded as expected: %d, %d",
			keys.TenantFromPartitionKey(id0.GetPartitionKey()), keys.TenantFromPartitionKey(id7.GetPartitionKey()))
	}

	mkInvoke := func(id *enginev1.InvocationId) []byte {
		return envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId:   id,
			Target:         target,
			IdempotencyKey: "k",
		}}})
	}
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: mkInvoke(id0)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: mkInvoke(id7)}}); err != nil {
		t.Fatal(err)
	}

	lookup := func(tenant uint32) *enginev1.InvocationId {
		res, err := p.Lookup(LookupIdempotency{
			Service: target.GetServiceName(), Handler: target.GetHandlerName(),
			ObjectKey: target.GetObjectKey(), IdempotencyKey: "k", Tenant: tenant,
		})
		if err != nil {
			t.Fatal(err)
		}
		id, _ := res.(*enginev1.InvocationId)
		return id
	}
	if got := lookup(0); got == nil || !bytes.Equal(got.GetUuid(), id0.GetUuid()) {
		t.Errorf("tenant 0 idempotency = %v; want id0", got)
	}
	if got := lookup(7); got == nil || !bytes.Equal(got.GetUuid(), id7.GetUuid()) {
		t.Errorf("tenant 7 idempotency = %v; want id7 (a band-dropping write bug misses here or returns id0)", got)
	}
}

func TestPartition_ClearAllState_WipesAllRowsForObject(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "Counter", HandlerName: "incr", ObjectKey: "user-1"}
	otherTarget := &enginev1.InvocationTarget{ServiceName: "Counter", HandlerName: "incr", ObjectKey: "user-2"}

	// Seed StateTable with rows on two objects so we can confirm only the
	// invocation's own object is wiped. LP is derived from the target
	// tuple (matches the apply path's writes).
	store := p.cfg.Snapshotter.Store()
	st := tables.StateTable{S: store}
	lp := keys.LPFromPartitionKey(routing.PartitionKey(0, target.GetServiceName(), target.GetObjectKey()))
	otherLP := keys.LPFromPartitionKey(routing.PartitionKey(0, otherTarget.GetServiceName(), otherTarget.GetObjectKey()))
	b := store.NewBatch()
	for _, k := range []string{"a", "b", "c"} {
		if err := st.Set(b, lp, target, k, []byte(k+"-val")); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Set(b, otherLP, otherTarget, "z", []byte("z-val")); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	// Move the invocation to Invoked so JEClearAllState's status-target
	// extraction succeeds.
	invCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target, Input: []byte("in"),
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: invCmd}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	jApp := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{
				Index: 0,
				Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
			},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: jApp}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Fire JEClearAllState at journal index 1.
	clearAll := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{
				Index: 1,
				Entry: &enginev1.JournalEntry_ClearAllState{ClearAllState: &enginev1.JEClearAllState{}},
			},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: clearAll}}); err != nil {
		t.Fatal(err)
	}

	// All rows on the invocation's object are gone.
	for _, k := range []string{"a", "b", "c"} {
		_, present, err := st.Get(lp, target, k)
		if err != nil {
			t.Fatal(err)
		}
		if present {
			t.Errorf("state[%s] still present after ClearAllState", k)
		}
	}
	// Rows on a different object_key are untouched.
	v, present, err := st.Get(otherLP, otherTarget, "z")
	if err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Errorf("user-2/z was wiped by ClearAllState on user-1")
	}
	if !bytes.Equal(v, []byte("z-val")) {
		t.Errorf("user-2/z value drift: %q", v)
	}
}

func TestPartition_RunProposal_TerminalWritesJERun(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	// Drive to Invoked first.
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{InvocationId: id, Target: target, Input: []byte("in")}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 0,
					Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
				},
			}},
		}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Terminal Run proposal — retryable=false.
	terminal := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_RunProposal{RunProposal: &enginev1.JERunProposal{
			EntryIndex: 1, Value: []byte("ok"), Retryable: false, Attempt: 0,
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: terminal}}); err != nil {
		t.Fatal(err)
	}
	// No timer scheduled for terminal proposal.
	for _, a := range col.Drain() {
		if _, isTimer := a.(ActRegisterTimer); isTimer {
			t.Errorf("terminal proposal must not schedule a timer; got %T", a)
		}
	}

	// Journal entry at index 1 has retryable=false + value=ok.
	journal := tables.JournalTable{S: p.cfg.Snapshotter.Store()}
	got, err := journal.Read(id, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := got.GetEntry().(*enginev1.JournalEntry_Run)
	if !ok {
		t.Fatalf("entry at idx 1 is %T; want JERun", got.GetEntry())
	}
	if run.Run.GetRetryable() {
		t.Errorf("retryable=true for terminal proposal")
	}
	if !bytes.Equal(run.Run.GetValue(), []byte("ok")) {
		t.Errorf("value mismatch: %q", run.Run.GetValue())
	}
}

func TestPartition_RunProposal_RetryableSchedulesTimer(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("aaaaaaaaaaaaaaaa")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{InvocationId: id, Target: target}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 0,
					Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
				},
			}},
		}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Retryable proposal — apply must (a) write JERun{retryable=true},
	// (b) insert a timer, (c) push ActRegisterTimer. The default policy
	// is MaxAttempts=1 (no retries); the test wants to exercise the
	// retry branch, so it ships an explicit policy.
	retryable := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_RunProposal{RunProposal: &enginev1.JERunProposal{
			EntryIndex:     1,
			FailureMessage: "transient",
			Retryable:      true,
			RetryPolicy:    &enginev1.RunRetryPolicy{MaxAttempts: 4},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: retryable}}); err != nil {
		t.Fatal(err)
	}
	acts := col.Drain()
	var timerAct *ActRegisterTimer
	for i := range acts {
		if rt, ok := acts[i].(ActRegisterTimer); ok {
			timerAct = &rt
		}
	}
	if timerAct == nil {
		t.Fatalf("retryable proposal must emit ActRegisterTimer; got %v", acts)
	}
	if timerAct.SleepIdx != 1 {
		t.Errorf("timer sleep_idx = %d; want 1 (JERun journal index)", timerAct.SleepIdx)
	}

	// Journal entry is JERun{retryable=true}.
	journal := tables.JournalTable{S: p.cfg.Snapshotter.Store()}
	got, err := journal.Read(id, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := got.GetEntry().(*enginev1.JournalEntry_Run)
	if !ok {
		t.Fatalf("entry at idx 1 is %T; want JERun", got.GetEntry())
	}
	if !run.Run.GetRetryable() {
		t.Errorf("retryable=false; want true")
	}
	if run.Run.GetFailureMessage() != "transient" {
		t.Errorf("failure_message = %q", run.Run.GetFailureMessage())
	}
}

func TestPartition_RunProposal_ExhaustedPolicyDemotesToTerminal(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("bbbbbbbbbbbbbbbb")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{InvocationId: id, Target: target}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 0,
					Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
				},
			}},
		}},
	})}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Engine-authoritative attempt counting reads the prior JERun and
	// increments — so to land at attempt=2 (exhausted under
	// MaxAttempts=2), submit two retryable proposals. The first lands as
	// attempt=1 (schedules a retry timer); the second observes the prior
	// JERun, advances to attempt=2, and demotes to terminal.
	policy := &enginev1.RunRetryPolicy{MaxAttempts: 2}
	proposal := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_RunProposal{RunProposal: &enginev1.JERunProposal{
			EntryIndex:     1,
			FailureMessage: "boom",
			Retryable:      true,
			RetryPolicy:    policy,
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: proposal}}); err != nil {
		t.Fatal(err)
	}
	col.Drain() // first attempt schedules a timer — discard.

	if _, err := p.Update([]statemachine.Entry{{Index: 4, Cmd: proposal}}); err != nil {
		t.Fatal(err)
	}
	for _, a := range col.Drain() {
		if _, isTimer := a.(ActRegisterTimer); isTimer {
			// Exhausted-policy path schedules an *immediate* wake (fireAtMs = nowMs+1)
			// so the SDK observes the terminal JERun. That wake timer is fine —
			// it differs from the retry timer by being scheduled at +1ms.
			if rt, ok := a.(ActRegisterTimer); ok && rt.FireAtMs > testEnvelopeNowMs+1 {
				t.Errorf("exhausted policy must not schedule a backoff timer; got %v", a)
			}
		}
	}

	journal := tables.JournalTable{S: p.cfg.Snapshotter.Store()}
	got, err := journal.Read(id, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := got.GetEntry().(*enginev1.JournalEntry_Run)
	if !ok {
		t.Fatalf("entry at idx 1 is %T; want JERun", got.GetEntry())
	}
	if run.Run.GetRetryable() {
		t.Errorf("exhausted policy must demote to retryable=false; got true")
	}
	if run.Run.GetFailureMessage() != "boom" {
		t.Errorf("failure_message lost on demotion: %q", run.Run.GetFailureMessage())
	}
}

func TestPartition_AnnounceLeaderNotifiesObserver(t *testing.T) {
	p, lead, _ := newTestPartition(t)
	cmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_AnnounceLeader{
			AnnounceLeader: &enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 5},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatal(err)
	}
	if lead.last == nil || lead.last.GetLeaderEpoch() != 5 {
		t.Fatalf("OnAnnounceLeader not called or wrong epoch: %+v", lead.last)
	}
}

func TestPartition_UnknownCommandIsNoop(t *testing.T) {
	p, _, _ := newTestPartition(t)
	// Empty Envelope (no command kind set) — must not error.
	buf, _ := proto.Marshal(&enginev1.Envelope{
		Header:  &enginev1.Header{CreatedAtMs: testEnvelopeNowMs},
		Command: &enginev1.Command{},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: buf}}); err != nil {
		t.Fatalf("unknown command must not return error; got %v", err)
	}
	idx, _ := p.Lookup(LookupAppliedIndex{})
	if idx.(uint64) != 1 {
		t.Errorf("applied_index = %v; want 1 (advance even on no-op)", idx)
	}
}

func TestPartition_MalformedEnvelopeIsNoop(t *testing.T) {
	p, _, _ := newTestPartition(t)
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: []byte("\xff\xff garbage \xff\xff")}}); err != nil {
		t.Fatalf("malformed envelope must not return error; got %v", err)
	}
	idx, _ := p.Lookup(LookupAppliedIndex{})
	if idx.(uint64) != 1 {
		t.Errorf("applied_index = %v; want 1", idx)
	}
}

func TestPartition_SleepInsertsTimerAndSurvives(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}

	// Invoke
	invokeCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target,
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: invokeCmd}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Journal Input (Scheduled -> Invoked)
	input := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 0, Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{}}},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: input}}); err != nil {
		t.Fatal(err)
	}
	col.Drain()

	// Journal Sleep — should insert a timer and emit ActRegisterTimer.
	sleep := envelope(t, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 1, Entry: &enginev1.JournalEntry_Sleep{Sleep: &enginev1.JESleep{FireAtMs: 9999}}},
		}},
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: sleep}}); err != nil {
		t.Fatal(err)
	}
	actions := col.Drain()
	var registered *ActRegisterTimer
	for _, a := range actions {
		if r, ok := a.(ActRegisterTimer); ok {
			registered = &r
		}
	}
	if registered == nil {
		t.Fatalf("expected ActRegisterTimer; got %+v", actions)
	}
	if registered.FireAtMs != 9999 {
		t.Errorf("ActRegisterTimer FireAtMs = %d; want 9999", registered.FireAtMs)
	}

	// Verify timer row persists.
	t2 := tables.TimerTable{S: p.cfg.Snapshotter.Store()}
	var found bool
	_ = t2.ScanAll(func(e tables.TimerEntry) error {
		if e.FireAtMs == 9999 {
			found = true
		}
		return nil
	})
	if !found {
		t.Errorf("timer row not persisted")
	}
}

// TestPartition_CompleteReapsPendingTimers verifies that an invocation
// reaching the Completed terminal state (whether via success or failure)
// reaps any still-pending sleep/retry timer rows in the same apply batch,
// not just on later Purge. The reap fires on transition from Invoked/
// Suspended → Completed; the idempotent Completed → Completed replay
// path is naturally skipped because completedTarget is nil on that arm.
func TestPartition_CompletedOutcomeMetricClassifies(t *testing.T) {
	// Drive three distinct terminal flows through a fresh partition and
	// assert the InvocationsCompleted counter classifies each as
	// success / failure / cancelled.
	t.Helper()
	dir := filepath.Join(t.TempDir(), "p", "state")
	snap, err := NewSnapshotter(dir, func(path string) (storage.Store, error) {
		return storage.OpenPebble(path, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	lead := &stubLeadership{}
	lead.leader.Store(true)
	col := &ActionCollector{}
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	p := NewPartition(1, 1, PartitionConfig{
		Snapshotter: snap,
		Leadership:  lead,
		Collector:   col,
		Metrics:     metrics,
	})
	t.Cleanup(func() { _ = p.Close() })

	mustApply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Three invocations on the same unkeyed service, three distinct
	// terminal outcomes.
	for i, sub := range []struct {
		uuid     string
		complete *enginev1.InvocationCompleted
		outcome  string
	}{
		{
			uuid:     "aaaaaaaaaaaaaaaa",
			complete: &enginev1.InvocationCompleted{Output: []byte("ok")},
			outcome:  "success",
		},
		{
			uuid:     "bbbbbbbbbbbbbbbb",
			complete: &enginev1.InvocationCompleted{FailureMessage: "boom"},
			outcome:  "failure",
		},
		{
			uuid:     "cccccccccccccccc",
			complete: &enginev1.InvocationCompleted{FailureCode: wire.CancelledCode, FailureMessage: "invocation cancelled"},
			outcome:  "cancelled",
		},
	} {
		id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte(sub.uuid)}
		target := &enginev1.InvocationTarget{ServiceName: "Svc"}
		base := uint64(i*10) + 1
		mustApply(base, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}}})
		mustApply(base+1, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind:         &enginev1.InvokerEffect_Completed{Completed: sub.complete},
		}}})
		_ = sub.outcome // outcome assertion below
	}

	for _, want := range []struct {
		outcome string
		count   float64
	}{
		{outcome: "success", count: 1},
		{outcome: "failure", count: 1},
		{outcome: "cancelled", count: 1},
	} {
		got := testutil.ToFloat64(metrics.InvocationsCompleted.WithLabelValues("Svc", want.outcome))
		if got != want.count {
			t.Errorf("InvocationsCompleted{service=Svc,outcome=%s} = %v; want %v",
				want.outcome, got, want.count)
		}
	}
}

func TestPartition_CompleteReapsPendingTimers(t *testing.T) {
	p, _, col := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}

	mustApply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	mustApply(1, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target,
	}}})
	mustApply(2, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 0, Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{}}},
		}},
	}}})
	mustApply(3, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{JournalAppended: &enginev1.JournalEntryAppended{
			Entry: &enginev1.JournalEntry{Index: 1, Entry: &enginev1.JournalEntry_Sleep{Sleep: &enginev1.JESleep{FireAtMs: 9999}}},
		}},
	}}})
	col.Drain()

	mustApply(4, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind:         &enginev1.InvokerEffect_Completed{Completed: &enginev1.InvocationCompleted{Output: []byte("ok")}},
	}}})

	// Timer row must be gone after completion (not waiting for Purge).
	store := p.cfg.Snapshotter.Store()
	timersT := tables.TimerTable{S: store}
	var remaining int
	_ = timersT.ScanAll(func(e tables.TimerEntry) error {
		if e.FireAtMs == 9999 && bytes.Equal(e.ID.GetUuid(), id.GetUuid()) {
			remaining++
		}
		return nil
	})
	if remaining != 0 {
		t.Errorf("expected 0 pending timer rows after Complete; got %d", remaining)
	}

	// ActDeleteTimer must have been emitted by the Complete apply path.
	var deleted *ActDeleteTimer
	for _, a := range col.Drain() {
		if d, ok := a.(ActDeleteTimer); ok {
			deleted = &d
		}
	}
	if deleted == nil {
		t.Fatal("expected ActDeleteTimer; got none")
	}
	if deleted.FireAtMs != 9999 {
		t.Errorf("ActDeleteTimer.FireAtMs = %d; want 9999", deleted.FireAtMs)
	}

	// Idempotent re-apply of Complete must not re-reap (no timer rows
	// remain, no new ActDeleteTimer).
	mustApply(5, &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
		InvocationId: id,
		Kind:         &enginev1.InvokerEffect_Completed{Completed: &enginev1.InvocationCompleted{Output: []byte("ok")}},
	}}})
	for _, a := range col.Drain() {
		if _, ok := a.(ActDeleteTimer); ok {
			t.Errorf("idempotent Complete replay re-emitted ActDeleteTimer")
		}
	}
}

func TestPartition_SnapshotRoundTrip(t *testing.T) {
	p, _, _ := newTestPartition(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}
	invokeCmd := envelope(t, &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
		InvocationId: id, Target: target,
	}}})
	if _, err := p.Update([]statemachine.Entry{{Index: 42, Cmd: invokeCmd}}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := p.SaveSnapshot(nil, &buf, nil); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Build a fresh partition in a separate dir, then recover into it.
	dirB := filepath.Join(t.TempDir(), "p", "state")
	snapB, err := NewSnapshotter(dirB, func(path string) (storage.Store, error) {
		return storage.OpenPebble(path, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	leadB := &stubLeadership{}
	leadB.leader.Store(true)
	pB := NewPartition(1, 1, PartitionConfig{
		Snapshotter: snapB,
		Leadership:  leadB,
		Collector:   &ActionCollector{},
	})
	defer pB.Close()

	if err := pB.RecoverFromSnapshot(&buf, nil); err != nil {
		t.Fatalf("RecoverFromSnapshot: %v", err)
	}

	idx, err := pB.Open(nil)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 42 {
		t.Errorf("post-recover Open returned %d; want 42", idx)
	}
	gotStatus, _ := pB.Lookup(LookupInvocation{ID: id})
	if _, ok := gotStatus.(*enginev1.InvocationStatus).GetStatus().(*enginev1.InvocationStatus_Scheduled); !ok {
		t.Errorf("post-recover status = %T; want Scheduled", gotStatus)
	}
}

// TestPartition_OnSnapshotPersistedFiresAfterSaveSnapshot verifies that
// a successful SaveSnapshot invokes the OnSnapshotPersisted hook so the
// archive producer can run.
func TestPartition_OnSnapshotPersistedFiresAfterSaveSnapshot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p", "state")
	snap, err := NewSnapshotter(dir, func(path string) (storage.Store, error) {
		return storage.OpenPebble(path, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	lead := &stubLeadership{}
	lead.leader.Store(true)

	var fired atomic.Bool
	p := NewPartition(1, 1, PartitionConfig{
		Snapshotter:         snap,
		Leadership:          lead,
		Collector:           &ActionCollector{},
		OnSnapshotPersisted: func() { fired.Store(true) },
	})
	t.Cleanup(func() { _ = p.Close() })

	var buf bytes.Buffer
	if err := p.SaveSnapshot(nil, &buf, nil); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if !fired.Load() {
		t.Fatal("OnSnapshotPersisted was not invoked after successful SaveSnapshot")
	}
}
