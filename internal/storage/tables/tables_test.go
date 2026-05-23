package tables_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

type openFn func(t *testing.T) storage.Store

// testLP is the canonical LP used by per-test setups that don't care about
// realistic LP derivation — pick a constant so Put/Get share the same row.
const testLP uint32 = 7

// testTenant is the canonical tenant id used by per-test setups. The
// default-tenant sentinel (keys.TenantDefault == 0) is the right choice
// here — tests should be byte-equivalent to the pre-tenant world modulo
// the 4-byte zero prefix, so existing invariants and seed data continue
// to hold.
const testTenant uint32 = 0

func mkID(pk uint64, uuid string) *enginev1.InvocationId {
	return &enginev1.InvocationId{PartitionKey: pk, Uuid: []byte(uuid)}
}

func commit(t *testing.T, b storage.Batch) {
	t.Helper()
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()
}

func runTablesSuite(t *testing.T, name string, open openFn) {
	t.Helper()

	t.Run(name+"/Meta_GetMissingReturnsZero", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		m, err := tables.MetaTable{S: s}.Get()
		if err != nil {
			t.Fatal(err)
		}
		if m.GetAppliedIndex() != 0 || m.GetLeaderEpoch() != 0 {
			t.Errorf("expected zero meta, got %+v", m)
		}
	})

	t.Run(name+"/Meta_PutGet", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		mt := tables.MetaTable{S: s}
		b := s.NewBatch()
		if err := mt.Put(b, &enginev1.PartitionMeta{
			AppliedIndex:         42,
			LeaderEpoch:          7,
			LatestAnnouncedEpoch: 7,
		}); err != nil {
			t.Fatal(err)
		}
		commit(t, b)
		got, err := mt.Get()
		if err != nil {
			t.Fatal(err)
		}
		if got.GetAppliedIndex() != 42 || got.GetLeaderEpoch() != 7 {
			t.Errorf("meta mismatch: %+v", got)
		}
	})

	t.Run(name+"/Invocation_MissingReturnsFree", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		id := mkID(1, "0123456789abcdef")
		st, err := tables.InvocationTable{S: s}.Get(testTenant, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, isFree := st.GetStatus().(*enginev1.InvocationStatus_Free); !isFree {
			t.Errorf("expected Free, got %T", st.GetStatus())
		}
	})

	t.Run(name+"/Invocation_PutGetDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		it := tables.InvocationTable{S: s}
		id := mkID(1, "0123456789abcdef")
		put := &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Invoked{
				Invoked: &enginev1.Invoked{
					Target:      &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"},
					CreatedAtMs: 100,
					InvokedAtMs: 200,
				},
			},
		}
		b := s.NewBatch()
		if err := it.Put(b, testTenant, id, put); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := it.Get(testTenant, id)
		if err != nil {
			t.Fatal(err)
		}
		gi, ok := got.GetStatus().(*enginev1.InvocationStatus_Invoked)
		if !ok {
			t.Fatalf("status type = %T; want Invoked", got.GetStatus())
		}
		if gi.Invoked.GetCreatedAtMs() != 100 || gi.Invoked.GetInvokedAtMs() != 200 {
			t.Errorf("timestamps mismatch: %+v", gi.Invoked)
		}

		b2 := s.NewBatch()
		if err := it.Delete(b2, testTenant, id); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)

		got2, err := it.Get(testTenant, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, free := got2.GetStatus().(*enginev1.InvocationStatus_Free); !free {
			t.Errorf("expected Free after delete, got %T", got2.GetStatus())
		}
	})

	t.Run(name+"/Invocation_ScanAllSkipsFree", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		it := tables.InvocationTable{S: s}
		idA := mkID(1, "aaaaaaaaaaaaaaaa")
		idB := mkID(2, "bbbbbbbbbbbbbbbb")

		b := s.NewBatch()
		_ = it.Put(b, testTenant, idA, &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Scheduled{
				Scheduled: &enginev1.Scheduled{Target: &enginev1.InvocationTarget{ServiceName: "A"}},
			},
		})
		_ = it.Put(b, testTenant, idB, &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Free{Free: &enginev1.Free{}},
		})
		commit(t, b)

		var seen []string
		if err := it.ScanAll(context.Background(), func(_ uint32, id *enginev1.InvocationId, _ *enginev1.InvocationStatus) error {
			seen = append(seen, string(id.GetUuid()))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(seen) != 1 || seen[0] != "aaaaaaaaaaaaaaaa" {
			t.Errorf("scan = %v; want [aaaa...]", seen)
		}
	})

	t.Run(name+"/Invocation_ScanLPIsolatesOneLP", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		it := tables.InvocationTable{S: s}
		// Three invocations with distinct partition_keys whose LPs differ.
		// Insert; ScanLP for each LP must yield exactly its own invocation.
		ids := []*enginev1.InvocationId{
			mkID(0x1000, "aaaaaaaaaaaaaaaa"),
			mkID(0x1001, "bbbbbbbbbbbbbbbb"),
			mkID(0x1002, "cccccccccccccccc"),
		}
		// Skip if any LPs collide (cheap defense, unlikely for these pks).
		seen := map[uint32]bool{}
		for _, id := range ids {
			lp := keys.LPFromPartitionKey(id.GetPartitionKey())
			if seen[lp] {
				t.Skipf("test partition_keys collided on LP: %d", lp)
			}
			seen[lp] = true
		}
		b := s.NewBatch()
		for _, id := range ids {
			_ = it.Put(b, testTenant, id, &enginev1.InvocationStatus{
				Status: &enginev1.InvocationStatus_Scheduled{
					Scheduled: &enginev1.Scheduled{Target: &enginev1.InvocationTarget{ServiceName: "S"}},
				},
			})
		}
		commit(t, b)

		for _, id := range ids {
			lp := keys.LPFromPartitionKey(id.GetPartitionKey())
			var found []*enginev1.InvocationId
			if err := it.ScanLP(context.Background(), lp, func(_ uint32, gotID *enginev1.InvocationId, _ *enginev1.InvocationStatus) error {
				found = append(found, gotID)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(found) != 1 || !bytes.Equal(found[0].GetUuid(), id.GetUuid()) {
				t.Errorf("ScanLP(%d) = %+v; want [%s]", lp, found, id.GetUuid())
			}
		}
	})

	t.Run(name+"/Journal_AppendReadScanDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		jt := tables.JournalTable{S: s}
		id := mkID(1, "0123456789abcdef")

		b := s.NewBatch()
		for i := range uint32(5) {
			if err := jt.Append(b, testTenant, id, &enginev1.JournalEntry{
				Index: i,
				Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte{byte(i)}}},
			}); err != nil {
				t.Fatal(err)
			}
		}
		commit(t, b)

		// Read single
		got, err := jt.Read(testTenant, id, 3)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetIndex() != 3 {
			t.Errorf("read index = %d; want 3", got.GetIndex())
		}

		// Missing
		if _, err := jt.Read(testTenant, id, 99); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}

		// Scan in order
		var indexes []uint32
		if err := jt.Scan(testTenant, id, func(e *enginev1.JournalEntry) error {
			indexes = append(indexes, e.GetIndex())
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		want := []uint32{0, 1, 2, 3, 4}
		if len(indexes) != len(want) {
			t.Fatalf("scan len = %d; want %d (%v)", len(indexes), len(want), indexes)
		}
		for i := range want {
			if indexes[i] != want[i] {
				t.Errorf("scan[%d] = %d; want %d", i, indexes[i], want[i])
			}
		}

		// Delete prefix
		b2 := s.NewBatch()
		if err := jt.DeletePrefix(b2, testTenant, id); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		if _, err := jt.Read(testTenant, id, 0); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("expected NotFound after DeletePrefix, got %v", err)
		}
	})

	// Lock the JournalTable.Append contract that the apply path relies on
	// when re-running InvokerEffects on replay (e.g., parent-side
	// JECallResult written from a callee's Completed effect). Two re-appends
	// of the same entry must converge to the same stored bytes. The FSM
	// guarantees content-determinism per (id, index); the storage layer is
	// last-write-wins, which is safe under that guarantee.
	t.Run(name+"/Journal_AppendIdempotentOnDeterministicReplay", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		jt := tables.JournalTable{S: s}
		id := mkID(1, "ffffffffffffffff")

		entry := &enginev1.JournalEntry{
			Index: 7,
			Entry: &enginev1.JournalEntry_CallResult{CallResult: &enginev1.JECallResult{
				CallIndex: 6,
				Result:    []byte("v1"),
			}},
		}

		// First apply.
		b1 := s.NewBatch()
		if err := jt.Append(b1, testTenant, id, entry); err != nil {
			t.Fatal(err)
		}
		commit(t, b1)

		// Replay re-applies the same content.
		b2 := s.NewBatch()
		if err := jt.Append(b2, testTenant, id, entry); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)

		got, err := jt.Read(testTenant, id, 7)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetCallResult().GetCallIndex() != 6 || string(got.GetCallResult().GetResult()) != "v1" {
			t.Errorf("after replay: got call_index=%d result=%q; want 6/v1",
				got.GetCallResult().GetCallIndex(), got.GetCallResult().GetResult())
		}
	})

	t.Run(name+"/Timer_InsertScanDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		tt := tables.TimerTable{S: s}
		idA := mkID(1, "aaaaaaaaaaaaaaaa")
		idB := mkID(2, "bbbbbbbbbbbbbbbb")

		b := s.NewBatch()
		_ = tt.Insert(b, testTenant, 200, idB, 1)
		_ = tt.Insert(b, testTenant, 100, idA, 0)
		_ = tt.Insert(b, testTenant, 300, idA, 2)
		commit(t, b)

		// ScanAll yields all three in order
		var all []tables.TimerEntry
		if err := tt.ScanAll(func(e tables.TimerEntry) error {
			all = append(all, e)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(all) != 3 || all[0].FireAtMs != 100 || all[1].FireAtMs != 200 || all[2].FireAtMs != 300 {
			t.Errorf("ScanAll order wrong: %+v", all)
		}

		// Delete and rescan
		b2 := s.NewBatch()
		_ = tt.Delete(b2, testTenant, 200, idB)
		commit(t, b2)
		all = nil
		_ = tt.ScanAll(func(e tables.TimerEntry) error {
			all = append(all, e)
			return nil
		})
		if len(all) != 2 {
			t.Errorf("after delete: len(all) = %d; want 2", len(all))
		}
	})

	t.Run(name+"/Timer_DualWriteLPIndex", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		tt := tables.TimerTable{S: s}
		// Pick partition_keys whose low 12 bits differ so their LPs are
		// distinct. (0x1000 & 0xFFF == 0; 0x2000 & 0xFFF == 0 too — both
		// reduce to LP 0 because LPCount is 4096 and these are multiples.)
		idA := mkID(0x1001, "aaaaaaaaaaaaaaaa")
		idB := mkID(0x2002, "bbbbbbbbbbbbbbbb")

		b := s.NewBatch()
		_ = tt.Insert(b, testTenant, 100, idA, 0)
		_ = tt.Insert(b, testTenant, 200, idB, 1)
		commit(t, b)

		// Primary timer/ rows: should have two.
		var primary []tables.TimerEntry
		_ = tt.ScanAll(func(e tables.TimerEntry) error {
			primary = append(primary, e)
			return nil
		})
		if len(primary) != 2 {
			t.Fatalf("primary count = %d; want 2", len(primary))
		}

		// timer_lp/ per-LP scan: idA's LP returns idA only.
		lpA := keys.LPFromPartitionKey(idA.GetPartitionKey())
		var seenA []tables.TimerEntry
		if err := tt.ScanLP(lpA, func(e tables.TimerEntry) error {
			seenA = append(seenA, e)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(seenA) != 1 || seenA[0].FireAtMs != 100 || !bytes.Equal(seenA[0].ID.GetUuid(), idA.GetUuid()) {
			t.Errorf("ScanLP(%d) = %+v; want [{100, idA}]", lpA, seenA)
		}

		// Delete idA's timer: both primary and secondary indexes should drop.
		b2 := s.NewBatch()
		_ = tt.Delete(b2, testTenant, 100, idA)
		commit(t, b2)

		seenA = nil
		_ = tt.ScanLP(lpA, func(e tables.TimerEntry) error {
			seenA = append(seenA, e)
			return nil
		})
		if len(seenA) != 0 {
			t.Errorf("after delete: timer_lp/ row for idA still present: %+v", seenA)
		}
		// Primary side: only idB's row remains.
		primary = nil
		_ = tt.ScanAll(func(e tables.TimerEntry) error {
			primary = append(primary, e)
			return nil
		})
		if len(primary) != 1 || !bytes.Equal(primary[0].ID.GetUuid(), idB.GetUuid()) {
			t.Errorf("after delete: primary = %+v; want only idB", primary)
		}
	})

	t.Run(name+"/Dedup_SelfProposalDedupes", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		dt := tables.DedupTable{S: s}

		d1 := &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
			SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: 1, Seq: 5},
		}}
		// First time: not duplicate. Self dedup ignores lp.
		dup, err := dt.IsDuplicate(testLP, testTenant, d1)
		if err != nil {
			t.Fatal(err)
		}
		if dup {
			t.Fatal("first IsDuplicate should be false")
		}
		// Record.
		b := s.NewBatch()
		if err := dt.Record(b, testLP, testTenant, d1); err != nil {
			t.Fatal(err)
		}
		commit(t, b)
		// Same Dedup is now a duplicate.
		dup, _ = dt.IsDuplicate(testLP, testTenant, d1)
		if !dup {
			t.Fatal("second IsDuplicate should be true")
		}
		// Self dedup ignores lp — a different lp still sees the dup.
		dup, _ = dt.IsDuplicate(testLP+1, testTenant, d1)
		if !dup {
			t.Fatal("self dedup must be shard-scoped (lp-independent)")
		}
		// Exact-match keying: a lower-seq propose in the same epoch is a
		// FRESH entry, not a duplicate. The old high-water-mark scheme
		// would have rejected it, but that scheme false-positived when
		// concurrent goroutines allocated seq atomically and submitted to
		// raft out-of-order.
		dLower := &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
			SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: 1, Seq: 3},
		}}
		dup, _ = dt.IsDuplicate(testLP, testTenant, dLower)
		if dup {
			t.Fatal("lower-seq must not be dup under exact-match keying")
		}
		// A higher seq is not a duplicate either — fresh entry.
		dHigher := &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
			SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: 1, Seq: 6},
		}}
		dup, _ = dt.IsDuplicate(testLP, testTenant, dHigher)
		if dup {
			t.Fatal("higher seq should not be dup")
		}
		// Different epoch tracks independently.
		dEpoch2 := &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
			SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: 2, Seq: 1},
		}}
		dup, _ = dt.IsDuplicate(testLP, testTenant, dEpoch2)
		if dup {
			t.Fatal("different epoch must not be dup")
		}
	})

	t.Run(name+"/Dedup_Arbitrary", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		dt := tables.DedupTable{S: s}
		d := &enginev1.Dedup{Kind: &enginev1.Dedup_Arbitrary{
			Arbitrary: &enginev1.ArbitraryDedup{ProducerId: "client-x", Seq: 10},
		}}
		dup, _ := dt.IsDuplicate(testLP, testTenant, d)
		if dup {
			t.Fatal("first should not be dup")
		}
		b := s.NewBatch()
		_ = dt.Record(b, testLP, testTenant, d)
		commit(t, b)
		dup, _ = dt.IsDuplicate(testLP, testTenant, d)
		if !dup {
			t.Fatal("second should be dup")
		}
	})

	t.Run(name+"/Dedup_ArbitraryLPPartitions", func(t *testing.T) {
		// Arbitrary dedup is LP-prefixed: the same (producer, seq) under
		// two different LPs occupies two distinct rows, so recording at
		// LP A must not flag a fresh propose at LP B as duplicate.
		// Regression guard for the LP-prefix refactor.
		s := open(t)
		defer s.Close()
		dt := tables.DedupTable{S: s}
		d := &enginev1.Dedup{Kind: &enginev1.Dedup_Arbitrary{
			Arbitrary: &enginev1.ArbitraryDedup{ProducerId: "outbox/p1", Seq: 42},
		}}
		// Record under LP 7.
		b := s.NewBatch()
		if err := dt.Record(b, 7, testTenant, d); err != nil {
			t.Fatal(err)
		}
		commit(t, b)
		if dup, _ := dt.IsDuplicate(7, testTenant, d); !dup {
			t.Fatal("LP 7: second must be dup")
		}
		// Same (producer, seq) under LP 8 — a different logical partition
		// — must NOT be dup.
		if dup, _ := dt.IsDuplicate(8, testTenant, d); dup {
			t.Fatal("LP 8 must not see LP 7's row as dup")
		}
		// The LPNoLP sentinel is also a distinct slot.
		if dup, _ := dt.IsDuplicate(keys.LPNoLP, testTenant, d); dup {
			t.Fatal("LPNoLP must not see LP 7's row as dup")
		}
	})

	t.Run(name+"/Dedup_GCSelfBelowEpoch", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		dt := tables.DedupTable{S: s}
		// Record self-dedup rows at epochs 1, 2, 3 (with seq=1 each) plus
		// an arbitrary row that GC must NOT touch.
		rec := func(d *enginev1.Dedup) {
			b := s.NewBatch()
			if err := dt.Record(b, testLP, testTenant, d); err != nil {
				t.Fatal(err)
			}
			commit(t, b)
		}
		mk := func(epoch uint64) *enginev1.Dedup {
			return &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
				SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: epoch, Seq: 1},
			}}
		}
		rec(mk(1))
		rec(mk(2))
		rec(mk(3))
		arb := &enginev1.Dedup{Kind: &enginev1.Dedup_Arbitrary{
			Arbitrary: &enginev1.ArbitraryDedup{ProducerId: "ingress-1", Seq: 7},
		}}
		rec(arb)

		// GC strictly below epoch=3: epoch 1 and 2 vanish; epoch 3 survives.
		b := s.NewBatch()
		if err := dt.GCSelfBelowEpoch(b, 3); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		if dup, _ := dt.IsDuplicate(testLP, testTenant, mk(1)); dup {
			t.Error("epoch=1 should be GC'd")
		}
		if dup, _ := dt.IsDuplicate(testLP, testTenant, mk(2)); dup {
			t.Error("epoch=2 should be GC'd")
		}
		if dup, _ := dt.IsDuplicate(testLP, testTenant, mk(3)); !dup {
			t.Error("epoch=3 must survive GC below=3")
		}
		if dup, _ := dt.IsDuplicate(testLP, testTenant, arb); !dup {
			t.Error("Arbitrary dedup must not be touched by Self GC")
		}

		// GCSelfBelowEpoch(0) is a no-op.
		b = s.NewBatch()
		if err := dt.GCSelfBelowEpoch(b, 0); err != nil {
			t.Fatal(err)
		}
		commit(t, b)
		if dup, _ := dt.IsDuplicate(testLP, testTenant, mk(3)); !dup {
			t.Error("GC(0) must not delete anything")
		}
	})

	t.Run(name+"/Dedup_NilOrEmpty", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		dt := tables.DedupTable{S: s}
		// nil Dedup: not duplicate.
		dup, err := dt.IsDuplicate(testLP, testTenant, nil)
		if err != nil || dup {
			t.Errorf("nil dedup: dup=%v err=%v", dup, err)
		}
		// Empty (kind unset): also not duplicate.
		dup, err = dt.IsDuplicate(testLP, testTenant, &enginev1.Dedup{})
		if err != nil || dup {
			t.Errorf("empty dedup: dup=%v err=%v", dup, err)
		}
	})

	t.Run(name+"/State_SetGetClear", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		st := tables.StateTable{S: s}
		target := &enginev1.InvocationTarget{ServiceName: "Greeter", ObjectKey: "alice"}

		// Missing key: (nil, false, nil).
		v, ok, err := st.Get(testLP, testTenant, target, "balance")
		if err != nil || ok || v != nil {
			t.Errorf("missing key: v=%v ok=%v err=%v", v, ok, err)
		}

		b := s.NewBatch()
		if err := st.Set(b, testLP, testTenant, target, "balance", []byte{0x10}); err != nil {
			t.Fatal(err)
		}
		if err := st.Set(b, testLP, testTenant, target, "name", []byte("Alice")); err != nil {
			t.Fatal(err)
		}
		// Present-but-empty must be distinguishable from missing.
		if err := st.Set(b, testLP, testTenant, target, "empty", []byte{}); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		v, ok, _ = st.Get(testLP, testTenant, target, "balance")
		if !ok || !bytes.Equal(v, []byte{0x10}) {
			t.Errorf("balance: v=%v ok=%v", v, ok)
		}
		v, ok, _ = st.Get(testLP, testTenant, target, "empty")
		if !ok || len(v) != 0 {
			t.Errorf("empty: v=%v ok=%v (want ok && len==0)", v, ok)
		}

		// Clear removes the row.
		b2 := s.NewBatch()
		if err := st.Clear(b2, testLP, testTenant, target, "balance"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		_, ok, _ = st.Get(testLP, testTenant, target, "balance")
		if ok {
			t.Error("balance still present after Clear")
		}
	})

	t.Run(name+"/State_ScanObjectIsolation", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		st := tables.StateTable{S: s}
		alice := &enginev1.InvocationTarget{ServiceName: "Svc", ObjectKey: "alice"}
		bob := &enginev1.InvocationTarget{ServiceName: "Svc", ObjectKey: "bob"}
		other := &enginev1.InvocationTarget{ServiceName: "Other", ObjectKey: "alice"}
		unkeyed := &enginev1.InvocationTarget{ServiceName: "Unkeyed", ObjectKey: ""}

		b := s.NewBatch()
		_ = st.Set(b, testLP, testTenant, alice, "a", []byte("A"))
		_ = st.Set(b, testLP, testTenant, alice, "z", []byte("Z"))
		_ = st.Set(b, testLP, testTenant, bob, "a", []byte("Bob-A"))
		_ = st.Set(b, testLP, testTenant, other, "a", []byte("Other-A"))
		_ = st.Set(b, testLP, testTenant, unkeyed, "cfg", []byte("U"))
		commit(t, b)

		var pairs []string
		if err := st.ScanObject(testLP, testTenant, alice, func(k string, v []byte) error {
			pairs = append(pairs, k+"="+string(v))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(pairs) != 2 || pairs[0] != "a=A" || pairs[1] != "z=Z" {
			t.Errorf("alice scan = %v; want [a=A z=Z]", pairs)
		}

		// Unkeyed service scan returns its own rows only.
		var uPairs []string
		_ = st.ScanObject(testLP, testTenant, unkeyed, func(k string, v []byte) error {
			uPairs = append(uPairs, k+"="+string(v))
			return nil
		})
		if len(uPairs) != 1 || uPairs[0] != "cfg=U" {
			t.Errorf("unkeyed scan = %v; want [cfg=U]", uPairs)
		}
	})

	t.Run(name+"/Outbox_AppendPop", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		ot := tables.OutboxTable{S: s}

		env1 := &enginev1.OutboxEnvelope{
			Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: mkID(1, "0123456789abcdef"),
				Target:       &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"},
				Input:        []byte("payload"),
			}},
		}
		b := s.NewBatch()
		if err := ot.Append(b, 1, env1); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		var rows []tables.OutboxRow
		if err := ot.ScanFrom(0, func(r tables.OutboxRow) error {
			rows = append(rows, r)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Seq != 1 {
			t.Fatalf("ScanFrom = %+v; want one row at seq=1", rows)
		}
		if inv := rows[0].Envelope.GetInvoke(); inv == nil || string(inv.GetInput()) != "payload" {
			t.Errorf("envelope: %+v", rows[0].Envelope)
		}

		b2 := s.NewBatch()
		if err := ot.Pop(b2, 1); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		rows = nil
		if err := ot.ScanFrom(0, func(r tables.OutboxRow) error {
			rows = append(rows, r)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(rows) != 0 {
			t.Errorf("after pop: %d rows; want 0", len(rows))
		}
	})

	t.Run(name+"/Outbox_OrderingFifo", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		ot := tables.OutboxTable{S: s}

		// Insert out of order; ScanFrom must yield ascending seq.
		seqs := []uint64{5, 1, 7, 2, 100, 3}
		b := s.NewBatch()
		for _, seq := range seqs {
			env := &enginev1.OutboxEnvelope{
				Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
					InvocationId: mkID(seq, "0123456789abcdef"),
					Target:       &enginev1.InvocationTarget{ServiceName: "S"},
				}},
			}
			if err := ot.Append(b, seq, env); err != nil {
				t.Fatal(err)
			}
		}
		commit(t, b)

		var got []uint64
		if err := ot.ScanFrom(0, func(row tables.OutboxRow) error {
			got = append(got, row.Seq)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		want := []uint64{1, 2, 3, 5, 7, 100}
		if len(got) != len(want) {
			t.Fatalf("scan len = %d; want %d (%v)", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("scan[%d] = %d; want %d", i, got[i], want[i])
			}
		}

		// ScanFrom(seq=3) skips earlier rows.
		got = got[:0]
		_ = ot.ScanFrom(3, func(row tables.OutboxRow) error {
			got = append(got, row.Seq)
			return nil
		})
		want2 := []uint64{3, 5, 7, 100}
		if len(got) != len(want2) || got[0] != 3 {
			t.Errorf("ScanFrom(3) = %v; want %v", got, want2)
		}
	})

	t.Run(name+"/Awakeable_PutGetDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		at := tables.AwakeableTable{S: s}
		id := "awk_AAAAAAAAAAAAAAAAAAAAAA"
		owner := mkID(42, "0123456789abcdef")
		entry := &enginev1.AwakeableEntry{Owner: owner, EntryIndex: 7}

		// Missing.
		if _, err := at.Get(testTenant, id); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("missing: %v; want ErrNotFound", err)
		}

		b := s.NewBatch()
		if err := at.Put(b, testTenant, id, entry); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := at.Get(testTenant, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetOwner().GetPartitionKey() != 42 || got.GetEntryIndex() != 7 {
			t.Errorf("roundtrip: %+v", got)
		}

		b2 := s.NewBatch()
		if err := at.Delete(b2, testTenant, id); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		if _, err := at.Get(testTenant, id); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("after delete: %v; want ErrNotFound", err)
		}
	})

	t.Run(name+"/SignalInbox_PutGetDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		it := tables.SignalInboxTable{S: s}
		id := mkID(42, "0123456789abcdef")

		// Missing returns (nil, nil) — distinguishes from ErrNotFound.
		got, err := it.Get(testTenant, id, "ready")
		if err != nil || got != nil {
			t.Errorf("missing: got=%+v err=%v; want (nil, nil)", got, err)
		}

		entry := &enginev1.SignalInboxEntry{
			SignalName:    "ready",
			Payload:       []byte("payload-1"),
			DeliveredAtMs: 12345,
		}
		b := s.NewBatch()
		if err := it.Put(b, testTenant, id, "ready", entry); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err = it.Get(testTenant, id, "ready")
		if err != nil {
			t.Fatal(err)
		}
		if got.GetSignalName() != "ready" || string(got.GetPayload()) != "payload-1" || got.GetDeliveredAtMs() != 12345 {
			t.Errorf("roundtrip: %+v", got)
		}

		b2 := s.NewBatch()
		if err := it.Delete(b2, testTenant, id, "ready"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		got, err = it.Get(testTenant, id, "ready")
		if err != nil || got != nil {
			t.Errorf("after delete: got=%+v err=%v; want (nil, nil)", got, err)
		}
	})

	t.Run(name+"/SignalInbox_DeleteAllForInvocation", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		it := tables.SignalInboxTable{S: s}
		id := mkID(42, "0123456789abcdef")
		other := mkID(42, "fedcba9876543210")

		b := s.NewBatch()
		for _, name := range []string{"a", "b", "c"} {
			if err := it.Put(b, testTenant, id, name, &enginev1.SignalInboxEntry{SignalName: name}); err != nil {
				t.Fatal(err)
			}
		}
		// Plant a row for a sibling invocation so we verify the
		// range-delete doesn't bleed across.
		if err := it.Put(b, testTenant, other, "z", &enginev1.SignalInboxEntry{SignalName: "z"}); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		b2 := s.NewBatch()
		if err := it.DeleteAllForInvocation(b2, testTenant, id); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)

		for _, name := range []string{"a", "b", "c"} {
			got, _ := it.Get(testTenant, id, name)
			if got != nil {
				t.Errorf("name=%s: %+v survived range-delete", name, got)
			}
		}
		// Sibling untouched.
		got, _ := it.Get(testTenant, other, "z")
		if got == nil {
			t.Errorf("sibling invocation row z was deleted by mistake")
		}
	})

	t.Run(name+"/SignalAwaiter_PutGetDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		at := tables.SignalAwaiterTable{S: s}
		id := mkID(7, "0123456789abcdef")

		got, err := at.Get(testTenant, id, "ready")
		if err != nil || got != nil {
			t.Errorf("missing: got=%+v err=%v; want (nil, nil)", got, err)
		}

		entry := &enginev1.SignalAwaiter{Owner: id, EntryIndex: 11}
		b := s.NewBatch()
		if err := at.Put(b, testTenant, id, "ready", entry); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err = at.Get(testTenant, id, "ready")
		if err != nil {
			t.Fatal(err)
		}
		if got.GetEntryIndex() != 11 || got.GetOwner().GetPartitionKey() != 7 {
			t.Errorf("roundtrip: %+v", got)
		}

		b2 := s.NewBatch()
		if err := at.Delete(b2, testTenant, id, "ready"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		got, _ = at.Get(testTenant, id, "ready")
		if got != nil {
			t.Errorf("after delete: %+v; want nil", got)
		}
	})

	t.Run(name+"/KeyLease_MissingReturnsNil", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		got, err := tables.KeyLeaseTable{S: s}.Get(testLP, testTenant, "Greeter", "user-42")
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil for missing lease, got %+v", got)
		}
	})

	t.Run(name+"/KeyLease_PutGet", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		klt := tables.KeyLeaseTable{S: s}
		holder := mkID(7, "0123456789abcdef")
		queued := mkID(7, "fedcba9876543210")
		want := &enginev1.KeyLeaseStatus{
			State:             enginev1.KeyLeaseStatus_ACTIVE,
			CurrentInvocation: holder,
			Queue:             []*enginev1.InvocationId{queued},
		}

		b := s.NewBatch()
		if err := klt.Put(b, testLP, testTenant, "Counter", "shard-0", want); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := klt.Get(testLP, testTenant, "Counter", "shard-0")
		if err != nil {
			t.Fatal(err)
		}
		if got.GetState() != enginev1.KeyLeaseStatus_ACTIVE {
			t.Errorf("state mismatch: got %v", got.GetState())
		}
		if !bytes.Equal(got.GetCurrentInvocation().GetUuid(), holder.GetUuid()) {
			t.Errorf("current_invocation mismatch")
		}
		if len(got.GetQueue()) != 1 || !bytes.Equal(got.GetQueue()[0].GetUuid(), queued.GetUuid()) {
			t.Errorf("queue mismatch: %+v", got.GetQueue())
		}
	})

	t.Run(name+"/Idempotency_MissingReturnsNil", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		got, err := tables.IdempotencyTable{S: s}.Get(testLP, testTenant, "Svc", "h", "k", "ikey")
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil for missing idempotency entry, got %+v", got)
		}
	})

	t.Run(name+"/Idempotency_PutGetRoundtrip", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		idemT := tables.IdempotencyTable{S: s}
		id := mkID(11, "0123456789abcdef")

		b := s.NewBatch()
		if err := idemT.Put(b, testLP, testTenant, "Counter", "incr", "user-42", "req-123", id); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := idemT.Get(testLP, testTenant, "Counter", "incr", "user-42", "req-123")
		if err != nil {
			t.Fatal(err)
		}
		if got == nil {
			t.Fatal("expected hit, got nil")
		}
		if got.GetPartitionKey() != id.GetPartitionKey() {
			t.Errorf("partition_key mismatch: got %d", got.GetPartitionKey())
		}
		if !bytes.Equal(got.GetUuid(), id.GetUuid()) {
			t.Errorf("uuid mismatch: got %x", got.GetUuid())
		}

		// Distinct tuple components produce distinct entries.
		other, err := idemT.Get(testLP, testTenant, "Counter", "incr", "user-42", "req-999")
		if err != nil {
			t.Fatal(err)
		}
		if other != nil {
			t.Errorf("expected nil for different idempotency_key, got %+v", other)
		}
	})

	t.Run(name+"/Idempotency_AdjacentFieldsDoNotAlias", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		idemT := tables.IdempotencyTable{S: s}
		idA := mkID(1, "aaaaaaaaaaaaaaaa")
		idB := mkID(2, "bbbbbbbbbbbbbbbb")

		b := s.NewBatch()
		// (service="ab", handler="c", "", "k") vs ("a", "bc", "", "k"). Use
		// the same lp for both so the aliasing check isolates the SHA body
		// from any LP difference.
		if err := idemT.Put(b, testLP, testTenant, "ab", "c", "", "k", idA); err != nil {
			t.Fatal(err)
		}
		if err := idemT.Put(b, testLP, testTenant, "a", "bc", "", "k", idB); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		gotA, err := idemT.Get(testLP, testTenant, "ab", "c", "", "k")
		if err != nil {
			t.Fatal(err)
		}
		gotB, err := idemT.Get(testLP, testTenant, "a", "bc", "", "k")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotA.GetUuid(), idA.GetUuid()) {
			t.Errorf("tuple A: got %x want %x", gotA.GetUuid(), idA.GetUuid())
		}
		if !bytes.Equal(gotB.GetUuid(), idB.GetUuid()) {
			t.Errorf("tuple B: got %x want %x", gotB.GetUuid(), idB.GetUuid())
		}
	})

	t.Run(name+"/WorkflowRun_MissingReturnsNil", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		got, err := tables.WorkflowRunTable{S: s}.Get(testLP, testTenant, "Orders", "order-42")
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("missing: got=%+v; want nil", got)
		}
	})

	t.Run(name+"/WorkflowRun_PutGetDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		runT := tables.WorkflowRunTable{S: s}
		id := mkID(42, "0123456789abcdef")

		b := s.NewBatch()
		if err := runT.Put(b, testLP, testTenant, "Orders", "order-42", id); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := runT.Get(testLP, testTenant, "Orders", "order-42")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.GetUuid(), id.GetUuid()) {
			t.Errorf("uuid mismatch: got %x want %x", got.GetUuid(), id.GetUuid())
		}

		b2 := s.NewBatch()
		if err := runT.Delete(b2, testLP, testTenant, "Orders", "order-42"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		got, _ = runT.Get(testLP, testTenant, "Orders", "order-42")
		if got != nil {
			t.Errorf("after delete: %+v; want nil", got)
		}
	})

	t.Run(name+"/Promise_MissingReturnsNil", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		got, err := tables.PromiseTable{S: s}.Get(testLP, testTenant, "Wf", "k", "done")
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("missing: got=%+v; want nil", got)
		}
	})

	t.Run(name+"/Promise_PutGetDelete_Resolved", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		pt := tables.PromiseTable{S: s}
		pv := &enginev1.PromiseValue{
			State: &enginev1.PromiseValue_Resolved{
				Resolved: &enginev1.Resolved{Value: []byte("ok"), CompletedAtMs: 1000},
			},
			CreatedAtMs: 1000,
		}
		b := s.NewBatch()
		if err := pt.Put(b, testLP, testTenant, "Wf", "k", "done", pv); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := pt.Get(testLP, testTenant, "Wf", "k", "done")
		if err != nil {
			t.Fatal(err)
		}
		if got.GetResolved() == nil || string(got.GetResolved().GetValue()) != "ok" {
			t.Errorf("resolved value mismatch: %+v", got)
		}

		b2 := s.NewBatch()
		if err := pt.Delete(b2, testLP, testTenant, "Wf", "k", "done"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		got, _ = pt.Get(testLP, testTenant, "Wf", "k", "done")
		if got != nil {
			t.Errorf("after delete: %+v", got)
		}
	})

	t.Run(name+"/Promise_DeleteAllForWorkflow", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		pt := tables.PromiseTable{S: s}
		pv := &enginev1.PromiseValue{
			State: &enginev1.PromiseValue_Resolved{Resolved: &enginev1.Resolved{Value: []byte("v")}},
		}
		b := s.NewBatch()
		for _, n := range []string{"a", "b", "c"} {
			if err := pt.Put(b, testLP, testTenant, "Wf", "k1", n, pv); err != nil {
				t.Fatal(err)
			}
		}
		// Sibling scope under same service, different key.
		if err := pt.Put(b, testLP, testTenant, "Wf", "k2", "x", pv); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		b2 := s.NewBatch()
		if err := pt.DeleteAllForWorkflow(b2, testLP, testTenant, "Wf", "k1"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		for _, n := range []string{"a", "b", "c"} {
			got, _ := pt.Get(testLP, testTenant, "Wf", "k1", n)
			if got != nil {
				t.Errorf("name=%s survived range-delete", n)
			}
		}
		// k2 row untouched.
		got, _ := pt.Get(testLP, testTenant, "Wf", "k2", "x")
		if got == nil {
			t.Errorf("k2 row deleted by mistake")
		}
	})

	t.Run(name+"/PromiseAwaiter_MultiSlotPutScan", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		at := tables.PromiseAwaiterTable{S: s}
		idA := mkID(1, "aaaaaaaaaaaaaaaa")
		idB := mkID(2, "bbbbbbbbbbbbbbbb")
		idC := mkID(3, "cccccccccccccccc")

		// Three concurrent awaiters on the same (svc, key, name), each with
		// its own entry_index. Insertion order shuffled so the scan must
		// rely on key ordering, not insertion order.
		b := s.NewBatch()
		for _, e := range []*enginev1.PromiseAwaiter{
			{Owner: idC, EntryIndex: 30},
			{Owner: idA, EntryIndex: 10},
			{Owner: idB, EntryIndex: 20},
		} {
			if err := at.PutForSlot(b, testLP, testTenant, "Wf", "k", "done", e); err != nil {
				t.Fatal(err)
			}
		}
		// Unrelated row at a different name must not show up in the scan.
		if err := at.PutForSlot(b, testLP, testTenant, "Wf", "k", "other", &enginev1.PromiseAwaiter{Owner: idA, EntryIndex: 99}); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		var got []*enginev1.PromiseAwaiter
		if err := at.ScanForName(testLP, testTenant, "Wf", "k", "done", func(a *enginev1.PromiseAwaiter) error {
			got = append(got, a)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("scan returned %d awaiters; want 3 (got=%+v)", len(got), got)
		}
		wantIdx := []uint32{10, 20, 30}
		wantOwner := []uint64{1, 2, 3}
		for i, a := range got {
			if a.GetEntryIndex() != wantIdx[i] || a.GetOwner().GetPartitionKey() != wantOwner[i] {
				t.Errorf("row %d: got entry=%d owner.pk=%d; want entry=%d owner.pk=%d",
					i, a.GetEntryIndex(), a.GetOwner().GetPartitionKey(),
					wantIdx[i], wantOwner[i])
			}
		}

		// DeleteForSlot removes one row; the others survive.
		b2 := s.NewBatch()
		if err := at.DeleteForSlot(b2, testLP, testTenant, "Wf", "k", "done", 20); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		got = nil
		_ = at.ScanForName(testLP, testTenant, "Wf", "k", "done", func(a *enginev1.PromiseAwaiter) error {
			got = append(got, a)
			return nil
		})
		if len(got) != 2 || got[0].GetEntryIndex() != 10 || got[1].GetEntryIndex() != 30 {
			t.Errorf("after DeleteForSlot(20): got=%+v; want entries [10, 30]", got)
		}
	})

	t.Run(name+"/PromiseAwaiter_DeleteAllForWorkflow", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		at := tables.PromiseAwaiterTable{S: s}
		id := mkID(7, "0123456789abcdef")

		b := s.NewBatch()
		for _, n := range []string{"a", "b", "c"} {
			for _, idx := range []uint32{1, 2} {
				if err := at.PutForSlot(b, testLP, testTenant, "Wf", "k1", n, &enginev1.PromiseAwaiter{Owner: id, EntryIndex: idx}); err != nil {
					t.Fatal(err)
				}
			}
		}
		// Survivor on a different workflow_key.
		if err := at.PutForSlot(b, testLP, testTenant, "Wf", "k2", "a", &enginev1.PromiseAwaiter{Owner: id, EntryIndex: 1}); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		b2 := s.NewBatch()
		if err := at.DeleteAllForWorkflow(b2, testLP, testTenant, "Wf", "k1"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)

		for _, n := range []string{"a", "b", "c"} {
			var found []*enginev1.PromiseAwaiter
			_ = at.ScanForName(testLP, testTenant, "Wf", "k1", n, func(a *enginev1.PromiseAwaiter) error {
				found = append(found, a)
				return nil
			})
			if len(found) != 0 {
				t.Errorf("name=%s survived workflow-range-delete: %+v", n, found)
			}
		}
		var k2 []*enginev1.PromiseAwaiter
		_ = at.ScanForName(testLP, testTenant, "Wf", "k2", "a", func(a *enginev1.PromiseAwaiter) error {
			k2 = append(k2, a)
			return nil
		})
		if len(k2) != 1 {
			t.Errorf("k2 untouched: got %d rows; want 1", len(k2))
		}
	})

	t.Run(name+"/WorkflowReap_PutScanDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		rt := tables.WorkflowReapTable{S: s}

		// Insert out-of-order; scan must yield in fire_at_ms order.
		b := s.NewBatch()
		for _, row := range []struct {
			fire uint64
			svc  string
			key  string
		}{
			{fire: 300, svc: "Wf", key: "ord-c"},
			{fire: 100, svc: "Wf", key: "ord-a"},
			{fire: 200, svc: "Wf", key: "ord-b"},
		} {
			if err := rt.Put(b, row.fire, row.svc, row.key); err != nil {
				t.Fatal(err)
			}
		}
		commit(t, b)

		var got []tables.ReapRow
		if err := rt.ScanAll(func(r tables.ReapRow) error {
			got = append(got, r)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("scan returned %d rows; want 3", len(got))
		}
		wantFires := []uint64{100, 200, 300}
		wantKeys := []string{"ord-a", "ord-b", "ord-c"}
		for i, r := range got {
			if r.FireAtMs != wantFires[i] || r.WorkflowKey != wantKeys[i] {
				t.Errorf("row %d: got fire=%d key=%s; want fire=%d key=%s",
					i, r.FireAtMs, r.WorkflowKey, wantFires[i], wantKeys[i])
			}
		}

		// Delete the middle one; scan returns the remaining two.
		b2 := s.NewBatch()
		if err := rt.Delete(b2, 200, "Wf", "ord-b"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		got = nil
		_ = rt.ScanAll(func(r tables.ReapRow) error {
			got = append(got, r)
			return nil
		})
		if len(got) != 2 || got[0].FireAtMs != 100 || got[1].FireAtMs != 300 {
			t.Errorf("after delete(200): got=%+v; want fires [100, 300]", got)
		}
	})
}

func TestTables(t *testing.T) {
	runTablesSuite(t, "Mem", func(t *testing.T) storage.Store {
		return storage.NewMemStore()
	})
	runTablesSuite(t, "PebbleMemFS", func(t *testing.T) storage.Store {
		s, err := storage.OpenPebble("/p", &pebble.Options{FS: vfs.NewMem()})
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

// Defensive: ensure raw timer values aren't being misparsed across edits.
func TestTimer_ScanByInvocation(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	tt := tables.TimerTable{S: s}
	idA := mkID(1, "aaaaaaaaaaaaaaaa")
	idB := mkID(1, "bbbbbbbbbbbbbbbb")

	b := s.NewBatch()
	for _, fireAt := range []uint64{200, 50, 100} {
		if err := tt.Insert(b, testTenant, fireAt, idA, 7); err != nil {
			t.Fatal(err)
		}
	}
	if err := tt.Insert(b, testTenant, 75, idB, 9); err != nil {
		t.Fatal(err)
	}
	commit(t, b)

	// ScanByInvocation(idA) returns idA's three fire_at_ms in ascending
	// order and ignores idB.
	var got []uint64
	if err := tt.ScanByInvocation(testTenant, idA, func(fireAt uint64) error {
		got = append(got, fireAt)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []uint64{50, 100, 200}
	if len(got) != len(want) {
		t.Fatalf("got %v; want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("[%d] = %d; want %d", i, got[i], want[i])
		}
	}

	// Delete one — both primary and secondary should drop, ScanByInvocation
	// reflects it.
	b2 := s.NewBatch()
	if err := tt.Delete(b2, testTenant, 100, idA); err != nil {
		t.Fatal(err)
	}
	commit(t, b2)

	got = got[:0]
	_ = tt.ScanByInvocation(testTenant, idA, func(fireAt uint64) error {
		got = append(got, fireAt)
		return nil
	})
	if len(got) != 2 || got[0] != 50 || got[1] != 200 {
		t.Errorf("after delete: got %v; want [50 200]", got)
	}
	// Primary side must also be gone (no row at fire_at=100 for idA).
	var primaryCount int
	_ = tt.ScanAll(func(e tables.TimerEntry) error {
		if e.FireAtMs == 100 && bytes.Equal(e.ID.GetUuid(), idA.GetUuid()) {
			primaryCount++
		}
		return nil
	})
	if primaryCount != 0 {
		t.Errorf("primary row at fire_at=100 not deleted (count=%d)", primaryCount)
	}
}

func TestTimer_RawValueIs4BytesBE(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	tt := tables.TimerTable{S: s}
	id := mkID(1, "0123456789abcdef")
	b := s.NewBatch()
	if err := tt.Insert(b, testTenant, 1, id, 0x01020304); err != nil {
		t.Fatal(err)
	}
	commit(t, b)
	var entry tables.TimerEntry
	if err := tt.ScanAll(func(e tables.TimerEntry) error { entry = e; return nil }); err != nil {
		t.Fatal(err)
	}
	if entry.SleepIdx != 0x01020304 {
		t.Errorf("sleep_idx round-trip: got %x", entry.SleepIdx)
	}
	if !bytes.Equal(entry.ID.GetUuid(), id.GetUuid()) {
		t.Errorf("id uuid mismatch")
	}
}
