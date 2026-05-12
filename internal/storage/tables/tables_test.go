package tables_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

type openFn func(t *testing.T) storage.Store

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
		st, err := tables.InvocationTable{S: s}.Get(id)
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
		if err := it.Put(b, id, put); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := it.Get(id)
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
		if err := it.Delete(b2, id); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)

		got2, err := it.Get(id)
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
		_ = it.Put(b, idA, &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Scheduled{
				Scheduled: &enginev1.Scheduled{Target: &enginev1.InvocationTarget{ServiceName: "A"}},
			},
		})
		_ = it.Put(b, idB, &enginev1.InvocationStatus{
			Status: &enginev1.InvocationStatus_Free{Free: &enginev1.Free{}},
		})
		commit(t, b)

		var seen []string
		if err := it.ScanAll(func(id *enginev1.InvocationId, _ *enginev1.InvocationStatus) error {
			seen = append(seen, string(id.GetUuid()))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(seen) != 1 || seen[0] != "aaaaaaaaaaaaaaaa" {
			t.Errorf("scan = %v; want [aaaa...]", seen)
		}
	})

	t.Run(name+"/Journal_AppendReadScanDelete", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		jt := tables.JournalTable{S: s}
		id := mkID(1, "0123456789abcdef")

		b := s.NewBatch()
		for i := range uint32(5) {
			if err := jt.Append(b, id, &enginev1.JournalEntry{
				Index: i,
				Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte{byte(i)}}},
			}); err != nil {
				t.Fatal(err)
			}
		}
		commit(t, b)

		// Read single
		got, err := jt.Read(id, 3)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetIndex() != 3 {
			t.Errorf("read index = %d; want 3", got.GetIndex())
		}

		// Missing
		if _, err := jt.Read(id, 99); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}

		// Scan in order
		var indexes []uint32
		if err := jt.Scan(id, func(e *enginev1.JournalEntry) error {
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
		if err := jt.DeletePrefix(b2, id); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		if _, err := jt.Read(id, 0); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("expected NotFound after DeletePrefix, got %v", err)
		}
	})

	// Phase 2.5: lock the JournalTable.Append contract that the apply path
	// relies on when re-running InvokerEffects on replay (e.g., parent-side
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
		if err := jt.Append(b1, id, entry); err != nil {
			t.Fatal(err)
		}
		commit(t, b1)

		// Replay re-applies the same content.
		b2 := s.NewBatch()
		if err := jt.Append(b2, id, entry); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)

		got, err := jt.Read(id, 7)
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
		_ = tt.Insert(b, 200, idB, 1)
		_ = tt.Insert(b, 100, idA, 0)
		_ = tt.Insert(b, 300, idA, 2)
		commit(t, b)

		// ScanDue at t=150 -> only the 100ms timer
		var due []tables.TimerEntry
		if err := tt.ScanDue(150, func(e tables.TimerEntry) error {
			due = append(due, e)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(due) != 1 || due[0].FireAtMs != 100 {
			t.Errorf("ScanDue(150) = %+v; want one entry at 100", due)
		}

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
		_ = tt.Delete(b2, 200, idB)
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

	t.Run(name+"/Dedup_SelfProposalDedupes", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		dt := tables.DedupTable{S: s}

		d1 := &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
			SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: 1, Seq: 5},
		}}
		// First time: not duplicate.
		dup, err := dt.IsDuplicate(d1)
		if err != nil {
			t.Fatal(err)
		}
		if dup {
			t.Fatal("first IsDuplicate should be false")
		}
		// Record.
		b := s.NewBatch()
		if err := dt.Record(b, d1); err != nil {
			t.Fatal(err)
		}
		commit(t, b)
		// Same Dedup is now a duplicate.
		dup, _ = dt.IsDuplicate(d1)
		if !dup {
			t.Fatal("second IsDuplicate should be true")
		}
		// A lower-seq entry in the same epoch is also a duplicate (out of
		// order).
		dLower := &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
			SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: 1, Seq: 3},
		}}
		dup, _ = dt.IsDuplicate(dLower)
		if !dup {
			t.Fatal("lower-seq should be dup")
		}
		// A higher seq is not a duplicate.
		dHigher := &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
			SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: 1, Seq: 6},
		}}
		dup, _ = dt.IsDuplicate(dHigher)
		if dup {
			t.Fatal("higher seq should not be dup")
		}
		// Different epoch tracks independently.
		dEpoch2 := &enginev1.Dedup{Kind: &enginev1.Dedup_SelfProposal{
			SelfProposal: &enginev1.SelfProposalDedup{LeaderEpoch: 2, Seq: 1},
		}}
		dup, _ = dt.IsDuplicate(dEpoch2)
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
		dup, _ := dt.IsDuplicate(d)
		if dup {
			t.Fatal("first should not be dup")
		}
		b := s.NewBatch()
		_ = dt.Record(b, d)
		commit(t, b)
		dup, _ = dt.IsDuplicate(d)
		if !dup {
			t.Fatal("second should be dup")
		}
	})

	t.Run(name+"/Dedup_NilOrEmpty", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		dt := tables.DedupTable{S: s}
		// nil Dedup: not duplicate.
		dup, err := dt.IsDuplicate(nil)
		if err != nil || dup {
			t.Errorf("nil dedup: dup=%v err=%v", dup, err)
		}
		// Empty (kind unset): also not duplicate.
		dup, err = dt.IsDuplicate(&enginev1.Dedup{})
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
		v, ok, err := st.Get(target, "balance")
		if err != nil || ok || v != nil {
			t.Errorf("missing key: v=%v ok=%v err=%v", v, ok, err)
		}

		b := s.NewBatch()
		if err := st.Set(b, target, "balance", []byte{0x10}); err != nil {
			t.Fatal(err)
		}
		if err := st.Set(b, target, "name", []byte("Alice")); err != nil {
			t.Fatal(err)
		}
		// Present-but-empty must be distinguishable from missing.
		if err := st.Set(b, target, "empty", []byte{}); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		v, ok, _ = st.Get(target, "balance")
		if !ok || !bytes.Equal(v, []byte{0x10}) {
			t.Errorf("balance: v=%v ok=%v", v, ok)
		}
		v, ok, _ = st.Get(target, "empty")
		if !ok || len(v) != 0 {
			t.Errorf("empty: v=%v ok=%v (want ok && len==0)", v, ok)
		}

		// Clear removes the row.
		b2 := s.NewBatch()
		if err := st.Clear(b2, target, "balance"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		_, ok, _ = st.Get(target, "balance")
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
		_ = st.Set(b, alice, "a", []byte("A"))
		_ = st.Set(b, alice, "z", []byte("Z"))
		_ = st.Set(b, bob, "a", []byte("Bob-A"))
		_ = st.Set(b, other, "a", []byte("Other-A"))
		_ = st.Set(b, unkeyed, "cfg", []byte("U"))
		commit(t, b)

		var pairs []string
		if err := st.ScanObject(alice, func(k string, v []byte) error {
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
		_ = st.ScanObject(unkeyed, func(k string, v []byte) error {
			uPairs = append(uPairs, k+"="+string(v))
			return nil
		})
		if len(uPairs) != 1 || uPairs[0] != "cfg=U" {
			t.Errorf("unkeyed scan = %v; want [cfg=U]", uPairs)
		}
	})

	t.Run(name+"/Outbox_AppendPopGet", func(t *testing.T) {
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

		got, err := ot.Get(1)
		if err != nil {
			t.Fatal(err)
		}
		inv := got.GetInvoke()
		if inv == nil || string(inv.GetInput()) != "payload" {
			t.Errorf("get: %+v", got)
		}

		b2 := s.NewBatch()
		if err := ot.Pop(b2, 1); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		if _, err := ot.Get(1); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("after pop: %v; want ErrNotFound", err)
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
		if _, err := at.Get(id); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("missing: %v; want ErrNotFound", err)
		}

		b := s.NewBatch()
		if err := at.Put(b, id, entry); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := at.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetOwner().GetPartitionKey() != 42 || got.GetEntryIndex() != 7 {
			t.Errorf("roundtrip: %+v", got)
		}

		b2 := s.NewBatch()
		if err := at.Delete(b2, id); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		if _, err := at.Get(id); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("after delete: %v; want ErrNotFound", err)
		}
	})

	t.Run(name+"/KeyLease_MissingReturnsNil", func(t *testing.T) {
		s := open(t)
		defer s.Close()
		got, err := tables.KeyLeaseTable{S: s}.Get("Greeter", "user-42")
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil for missing lease, got %+v", got)
		}
	})

	t.Run(name+"/KeyLease_PutGetDelete", func(t *testing.T) {
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
		if err := klt.Put(b, "Counter", "shard-0", want); err != nil {
			t.Fatal(err)
		}
		commit(t, b)

		got, err := klt.Get("Counter", "shard-0")
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

		b2 := s.NewBatch()
		if err := klt.Delete(b2, "Counter", "shard-0"); err != nil {
			t.Fatal(err)
		}
		commit(t, b2)
		got, err = klt.Get("Counter", "shard-0")
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil after delete, got %+v", got)
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
func TestTimer_RawValueIs4BytesBE(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	tt := tables.TimerTable{S: s}
	id := mkID(1, "0123456789abcdef")
	b := s.NewBatch()
	if err := tt.Insert(b, 1, id, 0x01020304); err != nil {
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
