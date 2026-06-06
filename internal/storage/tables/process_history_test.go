package tables_test

import (
	"testing"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// TestProcessHistory_RoundTripAndIsolation covers the append-only timeline table:
// ordered scan, the after-cursor resume, single-row DeleteAt (keep-last-N
// eviction), and whole-instance DeleteInstance — and that two distinct instances
// (same LP) never alias, since the key segments the fixed-width 24-byte root id.
func TestProcessHistory_RoundTripAndIsolation(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	ht := tables.ProcessHistoryTable{S: s}
	rootA := mkID(1, "aaaaaaaaaaaaaaaa")
	rootB := mkID(1, "bbbbbbbbbbbbbbbb") // same pk/LP, different id

	b := s.NewBatch()
	for seq := uint64(1); seq <= 5; seq++ {
		if err := ht.Append(b, rootA, seq, &enginev1.ProcessHistoryEvent{Seq: seq, NodeId: "A"}); err != nil {
			t.Fatal(err)
		}
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if err := ht.Append(b, rootB, seq, &enginev1.ProcessHistoryEvent{Seq: seq, NodeId: "B"}); err != nil {
			t.Fatal(err)
		}
	}
	commit(t, b)

	scan := func(root *enginev1.InvocationId, after uint64) []uint64 {
		t.Helper()
		var got []uint64
		if err := ht.ScanByInstance(root, after, func(ev *enginev1.ProcessHistoryEvent) error {
			got = append(got, ev.GetSeq())
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}

	if got := scan(rootA, 0); len(got) != 5 || got[0] != 1 || got[4] != 5 {
		t.Fatalf("rootA full scan: %v", got)
	}
	if got := scan(rootA, 3); len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Fatalf("rootA resume after seq 3: %v", got)
	}
	if got := scan(rootB, 0); len(got) != 3 {
		t.Fatalf("rootB scan: %v", got)
	}

	// DeleteAt removes one row (the keep-last-N eviction primitive).
	b = s.NewBatch()
	if err := ht.DeleteAt(b, rootA, 1); err != nil {
		t.Fatal(err)
	}
	commit(t, b)
	if got := scan(rootA, 0); len(got) != 4 || got[0] != 2 {
		t.Fatalf("rootA after DeleteAt(1): %v", got)
	}

	// DeleteInstance clears rootA only; rootB is untouched (no prefix aliasing).
	b = s.NewBatch()
	if err := ht.DeleteInstance(b, rootA); err != nil {
		t.Fatal(err)
	}
	commit(t, b)
	if got := scan(rootA, 0); len(got) != 0 {
		t.Fatalf("rootA after DeleteInstance: %v", got)
	}
	if got := scan(rootB, 0); len(got) != 3 {
		t.Fatalf("rootB must survive rootA DeleteInstance: %v", got)
	}
}
