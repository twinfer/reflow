package invoker

import (
	"context"
	"sync"
	"testing"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// fakeProposer is a test double for engine.RaftProposer. JournalAppended
// effects are applied to the local store so prepare() picks up the
// resulting journal entry on subsequent loads; RunProposal mirrors the
// FSM by writing a JERun entry at the proposal's entry_index. Terminal
// effects (Completed/Suspended) are recorded but do not mutate state.
type fakeProposer struct {
	mu    sync.Mutex
	cmds  []*enginev1.Command
	store storage.Store
	// errOnNext, if set, returns this error from the next ProposeSelf
	// call and clears itself. Used to exercise propose-failure paths.
	errOnNext error
}

func (f *fakeProposer) ProposeSelf(_ context.Context, cmd *enginev1.Command) error {
	f.mu.Lock()
	if f.errOnNext != nil {
		err := f.errOnNext
		f.errOnNext = nil
		f.cmds = append(f.cmds, cmd)
		f.mu.Unlock()
		return err
	}
	f.cmds = append(f.cmds, cmd)
	store := f.store
	f.mu.Unlock()

	if store == nil {
		return nil
	}
	eff := cmd.GetInvokerEffect()
	if eff == nil {
		return nil
	}
	switch k := eff.GetKind().(type) {
	case *enginev1.InvokerEffect_JournalAppended:
		b := store.NewBatch()
		defer b.Close()
		if err := (tables.JournalTable{S: store}).Append(b, eff.GetInvocationId(), k.JournalAppended.GetEntry()); err != nil {
			return err
		}
		return b.Commit(true)
	case *enginev1.InvokerEffect_RunProposal:
		b := store.NewBatch()
		defer b.Close()
		entry := &enginev1.JournalEntry{
			Index: k.RunProposal.GetEntryIndex(),
			Entry: &enginev1.JournalEntry_Run{Run: &enginev1.JERun{
				Value:          k.RunProposal.GetValue(),
				FailureMessage: k.RunProposal.GetFailureMessage(),
			}},
		}
		if err := (tables.JournalTable{S: store}).Append(b, eff.GetInvocationId(), entry); err != nil {
			return err
		}
		return b.Commit(true)
	}
	return nil
}

func newID(pk uint64, uuid string) *enginev1.InvocationId {
	b := []byte(uuid)
	if len(b) > 16 {
		b = b[:16]
	}
	if len(b) < 16 {
		pad := make([]byte, 16)
		copy(pad, b)
		b = pad
	}
	return &enginev1.InvocationId{PartitionKey: pk, Uuid: b}
}

func TestJournalReader_Empty(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	jr := NewJournalReader(tables.JournalTable{S: s})
	entries, err := jr.Load(newID(1, "id"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("entries = %d; want 0", len(entries))
	}
}

func TestJournalReader_InOrder(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	jt := tables.JournalTable{S: s}
	id := newID(1, "abc")

	// Append entries out-of-order; Scan must return them in index order.
	for _, idx := range []uint32{3, 1, 2} {
		b := s.NewBatch()
		entry := &enginev1.JournalEntry{Index: idx, Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte{byte(idx)}}}}
		if err := jt.Append(b, id, entry); err != nil {
			t.Fatal(err)
		}
		if err := b.Commit(true); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}

	entries, err := NewJournalReader(jt).Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("len = %d; want 3", len(entries))
	}
	for i, e := range entries {
		if e.GetIndex() != uint32(i+1) {
			t.Errorf("entries[%d].Index = %d; want %d", i, e.GetIndex(), i+1)
		}
	}
}

func TestSessionKey_Stable(t *testing.T) {
	id1 := newID(42, "abc")
	id2 := newID(42, "abc")
	if sessionKey(id1) != sessionKey(id2) {
		t.Error("same id → different keys")
	}
	id3 := newID(43, "abc")
	if sessionKey(id1) == sessionKey(id3) {
		t.Error("different partition_key → same key")
	}
	if sessionKey(nil) != "" {
		t.Error("nil id should produce empty key")
	}
}
