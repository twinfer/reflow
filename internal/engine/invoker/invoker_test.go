package invoker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
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

// effects returns a snapshot of every proposed InvokerEffect.kind in
// order. Convenient for assertions.
func (f *fakeProposer) effects() []*enginev1.InvokerEffect {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*enginev1.InvokerEffect, 0, len(f.cmds))
	for _, c := range f.cmds {
		if eff := c.GetInvokerEffect(); eff != nil {
			out = append(out, eff)
		}
	}
	return out
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestInvoker(t *testing.T, reg *sdk.Registry) (*Invoker, *fakeProposer, storage.Store) {
	t.Helper()
	s := storage.NewMemStore()
	t.Cleanup(func() { s.Close() })
	fp := &fakeProposer{store: s}
	inv := New(Config{
		Registry:        NewRegistry(reg),
		JournalTable:    tables.JournalTable{S: s},
		InvocationTable: tables.InvocationTable{S: s},
		StateTable:      tables.StateTable{S: s},
		Proposer:        fp,
		Log:             discardLogger(),
	})
	return inv, fp, s
}

// blockingHandler stays alive until the invocation context is cancelled.
// Sessions running blockingHandler exit only via abort, which keeps tests
// that assert on activeSessions deterministic.
func blockingHandler(c sdk.Context, _ []byte) ([]byte, error) {
	<-c.Context().Done()
	return nil, c.Context().Err()
}

// seedInvoked writes an Invoked InvocationStatus for id so prepare()
// skips the JEInput propose path. Use when the test's interest is the
// session-lifecycle plumbing, not the FSM transition.
func seedInvoked(t *testing.T, s storage.Store, id *enginev1.InvocationId, target *enginev1.InvocationTarget) {
	t.Helper()
	b := s.NewBatch()
	defer b.Close()
	status := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Invoked{
			Invoked: &enginev1.Invoked{Target: target},
		},
	}
	if err := (tables.InvocationTable{S: s}).Put(b, id, status); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// waitForSessionCount busy-polls activeSessions until len matches want or
// the deadline elapses. Helper for tests where the session-cleanup
// goroutine and the assertion race.
func waitForSessionCount(t *testing.T, inv *Invoker, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := inv.activeSessions(); len(got) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("session count never reached %d; last=%v", want, inv.activeSessions())
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

func TestRegistry_LookupViaTarget(t *testing.T) {
	r := sdk.NewRegistry()
	called := 0
	if err := r.Register("Greeter", "hello", func(_ sdk.Context, _ []byte) ([]byte, error) {
		called++
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	w := NewRegistry(r)

	target := &enginev1.InvocationTarget{ServiceName: "Greeter", HandlerName: "hello", ObjectKey: "ignored"}
	h, ok := w.Lookup(target)
	if !ok || h == nil {
		t.Fatal("Lookup miss")
	}
	if _, err := h(nil, nil); err != nil {
		t.Fatalf("h: %v", err)
	}
	if called != 1 {
		t.Errorf("called = %d; want 1", called)
	}

	if _, ok := w.Lookup(&enginev1.InvocationTarget{ServiceName: "Nope", HandlerName: "x"}); ok {
		t.Error("expected miss")
	}
}

func TestRegistry_NilInner(t *testing.T) {
	w := NewRegistry(nil)
	if _, ok := w.Lookup(&enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}); ok {
		t.Error("nil inner: expected miss")
	}
	if _, ok := w.Lookup(nil); ok {
		t.Error("nil target: expected miss")
	}
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

func TestChanTransport_RoundTrip(t *testing.T) {
	eng, sdkSide := NewChanTransport()
	defer eng.Close()
	defer sdkSide.Close()

	msg := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("16-bytes-padding")}
	_ = msg // just to ensure proto compiles in tests

	// Engine sends; SDK receives.
	go func() {
		_ = eng.Send(nil)
	}()
	select {
	case <-time.After(time.Second):
		t.Fatal("recv timeout")
	default:
	}
	if _, err := sdkSide.Recv(); err != nil {
		t.Fatalf("recv: %v", err)
	}

	// Closing one side closes both.
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sdkSide.Recv(); err != ErrTransportClosed {
		t.Errorf("recv after close: got %v; want ErrTransportClosed", err)
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

func TestInvoker_StartInvocationBeforeStart(t *testing.T) {
	r := sdk.NewRegistry()
	_ = r.Register("S", "h", blockingHandler)
	inv, _, s := newTestInvoker(t, r)

	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}
	id := newID(1, "x")
	seedInvoked(t, s, id, target)

	// StartInvocation before Start: must be buffered, not dropped. No
	// session spawns yet (started=false), but the request is queued.
	inv.StartInvocation(id, target)
	if got := inv.activeSessions(); len(got) != 0 {
		t.Errorf("active before Start = %v; want none", got)
	}

	// Start drains the buffer and the deferred session spawns.
	inv.Start(context.Background())
	defer inv.Stop()
	waitForSessionCount(t, inv, 1)
}

func TestInvoker_StartInvocationMissingHandler(t *testing.T) {
	inv, _, _ := newTestInvoker(t, sdk.NewRegistry())
	inv.Start(context.Background())
	defer inv.Stop()

	inv.StartInvocation(newID(1, "x"), &enginev1.InvocationTarget{ServiceName: "Nope", HandlerName: "x"})
	if got := inv.activeSessions(); len(got) != 0 {
		t.Errorf("active = %v; want none (handler missing)", got)
	}
}

func TestInvoker_StartInvocationSpawnsSession(t *testing.T) {
	r := sdk.NewRegistry()
	_ = r.Register("S", "h", blockingHandler)
	inv, _, s := newTestInvoker(t, r)
	inv.Start(context.Background())
	defer inv.Stop()

	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}
	id := newID(1, "abc")
	seedInvoked(t, s, id, target)
	inv.StartInvocation(id, target)
	waitForSessionCount(t, inv, 1)

	// Re-calling StartInvocation for the same id is idempotent while the
	// existing session is still running.
	inv.StartInvocation(id, target)
	if got := inv.activeSessions(); len(got) != 1 {
		t.Errorf("active after re-start = %v; want still 1", got)
	}
}

func TestInvoker_AbortInvocation(t *testing.T) {
	r := sdk.NewRegistry()
	_ = r.Register("S", "h", blockingHandler)
	inv, _, s := newTestInvoker(t, r)
	inv.Start(context.Background())

	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}
	id := newID(1, "abc")
	seedInvoked(t, s, id, target)
	inv.StartInvocation(id, target)
	waitForSessionCount(t, inv, 1)

	inv.AbortInvocation(id)
	if got := inv.activeSessions(); len(got) != 0 {
		t.Errorf("active after abort = %v; want none", got)
	}

	// Aborting an unknown id is a no-op.
	inv.AbortInvocation(newID(99, "missing"))
}

func TestInvoker_Stop(t *testing.T) {
	r := sdk.NewRegistry()
	_ = r.Register("S", "h", blockingHandler)
	inv, _, s := newTestInvoker(t, r)
	inv.Start(context.Background())

	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}
	for _, u := range []string{"a", "b", "c"} {
		id := newID(1, u)
		seedInvoked(t, s, id, target)
		inv.StartInvocation(id, target)
	}
	waitForSessionCount(t, inv, 3)

	done := make(chan struct{})
	go func() { inv.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2s")
	}
	if got := inv.activeSessions(); len(got) != 0 {
		t.Errorf("active after Stop = %v; want none", got)
	}

	// Stop is idempotent.
	inv.Stop()
}
