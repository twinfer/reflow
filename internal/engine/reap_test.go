package engine

import (
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func newReapSvc(t *testing.T, opts ReapServiceOptions) (*ReapService, *fakeProposer) {
	t.Helper()
	s := storage.NewMemStore()
	t.Cleanup(func() { _ = s.Close() })
	fp := &fakeProposer{}
	scanAll := func(emit func(reapEntry) error) error {
		return (tables.ReapTable{S: s}).ScanAll(func(rr tables.ReapRow) error {
			return emit(invocationReapEntry{fireAt: rr.FireAtMs, id: rr.ID})
		})
	}
	return NewReapService(scanAll, fp, opts), fp
}

func reapID(pk uint64, b byte) *enginev1.InvocationId {
	uuid := make([]byte, 16)
	uuid[15] = b
	return &enginev1.InvocationId{PartitionKey: pk, Uuid: uuid}
}

// TestReapService_CountCapFiresOldestEarly proves the backstop: with the
// pending set over MaxPending and nothing time-due, the soonest-to-expire
// entries are reaped early down to the cap, and the rest are left for the
// timer.
func TestReapService_CountCapFiresOldestEarly(t *testing.T) {
	// Frozen clock well before every fireAt so the time-based branch never
	// fires — the count-cap is the only thing that can reap here.
	const base uint64 = 1_000_000
	svc, fp := newReapSvc(t, ReapServiceOptions{
		Now:        func() uint64 { return base },
		MaxPending: 3,
	})

	// Five entries, distinct fire times far in the future. Excess = 5-3 = 2,
	// so the two smallest fireAtMs (base+100, base+200) must fire.
	for i := uint64(1); i <= 5; i++ {
		svc.Push(invocationReapEntry{fireAt: base + i*100, id: reapID(i, byte(i))})
	}

	ctx := t.Context()
	go svc.Run(ctx)
	defer svc.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(fp.Cmds()) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cmds := fp.Cmds()
	if len(cmds) != 2 {
		t.Fatalf("expected 2 early reaps (excess over cap), got %d", len(cmds))
	}
	for i, want := range []uint64{base + 100, base + 200} {
		ri := cmds[i].GetReapInvocation()
		if ri == nil {
			t.Fatalf("cmd %d is not a ReapInvocation: %v", i, cmds[i])
		}
		if ri.GetFireAtMs() != want {
			t.Errorf("early reap %d: fire_at_ms=%d want %d (soonest-to-expire first)", i, ri.GetFireAtMs(), want)
		}
	}

	// Settling: cap is met (3 remain), clock still frozen — no further fires.
	time.Sleep(50 * time.Millisecond)
	if got := len(fp.Cmds()); got != 2 {
		t.Fatalf("count-cap over-fired: got %d, want 2", got)
	}
}

// TestReapService_CountCapDisabled proves a negative MaxPending disables the
// backstop entirely — nothing fires while the clock stays before every window.
func TestReapService_CountCapDisabled(t *testing.T) {
	const base uint64 = 1_000_000
	svc, fp := newReapSvc(t, ReapServiceOptions{
		Now:        func() uint64 { return base },
		MaxPending: -1,
	})
	for i := uint64(1); i <= 5; i++ {
		svc.Push(invocationReapEntry{fireAt: base + i*100, id: reapID(i, byte(i))})
	}
	ctx := t.Context()
	go svc.Run(ctx)
	defer svc.Stop()

	time.Sleep(100 * time.Millisecond)
	if got := len(fp.Cmds()); got != 0 {
		t.Fatalf("backstop disabled but %d reaps fired", got)
	}
}
