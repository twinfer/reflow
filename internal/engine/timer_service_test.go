package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

type fakeProposer struct {
	mu      sync.Mutex
	cmds    []*enginev1.Command
	failNxt atomic.Int32 // number of remaining failures before succeeding
}

func (f *fakeProposer) ProposeSelf(_ context.Context, cmd *enginev1.Command) error {
	if f.failNxt.Load() > 0 {
		f.failNxt.Add(-1)
		return errors.New("forced propose failure")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, cmd)
	return nil
}

func (f *fakeProposer) Cmds() []*enginev1.Command {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*enginev1.Command, len(f.cmds))
	copy(out, f.cmds)
	return out
}

func newTimerSvc(t *testing.T) (*TimerService, tables.TimerTable, *fakeProposer) {
	t.Helper()
	s := storage.NewMemStore()
	t.Cleanup(func() { _ = s.Close() })
	table := tables.TimerTable{S: s}
	fp := &fakeProposer{}
	svc := NewTimerService(table, fp, TimerServiceOptions{})
	return svc, table, fp
}

func TestTimerService_FiresOnTime(t *testing.T) {
	svc, _, fp := newTimerSvc(t)

	ctx := t.Context()
	go svc.Run(ctx)
	defer svc.Stop()

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	fireAt := uint64(time.Now().UnixMilli()) + 50
	if err := svc.Push(fireAt, id, 0); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(fp.Cmds()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(fp.Cmds()) != 1 {
		t.Fatalf("expected one TimerFired, got %d", len(fp.Cmds()))
	}
	cmd := fp.Cmds()[0]
	tf := cmd.GetTimerFired()
	if tf == nil {
		t.Fatalf("not a TimerFired: %+v", cmd)
	}
	if tf.GetFireAtMs() != fireAt {
		t.Errorf("fire_at_ms = %d; want %d", tf.GetFireAtMs(), fireAt)
	}
}

func TestTimerService_RebuildFromTable(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	table := tables.TimerTable{S: s}

	now := uint64(time.Now().UnixMilli())
	id1 := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("aaaaaaaaaaaaaaaa")}
	id2 := &enginev1.InvocationId{PartitionKey: 2, Uuid: []byte("bbbbbbbbbbbbbbbb")}

	b := s.NewBatch()
	if err := table.Insert(b, now+30, id1, 0); err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(b, now+10, id2, 1); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	fp := &fakeProposer{}
	svc := NewTimerService(table, fp, TimerServiceOptions{})
	if err := svc.Rebuild(); err != nil {
		t.Fatal(err)
	}
	if svc.heapLen() != 2 {
		t.Fatalf("heap length after rebuild = %d; want 2", svc.heapLen())
	}

	ctx := t.Context()
	go svc.Run(ctx)
	defer svc.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(fp.Cmds()) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(fp.Cmds()) != 2 {
		t.Fatalf("expected 2 fired timers, got %d", len(fp.Cmds()))
	}
	// First fired must be the earlier fire_at (now+10, id2).
	first := fp.Cmds()[0].GetTimerFired()
	if first == nil || first.GetSleepIndex() != 1 {
		t.Errorf("first fired = %+v; want sleep_idx=1", first)
	}
}

func TestTimerService_RepushOnProposeFail(t *testing.T) {
	svc, _, fp := newTimerSvc(t)
	fp.failNxt.Store(1) // first attempt fails

	ctx := t.Context()
	go svc.Run(ctx)
	defer svc.Stop()

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	if err := svc.Push(uint64(time.Now().UnixMilli())+30, id, 0); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(fp.Cmds()) >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(fp.Cmds()) == 0 {
		t.Fatal("after propose failure + retry, expected at least one Cmd")
	}
}

func TestTimerService_Delete(t *testing.T) {
	svc, _, _ := newTimerSvc(t)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	fireAt := uint64(time.Now().UnixMilli()) + 1000
	_ = svc.Push(fireAt, id, 0)
	if svc.heapLen() != 1 {
		t.Fatalf("heap = %d; want 1", svc.heapLen())
	}
	_ = svc.Delete(fireAt, id)
	if svc.heapLen() != 0 {
		t.Errorf("heap after delete = %d; want 0", svc.heapLen())
	}
}

func TestTimerService_ConcurrentPushDelete_Race(t *testing.T) {
	svc, _, _ := newTimerSvc(t)
	ctx := t.Context()
	go svc.Run(ctx)
	defer svc.Stop()

	const N = 100
	var wg sync.WaitGroup
	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := &enginev1.InvocationId{PartitionKey: uint64(i), Uuid: []byte("0123456789abcdef")}
			fireAt := uint64(time.Now().UnixMilli()) + uint64(60_000+i)
			_ = svc.Push(fireAt, id, 0)
			_ = svc.Delete(fireAt, id)
		}(i)
	}
	wg.Wait()
}
