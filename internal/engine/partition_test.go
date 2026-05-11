package engine

import (
	"bytes"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
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
	now := uint64(1000)
	p := NewPartition(1, 1, PartitionConfig{
		Snapshotter: snap,
		Leadership:  lead,
		Collector:   col,
		NowFn:       func() uint64 { return now },
	})
	t.Cleanup(func() { _ = p.Close() })
	return p, lead, col
}

func envelope(t *testing.T, cmd *enginev1.Command) []byte {
	t.Helper()
	buf, err := proto.Marshal(&enginev1.Envelope{Command: cmd})
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func envelopeWithDedup(t *testing.T, d *enginev1.Dedup, cmd *enginev1.Command) []byte {
	t.Helper()
	buf, err := proto.Marshal(&enginev1.Envelope{
		Header:  &enginev1.Header{Dedup: d},
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
	buf, _ := proto.Marshal(&enginev1.Envelope{Command: &enginev1.Command{}})
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
		NowFn:       func() uint64 { return 1000 },
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
