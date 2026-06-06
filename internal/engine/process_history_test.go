package engine

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/limits"
	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func procHistEvents(t *testing.T, p *Partition, pk uint64, svc, key string) []*enginev1.ProcessHistoryEvent {
	t.Helper()
	root := processRootID(pk, svc, key)
	var out []*enginev1.ProcessHistoryEvent
	if err := (tables.ProcessHistoryTable{S: p.cfg.Snapshotter.Store()}).ScanByInstance(root, 0,
		func(ev *enginev1.ProcessHistoryEvent) error {
			out = append(out, ev)
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestProcess_HistoryStreamRecorded pins the Tier-A apply-path timeline: an
// instance's start, its turn's outbound effects (task dispatch + timer arm), an
// inbound task completion, and the terminal each append one ordered row with the
// right kind, node id, monotonic seq, and a stamped ts.
func TestProcess_HistoryStreamRecorded(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Proc", "i1"
	pk := routing.PartitionKey(svc, key)
	target := &enginev1.InvocationTarget{ServiceName: "Cap", HandlerName: "do"}
	apply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		col.Drain()
	}

	apply(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"}))
	apply(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
		Invoke:   []*enginev1.TaskInvoke{{NodeId: "Task1", Target: target, Input: []byte("ti")}},
		ArmTimer: []*enginev1.TimerArm{{NodeId: "Timer1", FireAtMs: testEnvelopeNowMs + 5000, Slot: 1}},
	}}})
	apply(3, &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: svc, InstanceKey: key,
		Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TaskCompleted{
			TaskCompleted: &enginev1.ProcessTaskCompleted{NodeId: "Task1", Output: []byte("o")},
		}},
	}}})
	apply(4, procAdvancedCmd(pk, svc, key, []byte("final"),
		&enginev1.ProcessTerminal{Output: []byte("out"), RetentionMs: 60_000}))

	evs := procHistEvents(t, p, pk, svc, key)
	want := []enginev1.ProcessHistoryKind{
		enginev1.ProcessHistoryKind_PROCESS_HISTORY_STARTED,
		enginev1.ProcessHistoryKind_PROCESS_HISTORY_TASK_DISPATCHED,
		enginev1.ProcessHistoryKind_PROCESS_HISTORY_TIMER_ARMED,
		enginev1.ProcessHistoryKind_PROCESS_HISTORY_TASK_COMPLETED,
		enginev1.ProcessHistoryKind_PROCESS_HISTORY_COMPLETED,
	}
	if len(evs) != len(want) {
		t.Fatalf("history len=%d, want %d: %+v", len(evs), len(want), evs)
	}
	for i, w := range want {
		if evs[i].GetKind() != w {
			t.Fatalf("history[%d] kind=%v, want %v", i, evs[i].GetKind(), w)
		}
		if evs[i].GetSeq() != uint64(i+1) {
			t.Fatalf("history[%d] seq=%d, want %d", i, evs[i].GetSeq(), i+1)
		}
		if evs[i].GetTsMs() == 0 {
			t.Fatalf("history[%d] ts_ms unset", i)
		}
	}
	if evs[1].GetNodeId() != "Task1" || evs[2].GetNodeId() != "Timer1" || evs[3].GetNodeId() != "Task1" {
		t.Fatalf("node ids: dispatch=%q arm=%q complete=%q", evs[1].GetNodeId(), evs[2].GetNodeId(), evs[3].GetNodeId())
	}
}

// TestProcess_HistoryRetentionZeroDeletes: retention_ms==0 drops the whole
// timeline with the record on terminal (no post-mortem history without a window).
func TestProcess_HistoryRetentionZeroDeletes(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "P", "k0"
	pk := routing.PartitionKey(svc, key)
	apply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		col.Drain()
	}
	apply(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	apply(2, procAdvancedCmd(pk, svc, key, []byte("final"), &enginev1.ProcessTerminal{}))
	if evs := procHistEvents(t, p, pk, svc, key); len(evs) != 0 {
		t.Fatalf("retention 0 must delete history with the record, got %d rows", len(evs))
	}
}

// TestProcess_HistoryReapDeletes: a retained instance keeps its timeline until the
// windowed reap, which clears record + timeline together.
func TestProcess_HistoryReapDeletes(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "P", "kr"
	pk := routing.PartitionKey(svc, key)
	const retention uint64 = 60_000
	apply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		col.Drain()
	}
	apply(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Name: svc}))
	apply(2, procAdvancedCmd(pk, svc, key, []byte("final"), &enginev1.ProcessTerminal{RetentionMs: retention}))
	if evs := procHistEvents(t, p, pk, svc, key); len(evs) == 0 {
		t.Fatal("retained terminal must keep its history")
	}
	apply(3, &enginev1.Command{Kind: &enginev1.Command_ReapProcessInstance{ReapProcessInstance: &enginev1.ReapProcessInstance{
		Pk: pk, Service: svc, InstanceKey: key, FireAtMs: testEnvelopeNowMs + retention,
	}}})
	if evs := procHistEvents(t, p, pk, svc, key); len(evs) != 0 {
		t.Fatalf("reap must delete history, got %d rows", len(evs))
	}
}

// TestProcess_HistoryKeepLastN pins the live keep-last-N cap: once hist_seq passes
// the cap, each append evicts the row that fell out of the window, so the stored
// timeline stays bounded at DefaultMaxProcessHistoryEvents (the most recent rows).
func TestProcess_HistoryKeepLastN(t *testing.T) {
	p, _, _ := newTestPartition(t)
	pk := routing.PartitionKey("Cap", "kN")
	rec := &enginev1.ProcessInstanceRecord{RootId: processRootID(pk, "Cap", "kN")}
	store := p.cfg.Snapshotter.Store()

	b := store.NewBatch()
	n := limits.DefaultMaxProcessHistoryEvents + 5
	for range n {
		if err := p.appendProcessHistory(b, rec, &enginev1.ProcessHistoryEvent{
			Kind: enginev1.ProcessHistoryKind_PROCESS_HISTORY_EVENT_RECEIVED, TsMs: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	var seqs []uint64
	if err := (tables.ProcessHistoryTable{S: store}).ScanByInstance(rec.GetRootId(), 0,
		func(ev *enginev1.ProcessHistoryEvent) error {
			seqs = append(seqs, ev.GetSeq())
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if uint64(len(seqs)) != limits.DefaultMaxProcessHistoryEvents {
		t.Fatalf("kept %d rows, want %d (keep-last-N)", len(seqs), limits.DefaultMaxProcessHistoryEvents)
	}
	if seqs[0] != 6 || seqs[len(seqs)-1] != n {
		t.Fatalf("window=[%d..%d], want [6..%d]", seqs[0], seqs[len(seqs)-1], n)
	}
	if rec.GetHistSeq() != n {
		t.Fatalf("hist_seq=%d, want %d", rec.GetHistSeq(), n)
	}
}
