package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/invoker"
	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/internal/storage/tables"
	"github.com/twinfer/reflw/pkg/reflw/processengine"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// msgCatchModelXML: Start -> IntermediateCatch(message "shipped", correlate
// orderId) -> End.
const msgCatchModelXML = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <message id="shipped" name="shipped"/>
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <intermediateCatchEvent id="wait">
      <extensionElements><correlate var="orderId"/></extensionElements>
      <incoming>f1</incoming><outgoing>f2</outgoing>
      <messageEventDefinition messageRef="shipped"/>
    </intermediateCatchEvent>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="wait"/>
    <sequenceFlow id="f2" sourceRef="wait" targetRef="end"/>
  </process>
</definitions>`

func deliverMsgCmd(pk uint64, name, corr string, payload []byte) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_DeliverProcessMessage{DeliverProcessMessage: &enginev1.DeliverProcessMessage{
		Pk: pk, MessageName: name, CorrelationKey: corr, Payload: payload,
	}}}
}

func countSubscriptions(t *testing.T, p *Partition, name, corr string) int {
	t.Helper()
	lp := keys.LPFromPartitionKey(routing.PartitionKey(name, corr))
	subT := tables.MessageSubscriptionTable{S: p.cfg.Snapshotter.Store()}
	n := 0
	if err := subT.ScanByCorrelation(lp, name, corr, func(_ []byte, _ *enginev1.MessageSubscription) error {
		n++
		return nil
	}); err != nil {
		t.Fatalf("scan subscriptions: %v", err)
	}
	return n
}

// TestProcess_MessageCorrelationReadPath drives a real BPMN message-catch model
// through the real processengine.Adapter and the real partition state machine over
// the full correlation read path: start -> park + write subscription -> deliver a
// correlated message -> fan ProcessMessageReceived to the instance + one-shot
// consume the subscription -> resume -> terminal reap. Single partition, so the
// subscription is co-located (same-shard write); the cross-shard outbox split is
// asserted separately in TestProcess_SubscribeCrossShardUsesOutbox.
func TestProcess_MessageCorrelationReadPath(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "msgproc", "o-1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, inbox := procStore(p)

	res := processengine.NewMapResolver()
	if err := res.ParseBPMN("msgproc", "v1", []byte(msgCatchModelXML)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	adapter := processengine.New(res)

	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}
	runTurn := func() *enginev1.ProcessAdvanced {
		t.Helper()
		act := firstAdvance(col.Drain(), svc)
		if act == nil {
			t.Fatal("no ActAdvanceProcess produced")
		}
		rec, ok, err := procs.Get(lp, svc, key)
		if err != nil || !ok {
			t.Fatalf("record load: ok=%v err=%v", ok, err)
		}
		adv, err := adapter.Advance(context.Background(), invoker.ProcessAdvanceInput{
			Pk: pk, Service: svc, InstanceKey: key, Record: rec, Entry: act.Entry,
		})
		if err != nil {
			t.Fatalf("adapter.Advance: %v", err)
		}
		return adv
	}

	// 1. Start: the adapter parks at the message catch, emitting a Subscribe.
	must(1, procEventCmd(pk, svc, key, []byte(`{"orderId":"A-1"}`),
		&enginev1.ModelRef{Kind: "bpmn", Name: "msgproc", Version: "v1"}))
	adv1 := runTurn()
	if len(adv1.GetSubscribe()) != 1 {
		t.Fatalf("start turn: want 1 subscribe, got %v", adv1.GetSubscribe())
	}
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv1}})

	// The subscription row is written, co-located with the message routing key,
	// and the instance is parked (idle: no active turn).
	if n := countSubscriptions(t, p, "shipped", "A-1"); n != 1 {
		t.Fatalf("after subscribe: want 1 subscription row, got %d", n)
	}
	if r, ok, _ := procs.Get(lp, svc, key); !ok || r.GetActiveSeq() != 0 {
		t.Fatalf("after subscribe: want parked RUNNING (active_seq 0), got ok=%v rec=%+v", ok, r)
	}

	// A non-matching correlation key must not consume the subscription.
	must(3, deliverMsgCmd(routing.PartitionKey("shipped", "OTHER"), "shipped", "OTHER", []byte(`{}`)))
	if n := countSubscriptions(t, p, "shipped", "A-1"); n != 1 {
		t.Fatalf("after non-matching delivery: subscription must survive, got %d", n)
	}

	// 2. Deliver the matching message → fans a ProcessMessageReceived to the
	//    instance inbox and one-shot-consumes the subscription.
	must(4, deliverMsgCmd(routing.PartitionKey("shipped", "A-1"), "shipped", "A-1", []byte(`{"tracking":"Z9"}`)))
	if n := countSubscriptions(t, p, "shipped", "A-1"); n != 0 {
		t.Fatalf("after delivery: want subscription consumed, got %d rows", n)
	}
	if e, ok, _ := inbox.Get(lp, svc, key, 2); !ok || e.GetPayload().GetMessageReceived() == nil {
		t.Fatalf("after delivery: want MessageReceived queued at seq 2, ok=%v entry=%+v", ok, e)
	}

	// 3. Run the resume turn → terminal; apply it → instance reaped.
	adv2 := runTurn()
	if adv2.GetTerminal() == nil || adv2.GetTerminal().GetFailed() {
		t.Fatalf("resume turn: want successful terminal, got %+v", adv2.GetTerminal())
	}
	must(5, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv2}})
	if _, ok, _ := procs.Get(lp, svc, key); ok {
		t.Fatalf("after terminal: want instance reaped, record still present")
	}
}

// TestProcess_DuplicateStartIsDropped proves the apply-path idempotency guard: a
// second start ProcessEvent for an already-existing instance is dropped rather
// than mis-fed as a continuation (which would corrupt the running turn).
func TestProcess_DuplicateStartIsDropped(t *testing.T) {
	p, _, _ := newTestPartition(t)
	const svc, key = "dup", "i1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, inbox := procStore(p)
	mr := &enginev1.ModelRef{Kind: "bpmn", Name: "dup", Version: "v1"}

	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(pk, svc, key, []byte(`{"a":1}`), mr))
	rec1, ok, err := procs.Get(lp, svc, key)
	if err != nil || !ok {
		t.Fatalf("after first start: ok=%v err=%v", ok, err)
	}
	if rec1.GetNextSeq() != 2 {
		t.Fatalf("after first start: want next_seq 2, got %d", rec1.GetNextSeq())
	}

	// Second start for the same (service, key): must be dropped, leaving the
	// record and inbox cursor untouched (no seq-2 continuation appended).
	must(2, procEventCmd(pk, svc, key, []byte(`{"a":2}`), mr))
	rec2, _, _ := procs.Get(lp, svc, key)
	if rec2.GetNextSeq() != 2 {
		t.Fatalf("after duplicate start: want next_seq still 2 (dropped), got %d", rec2.GetNextSeq())
	}
	if e, ok, _ := inbox.Get(lp, svc, key, 2); ok {
		t.Fatalf("after duplicate start: no seq-2 inbox entry expected, got %+v", e)
	}
}

// TestProcess_SubscribeCrossShardUsesOutbox proves that when the message routing
// key (message_name, correlation_key) hashes to a different shard than the
// instance, actuateProcessInstructions ships the subscription via the outbox
// (so it lands co-located with where DeliverProcessMessage will look) instead of
// writing it locally.
func TestProcess_SubscribeCrossShardUsesOutbox(t *testing.T) {
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

	// Route the message key (shipped/A-1) to shard 2; this partition is shard 1.
	part := routing.NewPartitioner(4)
	msgLP := keys.LPFromPartitionKey(routing.PartitionKey("shipped", "A-1"))
	part.SetLPOwnersSnapshot(map[uint32]uint64{msgLP: 2})

	p := NewPartition(1, 1, PartitionConfig{
		Snapshotter: snap,
		Leadership:  lead,
		Collector:   col,
		Partitioner: *part,
	})
	t.Cleanup(func() { _ = p.Close() })

	const svc, key = "msgproc", "o-1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	procs, _ := procStore(p)

	res := processengine.NewMapResolver()
	if err := res.ParseBPMN("msgproc", "v1", []byte(msgCatchModelXML)); err != nil {
		t.Fatalf("parse model: %v", err)
	}
	adapter := processengine.New(res)

	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(pk, svc, key, []byte(`{"orderId":"A-1"}`),
		&enginev1.ModelRef{Kind: "bpmn", Name: "msgproc", Version: "v1"}))
	act := firstAdvance(col.Drain(), svc)
	if act == nil {
		t.Fatal("no ActAdvanceProcess produced")
	}
	rec, _, _ := procs.Get(lp, svc, key)
	adv, err := adapter.Advance(context.Background(), invoker.ProcessAdvanceInput{
		Pk: pk, Service: svc, InstanceKey: key, Record: rec, Entry: act.Entry,
	})
	if err != nil {
		t.Fatalf("adapter.Advance: %v", err)
	}
	if len(adv.GetSubscribe()) != 1 {
		t.Fatalf("want 1 subscribe, got %v", adv.GetSubscribe())
	}
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv}})

	// No local subscription row — it was shipped cross-shard.
	if n := countSubscriptions(t, p, "shipped", "A-1"); n != 0 {
		t.Fatalf("cross-shard: want no local subscription row, got %d", n)
	}

	// An outbox envelope to shard 2 carries the ProcessSubscribe.
	ot := tables.OutboxTable{S: p.cfg.Snapshotter.Store()}
	found := false
	if err := ot.ScanFrom(0, func(row tables.OutboxRow) error {
		ps := row.Envelope.GetProcessSubscribe()
		if ps == nil {
			return nil
		}
		found = true
		if row.Envelope.GetDestinationShardId() != 2 {
			t.Errorf("dest shard = %d, want 2", row.Envelope.GetDestinationShardId())
		}
		if ps.GetSub().GetMessageName() != "shipped" || ps.GetSub().GetInstanceKey() != key {
			t.Errorf("subscription mismatch: %+v", ps.GetSub())
		}
		if ps.GetSub().GetInstancePk() != pk {
			t.Errorf("subscription instance_pk = %d, want %d", ps.GetSub().GetInstancePk(), pk)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan outbox: %v", err)
	}
	if !found {
		t.Fatal("cross-shard: no ProcessSubscribe outbox envelope found")
	}
}

// TestProcess_CancelChildCrossShardUsesOutbox proves the cascade crosses shards:
// when a parent's child lives on a different shard, finishProcessInstance ships a
// ProcessCancel via the outbox to the child's shard (where onProcessCancel applies
// it) instead of cancelling inline.
func TestProcess_CancelChildCrossShardUsesOutbox(t *testing.T) {
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

	const psvc, pkey = "Parent", "p1"
	const csvc, ckey = "Child", "c1"
	ppk := routing.PartitionKey(psvc, pkey)
	cpk := routing.PartitionKey(csvc, ckey)

	// Route the child's LP to shard 2; this partition is shard 1. (Leave the
	// parent's LP at its default so its own ops stay local.)
	part := routing.NewPartitioner(4)
	part.SetLPOwnersSnapshot(map[uint32]uint64{keys.LPFromPartitionKey(cpk): 2})

	p := NewPartition(1, 1, PartitionConfig{
		Snapshotter: snap,
		Leadership:  lead,
		Collector:   col,
		Partitioner: *part,
	})
	t.Cleanup(func() { _ = p.Close() })

	must := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
	}

	must(1, procEventCmd(ppk, psvc, pkey, []byte("pv"), &enginev1.ModelRef{Name: psvc}))
	col.Drain()
	// Parent starts the child (shipped cross-shard); the child-index row is written
	// locally on the parent's shard regardless.
	must(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: ppk, Service: psvc, InstanceKey: pkey, NewState: []byte("ps1"),
		StartChild: []*enginev1.ChildStart{{
			NodeId: "CA1", ModelRef: &enginev1.ModelRef{Name: csvc},
			Kind: enginev1.ProcessKind_PROCESS_KIND_BPMN, InstanceKey: ckey,
		}},
	}}})
	col.Drain()

	// Terminate the parent.
	must(3, procExtCmd(ppk, psvc, pkey, []byte("kill")))
	col.Drain()
	must(4, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: ppk, Service: psvc, InstanceKey: pkey, NewState: []byte("ps2"),
		Terminal: &enginev1.ProcessTerminal{Failed: false},
	}}})

	// A ProcessCancel envelope addressed to shard 2 carries the child's address.
	ot := tables.OutboxTable{S: p.cfg.Snapshotter.Store()}
	found := false
	if err := ot.ScanFrom(0, func(row tables.OutboxRow) error {
		pc := row.Envelope.GetProcessCancel()
		if pc == nil {
			return nil
		}
		found = true
		if row.Envelope.GetDestinationShardId() != 2 {
			t.Errorf("dest shard = %d, want 2", row.Envelope.GetDestinationShardId())
		}
		if pc.GetService() != csvc || pc.GetInstanceKey() != ckey || pc.GetPk() != cpk {
			t.Errorf("cancel address mismatch: %+v", pc)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan outbox: %v", err)
	}
	if !found {
		t.Fatal("cross-shard: no ProcessCancel outbox envelope found")
	}
}
