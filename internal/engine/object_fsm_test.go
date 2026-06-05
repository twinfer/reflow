package engine

import (
	"bytes"
	"context"
	"testing"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func mkInvID(uuid string) *enginev1.InvocationId {
	return &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte(uuid)}
}

// captureActivations is a helper that records every invocation the FSM grants
// the lease to, in order.
func captureActivations() (*[]*enginev1.InvocationId, func(*enginev1.InvocationId)) {
	out := &[]*enginev1.InvocationId{}
	return out, func(id *enginev1.InvocationId) { *out = append(*out, id) }
}

func TestObjectFSM_EnqueueWhenIdleActivates(t *testing.T) {
	got, onActivate := captureActivations()
	sm, next := buildObjectFSM(nil, onActivate)
	a := mkInvID("a-aaaaaaaaaaaaaaa")

	if err := sm.Fire(vobjEnqueue, a); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if next.GetState() != enginev1.KeyLeaseStatus_ACTIVE {
		t.Errorf("state: got %v want ACTIVE", next.GetState())
	}
	if !bytes.Equal(next.GetCurrentInvocation().GetUuid(), a.GetUuid()) {
		t.Errorf("current_invocation: got %x", next.GetCurrentInvocation().GetUuid())
	}
	if len(next.GetQueue()) != 0 {
		t.Errorf("queue: got %d entries, want 0", len(next.GetQueue()))
	}
	if len(*got) != 1 || !bytes.Equal((*got)[0].GetUuid(), a.GetUuid()) {
		t.Errorf("activations: %+v", *got)
	}
}

func TestObjectFSM_EnqueueWhenActiveAppendsNoActivate(t *testing.T) {
	got, onActivate := captureActivations()
	a := mkInvID("a-aaaaaaaaaaaaaaa")
	b := mkInvID("b-bbbbbbbbbbbbbbb")
	cur := &enginev1.KeyLeaseStatus{
		State:             enginev1.KeyLeaseStatus_ACTIVE,
		CurrentInvocation: a,
	}
	sm, next := buildObjectFSM(cur, onActivate)

	if err := sm.Fire(vobjEnqueue, b); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if next.GetState() != enginev1.KeyLeaseStatus_ACTIVE {
		t.Errorf("state changed unexpectedly: %v", next.GetState())
	}
	if !bytes.Equal(next.GetCurrentInvocation().GetUuid(), a.GetUuid()) {
		t.Errorf("holder changed unexpectedly")
	}
	if len(next.GetQueue()) != 1 || !bytes.Equal(next.GetQueue()[0].GetUuid(), b.GetUuid()) {
		t.Errorf("queue: %+v", next.GetQueue())
	}
	if len(*got) != 0 {
		t.Errorf("activations: got %d, want 0 (holder still running)", len(*got))
	}
}

func TestObjectFSM_CompleteEmptyQueueReturnsIdle(t *testing.T) {
	got, onActivate := captureActivations()
	a := mkInvID("a-aaaaaaaaaaaaaaa")
	cur := &enginev1.KeyLeaseStatus{
		State:             enginev1.KeyLeaseStatus_ACTIVE,
		CurrentInvocation: a,
	}
	sm, next := buildObjectFSM(cur, onActivate)

	if err := sm.Fire(vobjComplete); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if next.GetState() != enginev1.KeyLeaseStatus_IDLE {
		t.Errorf("state: got %v want IDLE", next.GetState())
	}
	if next.GetCurrentInvocation() != nil {
		t.Errorf("current_invocation not cleared: %+v", next.GetCurrentInvocation())
	}
	if len(*got) != 0 {
		t.Errorf("activations: got %d, want 0", len(*got))
	}
}

func TestObjectFSM_CompleteNonEmptyQueueActivatesNext(t *testing.T) {
	got, onActivate := captureActivations()
	a := mkInvID("a-aaaaaaaaaaaaaaa")
	b := mkInvID("b-bbbbbbbbbbbbbbb")
	c := mkInvID("c-ccccccccccccccc")
	cur := &enginev1.KeyLeaseStatus{
		State:             enginev1.KeyLeaseStatus_ACTIVE,
		CurrentInvocation: a,
		Queue:             []*enginev1.InvocationId{b, c},
	}
	sm, next := buildObjectFSM(cur, onActivate)

	if err := sm.Fire(vobjComplete); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if next.GetState() != enginev1.KeyLeaseStatus_ACTIVE {
		t.Errorf("state: got %v want ACTIVE", next.GetState())
	}
	if !bytes.Equal(next.GetCurrentInvocation().GetUuid(), b.GetUuid()) {
		t.Errorf("new holder: %x want %x", next.GetCurrentInvocation().GetUuid(), b.GetUuid())
	}
	if len(next.GetQueue()) != 1 || !bytes.Equal(next.GetQueue()[0].GetUuid(), c.GetUuid()) {
		t.Errorf("queue tail: %+v", next.GetQueue())
	}
	if len(*got) != 1 || !bytes.Equal((*got)[0].GetUuid(), b.GetUuid()) {
		t.Errorf("activations: %+v", *got)
	}
}

func TestObjectFSM_FullLifecycleFIFO(t *testing.T) {
	got, onActivate := captureActivations()
	sm, next := buildObjectFSM(nil, onActivate)
	a := mkInvID("a-aaaaaaaaaaaaaaa")
	b := mkInvID("b-bbbbbbbbbbbbbbb")
	c := mkInvID("c-ccccccccccccccc")

	// Submit three in order while idle/active.
	for _, id := range []*enginev1.InvocationId{a, b, c} {
		if err := sm.Fire(vobjEnqueue, id); err != nil {
			t.Fatalf("enqueue %x: %v", id.GetUuid(), err)
		}
	}
	if !bytes.Equal(next.GetCurrentInvocation().GetUuid(), a.GetUuid()) {
		t.Fatalf("first activation: got %x", next.GetCurrentInvocation().GetUuid())
	}
	if len(next.GetQueue()) != 2 {
		t.Fatalf("queue depth after 3 enqueues: %d", len(next.GetQueue()))
	}

	// Complete a → b activates.
	if err := sm.Fire(vobjComplete); err != nil {
		t.Fatalf("complete a: %v", err)
	}
	if !bytes.Equal(next.GetCurrentInvocation().GetUuid(), b.GetUuid()) {
		t.Fatalf("after a: got %x", next.GetCurrentInvocation().GetUuid())
	}

	// Complete b → c activates.
	if err := sm.Fire(vobjComplete); err != nil {
		t.Fatalf("complete b: %v", err)
	}
	if !bytes.Equal(next.GetCurrentInvocation().GetUuid(), c.GetUuid()) {
		t.Fatalf("after b: got %x", next.GetCurrentInvocation().GetUuid())
	}

	// Complete c → idle.
	if err := sm.Fire(vobjComplete); err != nil {
		t.Fatalf("complete c: %v", err)
	}
	if next.GetState() != enginev1.KeyLeaseStatus_IDLE {
		t.Fatalf("after c: state = %v", next.GetState())
	}
	if next.GetCurrentInvocation() != nil {
		t.Fatalf("after c: current_invocation = %+v", next.GetCurrentInvocation())
	}

	// Activation order must be strict FIFO a, b, c.
	if len(*got) != 3 {
		t.Fatalf("activations len = %d", len(*got))
	}
	for i, want := range []*enginev1.InvocationId{a, b, c} {
		if !bytes.Equal((*got)[i].GetUuid(), want.GetUuid()) {
			t.Errorf("activations[%d]: got %x want %x", i, (*got)[i].GetUuid(), want.GetUuid())
		}
	}
}

// Confirm SM.State() reports the right thing through the external-storage
// accessor — sanity check that the wiring works in both directions.
func TestObjectFSM_StateAccessorReflectsProto(t *testing.T) {
	_, onActivate := captureActivations()
	sm, _ := buildObjectFSM(nil, onActivate)
	st, err := sm.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st != vobjIdle {
		t.Errorf("initial state: got %v want vobjIdle", st)
	}
	if err := sm.Fire(vobjEnqueue, mkInvID("a-aaaaaaaaaaaaaaa")); err != nil {
		t.Fatal(err)
	}
	st, err = sm.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st != vobjActive {
		t.Errorf("after enqueue: got %v want vobjActive", st)
	}
}
