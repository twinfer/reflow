package iflowengine

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/twinfer/reflow/internal/engine/invoker"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Start -> CallActivity(calledElement="childProc") -> End.
const callActivityBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="ca"/>
    <callActivity id="ca" calledElement="childProc"><incoming>f1</incoming><outgoing>f2</outgoing></callActivity>
    <sequenceFlow id="f2" sourceRef="ca" targetRef="end"/>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
  </process>
</definitions>`

func childCompletedPayload(node string, out []byte) *enginev1.ProcessEventPayload {
	return &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_ChildCompleted{
		ChildCompleted: &enginev1.ProcessChildCompleted{NodeId: node, Output: out},
	}}
}

func TestAdvanceBPMN_CallActivityStartsChild(t *testing.T) {
	a := New(mustResolver(t, "caparent", callActivityBPMN))

	adv, err := a.Advance(context.Background(), startInput("caparent", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if len(adv.GetStartChild()) != 1 {
		t.Fatalf("want 1 StartChild, got %d", len(adv.GetStartChild()))
	}
	c := adv.GetStartChild()[0]
	if c.GetNodeId() != "ca" {
		t.Errorf("child node = %q, want ca", c.GetNodeId())
	}
	if c.GetModelRef().GetName() != "childProc" || c.GetModelRef().GetVersion() != "v1" || c.GetModelRef().GetKind() != "bpmn" {
		t.Errorf("child ModelRef = %v, want bpmn/childProc/v1 (convention)", c.GetModelRef())
	}
	if c.GetKind() != enginev1.ProcessKind_PROCESS_KIND_BPMN {
		t.Errorf("child kind = %v, want BPMN", c.GetKind())
	}
	if c.GetInstanceKey() != "i1/ca/" {
		t.Errorf("child instance key = %q, want deterministic i1/ca/", c.GetInstanceKey())
	}
}

func TestAdvanceBPMN_ChildCompletionCompletesParent(t *testing.T) {
	a := New(mustResolver(t, "caparent", callActivityBPMN))

	start, err := a.Advance(context.Background(), startInput("caparent", nil, 1000))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	cont := invoker.ProcessAdvanceInput{
		Pk: 0, Service: "caparent", InstanceKey: "i1",
		Record: bpmnRecord("caparent", start.GetNewState()),
		Entry:  &enginev1.ProcessInboxEntry{Payload: childCompletedPayload("ca", []byte(`{"r":1}`)), LogicalTimeMs: 2000},
	}
	adv, err := a.Advance(context.Background(), cont)
	if err != nil {
		t.Fatalf("child completion: %v", err)
	}
	if adv.GetTerminal() == nil || adv.GetTerminal().GetFailed() {
		t.Fatalf("want successful terminal after child completion, got %+v", adv.GetTerminal())
	}
}

// TestMapResolver_ConcurrentRegisterAndServe drives the read methods from many
// goroutines while other goroutines register models/decisions/children. Run
// under -race it is the proof that MapResolver is concurrency-safe: the RWMutex
// (and the under-lock decision snapshot, exercised by invoking the returned
// resolver) means no read races a registration. Without synchronization the race
// detector flags the map access.
func TestMapResolver_ConcurrentRegisterAndServe(t *testing.T) {
	r := mustResolver(t, "seed", echoServiceTaskBPMN) // "seed"/v1 has a graph
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "seed", Version: "v1"}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = r.BPMN(ref)
				if dr := r.BPMNDecisions(ref); dr != nil {
					_, _, _ = dr("d0") // exercise the snapshot the closure reads
				}
				_, _ = r.ChildRef(ref, "bpmn", "c0")
				_, _ = r.CMMN(ref)
			}
		})
	}

	var writers sync.WaitGroup
	for i := range 4 {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			name := fmt.Sprintf("m%d", i)
			for j := range 200 {
				r.AddBPMN(name, "v1", nil)
				r.AddDecision("seed", "v1", fmt.Sprintf("d%d", j), nil)
				r.AddChildRef("seed", "v1", fmt.Sprintf("c%d", j), ref)
			}
		}(i)
	}
	writers.Wait()
	close(stop)
	readers.Wait()
}

func TestMapResolver_ChildRefOverride(t *testing.T) {
	r := mustResolver(t, "caparent", callActivityBPMN)
	r.AddChildRef("caparent", "v1", "childProc", &enginev1.ModelRef{Kind: "bpmn", Name: "OtherChild", Version: "v9"})

	got, err := r.ChildRef(&enginev1.ModelRef{Kind: "bpmn", Name: "caparent", Version: "v1"}, "bpmn", "childProc")
	if err != nil {
		t.Fatalf("ChildRef: %v", err)
	}
	if got.GetName() != "OtherChild" || got.GetVersion() != "v9" {
		t.Errorf("override ChildRef = %v, want OtherChild/v9", got)
	}
}
