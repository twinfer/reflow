package iflowengine

import (
	"context"
	"errors"
	"testing"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func bpmnModelRecord(name, version, xml string) *enginev1.ModelRecord {
	return &enginev1.ModelRecord{
		ModelRef: &enginev1.ModelRef{Kind: "bpmn", Name: name, Version: version},
		Xml:      []byte(xml),
	}
}

func TestTableResolver_ReconcileBPMNAndRetention(t *testing.T) {
	r := NewTableResolver(nil)
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "Order", Version: "v1"}

	// Before any reconcile: not found.
	if _, err := r.BPMN(ref); !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("pre-reconcile BPMN err = %v, want ErrModelNotFound", err)
	}

	r.Reconcile([]*enginev1.ModelRecord{bpmnModelRecord("Order", "v1", bpmnWithTTL)})

	g, err := r.BPMN(ref)
	if err != nil || g == nil {
		t.Fatalf("BPMN after reconcile: %v", err)
	}
	if got := r.RetentionMs(ref); got != 30*dayMs {
		t.Fatalf("RetentionMs = %d, want %d", got, 30*dayMs)
	}
	// An unregistered ref is still not found.
	if _, err := r.BPMN(&enginev1.ModelRef{Kind: "bpmn", Name: "Nope", Version: "v1"}); !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("unknown model err = %v, want ErrModelNotFound", err)
	}
}

func TestTableResolver_ParseFailurePreservesPrevious(t *testing.T) {
	r := NewTableResolver(nil)
	ref := &enginev1.ModelRef{Kind: "bpmn", Name: "Order", Version: "v1"}

	r.Reconcile([]*enginev1.ModelRecord{bpmnModelRecord("Order", "v1", bpmnWithTTL)})
	// A later reconcile where the same model is unparseable must keep the prior
	// good entry rather than dropping the model.
	r.Reconcile([]*enginev1.ModelRecord{bpmnModelRecord("Order", "v1", "<<<not xml")})

	if _, err := r.BPMN(ref); err != nil {
		t.Fatalf("parse-fail should preserve previous graph: %v", err)
	}
	if got := r.RetentionMs(ref); got != 30*dayMs {
		t.Fatalf("retention should be preserved: got %d, want %d", got, 30*dayMs)
	}
}

func TestTableResolver_RunReconcilerFromReader(t *testing.T) {
	r := NewTableResolver(nil)
	reader := fakeModelReader{records: []*enginev1.ModelRecord{bpmnModelRecord("Order", "v1", bpmnWithTTL)}}
	// reconcileFromReader is the single-pass body RunReconciler loops; exercise
	// it directly so the test doesn't depend on goroutine timing.
	r.reconcileFromReader(context.Background(), reader)
	if _, err := r.BPMN(&enginev1.ModelRef{Kind: "bpmn", Name: "Order", Version: "v1"}); err != nil {
		t.Fatalf("BPMN after reader reconcile: %v", err)
	}
}

type fakeModelReader struct {
	records []*enginev1.ModelRecord
}

func (f fakeModelReader) ListModels(_ context.Context) ([]*enginev1.ModelRecord, uint64, error) {
	return f.records, 1, nil
}
