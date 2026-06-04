package iflowengine

import (
	"context"
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

const dayMs = 24 * 60 * 60 * 1000

// bpmnWithTTL is emptyBPMN (start -> end, completes on start) plus a namespaced
// camunda:historyTimeToLive — exercises both iflow's parser and the TTL extract.
const bpmnWithTTL = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:camunda="http://camunda.org/schema/1.0/bpmn" targetNamespace="test">
  <process id="p" isExecutable="true" camunda:historyTimeToLive="P30D">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="end"/>
    <endEvent id="end"><incoming>f1</incoming></endEvent>
  </process>
</definitions>`

func TestHistoryTTLms(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"", 0},
		{"P30D", 30 * dayMs},
		{"PT1H", 60 * 60 * 1000},
		{"PT15M", 15 * 60 * 1000},
		{"P1DT12H", dayMs + 12*60*60*1000},
		{"7", 7 * dayMs}, // legacy Camunda 7: a bare integer is a count of days
		{"0", 0},
		{"garbage", 0},
		{"  P5D  ", 5 * dayMs}, // trimmed
	}
	for _, c := range cases {
		if got := historyTTLms(c.in); got != c.want {
			t.Errorf("historyTTLms(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestHistoryTTLFromBPMN(t *testing.T) {
	if got := historyTTLFromBPMN([]byte(bpmnWithTTL)); got != 30*dayMs {
		t.Errorf("namespaced camunda:historyTimeToLive: got %d, want %d", got, 30*dayMs)
	}
	const noTTL = `<definitions><process id="p" isExecutable="true"/></definitions>`
	if got := historyTTLFromBPMN([]byte(noTTL)); got != 0 {
		t.Errorf("no TTL: got %d, want 0", got)
	}
	// The first executable process is selected over a non-executable one.
	const multi = `<definitions>
  <process id="a" isExecutable="false" historyTimeToLive="P1D"/>
  <process id="b" isExecutable="true" historyTimeToLive="P7D"/>
</definitions>`
	if got := historyTTLFromBPMN([]byte(multi)); got != 7*dayMs {
		t.Errorf("multi: got %d, want %d (the executable process)", got, 7*dayMs)
	}
}

func TestHistoryTTLFromCMMN(t *testing.T) {
	const cmmnTTL = `<cmmn:definitions xmlns:cmmn="http://www.omg.org/spec/CMMN/20151109/MODEL" xmlns:camunda="http://camunda.org/schema/1.0/cmmn">
  <cmmn:case id="c" camunda:historyTimeToLive="PT2H"/>
</cmmn:definitions>`
	if got := historyTTLFromCMMN([]byte(cmmnTTL)); got != 2*60*60*1000 {
		t.Errorf("historyTTLFromCMMN: got %d, want %d", got, 2*60*60*1000)
	}
}

func TestMapResolver_RetentionMs(t *testing.T) {
	r := NewMapResolver()
	if err := r.ParseBPMN("Order", "v1", []byte(bpmnWithTTL)); err != nil {
		t.Fatalf("ParseBPMN: %v", err)
	}
	if got := r.RetentionMs(&enginev1.ModelRef{Kind: "bpmn", Name: "Order", Version: "v1"}); got != 30*dayMs {
		t.Fatalf("RetentionMs = %d, want %d", got, 30*dayMs)
	}
	// A model with no declared TTL resolves to 0 (immediate delete).
	if err := r.ParseBPMN("NoTTL", "v1", []byte(emptyBPMN)); err != nil {
		t.Fatalf("ParseBPMN: %v", err)
	}
	if got := r.RetentionMs(&enginev1.ModelRef{Kind: "bpmn", Name: "NoTTL", Version: "v1"}); got != 0 {
		t.Fatalf("no-TTL RetentionMs = %d, want 0", got)
	}
}

// TestAdvanceBPMN_StampsRetention is the end-to-end stamp: a model declaring
// historyTimeToLive completes with that window on its ProcessTerminal.
func TestAdvanceBPMN_StampsRetention(t *testing.T) {
	a := New(mustResolver(t, "ttl", bpmnWithTTL))
	adv, err := a.Advance(context.Background(), startInput("ttl", nil, 1000))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	term := adv.GetTerminal()
	if term == nil || term.GetFailed() {
		t.Fatalf("want successful terminal, got %+v", term)
	}
	if got := term.GetRetentionMs(); got != 30*dayMs {
		t.Fatalf("terminal RetentionMs = %d, want %d", got, 30*dayMs)
	}
}
