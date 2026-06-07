package engine

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// procIncidentAdvancedCmd builds the ProcessAdvanced the adapter emits for a
// top-level uncaught failure: new state plus an incident (no terminal).
func procIncidentAdvancedCmd(pk uint64, service, key string, newState []byte, node, cause string) *enginev1.Command {
	return &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: service, InstanceKey: key, NewState: newState,
		Incident: &enginev1.ProcessIncident{NodeId: node, Cause: cause},
	}}}
}

// TestProcess_IncidentParkSurviveReapTerminate drives a top-level instance into an
// incident (parked, non-terminal, failing state retained), confirms it is
// observable and survives a reap fire, then resolves it with TERMINATE (record +
// timeline deleted).
func TestProcess_IncidentParkSurviveReapTerminate(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Proc", "inc1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	apply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		col.Drain()
	}
	procT, _ := procStore(p)

	apply(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"}))
	apply(2, procIncidentAdvancedCmd(pk, svc, key, []byte("failed-state"), "Task1", "boom"))

	rec, ok, err := procT.Get(lp, svc, key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("incident instance must remain present (not reaped)")
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_INCIDENT {
		t.Fatalf("status = %v, want INCIDENT", rec.GetStatus())
	}
	if rec.GetActiveSeq() != 0 || rec.GetOutstanding() != 0 {
		t.Fatalf("parked incident must be idle: active_seq=%d outstanding=%d", rec.GetActiveSeq(), rec.GetOutstanding())
	}
	if inc := rec.GetIncident(); inc.GetNodeId() != "Task1" || inc.GetCause() != "boom" || inc.GetRaisedAtMs() == 0 {
		t.Fatalf("incident = %+v", rec.GetIncident())
	}
	if string(rec.GetStateBlob()) != "failed-state" {
		t.Fatalf("incident must retain the failing state, got %q", rec.GetStateBlob())
	}

	// The timeline ends with INCIDENT_RAISED.
	evs := procHistEvents(t, p, pk, svc, key)
	if n := len(evs); n == 0 || evs[n-1].GetKind() != enginev1.ProcessHistoryKind_PROCESS_HISTORY_INCIDENT_RAISED {
		t.Fatalf("last history kind = %v, want INCIDENT_RAISED: %+v", lastHistKind(evs), evs)
	}

	// A reap fire must not delete a parked incident (it never schedules one, so
	// this is the no-row no-op path; the status guard is the defensive backstop).
	apply(3, &enginev1.Command{Kind: &enginev1.Command_ReapProcessInstance{ReapProcessInstance: &enginev1.ReapProcessInstance{
		Pk: pk, Service: svc, InstanceKey: key, FireAtMs: testEnvelopeNowMs + 1,
	}}})
	if _, ok, _ := procT.Get(lp, svc, key); !ok {
		t.Fatal("reap must not delete an incident instance")
	}

	// TERMINATE resolves it: record + timeline deleted (retention 0).
	apply(4, &enginev1.Command{Kind: &enginev1.Command_ResolveProcessIncident{ResolveProcessIncident: &enginev1.ResolveProcessIncident{
		Pk: pk, Service: svc, InstanceKey: key,
		Resolution: enginev1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_TERMINATE,
	}}})
	if _, ok, _ := procT.Get(lp, svc, key); ok {
		t.Fatal("TERMINATE must delete the record")
	}
	if evs := procHistEvents(t, p, pk, svc, key); len(evs) != 0 {
		t.Fatalf("TERMINATE must delete the timeline, got %d rows", len(evs))
	}
}

// TestProcess_ResolveIncidentNonIncidentNoop: resolving an instance that isn't in
// INCIDENT (here, never started) is a benign no-op — it must not halt the shard.
func TestProcess_ResolveIncidentNonIncidentNoop(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Proc", "noinc"
	pk := routing.PartitionKey(svc, key)
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_ResolveProcessIncident{ResolveProcessIncident: &enginev1.ResolveProcessIncident{
			Pk: pk, Service: svc, InstanceKey: key,
			Resolution: enginev1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_TERMINATE,
		}},
	})}}); err != nil {
		t.Fatalf("resolve on absent instance must be a no-op, got %v", err)
	}
	col.Drain()
}

func lastHistKind(evs []*enginev1.ProcessHistoryEvent) enginev1.ProcessHistoryKind {
	if len(evs) == 0 {
		return enginev1.ProcessHistoryKind_PROCESS_HISTORY_KIND_UNSPECIFIED
	}
	return evs[len(evs)-1].GetKind()
}

// TestProcess_ResolveIncidentRetryUnparks drives a BPMN instance into an incident,
// then RETRY: the apply path un-parks it (RUNNING, incident cleared), enqueues a
// retry turn carrying the failed node + operator var patch, activates it
// (ActAdvanceProcess), and records INCIDENT_RESOLVED. (The reflwos re-dispatch
// itself is covered by bpmn/engine_retry_test.go and the ingress e2e test.)
func TestProcess_ResolveIncidentRetryUnparks(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Proc", "ret1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	apply := func(idx uint64, cmd *enginev1.Command) []Action {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		return col.Drain()
	}
	procT, inboxT := procStore(p)

	apply(1, procEventCmd(pk, svc, key, []byte("v"), &enginev1.ModelRef{Kind: "bpmn", Name: svc, Version: "v1"}))
	apply(2, procIncidentAdvancedCmd(pk, svc, key, []byte("failed-state"), "gw", "no match"))

	acts := apply(3, &enginev1.Command{Kind: &enginev1.Command_ResolveProcessIncident{ResolveProcessIncident: &enginev1.ResolveProcessIncident{
		Pk: pk, Service: svc, InstanceKey: key,
		Resolution: enginev1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_RETRY,
		VarPatch:   []byte(`{"status":"approved"}`),
	}}})

	rec, ok, err := procT.Get(lp, svc, key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("retried instance must remain present")
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		t.Fatalf("status = %v, want RUNNING", rec.GetStatus())
	}
	if rec.GetIncident() != nil {
		t.Fatalf("incident must be cleared, got %+v", rec.GetIncident())
	}
	if rec.GetActiveSeq() == 0 {
		t.Fatal("retry turn must be active (active_seq != 0)")
	}

	// The active inbox turn is a Retry carrying the failed node + the operator patch.
	entry, eok, err := inboxT.Get(lp, svc, key, rec.GetActiveSeq())
	if err != nil {
		t.Fatal(err)
	}
	if !eok {
		t.Fatal("retry inbox entry missing")
	}
	r := entry.GetPayload().GetRetry()
	if r == nil || r.GetNodeId() != "gw" {
		t.Fatalf("inbox payload = %+v, want Retry{node:gw}", entry.GetPayload())
	}
	if string(r.GetVarPatch()) != `{"status":"approved"}` {
		t.Fatalf("retry var patch = %q", r.GetVarPatch())
	}

	// The leader was asked to drive the retry turn.
	if firstAdvance(acts, svc) == nil {
		t.Fatal("RETRY must push ActAdvanceProcess to drive the retry turn")
	}

	// The timeline ends with INCIDENT_RESOLVED.
	if k := lastHistKind(procHistEvents(t, p, pk, svc, key)); k != enginev1.ProcessHistoryKind_PROCESS_HISTORY_INCIDENT_RESOLVED {
		t.Fatalf("last history kind = %v, want INCIDENT_RESOLVED", k)
	}
}

// TestProcess_ResolveIncidentRetryCMMN: a CMMN incident parks at quiescence
// (outstanding==0), and RETRY now un-parks it (CMMN reactivate: Failed → Active),
// enqueuing a retry turn carrying the failed item + operator var patch and
// recording INCIDENT_RESOLVED. (The reflwos ManualReactivate re-dispatch is
// covered by cmmn/termination_test.go and the adapter mapping test.)
func TestProcess_ResolveIncidentRetryCMMN(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Case", "cinc1"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	apply := func(idx uint64, cmd *enginev1.Command) []Action {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		return col.Drain()
	}
	procT, inboxT := procStore(p)

	// A CMMN-kind instance; the fault turn has no in-flight sibling, so it parks
	// immediately (quiescence: outstanding==0).
	apply(1, &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: svc, InstanceKey: key, Payload: extPayload([]byte("v")),
		ModelRef: &enginev1.ModelRef{Kind: "cmmn", Name: svc, Version: "v1"},
		Kind:     enginev1.ProcessKind_PROCESS_KIND_CMMN,
	}}})
	apply(2, procIncidentAdvancedCmd(pk, svc, key, []byte("failed"), "Item1", "boom"))

	rec, _, err := procT.Get(lp, svc, key)
	if err != nil {
		t.Fatal(err)
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_INCIDENT {
		t.Fatalf("CMMN fault must park at quiescence: status=%v", rec.GetStatus())
	}
	if inc := rec.GetIncident(); inc.GetNodeId() != "Item1" || inc.GetRaisedAtMs() == 0 {
		t.Fatalf("incident = %+v", rec.GetIncident())
	}

	acts := apply(3, &enginev1.Command{Kind: &enginev1.Command_ResolveProcessIncident{ResolveProcessIncident: &enginev1.ResolveProcessIncident{
		Pk: pk, Service: svc, InstanceKey: key,
		Resolution: enginev1.ProcessIncidentResolution_PROCESS_INCIDENT_RESOLUTION_RETRY,
		VarPatch:   []byte(`{"fixed":true}`),
	}}})

	rec, ok, err := procT.Get(lp, svc, key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		t.Fatalf("RETRY must un-park to RUNNING: ok=%v status=%v", ok, rec.GetStatus())
	}
	if rec.GetIncident() != nil {
		t.Fatalf("incident must be cleared, got %+v", rec.GetIncident())
	}
	if rec.GetActiveSeq() == 0 {
		t.Fatal("retry turn must be active")
	}
	entry, eok, err := inboxT.Get(lp, svc, key, rec.GetActiveSeq())
	if err != nil {
		t.Fatal(err)
	}
	if !eok {
		t.Fatal("retry inbox entry missing")
	}
	if r := entry.GetPayload().GetRetry(); r == nil || r.GetNodeId() != "Item1" || string(r.GetVarPatch()) != `{"fixed":true}` {
		t.Fatalf("inbox payload = %+v, want Retry{node:Item1, patch}", entry.GetPayload())
	}
	if firstAdvance(acts, svc) == nil {
		t.Fatal("RETRY must push ActAdvanceProcess to drive the retry turn")
	}
	if k := lastHistKind(procHistEvents(t, p, pk, svc, key)); k != enginev1.ProcessHistoryKind_PROCESS_HISTORY_INCIDENT_RESOLVED {
		t.Fatalf("last history kind = %v, want INCIDENT_RESOLVED", k)
	}
}

// TestProcess_CMMNIncidentDefersParkUntilQuiescent: a CMMN fault must NOT park
// the instance while sibling work is in flight (Outstanding>0) — fault does not
// propagate (CMMN §8.4), so the case keeps running. It parks only once the case
// is quiescent (the last in-flight action completes with the incident still open).
func TestProcess_CMMNIncidentDefersParkUntilQuiescent(t *testing.T) {
	p, _, col := newTestPartition(t)
	const svc, key = "Case", "qpark"
	pk := routing.PartitionKey(svc, key)
	lp := keys.LPFromPartitionKey(pk)
	apply := func(idx uint64, cmd *enginev1.Command) {
		t.Helper()
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: envelope(t, cmd)}}); err != nil {
			t.Fatal(err)
		}
		col.Drain()
	}
	procT, _ := procStore(p)

	// Start a CMMN case; its first turn arms a timer → Outstanding=1 (a sibling
	// in flight).
	apply(1, &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: svc, InstanceKey: key, Payload: extPayload([]byte("v")),
		ModelRef: &enginev1.ModelRef{Kind: "cmmn", Name: svc, Version: "v1"},
		Kind:     enginev1.ProcessKind_PROCESS_KIND_CMMN,
	}}})
	apply(2, &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: &enginev1.ProcessAdvanced{
		Pk: pk, Service: svc, InstanceKey: key, NewState: []byte("s1"),
		ArmTimer: []*enginev1.TimerArm{{NodeId: "Timer1", FireAtMs: testEnvelopeNowMs + 5000, Slot: 1}},
	}}})
	if rec, _, _ := procT.Get(lp, svc, key); rec.GetOutstanding() != 1 {
		t.Fatalf("setup: outstanding = %d, want 1", rec.GetOutstanding())
	}

	// A later turn faults a plan item while the timer is still outstanding.
	apply(3, &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: svc, InstanceKey: key, Payload: extPayload([]byte("e2")),
	}}})
	apply(4, procIncidentAdvancedCmd(pk, svc, key, []byte("s2"), "Item1", "boom"))

	rec, ok, err := procT.Get(lp, svc, key)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		t.Fatalf("must stay RUNNING while a sibling is outstanding: ok=%v status=%v", ok, rec.GetStatus())
	}
	if rec.GetIncident() != nil {
		t.Fatalf("must not park (stamp incident) before quiescence: %+v", rec.GetIncident())
	}

	// The timer fires (Outstanding→0); the quiescence turn still carries the open
	// incident (the adapter re-derives it from state) → now it parks.
	apply(5, &enginev1.Command{Kind: &enginev1.Command_ProcessEvent{ProcessEvent: &enginev1.ProcessEvent{
		Pk: pk, Service: svc, InstanceKey: key,
		Payload: &enginev1.ProcessEventPayload{Of: &enginev1.ProcessEventPayload_TimerFired{
			TimerFired: &enginev1.ProcessTimerFired{NodeId: "Timer1", Slot: 1},
		}},
	}}})
	apply(6, procIncidentAdvancedCmd(pk, svc, key, []byte("s3"), "Item1", "boom"))

	rec, _, err = procT.Get(lp, svc, key)
	if err != nil {
		t.Fatal(err)
	}
	if rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_INCIDENT {
		t.Fatalf("must park once quiescent: status=%v", rec.GetStatus())
	}
	if rec.GetIncident().GetNodeId() != "Item1" {
		t.Fatalf("parked incident = %+v, want Item1", rec.GetIncident())
	}
}
