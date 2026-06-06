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
