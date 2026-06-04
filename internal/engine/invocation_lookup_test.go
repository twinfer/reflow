package engine

import (
	"bytes"
	"testing"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestInvocations_Lookup exercises the shard-side scan that backs the
// ListInvocations fan-out: whole-namespace scan, target-service + state filters,
// the limit cap, and the page cursor. The invocation-plane twin of
// TestProcess_LookupProcessInstances over the shared scanList substrate.
func TestInvocations_Lookup(t *testing.T) {
	p, _, _ := newTestPartition(t)
	store := p.cfg.Snapshotter.Store()
	const svc = "Orders"

	seed := func(key string, status *enginev1.InvocationStatus) {
		t.Helper()
		uuid := make([]byte, 16)
		copy(uuid, key)
		id := &enginev1.InvocationId{PartitionKey: routing.PartitionKey(svc, key), Uuid: uuid}
		b := store.NewBatch()
		if err := (tables.InvocationTable{S: b}).Put(b, id, status); err != nil {
			t.Fatal(err)
		}
		if err := b.Commit(false); err != nil {
			t.Fatal(err)
		}
	}
	target := func(service string) *enginev1.InvocationTarget {
		return &enginev1.InvocationTarget{ServiceName: service, HandlerName: "h"}
	}

	// Two Orders invocations (one Scheduled, one Completed) + one other service.
	seed("a", &enginev1.InvocationStatus{Status: &enginev1.InvocationStatus_Scheduled{
		Scheduled: &enginev1.Scheduled{Target: target(svc), CreatedAtMs: 1000}}})
	seed("b", &enginev1.InvocationStatus{Status: &enginev1.InvocationStatus_Completed{
		Completed: &enginev1.Completed{Target: target(svc), CompletedAtMs: 2000}}})
	seed("c", &enginev1.InvocationStatus{Status: &enginev1.InvocationStatus_Scheduled{
		Scheduled: &enginev1.Scheduled{Target: target("Other"), CreatedAtMs: 1500}}})

	list := func(q LookupInvocations) []InvocationSummary {
		t.Helper()
		res, err := p.Lookup(q)
		if err != nil {
			t.Fatal(err)
		}
		r, ok := res.(InvocationsLookupResult)
		if !ok {
			t.Fatalf("unexpected lookup result type %T", res)
		}
		return r.Invocations
	}

	if all := list(LookupInvocations{}); len(all) != 3 {
		t.Fatalf("list all: got %d, want 3", len(all))
	}
	if bySvc := list(LookupInvocations{Service: svc}); len(bySvc) != 2 {
		t.Fatalf("list service=%s: got %d, want 2", svc, len(bySvc))
	}
	completed := list(LookupInvocations{Service: svc,
		StateFilter: []enginev1.InvocationState{enginev1.InvocationState_INVOCATION_STATE_COMPLETED}})
	if len(completed) != 1 {
		t.Fatalf("list completed: got %d, want 1", len(completed))
	}
	if got := completed[0].CompletedAtMs; got != 2000 {
		t.Fatalf("completed_at_ms: got %d, want 2000", got)
	}
	if got := completed[0].Target.GetServiceName(); got != svc {
		t.Fatalf("target service: got %q, want %q", got, svc)
	}
	if got := completed[0].State; got != enginev1.InvocationState_INVOCATION_STATE_COMPLETED {
		t.Fatalf("state: got %v, want COMPLETED", got)
	}
	if capped := list(LookupInvocations{Limit: 1}); len(capped) != 1 {
		t.Fatalf("list limit 1: got %d, want 1", len(capped))
	}

	// created_at window: only Scheduled rows carry created_at (a=1000, c=1500);
	// the Completed row b reports 0. [1200, ∞) keeps only c; [0, 1200) keeps a+b.
	if after := list(LookupInvocations{CreatedAfterMs: 1200}); len(after) != 1 {
		t.Fatalf("created_after 1200: got %d, want 1", len(after))
	}
	if before := list(LookupInvocations{CreatedBeforeMs: 1200}); len(before) != 2 {
		t.Fatalf("created_before 1200: got %d, want 2", len(before))
	}

	// Page cursor: After = the first row's key resumes strictly past it, yielding
	// exactly the remaining rows in the same (lp asc, key asc) order.
	all := list(LookupInvocations{})
	cursor, err := keys.InvocationKey(all[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	resumed := list(LookupInvocations{After: cursor})
	if len(resumed) != len(all)-1 {
		t.Fatalf("resume after first: got %d, want %d", len(resumed), len(all)-1)
	}
	for i := range resumed {
		if !bytes.Equal(resumed[i].ID.GetUuid(), all[i+1].ID.GetUuid()) {
			t.Fatalf("resume row %d: got uuid %x, want %x", i, resumed[i].ID.GetUuid(), all[i+1].ID.GetUuid())
		}
	}
}
