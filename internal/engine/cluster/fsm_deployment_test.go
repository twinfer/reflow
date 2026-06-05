package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func registerDeploymentEnvelope(t *testing.T, rec *enginev1.DeploymentRecord, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_RegisterDeployment{
				RegisterDeployment: &enginev1.RegisterDeployment{Record: rec},
			},
		},
	}
	if ifRev != 0 {
		env.Precondition = &enginev1.Precondition{IfTableRevisionEq: ifRev}
	}
	buf, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func deleteDeploymentEnvelope(t *testing.T, id string, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_001},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_DeleteDeployment{
				DeleteDeployment: &enginev1.DeleteDeployment{Id: id},
			},
		},
	}
	if ifRev != 0 {
		env.Precondition = &enginev1.Precondition{IfTableRevisionEq: ifRev}
	}
	buf, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func sampleDeployment(id string) *enginev1.DeploymentRecord {
	return &enginev1.DeploymentRecord{
		Id:             id,
		Url:            "http://handler.example/" + id,
		RegisteredAtMs: 1_700_000_000_000,
		Handlers: []*enginev1.DeploymentHandler{
			{Service: "Billing", Handler: "OnCharge", Kind: 1},
			{Service: "Billing", Handler: "OnRefund", Kind: 1},
		},
	}
}

func TestCluster_RegisterDeployment_BumpsRevisionAndIndex(t *testing.T) {
	f, _, _ := newTestFSM(t)
	rec := sampleDeployment("dep-1")
	entries := []statemachine.Entry{{Index: 10, Cmd: registerDeploymentEnvelope(t, rec, 0)}}
	res, err := f.Update(entries)
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value == ResultValueFailedPrecondition {
		t.Fatalf("first register should not trip CAS")
	}
	store := f.cfg.Snapshotter.Store()
	rev, err := (RevisionTable{S: store}).Get(RevisionTableDeployment)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
	got, err := (DeploymentTable{S: store}).Get("dep-1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("deployment row missing after Register")
	}
	// Index entries should point at dep-1 for both handlers.
	for _, h := range rec.GetHandlers() {
		id, err := (DeploymentIndexTable{S: store}).Get(h.Service, h.Handler)
		if err != nil {
			t.Fatal(err)
		}
		if id != "dep-1" {
			t.Fatalf("index[%s/%s]=%q; want dep-1", h.Service, h.Handler, id)
		}
	}
}

func TestCluster_RegisterDeployment_CASMismatch(t *testing.T) {
	f, _, _ := newTestFSM(t)
	rec := sampleDeployment("dep-1")
	// First register with no precondition takes revision 0 → 1.
	if _, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: registerDeploymentEnvelope(t, rec, 0)}}); err != nil {
		t.Fatal(err)
	}
	// Second register with stale precondition (expects rev=0) must trip CAS.
	rec2 := sampleDeployment("dep-2")
	res, err := f.Update([]statemachine.Entry{{Index: 11, Cmd: registerDeploymentEnvelope(t, rec2, 1)}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value == ResultValueFailedPrecondition {
		t.Fatalf("ifRev=1 should match current rev=1 and succeed")
	}
	// Now revision is 2; another with ifRev=99 must fail.
	res, err = f.Update([]statemachine.Entry{{Index: 12, Cmd: registerDeploymentEnvelope(t, sampleDeployment("dep-3"), 99)}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("expected CAS mismatch sentinel; got result.value=%d", res[0].Result.Value)
	}
	// dep-3 must NOT be persisted after the CAS failure.
	store := f.cfg.Snapshotter.Store()
	got, err := (DeploymentTable{S: store}).Get("dep-3")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("dep-3 row written despite CAS failure")
	}
}

func TestCluster_DeleteDeployment_RemovesRowAndIndex(t *testing.T) {
	f, _, _ := newTestFSM(t)
	rec := sampleDeployment("dep-1")
	if _, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: registerDeploymentEnvelope(t, rec, 0)}}); err != nil {
		t.Fatal(err)
	}
	// Delete dep-1; CAS ifRev=1.
	res, err := f.Update([]statemachine.Entry{{Index: 11, Cmd: deleteDeploymentEnvelope(t, "dep-1", 1)}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value == ResultValueFailedPrecondition {
		t.Fatalf("delete with matching ifRev should succeed")
	}
	store := f.cfg.Snapshotter.Store()
	if got, _ := (DeploymentTable{S: store}).Get("dep-1"); got != nil {
		t.Fatalf("deployment row still present after delete")
	}
	for _, h := range rec.GetHandlers() {
		id, err := (DeploymentIndexTable{S: store}).Get(h.Service, h.Handler)
		if err != nil {
			t.Fatal(err)
		}
		if id != "" {
			t.Fatalf("index[%s/%s]=%q still present after delete", h.Service, h.Handler, id)
		}
	}
	rev, _ := (RevisionTable{S: store}).Get(RevisionTableDeployment)
	if rev != 2 {
		t.Fatalf("rev=%d after delete; want 2", rev)
	}
}

// Delete must preserve index entries that another deployment has since
// taken over: dep-1 owns (Billing/OnCharge); dep-2 takes over the same
// (service, handler); deleting dep-1 must NOT evict dep-2's index row.
func TestCluster_DeleteDeployment_PreservesOverriddenIndex(t *testing.T) {
	f, _, _ := newTestFSM(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 10, Cmd: registerDeploymentEnvelope(t, sampleDeployment("dep-1"), 0)},
		{Index: 11, Cmd: registerDeploymentEnvelope(t, sampleDeployment("dep-2"), 1)},
	}); err != nil {
		t.Fatal(err)
	}
	store := f.cfg.Snapshotter.Store()
	// Index should now point at dep-2 for both handlers (newer wins).
	id, _ := (DeploymentIndexTable{S: store}).Get("Billing", "OnCharge")
	if id != "dep-2" {
		t.Fatalf("after re-register, index=%q; want dep-2", id)
	}
	// Deleting dep-1 should leave dep-2's index alone.
	if _, err := f.Update([]statemachine.Entry{{Index: 12, Cmd: deleteDeploymentEnvelope(t, "dep-1", 2)}}); err != nil {
		t.Fatal(err)
	}
	store = f.cfg.Snapshotter.Store()
	id, _ = (DeploymentIndexTable{S: store}).Get("Billing", "OnCharge")
	if id != "dep-2" {
		t.Fatalf("after delete of dep-1, index=%q; want dep-2 (must not evict overridden entry)", id)
	}
}

func TestCluster_DeleteDeployment_OfAbsent_BumpsRevision(t *testing.T) {
	f, _, _ := newTestFSM(t)
	res, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: deleteDeploymentEnvelope(t, "ghost", 0)}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value == ResultValueFailedPrecondition {
		t.Fatalf("delete-of-absent must not fail precondition")
	}
	store := f.cfg.Snapshotter.Store()
	rev, _ := (RevisionTable{S: store}).Get(RevisionTableDeployment)
	if rev != 1 {
		t.Fatalf("rev=%d after delete-of-absent; want 1 (revision bumps even when row absent)", rev)
	}
}
