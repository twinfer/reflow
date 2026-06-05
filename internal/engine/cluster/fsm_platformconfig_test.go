package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func upsertPlatformConfigEnvelope(t *testing.T, text string, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_UpsertPlatformConfig{
				UpsertPlatformConfig: &enginev1.UpsertPlatformConfig{
					Record: &enginev1.PlatformConfigRecord{ClusterAuthzPolicyText: text},
				},
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

func newFSMWithPlatformConfigNotifier(t *testing.T) (*FSM, *TableNotifier) {
	t.Helper()
	f, _, _ := newTestFSM(t)
	notifier := NewTableNotifier()
	f.cfg.Notifiers.PlatformConfigTable = notifier
	return f, notifier
}

func TestCluster_UpsertPlatformConfig_BumpAndNotify(t *testing.T) {
	f, notifier := newFSMWithPlatformConfigNotifier(t)
	const policy = `permit (principal is ClusterOperator, action, resource);`
	res, err := f.Update([]statemachine.Entry{{Index: 10, Cmd: upsertPlatformConfigEnvelope(t, policy, 0)}})
	if err != nil {
		t.Fatal(err)
	}
	if got := res[0].Result.Value; got == ResultValueFailedPrecondition {
		t.Fatalf("first upsert should not fail precondition; result.value=%d", got)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("expected notifier to fire after Upsert")
	}
	store := f.cfg.Snapshotter.Store()
	rev, err := (RevisionTable{S: store}).Get(RevisionTablePlatformConfig)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
	got, err := (PlatformConfigTable{S: store}).Get()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetClusterAuthzPolicyText() != policy {
		t.Fatalf("row missing or policy mismatch: %+v", got)
	}
}

func TestCluster_PlatformConfigCAS_RoundTrip(t *testing.T) {
	f, _ := newFSMWithPlatformConfigNotifier(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertPlatformConfigEnvelope(t, "v1", 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// Stale CAS — expecting 0, table is now at 1.
	res, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: upsertPlatformConfigEnvelope(t, "v2", 999)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("expected failed-precondition sentinel; got %d", res[0].Result.Value)
	}
	store := f.cfg.Snapshotter.Store()
	got, _ := (PlatformConfigTable{S: store}).Get()
	if got.GetClusterAuthzPolicyText() != "v1" {
		t.Fatalf("CAS-failed upsert leaked through; text=%q", got.GetClusterAuthzPolicyText())
	}
	rev, _ := (RevisionTable{S: store}).Get(RevisionTablePlatformConfig)
	if rev != 1 {
		t.Fatalf("rev=%d; want 1 (CAS-fail must not bump)", rev)
	}
	// Correct CAS=1 succeeds and bumps to 2.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 3, Cmd: upsertPlatformConfigEnvelope(t, "v2", 1)},
	}); err != nil {
		t.Fatal(err)
	}
	rev, _ = (RevisionTable{S: store}).Get(RevisionTablePlatformConfig)
	if rev != 2 {
		t.Fatalf("rev=%d; want 2", rev)
	}
}

func TestCluster_PlatformConfigLookup(t *testing.T) {
	f, _ := newFSMWithPlatformConfigNotifier(t)
	// Empty before any upsert: nil record, revision 0.
	res, err := f.Lookup(LookupPlatformConfig{})
	if err != nil {
		t.Fatal(err)
	}
	empty, ok := res.(*PlatformConfigResult)
	if !ok {
		t.Fatalf("Lookup type = %T; want *PlatformConfigResult", res)
	}
	if empty.Record != nil || empty.TableRevision != 0 {
		t.Fatalf("fresh lookup = %+v; want nil record, rev 0", empty)
	}
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertPlatformConfigEnvelope(t, "policy-text", 0)},
	}); err != nil {
		t.Fatal(err)
	}
	res, err = f.Lookup(LookupPlatformConfig{})
	if err != nil {
		t.Fatal(err)
	}
	out := res.(*PlatformConfigResult)
	if out.Record == nil || out.Record.GetClusterAuthzPolicyText() != "policy-text" {
		t.Fatalf("record mismatch: %+v", out.Record)
	}
	if out.TableRevision != 1 {
		t.Fatalf("rev=%d; want 1", out.TableRevision)
	}
}
