package apimap

import (
	"testing"

	apiv1 "github.com/twinfer/reflw/proto/apiv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// TestEnumValueSetsMatch is the load-bearing guard: every apimap enum cast is a
// plain numeric conversion, which is only correct if the apiv1 enum carries the
// exact same set of numeric values as its enginev1 counterpart. If someone adds
// a value to an engine enum without mirroring it in apiv1 (or vice versa), this
// fails before the silent miscast can ship.
func TestEnumValueSetsMatch(t *testing.T) {
	cases := []struct {
		name   string
		engine map[int32]string
		api    map[int32]string
	}{
		{"InvocationState", enginev1.InvocationState_name, apiv1.InvocationState_name},
		{"ProcessStatus", enginev1.ProcessStatus_name, apiv1.ProcessStatus_name},
		{"ProcessKind", enginev1.ProcessKind_name, apiv1.ProcessKind_name},
		{"ProcessIncidentResolution", enginev1.ProcessIncidentResolution_name, apiv1.ProcessIncidentResolution_name},
		{"ProcessHistoryKind", enginev1.ProcessHistoryKind_name, apiv1.ProcessHistoryKind_name},
		{"LPTransferPhase", enginev1.LPTransferPhase_name, apiv1.LPTransferPhase_name},
	}
	for _, c := range cases {
		for v := range c.engine {
			if _, ok := c.api[v]; !ok {
				t.Errorf("%s: engine value %d (%q) has no apiv1 counterpart", c.name, v, c.engine[v])
			}
		}
		for v := range c.api {
			if _, ok := c.engine[v]; !ok {
				t.Errorf("%s: apiv1 value %d (%q) has no engine counterpart", c.name, v, c.api[v])
			}
		}
	}
}

func TestInvocationStatusView_Completed(t *testing.T) {
	st := &enginev1.InvocationStatus{
		DeploymentId: "dep-1",
		Kind:         2,
		Status: &enginev1.InvocationStatus_Completed{Completed: &enginev1.Completed{
			Target:         &enginev1.InvocationTarget{ServiceName: "svc", HandlerName: "h", ObjectKey: "k"},
			Output:         []byte("out"),
			FailureMessage: "",
			CompletedAtMs:  123,
			FailureCode:    0,
		}},
	}
	v := InvocationStatusView(st)
	if v.GetState() != apiv1.InvocationState_INVOCATION_STATE_COMPLETED {
		t.Fatalf("state = %v, want COMPLETED", v.GetState())
	}
	if !v.GetCompleted() || string(v.GetOutput()) != "out" || v.GetCompletedAtMs() != 123 {
		t.Fatalf("completed fields wrong: %+v", v)
	}
	if v.GetService() != "svc" || v.GetHandler() != "h" || v.GetObjectKey() != "k" {
		t.Fatalf("target not flattened: %+v", v)
	}
	if v.GetDeploymentId() != "dep-1" || v.GetKind() != 2 {
		t.Fatalf("deployment/kind wrong: %+v", v)
	}
}

func TestInvocationStatusView_Suspended(t *testing.T) {
	st := &enginev1.InvocationStatus{
		Status: &enginev1.InvocationStatus_Suspended{Suspended: &enginev1.Suspended{
			Target:     &enginev1.InvocationTarget{ServiceName: "svc"},
			AwaitingOn: []string{"awk_1", "awk_2"},
		}},
	}
	v := InvocationStatusView(st)
	if v.GetState() != apiv1.InvocationState_INVOCATION_STATE_SUSPENDED {
		t.Fatalf("state = %v, want SUSPENDED", v.GetState())
	}
	if len(v.GetAwaitingOn()) != 2 || v.GetAwaitingOn()[0] != "awk_1" {
		t.Fatalf("awaiting_on wrong: %+v", v.GetAwaitingOn())
	}
	if v.GetCompleted() {
		t.Fatalf("suspended should not be completed")
	}
}

func TestInvocationStatusView_Nil(t *testing.T) {
	if InvocationStatusView(nil) != nil {
		t.Fatal("nil status should map to nil view")
	}
}

func TestInvocationView(t *testing.T) {
	v := InvocationView("inv_abc", &enginev1.InvocationTarget{ServiceName: "svc", HandlerName: "h"},
		enginev1.InvocationState_INVOCATION_STATE_INVOKED, "dep", 10, 0)
	if v.GetInvocationId() != "inv_abc" || v.GetService() != "svc" || v.GetHandler() != "h" {
		t.Fatalf("fields wrong: %+v", v)
	}
	if v.GetState() != apiv1.InvocationState_INVOCATION_STATE_INVOKED {
		t.Fatalf("state = %v", v.GetState())
	}
}

func TestProcessInstanceView(t *testing.T) {
	rec := &enginev1.ProcessInstanceRecord{
		Status:      enginev1.ProcessStatus_PROCESS_STATUS_INCIDENT,
		Kind:        enginev1.ProcessKind_PROCESS_KIND_BPMN,
		ActiveSeq:   3,
		NextSeq:     5,
		Outstanding: 1,
		Incident:    &enginev1.ProcessIncident{NodeId: "n1", Cause: "boom", RaisedAtMs: 7},
	}
	v := ProcessInstanceView(rec, "svc", "k1")
	if v.GetService() != "svc" || v.GetInstanceKey() != "k1" {
		t.Fatalf("identity wrong: %+v", v)
	}
	if v.GetStatus() != apiv1.ProcessStatus_PROCESS_STATUS_INCIDENT || v.GetKind() != apiv1.ProcessKind_PROCESS_KIND_BPMN {
		t.Fatalf("status/kind wrong: %+v", v)
	}
	if v.GetIncident() == nil || v.GetIncident().GetCause() != "boom" {
		t.Fatalf("incident not mapped: %+v", v.GetIncident())
	}
	if v.GetAwaitingTasks() != nil {
		t.Fatalf("awaiting_tasks should be left for the caller to fill")
	}
}

func TestDeploymentView(t *testing.T) {
	d := &enginev1.DeploymentRecord{
		Id: "d1", Url: "https://h", RegisteredAtMs: 9,
		Handlers: []*enginev1.DeploymentHandler{{Service: "svc", Handler: "go", Kind: 1}},
	}
	v := DeploymentView(d)
	if v.GetId() != "d1" || v.GetUrl() != "https://h" || v.GetRegisteredAtMs() != 9 {
		t.Fatalf("fields wrong: %+v", v)
	}
	if len(v.GetHandlers()) != 1 || v.GetHandlers()[0].GetHandler() != "go" {
		t.Fatalf("handlers wrong: %+v", v.GetHandlers())
	}
}

func TestSecretView_NoPlaintext(t *testing.T) {
	s := &enginev1.SecretRecord{
		Name:   "api-key",
		Source: &enginev1.SecretRecord_RemoteEncrypted{RemoteEncrypted: &enginev1.RemoteEncryptedSecret{BlobUri: "mem://x", KekUri: "blobkms+mem://k"}},
	}
	v := SecretView(s)
	if v.GetName() != "api-key" || v.GetBlobUri() != "mem://x" || v.GetKekUri() != "blobkms+mem://k" {
		t.Fatalf("secret view wrong: %+v", v)
	}
}

func TestPartitionTableView(t *testing.T) {
	tbl := &enginev1.PartitionTable{
		Shards:          map[uint64]*enginev1.ReplicaSet{1: {NodeIds: []uint64{1, 2, 3}}},
		AssignmentEpoch: 4,
		MetaReplicas:    &enginev1.ReplicaSet{NodeIds: []uint64{1}},
	}
	v := PartitionTableView(tbl)
	if v.GetAssignmentEpoch() != 4 || len(v.GetShards()) != 1 {
		t.Fatalf("table wrong: %+v", v)
	}
	if v.GetShards()[0].GetShardId() != 1 || len(v.GetShards()[0].GetNodeIds()) != 3 {
		t.Fatalf("shard wrong: %+v", v.GetShards()[0])
	}
	if len(v.GetMetaReplicas()) != 1 || v.GetMetaReplicas()[0] != 1 {
		t.Fatalf("meta replicas wrong: %+v", v.GetMetaReplicas())
	}
}

func TestNilMappersReturnNil(t *testing.T) {
	if DeploymentView(nil) != nil || SecretView(nil) != nil || ModelView(nil) != nil ||
		NodeView(nil) != nil || PartitionTableView(nil) != nil || LPTransferView(nil) != nil ||
		ProcessInstanceView(nil, "", "") != nil || ProcessIncidentView(nil) != nil ||
		ProcessHistoryEventView(nil) != nil {
		t.Fatal("nil input should map to nil output")
	}
}
