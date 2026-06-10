package engine_test

import (
	"context"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/admin"
	"github.com/twinfer/reflw/internal/loadgen"
	"github.com/twinfer/reflw/pkg/reflw/processengine"
	adminv1 "github.com/twinfer/reflw/proto/adminv1"
)

// validBPMN is a minimal executable process: start → end. Parses and passes
// reflwos static validation.
const validBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="end"/>
    <endEvent id="end"><incoming>f1</incoming></endEvent>
  </process>
</definitions>`

// staticInvalidBPMN is well-formed XML and parses cleanly, but an endEvent with
// an outgoing flow is a structural defect (reflwos BPM001). The shallow
// well-formed-XML check the config layer applies without an injected planner
// would accept it; processengine.PlanModelSet must reject it.
const staticInvalidBPMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="test">
  <process id="p" isExecutable="true">
    <startEvent id="start"><outgoing>f1</outgoing></startEvent>
    <endEvent id="mid"><incoming>f1</incoming><outgoing>f2</outgoing></endEvent>
    <endEvent id="end"><incoming>f2</incoming></endEvent>
    <sequenceFlow id="f1" sourceRef="start" targetRef="mid"/>
    <sequenceFlow id="f2" sourceRef="mid" targetRef="end"/>
  </process>
</definitions>`

// TestConfig_ModelValidationGate verifies the injected reflwos planner gates
// RegisterModelSet: a structurally-broken-but-well-formed model is rejected with
// InvalidArgument and never enters the Raft log (the silent-per-node-reconcile
// failure the seam closes), while a valid model registers and lists. This is
// the exact wiring pkg/reflw/run.go installs when the process plane is on.
func TestConfig_ModelValidationGate(t *testing.T) {
	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 1})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	host := findMetadataLeader(t, cluster).Host

	srv, err := admin.NewServer(admin.Config{
		Host:         host,
		Runner:       host.MetadataRunner(),
		PlanModelSet: processengine.PlanModelSet, // the production injection
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cli, closeCli := newDeploymentClient(t, ctx, srv)
	defer closeCli()

	// A structurally-invalid model is rejected at the gate, not committed.
	_, err = cli.RegisterModelSet(ctx, connect.NewRequest(&adminv1.RegisterModelSetRequest{
		Entries: []*adminv1.ModelSetEntry{{
			Kind: "bpmn", Name: "Broken", Version: "v1",
			Xml: []byte(staticInvalidBPMN),
		}},
	}))
	if err == nil {
		t.Fatal("RegisterModelSet accepted a statically-invalid model; want InvalidArgument")
	}
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("RegisterModelSet(invalid) code = %v, want InvalidArgument: %v", got, err)
	}

	// It must not have landed: the table is still empty.
	listResp, err := cli.ListModels(ctx, connect.NewRequest(&adminv1.ListModelsRequest{}))
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if got := len(listResp.Msg.GetModels()); got != 0 {
		t.Fatalf("rejected model leaked into the table: %d records, want 0", got)
	}

	// A valid model registers and bumps the revision.
	upResp, err := cli.RegisterModelSet(ctx, connect.NewRequest(&adminv1.RegisterModelSetRequest{
		Entries: []*adminv1.ModelSetEntry{{
			Kind: "bpmn", Name: "Good", Version: "v1",
			Xml: []byte(validBPMN),
		}},
	}))
	if err != nil {
		t.Fatalf("RegisterModelSet(valid): %v", err)
	}
	if upResp.Msg.GetTableRevision() == 0 {
		t.Fatal("table_revision after valid register is 0; want >0")
	}

	listResp, err = cli.ListModels(ctx, connect.NewRequest(&adminv1.ListModelsRequest{}))
	if err != nil {
		t.Fatalf("ListModels #2: %v", err)
	}
	if got := len(listResp.Msg.GetModels()); got != 1 {
		t.Fatalf("after valid upsert: %d records, want 1", got)
	}
	if name := listResp.Msg.GetModels()[0].GetName(); name != "Good" {
		t.Fatalf("listed model name = %q, want Good", name)
	}
}
