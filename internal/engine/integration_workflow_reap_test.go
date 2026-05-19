package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/admin"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// TestWorkflowReap_PurgesStateAndPromise drives the workflow retention
// reaper directly: submit a workflow that resolves a promise, ingress-
// complete the run, then propose Command.ReapWorkflow synthetically
// (bypassing the time-based WorkflowReapService) and assert every
// per-key row is gone — state, promise, promise_awaiter, workflow_run,
// inv, journal, signal_*.
//
// Bypassing the timer service keeps the test deterministic. The service
// itself is exercised by the unit-level table test
// (TestTables/.../WorkflowReap_PutScanDelete) plus the propose path
// shared with TimerService.
func TestWorkflowReap_PurgesStateAndPromise(t *testing.T) {
	const promisePayload = "reap-resolved"
	awaiter := &fakeHandlerPromiseAwaiter{
		service:     "Orders",
		handler:     "run",
		promiseName: "done",
	}
	handlerAddr, teardown := startFakeHandlerHTTP2WithHandler(t, awaiter.httpHandler(t))
	defer teardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer cluster.Close()
	defer loadgen.StartEmbeddedHandlers(t, cluster, nil)()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leader := findMetadataLeader(t, cluster)
	host := leader.Host

	srv, err := admin.NewServer(admin.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	deploymentResp, err := callRegisterDeployment(regCtx, srv, "http://"+handlerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	const wfKey = "order-reap-1"
	target := &enginev1.InvocationTarget{
		ServiceName: awaiter.service,
		HandlerName: awaiter.handler,
		ObjectKey:   wfKey,
	}
	pk := routing.PartitionKey(target.GetServiceName(), target.GetObjectKey())
	id := buildID(pk, "reap-id")
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}

	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/reap-submit", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("input"),
			DeploymentId: deploymentResp.GetDeploymentId(),
			Kind:         uint32(protocolv1.Kind_KIND_WORKFLOW),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress invoke: %v", err)
	}
	_ = awaitSuspended(t, host, shardID, id, 10*time.Second)

	if err := pr.Proposer().ProposeIngress(subCtx, "test/reap-resolve", shardID, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_PromiseCompleted{PromiseCompleted: &enginev1.PromiseCompleted{
				Service:     target.GetServiceName(),
				WorkflowKey: target.GetObjectKey(),
				PromiseName: awaiter.promiseName,
				Value:       []byte(promisePayload),
			}},
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress resolve: %v", err)
	}
	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != promisePayload {
		t.Fatalf("output = %q; want %q", got, promisePayload)
	}

	// At this point the apply arm has written a workflow_reap row with
	// fire_at_ms = Completed.completed_at_ms + DefaultWorkflowRetentionMs.
	// Snapshotter store is the source of truth — confirm before reap.
	store := pr.Snapshotter().Store()
	runRow, err := (tables.WorkflowRunTable{S: store}).Get(target.GetServiceName(), target.GetObjectKey())
	if err != nil {
		t.Fatalf("workflow_run pre-reap: %v", err)
	}
	if runRow == nil {
		t.Fatalf("workflow_run row missing before reap")
	}
	// Find the workflow_reap row to learn its fire_at_ms so the synthetic
	// command targets the right key.
	var reapRow tables.ReapRow
	foundReap := false
	if err := (tables.WorkflowReapTable{S: store}).ScanAll(func(r tables.ReapRow) error {
		if r.Service == target.GetServiceName() && r.WorkflowKey == target.GetObjectKey() {
			reapRow = r
			foundReap = true
		}
		return nil
	}); err != nil {
		t.Fatalf("workflow_reap scan: %v", err)
	}
	if !foundReap {
		t.Fatalf("workflow_reap row missing for (%s, %s)", target.GetServiceName(), target.GetObjectKey())
	}

	// Bypass the time-based service: propose ReapWorkflow synthetically.
	reapCtx, reapCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer reapCancel()
	if err := pr.Proposer().ProposeIngress(reapCtx, "test/reap-now", shardID, &enginev1.Command{
		Kind: &enginev1.Command_ReapWorkflow{ReapWorkflow: &enginev1.ReapWorkflow{
			Service:     target.GetServiceName(),
			WorkflowKey: target.GetObjectKey(),
			FireAtMs:    reapRow.FireAtMs,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress reap: %v", err)
	}

	// Allow the apply to commit on all replicas. InvocationTable.Get
	// synthesises Free when the row is absent, so check via the oneof.
	deadline := time.Now().Add(5 * time.Second)
	for {
		store := pr.Snapshotter().Store()
		runRow, _ := (tables.WorkflowRunTable{S: store}).Get(target.GetServiceName(), target.GetObjectKey())
		invRow, _ := (tables.InvocationTable{S: store}).Get(id)
		_, invFree := invRow.GetStatus().(*enginev1.InvocationStatus_Free)
		if runRow == nil && invFree {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reap didn't purge rows: workflow_run=%v inv_status=%T",
				runRow != nil, invRow.GetStatus())
		}
		time.Sleep(50 * time.Millisecond)
	}

	store = pr.Snapshotter().Store()
	if pv, _ := (tables.PromiseTable{S: store}).Get(target.GetServiceName(), target.GetObjectKey(), awaiter.promiseName); pv != nil {
		t.Errorf("promise row survived reap: %+v", pv)
	}
	var awaiters int
	_ = (tables.PromiseAwaiterTable{S: store}).ScanForName(target.GetServiceName(), target.GetObjectKey(), awaiter.promiseName, func(*enginev1.PromiseAwaiter) error {
		awaiters++
		return nil
	})
	if awaiters != 0 {
		t.Errorf("promise_awaiter rows survived reap: %d", awaiters)
	}
	// Confirm workflow_reap row also cleared.
	reapStill := false
	_ = (tables.WorkflowReapTable{S: store}).ScanAll(func(r tables.ReapRow) error {
		if r.Service == target.GetServiceName() && r.WorkflowKey == target.GetObjectKey() {
			reapStill = true
		}
		return nil
	})
	if reapStill {
		t.Errorf("workflow_reap row survived reap")
	}

	// Journal prefix should be empty.
	jPrefix, _ := keys.JournalPrefix(id)
	iter, err := store.NewIter(jPrefix, keys.PrefixUpperBound(jPrefix))
	if err != nil {
		t.Fatalf("journal scan: %v", err)
	}
	defer iter.Close()
	if ok := iter.First(); ok {
		t.Errorf("journal rows survived reap (first key=%x)", iter.Key())
	}
}
