package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/loadgen"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// TestWireDispatch_HTTP2_PromiseXshard_Conflict resolves the workflow's
// promise via ingress first, then submits a cross-partition resolver
// that tries to Resolve the same promise. The resolver must observe
// "promise already completed" — the conflict signal flows back over the
// PromiseCompletionAck path.
func TestWireDispatch_HTTP2_PromiseXshard_Conflict(t *testing.T) {
	const wantWorkflowPayload = "ingress-wins"
	wfH := &xshardWorkflowHandler{service: "Orders", handler: "run", promiseName: "done"}
	rsH := &xshardResolverHandler{
		service:      "Workers",
		handler:      "resolve",
		wfService:    "Orders",
		promiseName:  "done",
		resolveValue: []byte("late-loser"),
	}

	wfMux := mountFakeHandler(t, wfH.discovery(), wfH.serveInvoke)
	rsMux := mountFakeHandler(t, rsH.discovery(), rsH.serveInvoke)
	wfAddr, wfTeardown := startFakeHandlerHTTP2WithHandler(t, wfMux)
	defer wfTeardown()
	rsAddr, rsTeardown := startFakeHandlerHTTP2WithHandler(t, rsMux)
	defer rsTeardown()

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

	srv, err := config.NewServer(config.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	wfDep, err := callRegisterDeployment(regCtx, srv, "http://"+wfAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment(wf): %v", err)
	}
	rsDep, err := callRegisterDeployment(regCtx, srv, "http://"+rsAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment(rs): %v", err)
	}

	wfKey, rsKey := findCrossShardKeys(t, host.Partitioner(), wfH.service, rsH.service)
	rsH.wfKey = wfKey

	wfTarget := &enginev1.InvocationTarget{ServiceName: wfH.service, HandlerName: wfH.handler, ObjectKey: wfKey}
	wfID := buildID(routing.PartitionKey(wfH.service, wfKey), "conflict-wf")
	wfShard := host.Partitioner().ShardForInvocation(wfID)
	wfRunner := host.Partition(wfShard)
	if wfRunner == nil {
		t.Fatalf("wf partition %d not running", wfShard)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := wfRunner.Proposer().ProposeIngress(subCtx, "test/conflict-wf", wfShard, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: wfID,
			Target:       wfTarget,
			Input:        []byte("input"),
			DeploymentId: wfDep.GetDeploymentId(),
			Kind:         uint32(protocolv1.Kind_KIND_WORKFLOW),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress wf: %v", err)
	}
	_ = awaitSuspended(t, host, wfShard, wfID, 10*time.Second)

	// Resolve via INGRESS first — winner.
	resCtx, resCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer resCancel()
	winnerCmd := &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_PromiseCompleted{PromiseCompleted: &enginev1.PromiseCompleted{
				Service:     wfH.service,
				WorkflowKey: wfKey,
				PromiseName: wfH.promiseName,
				Value:       []byte(wantWorkflowPayload),
			}},
		}},
	}
	if err := wfRunner.Proposer().ProposeIngress(resCtx, "test/conflict-ingress", wfShard, winnerCmd); err != nil {
		t.Fatalf("ingress resolve: %v", err)
	}
	wfDone := awaitCompleted(t, host, wfShard, wfID, 10*time.Second)
	if got := string(wfDone.GetOutput()); got != wantWorkflowPayload {
		t.Fatalf("workflow output = %q; want %q", got, wantWorkflowPayload)
	}

	// Now submit the cross-partition resolver — it should hit the
	// already-completed conflict and surface "promise already completed".
	rsTarget := &enginev1.InvocationTarget{ServiceName: rsH.service, HandlerName: rsH.handler, ObjectKey: rsKey}
	rsID := buildID(routing.PartitionKey(rsH.service, rsKey), "conflict-rs")
	rsShard := host.Partitioner().ShardForInvocation(rsID)
	if rsShard == wfShard {
		t.Fatalf("test setup: workflow + resolver expected on different shards")
	}
	rsRunner := host.Partition(rsShard)
	if rsRunner == nil {
		t.Fatalf("rs partition %d not running", rsShard)
	}
	if err := rsRunner.Proposer().ProposeIngress(subCtx, "test/conflict-rs", rsShard, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: rsID,
			Target:       rsTarget,
			Input:        []byte("input"),
			DeploymentId: rsDep.GetDeploymentId(),
			Kind:         uint32(protocolv1.Kind_KIND_WORKFLOW),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress rs: %v", err)
	}

	rsDone := awaitCompleted(t, host, rsShard, rsID, 15*time.Second)
	got := string(rsDone.GetOutput())
	if got != "conflict:promise already completed" {
		t.Errorf("resolver output = %q; want %q",
			got, "conflict:promise already completed")
	}
}
