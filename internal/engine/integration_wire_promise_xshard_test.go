package engine_test

import (
	"context"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// xshardWorkflowHandler is the workflow side of the cross-partition
// promise test: KIND_WORKFLOW, awaits Promise("done").Result() scoped to
// its own (service, key), surfaces the resolved value as the invocation
// output.
type xshardWorkflowHandler struct {
	service     string
	handler     string
	promiseName string
}

func (h *xshardWorkflowHandler) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: h.service, Kind: protocolv1.Kind_KIND_WORKFLOW, HandlerNames: []string{h.handler}},
		},
	}
}

func (h *xshardWorkflowHandler) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	t.Helper()
	startFrame, err := stream.Receive()
	if err != nil {
		return err
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		return err
	}
	var resolved []byte
	delivered := false
	for range sm.GetKnownEntries() {
		fr, err := stream.Receive()
		if err != nil {
			return err
		}
		tc, _, _ := wire.UnpackHeader(fr.GetHeader())
		if tc == wire.TypeNoteGetPromise {
			var note protocolv1.GetPromiseCompletionNotificationMessage
			if err := proto.Unmarshal(fr.GetPayload(), &note); err != nil {
				return err
			}
			if v, ok := note.GetResult().(*protocolv1.GetPromiseCompletionNotificationMessage_Value); ok {
				resolved = v.Value.GetContent()
				delivered = true
			}
		}
	}
	if !delivered {
		getCmd := &protocolv1.GetPromiseCommandMessage{
			Service:            h.service,
			Key:                sm.GetKey(),
			Name:               h.promiseName,
			ResultCompletionId: 2,
		}
		gp, _ := proto.Marshal(getCmd)
		if err := stream.Send(frameFor(wire.TypeCmdGetPromise, gp)); err != nil {
			return err
		}
		sp, _ := proto.Marshal(&protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}})
		if err := stream.Send(frameFor(wire.TypeSuspension, sp)); err != nil {
			return err
		}
		ep, _ := proto.Marshal(&protocolv1.EndMessage{})
		if err := stream.Send(frameFor(wire.TypeEnd, ep)); err != nil {
			return err
		}
		return drainStream(stream)
	}
	out := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{Value: &protocolv1.Value{Content: resolved}},
	}
	op, _ := proto.Marshal(out)
	if err := stream.Send(frameFor(wire.TypeCmdOutput, op)); err != nil {
		return err
	}
	ep, _ := proto.Marshal(&protocolv1.EndMessage{})
	if err := stream.Send(frameFor(wire.TypeEnd, ep)); err != nil {
		return err
	}
	return drainStream(stream)
}

// xshardResolverHandler is the resolver side: it lives on a DIFFERENT
// (service, key) than the workflow. It emits a CompletePromiseCommand
// with explicit Service + Key pointing at the workflow's scope,
// simulating Context.WorkflowPromise(target, name).Resolve(value). The
// apply path routes the completion cross-partition via outbox.
type xshardResolverHandler struct {
	service      string
	handler      string
	wfService    string
	wfKey        string
	promiseName  string
	resolveValue []byte
}

func (h *xshardResolverHandler) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: h.service, Kind: protocolv1.Kind_KIND_WORKFLOW, HandlerNames: []string{h.handler}},
		},
	}
}

func (h *xshardResolverHandler) serveInvoke(t *testing.T, stream *connect.BidiStream[protocolv1.Frame, protocolv1.Frame]) error {
	t.Helper()
	startFrame, err := stream.Receive()
	if err != nil {
		return err
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		return err
	}
	// known_entries: 1 fresh, 2 = JEInput + JEPromiseCompleteResult after ack.
	var ackSucceeded bool
	var ackFailure string
	delivered := false
	for range sm.GetKnownEntries() {
		fr, err := stream.Receive()
		if err != nil {
			return err
		}
		tc, _, _ := wire.UnpackHeader(fr.GetHeader())
		if tc == wire.TypeNoteCompletePromise {
			var note protocolv1.CompletePromiseCompletionNotificationMessage
			if err := proto.Unmarshal(fr.GetPayload(), &note); err != nil {
				return err
			}
			if f := note.GetFailure(); f != nil {
				ackFailure = f.GetMessage()
			} else {
				ackSucceeded = true
			}
			delivered = true
		}
	}
	if !delivered {
		// Emit CompletePromise scoped to the foreign workflow's
		// (service, key); this is the cross-partition case.
		cmd := &protocolv1.CompletePromiseCommandMessage{
			Service:            h.wfService,
			Key:                h.wfKey,
			Name:               h.promiseName,
			ResultCompletionId: 2,
			Completion: &protocolv1.CompletePromiseCommandMessage_CompletionValue{
				CompletionValue: &protocolv1.Value{Content: h.resolveValue},
			},
		}
		cp, _ := proto.Marshal(cmd)
		if err := stream.Send(frameFor(wire.TypeCmdCompletePromise, cp)); err != nil {
			return err
		}
		sp, _ := proto.Marshal(&protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}})
		if err := stream.Send(frameFor(wire.TypeSuspension, sp)); err != nil {
			return err
		}
		ep, _ := proto.Marshal(&protocolv1.EndMessage{})
		if err := stream.Send(frameFor(wire.TypeEnd, ep)); err != nil {
			return err
		}
		return drainStream(stream)
	}
	// On respawn after ack — emit a status message as the output so the
	// test can read succeeded/failure.
	var content []byte
	if ackSucceeded {
		content = []byte("ok")
	} else {
		content = []byte("conflict:" + ackFailure)
	}
	out := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{Value: &protocolv1.Value{Content: content}},
	}
	op, _ := proto.Marshal(out)
	if err := stream.Send(frameFor(wire.TypeCmdOutput, op)); err != nil {
		return err
	}
	ep, _ := proto.Marshal(&protocolv1.EndMessage{})
	if err := stream.Send(frameFor(wire.TypeEnd, ep)); err != nil {
		return err
	}
	return drainStream(stream)
}

// findCrossShardKeys returns two object keys for the given services such
// that their shards differ in the supplied partitioner. Tries a small
// numeric range; fails the test if no pair fits (improbable for any
// reasonable cluster size, since the keyspace is uniformly hashed).
func findCrossShardKeys(t *testing.T, p routing.Partitioner, wfSvc, resolverSvc string) (string, string) {
	t.Helper()
	for i := range 64 {
		for j := range 64 {
			if i == j {
				continue
			}
			wfKey := "wf-" + itoa(i)
			rsKey := "rs-" + itoa(j)
			wfShard := p.ShardForTarget(0, &enginev1.InvocationTarget{ServiceName: wfSvc, ObjectKey: wfKey})
			rsShard := p.ShardForTarget(0, &enginev1.InvocationTarget{ServiceName: resolverSvc, ObjectKey: rsKey})
			if wfShard != rsShard {
				return wfKey, rsKey
			}
		}
	}
	t.Fatalf("could not find cross-shard key pair after 64×64 attempts")
	return "", ""
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}

// TestWireDispatch_HTTP2_PromiseXshard_Resolve exercises the cross-
// partition Promise.Resolve flow end-to-end:
//
//  1. Workflow W lives on (Orders, wfKey) — shard X.
//  2. Resolver R lives on (Workers, rsKey) — shard Y (different).
//  3. W calls Promise("done").Result() and suspends.
//  4. R calls WorkflowPromise(target={Orders, wfKey}, "done").Resolve(value).
//     The apply path on Y enqueues an OutboxEnvelope.PromiseCompletion
//     to X; X applies it (wakes W), then ships a PromiseCompletionAck
//     back to Y; Y appends JEPromiseCompleteResult on R's journal.
//  5. Both W and R complete.
func TestWireDispatch_HTTP2_PromiseXshard_Resolve(t *testing.T) {
	const wantPayload = "xshard-promise-payload"
	wfH := &xshardWorkflowHandler{service: "Orders", handler: "run", promiseName: "done"}
	rsH := &xshardResolverHandler{
		service:      "Workers",
		handler:      "resolve",
		wfService:    "Orders",
		promiseName:  "done",
		resolveValue: []byte(wantPayload),
	}

	// Both deployments register via /Discovery + /InvokeStream; we run
	// them on separate HTTP servers so RegisterDeployment can target
	// each independently.
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
	t.Logf("wf key=%s shard=%d; resolver key=%s shard=%d",
		wfKey, host.Partitioner().ShardForTarget(0, &enginev1.InvocationTarget{ServiceName: wfH.service, ObjectKey: wfKey}),
		rsKey, host.Partitioner().ShardForTarget(0, &enginev1.InvocationTarget{ServiceName: rsH.service, ObjectKey: rsKey}))

	// Submit workflow on its shard.
	wfTarget := &enginev1.InvocationTarget{ServiceName: wfH.service, HandlerName: wfH.handler, ObjectKey: wfKey}
	wfID := buildID(routing.PartitionKey(0, wfH.service, wfKey), "xshard-wf")
	wfShard := host.Partitioner().ShardForInvocation(wfID)
	wfRunner := host.Partition(wfShard)
	if wfRunner == nil {
		t.Fatalf("workflow partition %d not running", wfShard)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := wfRunner.Proposer().ProposeIngress(subCtx, "test/xshard-wf", wfShard, &enginev1.Command{
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

	// Submit resolver on its own (different) shard.
	rsTarget := &enginev1.InvocationTarget{ServiceName: rsH.service, HandlerName: rsH.handler, ObjectKey: rsKey}
	rsID := buildID(routing.PartitionKey(0, rsH.service, rsKey), "xshard-rs")
	rsShard := host.Partitioner().ShardForInvocation(rsID)
	rsRunner := host.Partition(rsShard)
	if rsRunner == nil {
		t.Fatalf("resolver partition %d not running", rsShard)
	}
	if rsShard == wfShard {
		t.Fatalf("findCrossShardKeys returned same-shard pair; want distinct")
	}
	if err := rsRunner.Proposer().ProposeIngress(subCtx, "test/xshard-rs", rsShard, &enginev1.Command{
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

	// Workflow should wake with the resolved payload.
	t.Run("workflow", func(t *testing.T) {
		completed := awaitCompleted(t, host, wfShard, wfID, 15*time.Second)
		if got := string(completed.GetOutput()); got != wantPayload {
			t.Errorf("workflow output = %q; want %q", got, wantPayload)
		}
		if msg := completed.GetFailureMessage(); msg != "" {
			t.Errorf("workflow failure = %q; want empty", msg)
		}
	})

	// Resolver should also complete with ok ack.
	t.Run("resolver", func(t *testing.T) {
		completed := awaitCompleted(t, host, rsShard, rsID, 15*time.Second)
		if got := string(completed.GetOutput()); got != "ok" {
			t.Errorf("resolver output = %q; want %q (ack succeeded)", got, "ok")
		}
	})
}
