package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/pkg/handler"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestTenantIDPropagation_EndToEnd verifies tenant_id rides the full
// hop chain established in PR 2:
//
//	InvokeCommand → InvocationStatus → slot-0 JEInput
//	                 ↳ outbound OutboxEnvelope_Invoke (for ctx.Call)
//
// A Caller submitted with tenant_id=42 must:
//
//  1. Produce a slot-0 JEInput.tenant_id == 42 in its own journal
//     (covers Free→Scheduled + invoker first-activation hops).
//
//  2. When Caller does ctx.Call(Callee), the spawned callee invocation
//     must also reach the same JEInput.tenant_id == 42 — proves the
//     outbox stamping on JECall inherits the parent's tenant.
//
// A drop on any hop fails one of the two assertions. PR 2's only data
// plumbing lives in these paths; the consumers (PR 4 value encryption,
// PR 5 OIDC, PR 6 quota) bolt on after.
func TestTenantIDPropagation_EndToEnd(t *testing.T) {
	const wantTenantID uint32 = 42

	reg := handler.NewRegistry()
	if err := reg.RegisterService("Callee", "do", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("from-callee:"), in...), nil
	}); err != nil {
		t.Fatalf("Register Callee: %v", err)
	}
	if err := reg.RegisterService("Caller", "go", func(c handler.Context, in []byte) ([]byte, error) {
		out, err := c.Call(handler.Target{Service: "Callee", Handler: "do"}, in).Result()
		if err != nil {
			return nil, err
		}
		return append([]byte("caller-wrap:"), out...), nil
	}); err != nil {
		t.Fatalf("Register Caller: %v", err)
	}
	handlerURL := startSDKServer(t, reg)

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")
	raftAddr := freeLocalAddr(t)

	h := openSingleNodeOnDir(t, dataDir, raftAddr)
	defer func() { _ = h.Close() }()
	registerDeploymentURL(t, h, handlerURL)
	r := h.Partition(1)

	callerID := buildID(1, "tenprop")
	target := &enginev1.InvocationTarget{ServiceName: "Caller", HandlerName: "go"}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := r.Proposer().ProposeIngress(propCtx, "test/tenprop", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: callerID,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: depID,
			TenantId:     wantTenantID,
		}},
	})
	propCancel()
	if err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	// Wait for the full Caller+Callee chain to land. awaitCompleted on
	// the caller proves both invocations finished, since Caller can only
	// terminate after Callee's JECallResult lands on its journal.
	completed := awaitCompleted(t, h, 1, callerID, 15*time.Second)
	if got := string(completed.GetOutput()); got != "caller-wrap:from-callee:hello" {
		t.Fatalf("output = %q; want caller-wrap:from-callee:hello", got)
	}

	jt := journalTableFor(r.Snapshotter().Store())

	// Assert 1: Caller's slot-0 JEInput.tenant_id (the durable journal
	// row written by the invoker's first-activation propose) carries the
	// submitted tenant.
	callerInput, err := jt.Read(callerID, 0)
	if err != nil {
		t.Fatalf("read caller slot 0: %v", err)
	}
	if got := callerInput.GetInput().GetTenantId(); got != wantTenantID {
		t.Fatalf("caller JEInput.tenant_id = %d; want %d (InvokeCommand → InvocationStatus → JEInput hop dropped the tenant)", got, wantTenantID)
	}

	// Assert 2: Callee's slot-0 JEInput.tenant_id is the same. The only
	// path to the callee's tenant is via the outbox-side InvokeCommand
	// stamping from cur.GetTenantId() on the parent's JECall apply arm.
	calleeTarget := &enginev1.InvocationTarget{ServiceName: "Callee", HandlerName: "do"}
	calleeID := deriveCalleeID(callerID, 1, calleeTarget)
	calleeInput, err := jt.Read(calleeID, 0)
	if err != nil {
		t.Fatalf("read callee slot 0: %v", err)
	}
	if got := calleeInput.GetInput().GetTenantId(); got != wantTenantID {
		t.Fatalf("callee JEInput.tenant_id = %d; want %d (outbox InvokeCommand stamping must inherit parent tenant)", got, wantTenantID)
	}
}
