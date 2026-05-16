package engine_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// openSingleNodeOnDir opens an engine.Host on dataDir / raftAddr with
// shard 0 + shard 1 already started and awaited. Lifetime is bound to t.
// Use for tests that need a fixed RaftAddr + DataDir across restarts.
//
// Metadata (shard 0) leadership is awaited BEFORE the partition's
// leader-await. The partition's outbox shuffler runs on leader gain and
// may immediately re-propose pending cross-handler Invoke commands; the
// receiving invoker resolves callee deployment_ids via SyncRead on
// shard 0, so shard 0 must be live before partition Apply starts
// dispatching.
func openSingleNodeOnDir(t *testing.T, dataDir, raftAddr string) *engine.Host {
	t.Helper()
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            dataDir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
	})
	if err != nil {
		t.Fatalf("NewHost(%s, %s): %v", dataDir, raftAddr, err)
	}
	if _, err := h.StartMetadataShard(); err != nil {
		_ = h.Close()
		t.Fatalf("StartMetadataShard: %v", err)
	}
	if _, err := h.StartPartition(1); err != nil {
		_ = h.Close()
		t.Fatalf("StartPartition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.AwaitMetadataLeader(ctx); err != nil {
		_ = h.Close()
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}
	if err := h.AwaitLeader(ctx, 1); err != nil {
		_ = h.Close()
		t.Fatalf("AwaitLeader(1): %v", err)
	}
	return h
}

// TestVirtualObject_FIFOSerializesSameKey submits five invocations
// concurrently against the same (service, object_key); each handler
// records its observed entry order in shared memory and then sleeps
// briefly to widen the window for any concurrency bug. The VO gate must
// serialize them — every handler sees the prior count + 1, and the
// global completion order matches the submission order.
func TestVirtualObject_FIFOSerializesSameKey(t *testing.T) {
	const N = 5
	var (
		stepMu       sync.Mutex
		step         int32
		observedMu   sync.Mutex
		observed     []int32
		inflightSeen atomic.Int32
		inflight     atomic.Int32
	)

	handler := func(_ sdk.Context, _ []byte) ([]byte, error) {
		// Track concurrent inflight count — for a correctly serialized VO
		// this never exceeds 1.
		cur := inflight.Add(1)
		defer inflight.Add(-1)
		if cur > inflightSeen.Load() {
			inflightSeen.Store(cur)
		}

		stepMu.Lock()
		step++
		mine := step
		stepMu.Unlock()
		// Yield a few times so a buggy concurrency window would manifest.
		time.Sleep(10 * time.Millisecond)
		observedMu.Lock()
		observed = append(observed, mine)
		observedMu.Unlock()
		return fmt.Appendf(nil, "step=%d", mine), nil
	}

	reg := sdk.NewRegistry()
	if err := reg.RegisterObject("Counter", "incr", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h := singleNodeWithHandlers(t, reg)
	r := h.Partition(1)

	target := &enginev1.InvocationTarget{
		ServiceName: "Counter",
		HandlerName: "incr",
		ObjectKey:   "user-1",
	}
	depID := resolveDeploymentID(t, h, target.ServiceName, target.HandlerName)

	// Submit N invocations as quickly as possible. ProposeIngress is
	// sequential on the proposer's goroutine, but they all hit onInvoke in
	// strict order — exactly the case the VO gate has to handle.
	ids := make([]*enginev1.InvocationId, N)
	for i := range N {
		id := buildID(1, fmt.Sprintf("fifo-%02d", i))
		ids[i] = id
		propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := r.Proposer().ProposeIngress(propCtx, fmt.Sprintf("fifo/%d", i), uint64(i+1), &enginev1.Command{
			Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: id, Target: target, Input: []byte("in"), DeploymentId: depID,
			}},
		})
		propCancel()
		if err != nil {
			t.Fatalf("ProposeIngress[%d]: %v", i, err)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		st, err := r.StatusOf(ids[N-1])
		if err == nil {
			if _, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// All N must be Completed in submission order.
	for i, id := range ids {
		st, err := r.StatusOf(id)
		if err != nil {
			t.Fatalf("StatusOf[%d]: %v", i, err)
		}
		c, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed)
		if !ok {
			t.Fatalf("invocation %d not completed: %T", i, st.GetStatus())
		}
		want := fmt.Sprintf("step=%d", i+1)
		if string(c.Completed.GetOutput()) != want {
			t.Errorf("invocation %d output=%q want %q", i, c.Completed.GetOutput(), want)
		}
	}

	observedMu.Lock()
	defer observedMu.Unlock()
	if len(observed) != N {
		t.Fatalf("observed %d completions; want %d", len(observed), N)
	}
	for i, v := range observed {
		if int(v) != i+1 {
			t.Errorf("step order broken at %d: got %d", i, v)
		}
	}
	if got := inflightSeen.Load(); got > 1 {
		t.Errorf("max concurrent handlers = %d; want 1 (FIFO violated)", got)
	}
}

// TestVirtualObject_DistinctKeysRunInParallel is the inverse: the
// VO gate is per-key, so concurrent invocations on different keys must NOT
// be serialized.
func TestVirtualObject_DistinctKeysRunInParallel(t *testing.T) {
	const N = 3
	var inflightMax atomic.Int32
	var inflight atomic.Int32
	started := make(chan struct{}, N)
	release := make(chan struct{})

	handler := func(_ sdk.Context, _ []byte) ([]byte, error) {
		cur := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			prev := inflightMax.Load()
			if cur <= prev || inflightMax.CompareAndSwap(prev, cur) {
				break
			}
		}
		started <- struct{}{}
		<-release
		return []byte("done"), nil
	}

	reg := sdk.NewRegistry()
	if err := reg.RegisterObject("Counter", "incr", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h := singleNodeWithHandlers(t, reg)
	r := h.Partition(1)
	depID := resolveDeploymentID(t, h, "Counter", "incr")

	ids := make([]*enginev1.InvocationId, N)
	for i := range N {
		id := buildID(1, fmt.Sprintf("par-%02d", i))
		ids[i] = id
		target := &enginev1.InvocationTarget{
			ServiceName: "Counter",
			HandlerName: "incr",
			ObjectKey:   fmt.Sprintf("user-%d", i), // distinct key per invocation
		}
		propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := r.Proposer().ProposeIngress(propCtx, fmt.Sprintf("par/%d", i), uint64(i+1), &enginev1.Command{
			Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: id, Target: target, Input: []byte("in"), DeploymentId: depID,
			}},
		})
		propCancel()
		if err != nil {
			t.Fatalf("ProposeIngress[%d]: %v", i, err)
		}
	}

	// Wait until all N handlers are running concurrently — proving the
	// gate doesn't serialize across keys.
	deadline := time.Now().Add(10 * time.Second)
	got := 0
	for got < N && time.Now().Before(deadline) {
		select {
		case <-started:
			got++
		case <-time.After(time.Until(deadline)):
		}
	}
	if got != N {
		t.Fatalf("only %d/%d handlers started concurrently within deadline", got, N)
	}
	if max := inflightMax.Load(); max < int32(N) {
		t.Errorf("max concurrent = %d; want %d (per-key gate over-serialized)", max, N)
	}

	close(release)

	// Drain to Completed.
	completionDeadline := time.Now().Add(10 * time.Second)
	for _, id := range ids {
		for time.Now().Before(completionDeadline) {
			st, err := r.StatusOf(id)
			if err == nil {
				if _, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// TestVirtualObject_QueueSurvivesRestart drives two invocations
// against the same key: the first runs and the second queues. The host is
// closed before either completes; on reopen, the engine resumes from the
// persisted KeyLeaseStatus and drains the queue in order.
//
// The SDK handler lives in a separate process (pkg/sdk/server) whose
// lifetime spans both engine epochs. A single closure reads the current
// gate channel from an atomic pointer; the test swaps it between phases
// to control when handlers complete.
func TestVirtualObject_QueueSurvivesRestart(t *testing.T) {
	var (
		holder atomic.Pointer[string]
		gate   atomic.Pointer[chan struct{}]
	)
	gate1 := make(chan struct{})
	gate.Store(&gate1)
	handler := func(c sdk.Context, in []byte) ([]byte, error) {
		s := string(in)
		holder.Store(&s)
		g := *gate.Load()
		select {
		case <-g:
		case <-c.Context().Done():
		case <-time.After(5 * time.Second):
		}
		return []byte("done:" + s), nil
	}

	reg := sdk.NewRegistry()
	if err := reg.RegisterObject("Counter", "incr", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	handlerURL := startSDKServer(t, reg)

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")
	raftAddr := freeLocalAddr(t)
	h1 := openSingleNodeOnDir(t, dataDir, raftAddr)
	registerDeploymentURL(t, h1, handlerURL)
	r := h1.Partition(1)

	target := &enginev1.InvocationTarget{
		ServiceName: "Counter",
		HandlerName: "incr",
		ObjectKey:   "user-X",
	}
	depID := resolveDeploymentID(t, h1, target.ServiceName, target.HandlerName)
	idA := buildID(1, "queue-A")
	idB := buildID(1, "queue-B")
	for i, id := range []*enginev1.InvocationId{idA, idB} {
		propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := r.Proposer().ProposeIngress(propCtx, fmt.Sprintf("queue/%d", i), uint64(i+1), &enginev1.Command{
			Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: id, Target: target, Input: []byte(string(rune('A' + i))), DeploymentId: depID,
			}},
		})
		propCancel()
		if err != nil {
			_ = h1.Close()
			t.Fatalf("ProposeIngress[%d]: %v", i, err)
		}
	}

	// Wait until the first handler is running (holder set to "A").
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := holder.Load(); p != nil && *p == "A" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if p := holder.Load(); p == nil || *p != "A" {
		_ = h1.Close()
		t.Fatalf("first handler did not start: holder=%v", holder.Load())
	}

	// Close the host while A is blocked and B is queued. The handler
	// running on the SDK server sees its stream cancelled via ctx and
	// returns; its result is discarded (no JECompleted on h1).
	if err := h1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Swap the gate to a closed channel so post-restart handler runs
	// (replayed A + dequeued B) complete immediately. Deployment
	// registration is durable in shard 0; the handler URL is unchanged.
	gate2 := make(chan struct{})
	close(gate2)
	gate.Store(&gate2)

	h2 := openSingleNodeOnDir(t, dataDir, raftAddr)
	r2 := h2.Partition(1)

	// Both must complete; B must come after A.
	completionDeadline := time.Now().Add(15 * time.Second)
	completed := func(id *enginev1.InvocationId) bool {
		st, err := r2.StatusOf(id)
		if err != nil {
			return false
		}
		_, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed)
		return ok
	}
	for time.Now().Before(completionDeadline) {
		if completed(idA) && completed(idB) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !completed(idA) {
		t.Fatalf("A did not complete after restart")
	}
	if !completed(idB) {
		t.Fatalf("B did not complete after restart (queue not preserved)")
	}
}
