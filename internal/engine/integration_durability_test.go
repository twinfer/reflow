package engine_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/loadgen"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// freeLocalAddr is a thin shim into loadgen.FreeLocalAddr so the
// existing engine_test call sites don't have to migrate imports.
func freeLocalAddr(t *testing.T) string { return loadgen.FreeLocalAddr(t) }

// bringUpSingleNode starts a one-partition Host. The raftAddr argument lets
// callers reuse the same advertised address across restarts so dragonboat's
// data-dir ownership check doesn't fail; pass "" to allocate a fresh port.
func bringUpSingleNode(t *testing.T, dir, raftAddr string) (*engine.Host, *engine.PartitionRunner, string) {
	t.Helper()
	if raftAddr == "" {
		raftAddr = freeLocalAddr(t)
	}
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            dir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	r, err := h.StartPartition(1)
	if err != nil {
		_ = h.Close()
		t.Fatalf("StartPartition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.AwaitLeader(ctx, 1); err != nil {
		_ = h.Close()
		t.Fatalf("AwaitLeader: %v", err)
	}
	return h, r, raftAddr
}

// bringUpSingleNodeWithDeployment is bringUpSingleNode plus the metadata
// shard plus a registered deployment for (service, handler) at url. Used
// by tests that drive the FSM through Invoke: ActInvoke spawns an invoker
// session, and without a resolvable deployment that session calls
// failTerminal — which propagates as InvokerEffect_Completed and the
// Invoked → Completed apply arm reaps every pending Sleep timer for the
// invocation (partition.go onInvokerEffect Completed case). Registering a
// resolvable record keeps the session past the resolve gate; the caller
// supplies a url whose listener doesn't drive the session to terminate
// (typically a black-hole TCP listener), so the session parks in driveLoop
// until h.Close cancels its ctx.
//
// Shard 0 is started + awaited before the partition shard so post-restart
// ResumeNonTerminal's wire sessions can SyncRead shard 0 without racing
// the metadata election. The deployment record is idempotent-upserted;
// callers re-invoke with the same id + url across restarts.
func bringUpSingleNodeWithDeployment(
	t *testing.T,
	dir, raftAddr, depID, url string,
	handlers []*enginev1.DeploymentHandler,
) (*engine.Host, *engine.PartitionRunner, string) {
	t.Helper()
	if raftAddr == "" {
		raftAddr = freeLocalAddr(t)
	}
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            dir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if _, err := h.StartMetadataShard(); err != nil {
		_ = h.Close()
		t.Fatalf("StartMetadataShard: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.AwaitMetadataLeader(ctx); err != nil {
		_ = h.Close()
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}
	r, err := h.StartPartition(1)
	if err != nil {
		_ = h.Close()
		t.Fatalf("StartPartition: %v", err)
	}
	if err := h.AwaitLeader(ctx, 1); err != nil {
		_ = h.Close()
		t.Fatalf("AwaitLeader: %v", err)
	}
	regCmd := &enginev1.Command{
		Kind: &enginev1.Command_RegisterDeployment{
			RegisterDeployment: &enginev1.RegisterDeployment{
				Record: &enginev1.DeploymentRecord{
					Id:             depID,
					Url:            url,
					Handlers:       handlers,
					RegisteredAtMs: uint64(time.Now().UnixMilli()),
				},
			},
		},
	}
	if err := h.MetadataRunner().Proposer().ProposeSelf(ctx, regCmd); err != nil {
		_ = h.Close()
		t.Fatalf("ProposeSelf RegisterDeployment: %v", err)
	}
	return h, r, raftAddr
}

func proposeInvoke(t *testing.T, r *engine.PartitionRunner, id *enginev1.InvocationId, target *enginev1.InvocationTarget, producerID string, seq uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := r.Proposer().ProposeIngress(ctx, producerID, seq, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("input"),
		}},
	})
	if err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
}

func proposeInvokerEffect(t *testing.T, r *engine.PartitionRunner, eff *enginev1.InvokerEffect) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Proposer().ProposeSelf(ctx, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: eff},
	}); err != nil {
		t.Fatalf("ProposeSelf: %v", err)
	}
}

func TestSingleNodeReplayAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")

	// Step A: bring up node, drive an invocation to completion through
	// Input -> Sleep -> SleepResult -> Output -> Completed.
	h, r, raftAddr := bringUpSingleNode(t, dataDir, "")
	_ = raftAddr

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "Greeter", HandlerName: "hello"}

	proposeInvoke(t, r, id, target, "ingress-1", 1)
	proposeInvokerEffect(t, r, &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{
			JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 0,
					Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("hello")}},
				},
			},
		},
	})

	// Sleep 100ms into the future. The TimerService should fire it.
	fireAt := uint64(time.Now().UnixMilli()) + 100
	proposeInvokerEffect(t, r, &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{
			JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 1,
					Entry: &enginev1.JournalEntry_Sleep{Sleep: &enginev1.JESleep{FireAtMs: fireAt}},
				},
			},
		},
	})

	// Wait up to ~2s for the SleepResult to appear (a TimerFired -> apply ->
	// SleepResult journal write happens behind the scenes).
	if err := waitForJournalEntry(h, id, 2, 3*time.Second); err != nil {
		t.Fatalf("SleepResult not observed: %v", err)
	}

	// Finalize.
	proposeInvokerEffect(t, r, &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_Completed{
			Completed: &enginev1.InvocationCompleted{Output: []byte("hello world")},
		},
	})

	// Eventually-consistent check: status should reach Completed.
	deadline := time.Now().Add(3 * time.Second)
	var preRestart *enginev1.InvocationStatus
	for time.Now().Before(deadline) {
		s, err := r.StatusOf(id)
		if err == nil {
			if _, ok := s.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				preRestart = s
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if preRestart == nil {
		t.Fatalf("invocation did not reach Completed pre-restart")
	}

	// Step B: close and reopen.
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	h2, r2, _ := bringUpSingleNode(t, dataDir, raftAddr)
	defer h2.Close()

	// State must survive.
	got, err := r2.StatusOf(id)
	if err != nil {
		t.Fatalf("StatusOf post-restart: %v", err)
	}
	cmp, ok := got.GetStatus().(*enginev1.InvocationStatus_Completed)
	if !ok {
		t.Fatalf("post-restart status = %T; want Completed", got.GetStatus())
	}
	if string(cmp.Completed.GetOutput()) != "hello world" {
		t.Errorf("post-restart output = %q; want %q", cmp.Completed.GetOutput(), "hello world")
	}
}

func TestDedupBlocksDuplicateIngress(t *testing.T) {
	dir := t.TempDir()
	h, r, _ := bringUpSingleNode(t, filepath.Join(dir, "node1"), "")
	defer h.Close()

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S"}

	// Same producer + seq twice. The second must be dedup'd.
	proposeInvoke(t, r, id, target, "ingress-X", 7)
	proposeInvoke(t, r, id, target, "ingress-X", 7)

	// We expect exactly one Scheduled invocation row. Verify by reading the
	// status — the second propose with same dedup is silently skipped, so
	// the state is still Scheduled.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s, err := r.StatusOf(id)
		if err == nil {
			if _, ok := s.GetStatus().(*enginev1.InvocationStatus_Scheduled); ok {
				return // success
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("invocation status not observed as Scheduled within timeout")
}

func TestTimerSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node1")

	// Black-hole listener: accepts TCP but never speaks HTTP/2. The
	// invoker's wire session parks here (sendStartAndReplay / driveLoop
	// awaitResponse) until h.Close cancels its ctx, at which point
	// failTerminal is a no-op (it checks ctx.Err before proposing
	// Completed). Without this, the session would fail-terminate the
	// invocation and the Invoked → Completed apply arm would reap the
	// pending Sleep timer we're trying to exercise across restart.
	blackhole, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen blackhole: %v", err)
	}
	defer blackhole.Close()

	const depID = "test-dep-timer"
	handlers := []*enginev1.DeploymentHandler{
		{Service: "S", Handler: "h", Kind: uint32(protocolv1.Kind_KIND_SERVICE)},
	}
	url := "http://" + blackhole.Addr().String()

	h, r, raftAddr := bringUpSingleNodeWithDeployment(t, dataDir, "", depID, url, handlers)

	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")}
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h"}

	// proposeInvoke stamps no deployment_id; inline the propose so we
	// can pin the invocation to depID and skip the (service, handler)
	// → deployment_id index lookup in resolveDeployment.
	ictx, icancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer icancel()
	if err := r.Proposer().ProposeIngress(ictx, "ingress-1", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("input"),
			DeploymentId: depID,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	proposeInvokerEffect(t, r, &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{
			JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 0,
					Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{}},
				},
			},
		},
	})
	// Sleep into the future, well past the close+reopen latency.
	fireAt := uint64(time.Now().UnixMilli()) + 1500
	proposeInvokerEffect(t, r, &enginev1.InvokerEffect{
		InvocationId: id,
		Kind: &enginev1.InvokerEffect_JournalAppended{
			JournalAppended: &enginev1.JournalEntryAppended{
				Entry: &enginev1.JournalEntry{
					Index: 1,
					Entry: &enginev1.JournalEntry_Sleep{Sleep: &enginev1.JESleep{FireAtMs: fireAt}},
				},
			},
		},
	})

	// Immediately restart while the timer is still pending.
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h2, _, _ := bringUpSingleNodeWithDeployment(t, dataDir, raftAddr, depID, url, handlers)
	defer h2.Close()

	// The timer rebuild on leader gain should re-arm the timer; SleepResult
	// must eventually be journaled.
	if err := waitForJournalEntry(h2, id, 2, 5*time.Second); err != nil {
		t.Fatalf("timer did not survive restart: %v", err)
	}
}

func waitForJournalEntry(h *engine.Host, id *enginev1.InvocationId, index uint32, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r := h.Partition(1)
		if r != nil {
			st, err := r.StatusOf(id)
			if err == nil && st.GetStatus() != nil {
				// We rely on the journal table being populated. Read it
				// directly via the snapshotter's store.
				if scanContains(h, id, index) {
					return nil
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("journal entry %d not observed within %s", index, timeout)
}

func scanContains(h *engine.Host, id *enginev1.InvocationId, idx uint32) bool {
	r := h.Partition(1)
	if r == nil {
		return false
	}
	store := r.Snapshotter().Store()
	if store == nil {
		return false
	}
	// Open a fresh table view because the snapshotter may rebind.
	jt := journalTableFor(store)
	got, err := jt.Read(id, idx)
	if err != nil || got == nil {
		return false
	}
	return got.GetIndex() == idx
}
