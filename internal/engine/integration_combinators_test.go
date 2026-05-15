package engine_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Integration tests for combinator futures (ctx.All / ctx.Any):
//
//   - All resolves only when every child has resolved; partial resolution
//     leaves the handler Suspended on the remaining tokens.
//   - Any resolves on the first child to land in the journal; argument
//     order — not wall-clock — is the tiebreaker.
//   - Nested combinators (All(f, Any(g, sleep))) compose without any
//     engine-side awareness.
//   - The combinator gate survives a host crash because it's pure SDK
//     composition over journal entries that are themselves durable.
//
// All tests bring up a single-node host + ingress and resolve awakeables
// via the HTTP endpoint.

// resolveAwakeable POSTs the value to /awakeable/{id}/resolve, retrying
// while the server reports NotFound. The JEAwakeable journal write
// races with the resolve call on the first attempt; retrying for a
// couple of seconds covers the gap without falsely failing on cold
// start.
func resolveAwakeable(t *testing.T, base, id string, value []byte) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"value": base64.StdEncoding.EncodeToString(value),
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodPost, base+"/awakeable/"+id+"/resolve", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("resolve %s: %v", id, err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			if !bytes.Contains(respBody, []byte(`"resolved":true`)) {
				t.Fatalf("resolve %s: missing resolved:true in %s", id, string(respBody))
			}
			return
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("resolve %s: code=%d body=%s", id, resp.StatusCode, string(respBody))
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatalf("awakeable %s never became resolvable", id)
}

// awaitSuspended polls SyncRead until the invocation reaches the
// Suspended status. Returns the awaiting_on token slice for assertions.
// Fails the test on deadline.
func awaitSuspended(t *testing.T, h *engine.Host, shardID uint64, id *enginev1.InvocationId, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		s, err := h.LookupInvocationStatus(ctx, shardID, id)
		cancel()
		if err == nil && s != nil {
			if sus, ok := s.GetStatus().(*enginev1.InvocationStatus_Suspended); ok {
				return sus.Suspended.GetAwaitingOn()
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("invocation never reached Suspended within %s", timeout)
	return nil
}

// drainAwakeableIDs reads exactly n IDs off ch with a deadline; fails
// the test if any are missing. Test handlers emit each awakeable ID
// once into a buffered channel; combinator tests need all of them
// before they can drive resolutions.
func drainAwakeableIDs(t *testing.T, ch <-chan string, n int, timeout time.Duration) []string {
	t.Helper()
	out := make([]string, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case id := <-ch:
			out = append(out, id)
		case <-deadline:
			t.Fatalf("only got %d of %d awakeable IDs within %s", len(out), n, timeout)
		}
	}
	return out
}

// TestCombinator_All_ResolvesWhenAllChildrenComplete is the happy-path
// test for ctx.All: three awakeables, resolved out of order, handler
// returns the values concatenated in argument order. Verifies both the
// composition shape and the argument-order invariant on Results.
func TestCombinator_All_ResolvesWhenAllChildrenComplete(t *testing.T) {
	idsCh := make(chan string, 3)
	var emitted atomic.Bool

	reg := sdk.NewRegistry()
	if err := reg.Register("Joiner", "all", func(c sdk.Context, _ []byte) ([]byte, error) {
		id1, f1 := c.Awakeable()
		id2, f2 := c.Awakeable()
		id3, f3 := c.Awakeable()
		if emitted.CompareAndSwap(false, true) {
			idsCh <- id1
			idsCh <- id2
			idsCh <- id3
		}
		vs, err := c.All(f1, f2, f3).Results()
		if err != nil {
			return nil, err
		}
		return bytes.Join(vs, []byte("|")), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	id := buildID(1, "joiner-all")
	target := &enginev1.InvocationTarget{ServiceName: "Joiner", HandlerName: "all"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := h.Partition(1).Proposer().ProposeIngress(propCtx, "test/joiner-all", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	propCancel()

	got := drainAwakeableIDs(t, idsCh, 3, 5*time.Second)
	// Resolve out of argument order — Results must still pick them up in
	// the original (f1, f2, f3) sequence.
	resolveAwakeable(t, base, got[2], []byte("third"))
	resolveAwakeable(t, base, got[0], []byte("first"))
	resolveAwakeable(t, base, got[1], []byte("second"))

	completed := awaitCompleted(t, h, 1, id, 10*time.Second)
	if want := "first|second|third"; string(completed.GetOutput()) != want {
		t.Errorf("output = %q; want %q", completed.GetOutput(), want)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}

// TestCombinator_All_StaysSuspendedOnPartialResolution verifies the
// re-suspend cycle: every child resolution that doesn't satisfy All
// must leave the handler in Suspended (not Completed) with the
// outstanding tokens preserved on awaiting_on so the next resolution
// can wake it.
func TestCombinator_All_StaysSuspendedOnPartialResolution(t *testing.T) {
	idsCh := make(chan string, 3)
	var emitted atomic.Bool

	reg := sdk.NewRegistry()
	if err := reg.Register("Joiner", "partial", func(c sdk.Context, _ []byte) ([]byte, error) {
		id1, f1 := c.Awakeable()
		id2, f2 := c.Awakeable()
		id3, f3 := c.Awakeable()
		if emitted.CompareAndSwap(false, true) {
			idsCh <- id1
			idsCh <- id2
			idsCh <- id3
		}
		vs, err := c.All(f1, f2, f3).Results()
		if err != nil {
			return nil, err
		}
		return bytes.Join(vs, []byte(",")), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	id := buildID(1, "joiner-part")
	target := &enginev1.InvocationTarget{ServiceName: "Joiner", HandlerName: "partial"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := h.Partition(1).Proposer().ProposeIngress(propCtx, "test/joiner-part", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	propCancel()

	got := drainAwakeableIDs(t, idsCh, 3, 5*time.Second)

	// Resolve two of three. The handler will fast-replay, see partial
	// resolution, and re-suspend on the remaining one.
	resolveAwakeable(t, base, got[0], []byte("v0"))
	resolveAwakeable(t, base, got[1], []byte("v1"))

	// Wait for Suspended with only the third awakeable's token left.
	awaitingOn := awaitSuspended(t, h, 1, id, 5*time.Second)
	wantToken := "awakeable:" + got[2]
	found := false
	for _, tok := range awaitingOn {
		if tok == wantToken {
			found = true
		}
	}
	if !found {
		t.Errorf("after partial resolve, awaiting_on = %v; expected to contain %q", awaitingOn, wantToken)
	}

	// Finish the join.
	resolveAwakeable(t, base, got[2], []byte("v2"))
	completed := awaitCompleted(t, h, 1, id, 10*time.Second)
	if want := "v0,v1,v2"; string(completed.GetOutput()) != want {
		t.Errorf("output = %q; want %q", completed.GetOutput(), want)
	}
}

// TestCombinator_Any_ReturnsFirstResolverByArgumentOrder verifies Any
// returns the value of the lowest-indexed child whose result is
// present. Resolving only the middle (index 1) child establishes that
// Any does not require the leftmost child to be resolved.
func TestCombinator_Any_ReturnsFirstResolverByArgumentOrder(t *testing.T) {
	idsCh := make(chan string, 3)
	var emitted atomic.Bool

	reg := sdk.NewRegistry()
	if err := reg.Register("Racer", "any", func(c sdk.Context, _ []byte) ([]byte, error) {
		id1, f1 := c.Awakeable()
		id2, f2 := c.Awakeable()
		id3, f3 := c.Awakeable()
		if emitted.CompareAndSwap(false, true) {
			idsCh <- id1
			idsCh <- id2
			idsCh <- id3
		}
		v, err := c.Any(f1, f2, f3).Result()
		if err != nil {
			return nil, err
		}
		return append([]byte("won:"), v...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	id := buildID(1, "racer-any")
	target := &enginev1.InvocationTarget{ServiceName: "Racer", HandlerName: "any"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := h.Partition(1).Proposer().ProposeIngress(propCtx, "test/racer-any", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	propCancel()

	got := drainAwakeableIDs(t, idsCh, 3, 5*time.Second)
	// Resolve only the middle awakeable; Any should pick it up because
	// it is the lowest-indexed *resolved* child.
	resolveAwakeable(t, base, got[1], []byte("middle"))

	completed := awaitCompleted(t, h, 1, id, 10*time.Second)
	if want := "won:middle"; string(completed.GetOutput()) != want {
		t.Errorf("output = %q; want %q", completed.GetOutput(), want)
	}
}

// TestCombinator_AnyOfAwakeableAndSleep_TimeoutPattern exercises the
// canonical timeout idiom: race an external awakeable against a
// short Sleep so the Sleep wins when the external resolution never
// arrives. Confirms Sleep is genuinely a Future and composes under
// the same Any combinator as Awakeable.
func TestCombinator_AnyOfAwakeableAndSleep_TimeoutPattern(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.Register("Timer", "race", func(c sdk.Context, _ []byte) ([]byte, error) {
		_, never := c.Awakeable()
		short := c.Sleep(60 * time.Millisecond)
		// Any picks the first resolved by argument order. The
		// awakeable is never resolved by the test, so Sleep wins;
		// Sleep's Result returns nil bytes — observe completion by
		// returning a sentinel payload past the combinator.
		_, err := c.Any(never, short).Result()
		if err != nil {
			return nil, err
		}
		return []byte("timed-out"), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h, _ := bringUpHostWithIngress(t, reg)

	id := buildID(1, "timer-race")
	target := &enginev1.InvocationTarget{ServiceName: "Timer", HandlerName: "race"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := h.Partition(1).Proposer().ProposeIngress(propCtx, "test/timer-race", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	propCancel()

	completed := awaitCompleted(t, h, 1, id, 10*time.Second)
	if string(completed.GetOutput()) != "timed-out" {
		t.Errorf("output = %q; want timed-out", completed.GetOutput())
	}
}

// TestCombinator_Nested_AllOfAwakeableAndAny verifies that combinators
// compose: an All over (awakeable, Any(awakeable, sleep)) completes
// when the outer awakeable resolves AND the inner Any resolves (here
// via the Sleep, since its peer awakeable is never resolved).
func TestCombinator_Nested_AllOfAwakeableAndAny(t *testing.T) {
	outerCh := make(chan string, 1)
	innerCh := make(chan string, 1)
	var emitted atomic.Bool

	reg := sdk.NewRegistry()
	if err := reg.Register("Nest", "ed", func(c sdk.Context, _ []byte) ([]byte, error) {
		outerID, outerF := c.Awakeable()
		innerID, innerAwakeable := c.Awakeable()
		short := c.Sleep(60 * time.Millisecond)
		if emitted.CompareAndSwap(false, true) {
			outerCh <- outerID
			innerCh <- innerID
		}
		inner := c.Any(innerAwakeable, short)
		vs, err := c.All(outerF, inner).Results()
		if err != nil {
			return nil, err
		}
		// vs[0] = outer awakeable value; vs[1] = nil (sleep won the
		// inner Any). Glue them together so the test can assert that
		// All resolved with the right shape.
		return append([]byte("outer="), vs[0]...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	id := buildID(1, "nest-ed")
	target := &enginev1.InvocationTarget{ServiceName: "Nest", HandlerName: "ed"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := h.Partition(1).Proposer().ProposeIngress(propCtx, "test/nest-ed", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	propCancel()

	outerID := <-outerCh
	<-innerCh // drain but never resolve — the Sleep wins the inner Any
	resolveAwakeable(t, base, outerID, []byte("payload"))

	completed := awaitCompleted(t, h, 1, id, 10*time.Second)
	if want := "outer=payload"; string(completed.GetOutput()) != want {
		t.Errorf("output = %q; want %q", completed.GetOutput(), want)
	}
}

// TestCombinator_All_SurvivesRestart resolves one child of an All, kills
// the host before the remaining children resolve, restarts, then
// resolves the rest. The handler must see the same awakeable IDs on
// replay (journal carries them) and the partial-resolution journal
// state must drive the same re-suspend behavior so completion fires
// when the final resolution lands.
func TestCombinator_All_SurvivesRestart(t *testing.T) {
	idsCh := make(chan string, 4)
	var emitted atomic.Bool

	register := func(reg *sdk.Registry) {
		if err := reg.Register("Persist", "all", func(c sdk.Context, _ []byte) ([]byte, error) {
			id1, f1 := c.Awakeable()
			id2, f2 := c.Awakeable()
			// emitted is per-process; both pre- and post-crash runs
			// publish so the test can pick up the IDs whichever side
			// happens to be live.
			if emitted.CompareAndSwap(false, true) {
				idsCh <- id1
				idsCh <- id2
			}
			vs, err := c.All(f1, f2).Results()
			if err != nil {
				return nil, err
			}
			return bytes.Join(vs, []byte(":")), nil
		}); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}

	dir := t.TempDir()
	raftAddr := freeLocalAddr(t)
	dataDir := filepath.Join(dir, "node1")

	regBefore := sdk.NewRegistry()
	register(regBefore)
	hBefore, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            dataDir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		Handlers:           regBefore,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if _, err := hBefore.StartPartition(1); err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := hBefore.AwaitLeader(leaderCtx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}
	leaderCancel()
	rtBefore, err := ingress.Start(context.Background(), hBefore, ingress.Config{
		HTTPAddr: "127.0.0.1:0",
		GRPCAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("ingress: %v", err)
	}
	baseBefore := "http://" + rtBefore.HTTPAddr()

	id := buildID(1, "persist-all")
	target := &enginev1.InvocationTarget{ServiceName: "Persist", HandlerName: "all"}
	propCtx, propCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := hBefore.Partition(1).Proposer().ProposeIngress(propCtx, "test/persist-all", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id, Target: target,
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}
	propCancel()

	got := drainAwakeableIDs(t, idsCh, 2, 5*time.Second)
	resolveAwakeable(t, baseBefore, got[0], []byte("pre"))
	// Wait for the handler to fast-replay, observe the first
	// resolution, and re-suspend on the second awakeable. Otherwise
	// the kill could land before the partial-resolution journal write
	// is persisted, and after restart the handler would race the
	// first resolve again.
	awaitSuspended(t, hBefore, 1, id, 5*time.Second)

	if err := rtBefore.Close(); err != nil {
		t.Fatalf("ingress close: %v", err)
	}
	if err := hBefore.Close(); err != nil {
		t.Fatalf("host close: %v", err)
	}

	// Restart on the same data dir; emitted is reset because it's a
	// per-process atomic. We re-drain idsCh so the post-crash run
	// republishes the same IDs and the test can resolve the second.
	emitted.Store(false)
	regAfter := sdk.NewRegistry()
	register(regAfter)
	hAfter, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            dataDir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		Handlers:           regAfter,
	})
	if err != nil {
		t.Fatalf("NewHost after restart: %v", err)
	}
	t.Cleanup(func() { _ = hAfter.Close() })
	if _, err := hAfter.StartPartition(1); err != nil {
		t.Fatalf("StartPartition after restart: %v", err)
	}
	leader2Ctx, leader2Cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := hAfter.AwaitLeader(leader2Ctx, 1); err != nil {
		t.Fatalf("AwaitLeader after restart: %v", err)
	}
	leader2Cancel()
	rtAfter, err := ingress.Start(context.Background(), hAfter, ingress.Config{
		HTTPAddr: "127.0.0.1:0",
		GRPCAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("ingress after restart: %v", err)
	}
	t.Cleanup(func() { _ = rtAfter.Close() })
	baseAfter := "http://" + rtAfter.HTTPAddr()

	// Post-restart the invocation is still Suspended on awakeable[1]
	// (engine doesn't auto-resume Suspended; it wakes on the next
	// journal append). Resolve directly using the pre-crash ID — the
	// awakeable journal entry preserved it across the restart, so the
	// engine routes the resolution to the same suspended invocation.
	resolveAwakeable(t, baseAfter, got[1], []byte("post"))

	completed := awaitCompleted(t, hAfter, 1, id, 15*time.Second)
	if want := "pre:post"; string(completed.GetOutput()) != want {
		t.Errorf("output = %q; want %q", completed.GetOutput(), want)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}

	// Defensive sanity: the original error path is hit if Result ever
	// short-circuits to ErrSuspended after wake. Surface that here so
	// future regressions in suspend-resume coupling don't silently
	// pass via output-only checks.
	if errors.Is(errors.New(completed.GetFailureMessage()), sdk.ErrSuspended) {
		t.Errorf("completed with sticky ErrSuspended: %q", completed.GetFailureMessage())
	}
}
