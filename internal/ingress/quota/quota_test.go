package quota

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// fakeReader returns a fixed snapshot.
type fakeReader struct {
	tenants []*enginev1.TenantRecord
	rev     uint64
	err     error
}

func (r *fakeReader) ListTenants(_ context.Context) ([]*enginev1.TenantRecord, uint64, error) {
	if r.err != nil {
		return nil, 0, r.err
	}
	return r.tenants, r.rev, nil
}

// fakeCounter satisfies PartitionCounter, returning per-(shard, tenant)
// in-flight totals from a map. Calls are counted so we can assert the
// reconciler fanned out correctly.
type fakeCounter struct {
	mu       sync.Mutex
	numShard uint64
	counts   map[uint64]map[uint32]int64 // shardID → tenantID → count
	calls    int
	err      error
}

func (c *fakeCounter) NumPartitionShards() uint64 { return c.numShard }

func (c *fakeCounter) LookupActiveInvocationCount(_ context.Context, shardID uint64, tenantID uint32) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.err != nil {
		return 0, c.err
	}
	if m, ok := c.counts[shardID]; ok {
		return m[tenantID], nil
	}
	return 0, nil
}

func newReader(tenants ...*enginev1.TenantRecord) *fakeReader {
	return &fakeReader{tenants: tenants, rev: 1}
}

func tenant(id uint32, max uint32) *enginev1.TenantRecord {
	return &enginev1.TenantRecord{Id: id, Name: "t", MaxConcurrentInvocations: max}
}

// TestEnforce_AdmitsUpToLimit drives the gate up to the configured
// ceiling and asserts every admit succeeds.
func TestEnforce_AdmitsUpToLimit(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 3)))

	for i := range 3 {
		if err := m.Enforce(context.Background(), 7); err != nil {
			t.Fatalf("Enforce[%d]=%v; want nil", i, err)
		}
	}
}

// TestEnforce_RejectsOverLimit asserts the N+1th submission within the
// over-admit window returns CodeResourceExhausted (reason=over_limit).
func TestEnforce_RejectsOverLimit(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 2)))

	if err := m.Enforce(context.Background(), 7); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	if err := m.Enforce(context.Background(), 7); err != nil {
		t.Fatalf("second admit: %v", err)
	}
	err := m.Enforce(context.Background(), 7)
	if err == nil {
		t.Fatal("third admit should have been rejected")
	}
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		t.Fatalf("rejection should be *connect.Error; got %T", err)
	}
	if cerr.Code() != connect.CodeResourceExhausted {
		t.Errorf("code=%s; want ResourceExhausted", cerr.Code())
	}
}

// TestEnforce_HardCapAt2xLimit asserts the over-admit backstop fires
// at exactly 2 × limit + 1 attempts. Local Enforce calls without
// reconciler intervention should max out at 2× and then hard-reject.
func TestEnforce_HardCapAt2xLimit(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 5)))

	// First 5 admit (in the limit).
	// Next 5 also admit because the counter is < hard cap 10, but
	// they don't pass the over_limit gate — wait actually they DO
	// get rejected by over_limit. The hard-cap branch only fires
	// when n > 2*limit. So sequentially calling Enforce: at n=6
	// we hit over_limit and counter is rolled back, n stays at 5.
	// Hard cap only matters when concurrent callers push n past 2×
	// before any decrement. Simulate that by direct Add.
	for i := range 5 {
		if err := m.Enforce(context.Background(), 7); err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
	}
	// Inject a flood of in-flight bumps to push past the over_limit
	// threshold (simulates N ingress nodes all admitting).
	c := m.getOrCreateCounter(7)
	c.Add(5) // now at 10 — exactly at 2× limit but not over.
	// Next Enforce: counter becomes 11 (> 10 = hard cap) → hard_cap reject.
	err := m.Enforce(context.Background(), 7)
	if err == nil {
		t.Fatal("expected hard-cap reject above 2×limit")
	}
	// And the counter should have rolled back to 10 (not stuck at 11).
	if got := c.Load(); got != 10 {
		t.Errorf("counter after hard-cap reject=%d; want 10", got)
	}
}

// TestEnforce_TenantZeroAlwaysAdmitted asserts the default-tenant
// sentinel is never gated.
func TestEnforce_TenantZeroAlwaysAdmitted(t *testing.T) {
	m := New(nil, nil)
	for i := range 1000 {
		if err := m.Enforce(context.Background(), 0); err != nil {
			t.Fatalf("tenant 0 admit %d: %v", i, err)
		}
	}
}

// TestEnforce_UnknownTenantAdmitted asserts tenants without a configured
// limit (limits-snapshot miss) are admitted. Models the bootstrap race
// where a freshly-created tenant submits before the reconciler has
// caught the TenantTable revision.
func TestEnforce_UnknownTenantAdmitted(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 1)))

	// Tenant 42 isn't in the snapshot — admit.
	for i := range 10 {
		if err := m.Enforce(context.Background(), 42); err != nil {
			t.Fatalf("unknown-tenant admit %d: %v", i, err)
		}
	}
}

// TestEnforce_ZeroLimitMeansUnlimited asserts the proto convention
// (max_concurrent_invocations=0 → unlimited).
func TestEnforce_ZeroLimitMeansUnlimited(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 0)))

	for i := range 100 {
		if err := m.Enforce(context.Background(), 7); err != nil {
			t.Fatalf("zero-limit admit %d: %v", i, err)
		}
	}
}

// TestDecrement_ReopenSlot asserts a Decrement after a successful
// Enforce makes room for another admit.
func TestDecrement_ReopenSlot(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 1)))

	if err := m.Enforce(context.Background(), 7); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	// Second admit should reject.
	if err := m.Enforce(context.Background(), 7); err == nil {
		t.Fatal("second admit should reject")
	}
	m.Decrement(7)
	// Now there is room again.
	if err := m.Enforce(context.Background(), 7); err != nil {
		t.Fatalf("post-decrement admit: %v", err)
	}
}

// TestDecrement_NegativeIsClampedToZero asserts a Decrement on a fresh
// (zero) counter clamps to zero rather than going negative.
func TestDecrement_NegativeIsClampedToZero(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 10)))

	// Touch the counter so it exists in the snapshot.
	_ = m.Enforce(context.Background(), 7)
	m.Decrement(7) // back to 0
	m.Decrement(7) // would go to -1; should clamp.
	if got := m.getOrCreateCounter(7).Load(); got < 0 {
		t.Errorf("counter=%d; want >= 0", got)
	}
}

// TestRefreshCounters_OverwritesLocalCount asserts the reconciler
// stores the authoritative SyncRead'd sum, overwriting any locally
// inflated Enforce counter.
func TestRefreshCounters_OverwritesLocalCount(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 100)))

	// Locally Add 50.
	c := m.getOrCreateCounter(7)
	c.Store(50)

	// Reconciler reports authoritative count = 3 across 2 shards.
	counter := &fakeCounter{
		numShard: 2,
		counts: map[uint64]map[uint32]int64{
			1: {7: 2},
			2: {7: 1},
		},
	}
	m.refreshCounters(context.Background(), counter)

	got := m.getOrCreateCounter(7).Load()
	if got != 3 {
		t.Errorf("counter after refresh=%d; want 3 (authoritative)", got)
	}
}

// TestRefreshCounters_PreservesPointer asserts in-flight Enforce/
// Decrement Adds keep targeting the same *atomic.Int64 even after a
// reconcile pass — the map *shape* may change but existing pointers
// are reused so the hot path doesn't lose track.
func TestRefreshCounters_PreservesPointer(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 100)))

	cBefore := m.getOrCreateCounter(7)
	cBefore.Store(5)

	counter := &fakeCounter{
		numShard: 1,
		counts:   map[uint64]map[uint32]int64{1: {7: 2}},
	}
	m.refreshCounters(context.Background(), counter)

	cAfter := m.getOrCreateCounter(7)
	if cAfter != cBefore {
		t.Errorf("counter pointer for tenant 7 changed across reconcile: before=%p after=%p", cBefore, cAfter)
	}
	if cAfter.Load() != 2 {
		t.Errorf("counter value=%d; want 2 (authoritative)", cAfter.Load())
	}
}

// TestRefreshCounters_NewTenantGetsFreshCounter asserts a tenant
// added to the limits snapshot since the last reconcile gets its
// authoritative count materialized as a new counter.
func TestRefreshCounters_NewTenantGetsFreshCounter(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 100), tenant(8, 100)))

	counter := &fakeCounter{
		numShard: 1,
		counts:   map[uint64]map[uint32]int64{1: {7: 4, 8: 9}},
	}
	m.refreshCounters(context.Background(), counter)

	if got := m.getOrCreateCounter(8).Load(); got != 9 {
		t.Errorf("new tenant 8 counter=%d; want 9", got)
	}
}

// TestRefreshLimits_AtomicSwap asserts a config change atomically
// retargets the limit a concurrent Enforce reads against.
func TestRefreshLimits_AtomicSwap(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(tenant(7, 100)))
	limit, _ := (*m.limits.Load())[7]
	if limit != 100 {
		t.Fatalf("initial limit=%d", limit)
	}
	m.refreshLimits(context.Background(), newReader(tenant(7, 5)))
	limit, _ = (*m.limits.Load())[7]
	if limit != 5 {
		t.Errorf("after refresh, limit=%d; want 5", limit)
	}
}

// TestRunReconciler_WakeOnNotifier proves the loop reacts to a signal
// on sub by re-reading the TenantTable.
func TestRunReconciler_WakeOnNotifier(t *testing.T) {
	m := New(nil, nil)
	ctx := t.Context()

	reader := &recordingReader{}
	sub := make(chan struct{}, 1)

	go func() {
		_ = m.RunReconciler(ctx, sub, reader, nil)
	}()

	// Initial pass.
	waitFor(t, func() bool { return reader.calls.Load() >= 1 })

	// Bump once; another pass should fire.
	sub <- struct{}{}
	waitFor(t, func() bool { return reader.calls.Load() >= 2 })
}

// TestRunReconciler_ContextCancelExits proves the loop exits cleanly
// when ctx is cancelled.
func TestRunReconciler_ContextCancelExits(t *testing.T) {
	m := New(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())

	reader := &recordingReader{}
	done := make(chan error, 1)
	go func() {
		done <- m.RunReconciler(ctx, nil, reader, nil)
	}()

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("RunReconciler exit err=%v; want Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunReconciler did not exit after cancel")
	}
}

// TestRunReconciler_NilManagerSafe asserts the nil-receiver guards
// don't panic.
func TestRunReconciler_NilManagerSafe(t *testing.T) {
	var m *Manager
	if err := m.Enforce(context.Background(), 1); err != nil {
		t.Errorf("nil Enforce returned err: %v", err)
	}
	m.Decrement(1)
	if err := m.RunReconciler(context.Background(), nil, nil, nil); err != nil {
		t.Errorf("nil RunReconciler returned err: %v", err)
	}
}

// TestRefreshLimits_SkipsZeroIDTenant asserts the default-tenant
// sentinel cannot leak into the limits snapshot (the FSM rejects
// id=0 on Put, but defense in depth here too).
func TestRefreshLimits_SkipsZeroIDTenant(t *testing.T) {
	m := New(nil, nil)
	m.refreshLimits(context.Background(), newReader(
		&enginev1.TenantRecord{Id: 0, MaxConcurrentInvocations: 5},
		tenant(7, 10),
	))
	if _, ok := (*m.limits.Load())[0]; ok {
		t.Error("zero-id tenant must not appear in limits snapshot")
	}
	if (*m.limits.Load())[7] != 10 {
		t.Error("real tenant 7 was dropped alongside zero-id row")
	}
}

// recordingReader counts the number of ListTenants calls.
type recordingReader struct {
	calls atomic.Int64
}

func (r *recordingReader) ListTenants(_ context.Context) ([]*enginev1.TenantRecord, uint64, error) {
	r.calls.Add(1)
	return nil, 1, nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
