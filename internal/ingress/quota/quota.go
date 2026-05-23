// Package quota enforces per-tenant soft caps on the number of
// in-flight (Scheduled, Invoked, Suspended) invocations at the ingress
// boundary. The Manager is the in-process Enforcer that
// internal/ingress.Server calls inline at SubmitInvocation; pkg/reflow
// constructs it and starts the reconciler goroutine.
//
// Design:
//
//   - Two atomic snapshots. `limits` (tenantID → max_concurrent) is
//     rebuilt only on TenantTable notifier wake (5s ticker backstop).
//     `counters` (tenantID → *atomic.Int64) is bumped per Enforce/
//     Decrement on the hot path; its map shape is rebuilt on the 30s
//     drift-correction tick when tenants are added/removed, but
//     existing counter *pointers* are preserved so in-flight callers
//     keep adding to the same Int64.
//
//   - Soft enforcement. Multiple ingress nodes Add to their own
//     counters independently; the reconciler queries each partition
//     for the authoritative non-terminal status count and Stores the
//     result back. Over-admission between reconciles is bounded by a
//     hard cap at 2 × limit.
//
//   - Tenant 0 is the default-tenant sentinel: always admitted, never
//     counted. Tenants without a configured limit (TenantTable miss
//     because the snapshot is cold, or the tenant was deleted but
//     in-flight calls remain) are also admitted; the reconciler
//     converges over the next two passes.
//
// Not a Connect interceptor: SubmitInvocation is called as a Go method
// from the REST facade, webhook dispatcher, and event-source
// dispatcher in addition to the Connect HTTP listener. An interceptor
// would silently fail to count those paths. The method-level Enforcer
// catches every caller.
package quota

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	connect "connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// limitRefreshInterval is the backstop tick for limit refresh from
// TenantTable. The notifier wake handles most updates; the ticker
// covers a missed Bump and a freshly-started node whose initial Sub
// has not fired yet.
const limitRefreshInterval = 5 * time.Second

// counterRefreshInterval is the drift-correction tick. The reconciler
// SyncReads LookupActiveInvocationCount on every partition and
// overwrites local counters. Trades freshness against SyncRead load:
// each pass is N_shards SyncReads per known tenant.
const counterRefreshInterval = 30 * time.Second

// overAdmitMultiplier caps the in-flight counter at multiplier × limit
// before hard-rejecting. Bounds worst-case drift between reconciler
// passes when N ingress nodes race up to `limit` simultaneously.
const overAdmitMultiplier = 2

// Enforcer is the surface ingress callers depend on. The Manager
// implements it; tests pass a no-op stub.
type Enforcer interface {
	// Enforce admits a new invocation for tenantID or returns a
	// connect.Error with CodeResourceExhausted. tenantID=0 (default
	// tenant) and tenants without a configured limit are always
	// admitted.
	Enforce(ctx context.Context, tenantID uint32) error
	// Decrement releases an in-flight slot for tenantID. No-op for
	// tenantID=0 and for tenants the snapshot has never seen.
	Decrement(tenantID uint32)
}

// NoopEnforcer admits every request without counting. Used when quota
// is structurally disabled (e.g. ingress on a node without the
// reconciler running).
type NoopEnforcer struct{}

func (NoopEnforcer) Enforce(context.Context, uint32) error { return nil }
func (NoopEnforcer) Decrement(uint32)                      {}

// TenantsReader is the seam the reconciler uses to fetch the current
// set of tenant limits. Production wiring adapts engine.Host.Tenants;
// tests pass a fake.
type TenantsReader interface {
	ListTenants(ctx context.Context) ([]*enginev1.TenantRecord, uint64, error)
}

// PartitionCounter is the seam the reconciler uses to authoritatively
// count in-flight invocations across partitions. Production wiring is
// engine.Host (NumPartitionShards + LookupActiveInvocationCount).
type PartitionCounter interface {
	NumPartitionShards() uint64
	LookupActiveInvocationCount(ctx context.Context, shardID uint64, tenantID uint32) (int64, error)
}

// Manager is the production Enforcer. Construct via New; start the
// reconciler goroutine via RunReconciler.
type Manager struct {
	metrics *Metrics
	log     *slog.Logger

	// limits is the per-tenant max-concurrent snapshot. Value 0 means
	// "unlimited" (matches TenantRecord proto convention). Rebuilt
	// only on TenantTable change; hot path Loads it per Enforce.
	limits atomic.Pointer[map[uint32]uint32]

	// counters carries per-tenant *atomic.Int64 in-flight counts.
	// The map shape changes on counter reconcile (new tenants get a
	// fresh Int64; removed tenants drop out); existing pointers are
	// preserved so in-flight hot-path Adds on those tenants land on
	// the same Int64 the reconciler then Stores into.
	counters atomic.Pointer[map[uint32]*atomic.Int64]

	// counterMu serializes reconciler-side counter map rebuilds. Hot
	// path never holds it.
	counterMu sync.Mutex
}

// New constructs a Manager with empty snapshots. RunReconciler will
// populate them on its first pass.
func New(reg prometheus.Registerer, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	m := &Manager{
		metrics: NewMetrics(reg),
		log:     log,
	}
	emptyLimits := map[uint32]uint32{}
	m.limits.Store(&emptyLimits)
	emptyCounters := map[uint32]*atomic.Int64{}
	m.counters.Store(&emptyCounters)
	return m
}

// Enforce admits or rejects a new invocation for tenantID. Returns nil
// on admit; a connect.CodeResourceExhausted error on reject.
func (m *Manager) Enforce(_ context.Context, tenantID uint32) error {
	if m == nil || tenantID == 0 {
		return nil
	}
	limits := m.limits.Load()
	if limits == nil {
		return nil
	}
	limit, ok := (*limits)[tenantID]
	if !ok || limit == 0 {
		// No limit configured (cold snapshot, deleted tenant, or
		// max_concurrent_invocations=0 meaning unlimited).
		return nil
	}
	counter := m.getOrCreateCounter(tenantID)
	hardCap := int64(limit) * overAdmitMultiplier
	n := counter.Add(1)
	if n > hardCap {
		counter.Add(-1)
		m.metrics.Rejections.WithLabelValues("hard_cap").Inc()
		return connect.NewError(connect.CodeResourceExhausted,
			fmt.Errorf("tenant %d: max concurrent invocations exceeded (in_flight=%d, limit=%d, hard_cap=%d)", tenantID, n-1, limit, hardCap))
	}
	if n > int64(limit) {
		counter.Add(-1)
		m.metrics.Rejections.WithLabelValues("over_limit").Inc()
		return connect.NewError(connect.CodeResourceExhausted,
			fmt.Errorf("tenant %d: max concurrent invocations exceeded (in_flight=%d, limit=%d)", tenantID, n-1, limit))
	}
	m.metrics.Admissions.Inc()
	return nil
}

// Decrement releases an in-flight slot. Safe to call with a tenantID
// that was never enforced (no-op).
func (m *Manager) Decrement(tenantID uint32) {
	if m == nil || tenantID == 0 {
		return
	}
	snap := m.counters.Load()
	if snap == nil {
		return
	}
	c, ok := (*snap)[tenantID]
	if !ok {
		return
	}
	if n := c.Add(-1); n < 0 {
		// Reconciler swapped in a fresh authoritative value while our
		// Decrement was in flight; clamp to floor.
		c.CompareAndSwap(n, 0)
	}
}

// getOrCreateCounter returns the *atomic.Int64 for tenantID, creating
// one if necessary. The create path is rare (first Enforce for a
// tenant) — takes counterMu to install a fresh map with the new
// pointer added, preserving every existing pointer.
func (m *Manager) getOrCreateCounter(tenantID uint32) *atomic.Int64 {
	snap := m.counters.Load()
	if snap != nil {
		if c, ok := (*snap)[tenantID]; ok {
			return c
		}
	}
	m.counterMu.Lock()
	defer m.counterMu.Unlock()
	// Re-check under lock.
	snap = m.counters.Load()
	if snap != nil {
		if c, ok := (*snap)[tenantID]; ok {
			return c
		}
	}
	next := cloneCounters(snap)
	c := &atomic.Int64{}
	next[tenantID] = c
	m.counters.Store(&next)
	return c
}

// RunReconciler is the production loop. Subscribes to the
// TenantTable notifier for limit refreshes and runs a 30s ticker for
// counter drift correction. counter may be nil — the limits side
// still runs; quota Enforce/Decrement keep functioning purely on
// local Adds (acceptable for single-node deployments where every
// counter is locally authoritative).
//
// Goroutine affinity: own dedicated goroutine, started from
// pkg/reflow.finishStartup. Never on the FSM apply path.
func (m *Manager) RunReconciler(ctx context.Context, sub <-chan struct{}, reader TenantsReader, counter PartitionCounter) error {
	if m == nil {
		return nil
	}
	if reader == nil {
		return errors.New("quota: tenants reader is required for reconcile loop")
	}
	m.refreshLimits(ctx, reader)
	if counter != nil {
		m.refreshCounters(ctx, counter)
	}
	limitTicker := time.NewTicker(limitRefreshInterval)
	defer limitTicker.Stop()
	counterTicker := time.NewTicker(counterRefreshInterval)
	defer counterTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			m.refreshLimits(ctx, reader)
		case <-limitTicker.C:
			m.refreshLimits(ctx, reader)
		case <-counterTicker.C:
			if counter != nil {
				m.refreshCounters(ctx, counter)
			}
		}
	}
}

// refreshLimits rebuilds the limits snapshot from the TenantTable.
// Atomically swaps; errors are logged + counted.
func (m *Manager) refreshLimits(ctx context.Context, reader TenantsReader) {
	tenants, rev, err := reader.ListTenants(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			m.log.Warn("quota: list tenants", "err", err)
			m.metrics.ReconcileErrors.WithLabelValues("limits").Inc()
		}
		return
	}
	next := make(map[uint32]uint32, len(tenants))
	for _, t := range tenants {
		id := t.GetId()
		if id == 0 {
			continue
		}
		next[id] = t.GetMaxConcurrentInvocations()
	}
	m.limits.Store(&next)
	m.metrics.TableRevision.Set(float64(rev))
}

// refreshCounters SyncReads LookupActiveInvocationCount on every
// partition for every known tenant and overwrites the local in-flight
// counters with the authoritative sum. Map *pointers* for existing
// tenants are preserved so in-flight Enforce / Decrement Adds keep
// targeting the same Int64 the reconciler then Stores into.
func (m *Manager) refreshCounters(ctx context.Context, counter PartitionCounter) {
	limits := m.limits.Load()
	if limits == nil || len(*limits) == 0 {
		return
	}
	n := counter.NumPartitionShards()
	if n == 0 {
		return
	}
	// Build the desired counter set. Per-tenant sum across partitions.
	authoritative := make(map[uint32]int64, len(*limits))
	for tenantID := range *limits {
		var sum int64
		var perTenantErr error
		for shardID := uint64(1); shardID <= n; shardID++ {
			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			c, err := counter.LookupActiveInvocationCount(readCtx, shardID, tenantID)
			cancel()
			if err != nil {
				perTenantErr = err
				break
			}
			sum += c
		}
		if perTenantErr != nil {
			if !errors.Is(perTenantErr, context.Canceled) {
				m.log.Warn("quota: count active invocations",
					"tenant_id", tenantID, "err", perTenantErr)
				m.metrics.ReconcileErrors.WithLabelValues("counters").Inc()
			}
			continue
		}
		authoritative[tenantID] = sum
	}
	m.counterMu.Lock()
	defer m.counterMu.Unlock()
	prev := m.counters.Load()
	next := make(map[uint32]*atomic.Int64, len(authoritative))
	for tenantID, sum := range authoritative {
		var c *atomic.Int64
		if prev != nil {
			c = (*prev)[tenantID]
		}
		if c == nil {
			c = &atomic.Int64{}
		}
		c.Store(sum)
		next[tenantID] = c
	}
	m.counters.Store(&next)
}

// cloneCounters returns a shallow copy of the counters map. Pointers
// are reused — callers mutate the map shape, not the underlying
// atomic.Int64s.
func cloneCounters(prev *map[uint32]*atomic.Int64) map[uint32]*atomic.Int64 {
	if prev == nil {
		return map[uint32]*atomic.Int64{}
	}
	out := make(map[uint32]*atomic.Int64, len(*prev)+1)
	maps.Copy(out, *prev)
	return out
}
