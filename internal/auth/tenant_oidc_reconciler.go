package auth

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// tenantOIDCReconcileInterval is the backstop tick. The notifier wake
// from cluster.Notifiers.TenantTable is the primary signal; the
// ticker just catches a missed wake (snapshot-recovery edge case).
const tenantOIDCReconcileInterval = 5 * time.Second

// TenantReader is the seam TenantOIDCReconciler uses to fetch the
// current TenantRecord set from shard 0. Production wiring is a thin
// adapter over engine.Host.Tenants; tests can hand in a fake.
type TenantReader interface {
	ListTenants(ctx context.Context) ([]*enginev1.TenantRecord, uint64 /*tableRev*/, error)
}

// TenantOIDCReconciler keeps the JWTVerifier's byIssuer snapshot in
// sync with the union of cluster-default OIDC issuers (configured at
// startup, frozen here) and per-tenant OIDC issuers (carried on
// TenantRecord.OidcIssuers, reconciled on TenantTable notifier wake).
//
// On each reconcile pass it builds a desired-state map keyed by
// issuer URL, reuses prior *issuerEntry pointers when the underlying
// OIDCIssuerConfig + tenantID binding is byte-identical (so the
// lazily-discovered *oidc.IDTokenVerifier and backoff state survive
// across passes), and atomically swaps the verifier's live map.
//
// Collision policy: issuer URL is the unique routing key. Per-tenant
// entries win over cluster-default entries for the same URL.
// Per-tenant ↔ per-tenant collisions are dropped deterministically —
// sort tenant ids ascending, first wins, loser is logged.
type TenantOIDCReconciler struct {
	verifier        *JWTVerifier
	clusterDefaults []OIDCIssuerConfig
	log             *slog.Logger

	mu sync.Mutex // serializes Reconcile passes (notifier + ticker)
}

// NewTenantOIDCReconciler constructs the reconciler. verifier may be
// nil — in that case the reconciler is a no-op and RunReconciler
// returns immediately. clusterDefaults is the unchanging set from
// cfg.Auth.OIDC; the reconciler seeds these into every snapshot and
// folds tenant entries in on top.
func NewTenantOIDCReconciler(verifier *JWTVerifier, clusterDefaults []OIDCIssuerConfig, log *slog.Logger) *TenantOIDCReconciler {
	if log == nil {
		log = slog.Default()
	}
	defaults := make([]OIDCIssuerConfig, len(clusterDefaults))
	copy(defaults, clusterDefaults)
	return &TenantOIDCReconciler{
		verifier:        verifier,
		clusterDefaults: defaults,
		log:             log,
	}
}

// RunReconciler is the production-mode reconcile loop. Wakes on the
// TenantTable notifier (FSM post-commit Bump) or a 5s ticker,
// SyncReads the desired state, and rebuilds the verifier snapshot.
// Errors are logged; the loop keeps running until ctx is cancelled.
//
// Goroutine affinity: own dedicated goroutine. Never runs on the FSM
// apply path — the notifier wake just signals; SyncRead happens
// off-loop, in line with internal/engine/CLAUDE.md.
func (r *TenantOIDCReconciler) RunReconciler(ctx context.Context, sub <-chan struct{}, reader TenantReader) error {
	if r == nil || r.verifier == nil {
		return nil
	}
	if reader == nil {
		return errors.New("auth: tenant_oidc reader is required for reconcile loop")
	}
	ticker := time.NewTicker(tenantOIDCReconcileInterval)
	defer ticker.Stop()
	r.reconcileFromReader(ctx, reader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			r.reconcileFromReader(ctx, reader)
		case <-ticker.C:
			r.reconcileFromReader(ctx, reader)
		}
	}
}

// reconcileFromReader does one ListTenants + Reconcile pass. Errors
// are logged, never propagated — the verifier keeps serving from
// whichever snapshot was last installed.
func (r *TenantOIDCReconciler) reconcileFromReader(ctx context.Context, reader TenantReader) {
	tenants, _, err := reader.ListTenants(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.log.Warn("auth: tenant_oidc read desired state", "err", err)
		}
		return
	}
	r.Reconcile(ctx, tenants)
}

// Reconcile rebuilds the verifier's byIssuer snapshot from the
// supplied tenants + the frozen cluster defaults. Exposed for tests
// that want to drive a single pass without spinning a goroutine.
func (r *TenantOIDCReconciler) Reconcile(ctx context.Context, tenants []*enginev1.TenantRecord) {
	if r == nil || r.verifier == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	prev := r.verifier.snapshot()
	next := make(map[string]*issuerEntry, len(r.clusterDefaults)+len(tenants))

	// 1. Seed cluster defaults (tenantID = 0).
	for _, cfg := range r.clusterDefaults {
		if cfg.IssuerURL == "" {
			continue
		}
		entry := r.reuseOrBuild(ctx, prev, cfg, 0)
		if entry != nil {
			next[cfg.IssuerURL] = entry
		}
	}

	// 2. Fold per-tenant issuers in. Sort tenant ids ascending so a
	//    per-tenant↔per-tenant URL collision picks a deterministic
	//    winner across every node.
	sorted := make([]*enginev1.TenantRecord, 0, len(tenants))
	for _, t := range tenants {
		if t != nil && t.GetId() != 0 {
			sorted = append(sorted, t)
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].GetId() < sorted[j].GetId() })

	for _, t := range sorted {
		tid := t.GetId()
		for _, pcfg := range t.GetOidcIssuers() {
			cfg := ProtoToOIDCIssuerConfig(pcfg)
			if cfg.IssuerURL == "" {
				continue
			}
			if len(cfg.Audiences) == 0 {
				r.log.Warn("auth: tenant_oidc skipping issuer without audiences",
					"tenant_id", tid, "issuer", cfg.IssuerURL)
				continue
			}
			// Collision check against what's already in `next`:
			//  - cluster default with same URL: per-tenant wins
			//    (drop the cluster entry by overwriting).
			//  - prior tenant with same URL: lower tenant id wins (we
			//    sorted ascending, so the incumbent is the winner).
			if existing, taken := next[cfg.IssuerURL]; taken {
				if existing.tenantID == 0 {
					r.log.Info("auth: tenant_oidc per-tenant issuer overrides cluster default",
						"tenant_id", tid, "issuer", cfg.IssuerURL)
				} else {
					r.log.Warn("auth: tenant_oidc issuer URL collision; dropping later tenant",
						"winner_tenant_id", existing.tenantID,
						"loser_tenant_id", tid,
						"issuer", cfg.IssuerURL)
					continue
				}
			}
			entry := r.reuseOrBuild(ctx, prev, cfg, tid)
			if entry != nil {
				next[cfg.IssuerURL] = entry
			}
		}
	}

	r.verifier.replaceSnapshot(next)
}

// reuseOrBuild returns the prior entry for issuerURL when its cfg +
// tenantID match the desired ones (cached verifier survives), or
// builds a fresh entry otherwise. JWKSFile / EagerDiscovery failures
// at build time are logged + the slot is dropped (the next reconcile
// pass retries).
func (r *TenantOIDCReconciler) reuseOrBuild(ctx context.Context, prev map[string]*issuerEntry, cfg OIDCIssuerConfig, tenantID uint32) *issuerEntry {
	if prevE, ok := prev[cfg.IssuerURL]; ok && prevE.tenantID == tenantID && reflect.DeepEqual(prevE.cfg, cfg) {
		return prevE
	}
	entry, err := buildIssuerEntry(ctx, cfg, tenantID, r.log)
	if err != nil {
		r.log.Warn("auth: tenant_oidc build issuer entry failed",
			"tenant_id", tenantID, "issuer", cfg.IssuerURL, "err", err)
		return nil
	}
	return entry
}
