package auth

import (
	"context"
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestTenantOIDCReconciler_PrevEntryReusedWhenConfigUnchanged: when a
// reconcile pass sees the same tenant+issuer config it produced last
// time, the verifier snapshot keeps the same *issuerEntry pointer.
// This is the property that lets the lazily-discovered
// *oidc.IDTokenVerifier survive across passes — without it, every
// reconcile would rebuild the entry and force IdP rediscovery on the
// next request.
func TestTenantOIDCReconciler_PrevEntryReusedWhenConfigUnchanged(t *testing.T) {
	ctx := context.Background()
	v, err := NewJWTVerifier(ctx, nil, nil)
	if err != nil {
		t.Fatalf("NewJWTVerifier: %v", err)
	}
	r := NewTenantOIDCReconciler(v, nil, nil)

	tenant := &enginev1.TenantRecord{
		Id:   7,
		Name: "acme",
		OidcIssuers: []*enginev1.OIDCIssuerConfig{{
			IssuerUrl: "https://idp.example.com/acme",
			Audiences: []string{"reflow"},
		}},
	}
	r.Reconcile(ctx, []*enginev1.TenantRecord{tenant})

	snap1 := v.snapshot()
	entry1 := snap1["https://idp.example.com/acme"]
	if entry1 == nil {
		t.Fatal("entry1 missing after first reconcile")
	}
	if entry1.tenantID != 7 {
		t.Errorf("entry1.tenantID=%d; want 7", entry1.tenantID)
	}

	// Reconcile again with byte-identical input. The entry pointer
	// must be preserved.
	r.Reconcile(ctx, []*enginev1.TenantRecord{tenant})
	snap2 := v.snapshot()
	entry2 := snap2["https://idp.example.com/acme"]
	if entry2 != entry1 {
		t.Errorf("entry pointer changed across identical reconcile passes (got %p, want %p)", entry2, entry1)
	}
}

// TestTenantOIDCReconciler_NewEntryWhenConfigChanges: when the
// underlying OIDCIssuerConfig changes (e.g. audiences rotated), the
// reconciler must NOT reuse the prior entry — the stale cached
// verifier would have a wrong-audience clock.
func TestTenantOIDCReconciler_NewEntryWhenConfigChanges(t *testing.T) {
	ctx := context.Background()
	v, _ := NewJWTVerifier(ctx, nil, nil)
	r := NewTenantOIDCReconciler(v, nil, nil)

	mkTenant := func(audiences []string) *enginev1.TenantRecord {
		return &enginev1.TenantRecord{
			Id:   7,
			Name: "acme",
			OidcIssuers: []*enginev1.OIDCIssuerConfig{{
				IssuerUrl: "https://idp.example.com/acme",
				Audiences: audiences,
			}},
		}
	}

	r.Reconcile(ctx, []*enginev1.TenantRecord{mkTenant([]string{"reflow"})})
	entry1 := v.snapshot()["https://idp.example.com/acme"]
	if entry1 == nil {
		t.Fatal("entry missing")
	}

	r.Reconcile(ctx, []*enginev1.TenantRecord{mkTenant([]string{"reflow", "internal"})})
	entry2 := v.snapshot()["https://idp.example.com/acme"]
	if entry2 == entry1 {
		t.Error("entry pointer reused despite audiences change — stale cached verifier risk")
	}
	if len(entry2.cfg.Audiences) != 2 {
		t.Errorf("entry2 audiences=%v; want 2 entries", entry2.cfg.Audiences)
	}
}

// TestTenantOIDCReconciler_PerTenantOverridesClusterDefault: when a
// tenant registers the same IssuerURL as a cluster-default entry, the
// per-tenant entry wins and the snapshot entry is tenant-bound.
func TestTenantOIDCReconciler_PerTenantOverridesClusterDefault(t *testing.T) {
	ctx := context.Background()
	v, _ := NewJWTVerifier(ctx, nil, nil)
	clusterDefaults := []OIDCIssuerConfig{{
		IssuerURL: "https://shared.example.com",
		Audiences: []string{"reflow"},
	}}
	r := NewTenantOIDCReconciler(v, clusterDefaults, nil)

	tenant := &enginev1.TenantRecord{
		Id:   42,
		Name: "acme",
		OidcIssuers: []*enginev1.OIDCIssuerConfig{{
			IssuerUrl: "https://shared.example.com",
			Audiences: []string{"reflow"},
		}},
	}
	r.Reconcile(ctx, []*enginev1.TenantRecord{tenant})

	entry := v.snapshot()["https://shared.example.com"]
	if entry == nil {
		t.Fatal("entry missing")
	}
	if entry.tenantID != 42 {
		t.Errorf("entry.tenantID=%d; want 42 (per-tenant should override cluster default)", entry.tenantID)
	}
}

// TestTenantOIDCReconciler_PerTenantCollisionDropsLoser: two tenants
// registering the same IssuerURL — lower tenant id wins (sorted
// ascending so every node lands on the same winner), the loser is
// dropped from the snapshot.
func TestTenantOIDCReconciler_PerTenantCollisionDropsLoser(t *testing.T) {
	ctx := context.Background()
	v, _ := NewJWTVerifier(ctx, nil, nil)
	r := NewTenantOIDCReconciler(v, nil, nil)

	// Submit in reverse order to confirm reconciler does its own sort.
	tenants := []*enginev1.TenantRecord{
		{
			Id: 99, Name: "later",
			OidcIssuers: []*enginev1.OIDCIssuerConfig{{
				IssuerUrl: "https://idp.example.com",
				Audiences: []string{"reflow"},
			}},
		},
		{
			Id: 5, Name: "earlier",
			OidcIssuers: []*enginev1.OIDCIssuerConfig{{
				IssuerUrl: "https://idp.example.com",
				Audiences: []string{"reflow"},
			}},
		},
	}
	r.Reconcile(ctx, tenants)

	entry := v.snapshot()["https://idp.example.com"]
	if entry == nil {
		t.Fatal("entry missing")
	}
	if entry.tenantID != 5 {
		t.Errorf("entry.tenantID=%d; want 5 (lowest tenant id wins)", entry.tenantID)
	}
}

// TestTenantOIDCReconciler_RemovedTenantDropsEntries: a tenant
// disappearing from the desired-state list removes its issuer entries
// from the next snapshot.
func TestTenantOIDCReconciler_RemovedTenantDropsEntries(t *testing.T) {
	ctx := context.Background()
	v, _ := NewJWTVerifier(ctx, nil, nil)
	r := NewTenantOIDCReconciler(v, nil, nil)

	tenantA := &enginev1.TenantRecord{
		Id: 1, Name: "a",
		OidcIssuers: []*enginev1.OIDCIssuerConfig{{
			IssuerUrl: "https://a.example.com",
			Audiences: []string{"reflow"},
		}},
	}
	tenantB := &enginev1.TenantRecord{
		Id: 2, Name: "b",
		OidcIssuers: []*enginev1.OIDCIssuerConfig{{
			IssuerUrl: "https://b.example.com",
			Audiences: []string{"reflow"},
		}},
	}

	r.Reconcile(ctx, []*enginev1.TenantRecord{tenantA, tenantB})
	if got := len(v.snapshot()); got != 2 {
		t.Fatalf("snapshot size after seed=%d; want 2", got)
	}

	// Remove tenantB.
	r.Reconcile(ctx, []*enginev1.TenantRecord{tenantA})
	snap := v.snapshot()
	if _, ok := snap["https://b.example.com"]; ok {
		t.Error("tenantB's entry persisted after removal")
	}
	if _, ok := snap["https://a.example.com"]; !ok {
		t.Error("tenantA's entry was lost across reconcile")
	}
}

// TestTenantOIDCReconciler_NilVerifierTolerated: NewTenantOIDCReconciler
// must accept a nil verifier (the SPIFFE-only deployment case) and
// have its loop / Reconcile become a no-op.
func TestTenantOIDCReconciler_NilVerifierTolerated(t *testing.T) {
	r := NewTenantOIDCReconciler(nil, nil, nil)
	r.Reconcile(context.Background(), []*enginev1.TenantRecord{{Id: 1, Name: "x"}})
	// RunReconciler should return nil immediately when verifier is nil.
	ctx := t.Context()
	if err := r.RunReconciler(ctx, nil, nil); err != nil {
		t.Errorf("RunReconciler with nil verifier returned %v; want nil", err)
	}
}

// TestTenantOIDCReconciler_SkipsZeroIdTenant: tenant_id=0 is the
// default-tenant sentinel; the reconciler must skip OIDC entries on
// such rows (they'd never be bound to a real tenant).
func TestTenantOIDCReconciler_SkipsZeroIdTenant(t *testing.T) {
	ctx := context.Background()
	v, _ := NewJWTVerifier(ctx, nil, nil)
	r := NewTenantOIDCReconciler(v, nil, nil)

	rec := &enginev1.TenantRecord{
		Id: 0, Name: "should-not-happen",
		OidcIssuers: []*enginev1.OIDCIssuerConfig{{
			IssuerUrl: "https://idp.example.com",
			Audiences: []string{"reflow"},
		}},
	}
	r.Reconcile(ctx, []*enginev1.TenantRecord{rec})
	if got := len(v.snapshot()); got != 0 {
		t.Errorf("zero-id tenant produced %d snapshot entries; want 0", got)
	}
}

// TestTenantOIDCReconciler_SkipsEmptyAudiences: an issuer without any
// audiences is invalid (NewJWTVerifier would reject it at startup).
// The reconciler logs + skips rather than aborting the whole pass.
func TestTenantOIDCReconciler_SkipsEmptyAudiences(t *testing.T) {
	ctx := context.Background()
	v, _ := NewJWTVerifier(ctx, nil, nil)
	r := NewTenantOIDCReconciler(v, nil, nil)

	rec := &enginev1.TenantRecord{
		Id: 7, Name: "acme",
		OidcIssuers: []*enginev1.OIDCIssuerConfig{
			{
				IssuerUrl: "https://bad.example.com",
				Audiences: nil, // invalid
			},
			{
				IssuerUrl: "https://good.example.com",
				Audiences: []string{"reflow"},
			},
		},
	}
	r.Reconcile(ctx, []*enginev1.TenantRecord{rec})
	snap := v.snapshot()
	if _, ok := snap["https://bad.example.com"]; ok {
		t.Error("issuer with empty audiences was added")
	}
	if _, ok := snap["https://good.example.com"]; !ok {
		t.Error("good issuer on the same tenant was dropped")
	}
}
