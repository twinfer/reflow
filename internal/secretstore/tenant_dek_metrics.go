package secretstore

import "github.com/prometheus/client_golang/prometheus"

// TenantDEKMetrics groups the Prometheus collectors for
// TenantDEKResolver. Mirrors *Metrics field-for-field but emits under
// reflow_tenant_dek_* names so operators can dash/alert on the two
// resolver subsystems independently. The decrypt-stage trio is shared
// with the SecretTable Resolver structurally (same labels,
// instrumented the same way) but lives in its own Prometheus series.
type TenantDEKMetrics struct {
	ReconcileErrors *prometheus.CounterVec
	TableRevision   prometheus.Gauge
	ResolveErrors   *prometheus.CounterVec
	// DecryptTotal / DecryptErrors / DecryptSeconds are passed into
	// ResolveRemoteEncrypted via shared() — that helper takes a
	// *Metrics, so we expose a thin adapter that re-emits to our
	// tenant-dek-named series.
	DecryptTotal   *prometheus.CounterVec
	DecryptErrors  *prometheus.CounterVec
	DecryptSeconds *prometheus.HistogramVec

	sharedView *Metrics
}

// NewTenantDEKMetrics builds the tenant_dek collectors. Pass nil to
// use the default registry. Tolerates re-registration in the same
// process (tests).
func NewTenantDEKMetrics(reg prometheus.Registerer) *TenantDEKMetrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &TenantDEKMetrics{
		ReconcileErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_tenant_dek_reconcile_errors_total",
			Help: "TenantDEKResolver reconciler failures. Key=\"*\" for table-level read errors.",
		}, []string{"key"}),
		TableRevision: registerOrExistingGauge(reg, prometheus.GaugeOpts{
			Name: "reflow_tenant_dek_table_revision",
			Help: "Latest CAS revision the local TenantDEKResolver observed via SyncRead.",
		}),
		ResolveErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_tenant_dek_resolve_errors_total",
			Help: "Per-source TenantDEKResolver resolution failures. The reconciler preserves the previous resolved AEAD on error rather than dropping the tenant.",
		}, []string{"source"}),
		DecryptTotal: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_tenant_dek_decrypt_total",
			Help: "Successful TenantDEKResolver remote_encrypted decrypts, labelled by KEK URI scheme.",
		}, []string{"kek_scheme"}),
		DecryptErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_tenant_dek_decrypt_errors_total",
			Help: "Errors during TenantDEKResolver remote_encrypted resolution. Stage values: parse, blob_open, blob_fetch, kms_lookup, kms_get_aead, decrypt.",
		}, []string{"kek_scheme", "stage"}),
		DecryptSeconds: registerOrExistingHistogramVec(reg, prometheus.HistogramOpts{
			Name:    "reflow_tenant_dek_decrypt_seconds",
			Help:    "End-to-end latency of TenantDEKResolver remote_encrypted resolution: blob fetch + KMS lookup + Decrypt. Buckets cover 1ms–2s.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}, []string{"kek_scheme"}),
	}
	// Pre-build a *Metrics view that aliases our collectors so the
	// shared ResolveRemoteEncrypted helper writes into our series
	// without knowing which resolver called it.
	m.sharedView = &Metrics{
		ReconcileErrors: m.ReconcileErrors,
		TableRevision:   m.TableRevision,
		ResolveErrors:   m.ResolveErrors,
		DecryptTotal:    m.DecryptTotal,
		DecryptErrors:   m.DecryptErrors,
		DecryptSeconds:  m.DecryptSeconds,
	}
	return m
}

// shared returns a *Metrics view aliasing our tenant_dek-named
// collectors. Passed to ResolveRemoteEncrypted so the decrypt-stage
// metrics show up under reflow_tenant_dek_* (not reflow_secretstore_*)
// when this resolver invokes the shared pipeline.
func (m *TenantDEKMetrics) shared() *Metrics {
	if m == nil {
		return nil
	}
	return m.sharedView
}
