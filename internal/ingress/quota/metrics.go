package quota

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics groups the Prometheus collectors for the quota Manager.
// Tenant id is intentionally NOT a label dimension — bounded
// cardinality requires aggregate counters. Per-tenant detail goes to
// logs.
type Metrics struct {
	// Admissions counts SubmitInvocation calls that passed the gate.
	Admissions prometheus.Counter
	// Rejections counts SubmitInvocation calls that the gate blocked.
	// reason ∈ {"over_limit", "hard_cap"} — over_limit is the
	// configured ceiling, hard_cap is the 2× over-admission backstop.
	Rejections *prometheus.CounterVec
	// ReconcileErrors counts reconciler-side faults.
	// kind ∈ {"limits", "counters"}.
	ReconcileErrors *prometheus.CounterVec
	// TableRevision tracks the latest TenantTable revision the local
	// reconciler observed via SyncRead.
	TableRevision prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	return &Metrics{
		Admissions: registerOrExistingCounter(reg, prometheus.CounterOpts{
			Name: "reflow_quota_admissions_total",
			Help: "Per-tenant ingress admissions that passed the quota gate.",
		}),
		Rejections: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_quota_rejections_total",
			Help: "Quota-gate rejections. reason=over_limit fires above the configured limit; reason=hard_cap fires above 2× the limit (over-admission backstop).",
		}, []string{"reason"}),
		ReconcileErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_quota_reconcile_errors_total",
			Help: "Quota reconciler failures. kind=limits is the TenantTable read; kind=counters is the per-tenant LookupActiveInvocationCount fan-out.",
		}, []string{"kind"}),
		TableRevision: registerOrExistingGauge(reg, prometheus.GaugeOpts{
			Name: "reflow_quota_table_revision",
			Help: "Latest CAS revision the quota reconciler observed on the TenantTable.",
		}),
	}
}

func registerOrExistingCounter(reg prometheus.Registerer, opts prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(opts)
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(prometheus.Counter); ok {
				return existing
			}
		}
	}
	return c
}

func registerOrExistingCounterVec(reg prometheus.Registerer, opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				return existing
			}
		}
	}
	return c
}

func registerOrExistingGauge(reg prometheus.Registerer, opts prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(opts)
	if err := reg.Register(g); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(prometheus.Gauge); ok {
				return existing
			}
		}
	}
	return g
}
