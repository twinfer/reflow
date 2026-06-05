package secretstore

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics groups the Prometheus collectors for the SecretStore Resolver.
// Decrypt-path counters are hand-instrumented because tink-go's
// monitoring.Client registration lives in
// tink-go/v2/internal/internalregistry and is blocked from external
// imports.
type Metrics struct {
	// ReconcileErrors counts SyncRead failures and table-level
	// reconcile faults. Key = "*" for table-level errors; key = <name>
	// for per-record errors.
	ReconcileErrors *prometheus.CounterVec
	// TableRevision tracks the latest CAS revision the local
	// reconciler observed via SyncRead. Single-series gauge (this is
	// the only table this package watches).
	TableRevision prometheus.Gauge
	// ResolveErrors counts per-record resolution failures (every stage
	// rolled up). Per-stage detail lives on DecryptErrors.
	ResolveErrors *prometheus.CounterVec
	// DecryptTotal / DecryptErrors / DecryptSeconds are the
	// hand-instrumented Tink resolve path.
	//   kek_scheme: URI prefix of the KEK (blobkms, aws-kms, gcp-kms, …)
	//   stage:      where in the resolve path the error occurred
	//               (parse, blob_open, blob_fetch, kms_lookup,
	//                kms_get_aead, decrypt)
	// Per-name detail goes to logs to keep label cardinality bounded —
	// fleet alerting wants aggregate by stage/scheme, not per-secret.
	DecryptTotal   *prometheus.CounterVec
	DecryptErrors  *prometheus.CounterVec
	DecryptSeconds *prometheus.HistogramVec
	// CASignTotal / CASignErrors track LookupForCASigning — the
	// dedicated path the cluster CA signing key resolves through.
	// Per-event audit log entries supplement these counters.
	//   name: secret name. Cardinality is bounded by the cluster's CA
	//         row count (1 typically; ≤handful with rotation), so
	//         per-name labelling is safe here.
	CASignTotal  *prometheus.CounterVec
	CASignErrors *prometheus.CounterVec
}

// NewMetrics builds the secretstore collectors. Pass nil to use the
// default registry. registerOrExisting* tolerates re-registration in
// the same process (tests, eventsource Manager sharing the
// reflw_config_* series).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	return &Metrics{
		ReconcileErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflw_secretstore_reconcile_errors_total",
			Help: "SecretStore reconciler failures. Key=\"*\" for table-level read errors; key=<name> for per-secret errors.",
		}, []string{"key"}),
		TableRevision: registerOrExistingGauge(reg, prometheus.GaugeOpts{
			Name: "reflw_secretstore_table_revision",
			Help: "Latest CAS revision the local SecretStore reconciler observed via SyncRead.",
		}),
		ResolveErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflw_secretstore_resolve_errors_total",
			Help: "Per-source SecretStore resolution failures. The reconciler preserves the previous resolved bytes on error rather than dropping the name. Per-name detail goes to logs to keep label cardinality bounded.",
		}, []string{"source"}),
		DecryptTotal: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflw_secretstore_decrypt_total",
			Help: "Successful SecretStore remote_encrypted decrypts, labelled by KEK URI scheme.",
		}, []string{"kek_scheme"}),
		DecryptErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflw_secretstore_decrypt_errors_total",
			Help: "Errors during SecretStore remote_encrypted resolution. Stage values: parse, blob_open, blob_fetch, kms_lookup, kms_get_aead, decrypt. Per-name detail goes to logs to keep label cardinality bounded.",
		}, []string{"kek_scheme", "stage"}),
		DecryptSeconds: registerOrExistingHistogramVec(reg, prometheus.HistogramOpts{
			Name:    "reflw_secretstore_decrypt_seconds",
			Help:    "End-to-end latency of SecretStore remote_encrypted resolution: blob fetch + KMS lookup + Decrypt. Buckets cover 1ms–2s.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}, []string{"kek_scheme"}),
		CASignTotal: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflw_pki_ca_sign_total",
			Help: "Successful CA-signing-key lookups via LookupForCASigning. Each increment maps to one signing operation by certmgr.ClusterIssuer.",
		}, []string{"name"}),
		CASignErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflw_pki_ca_sign_errors_total",
			Help: "CA-signing-key lookup failures. Reasons: missing (no secret row), unresolved (resolve still pending or in error).",
		}, []string{"name", "reason"}),
	}
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

func registerOrExistingHistogramVec(reg prometheus.Registerer, opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(opts, labels)
	if err := reg.Register(h); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
				return existing
			}
		}
	}
	return h
}
