package webhook

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// reconcileLabelTable is the table label every cluster-managed-config
// reconcile metric is stamped with on the webhook side. Matches
// internal/ingress/eventsource's constant so the two subsystems share
// the same `reflow_config_*` series with distinct labels.
const reconcileLabelTable = "webhooksrc"

// Metrics groups the Prometheus collectors for the webhook manager.
// Reconcile/table-revision counters are shared with eventsource via the
// `reflow_config_*` series (registerOrExisting* dedupes); webhook-only
// counters live under `reflow_webhook_*`.
type Metrics struct {
	// Shared with eventsource: reconcile error counter labelled by
	// table+key (key="*" for table-level errors, key=<name> for
	// per-source); table revision gauge labelled by table.
	ReconcileErrors *prometheus.CounterVec
	TableRevision   *prometheus.GaugeVec
	// Webhook-only.
	SecretResolveErrors *prometheus.CounterVec
	DuplicatePath       *prometheus.CounterVec
	UnknownPath         prometheus.Counter
	VerifyFailed        *prometheus.CounterVec
	// remote_encrypted SecretRef resolve path: hand-instrumented per-call
	// because tink-go's monitoring.Client registration lives in
	// tink-go/v2/internal/internalregistry and is blocked from external
	// imports. Labels:
	//   kek_scheme: URI prefix of the KEK (blobkms, aws-kms, gcp-kms, …)
	//   stage:      where in the resolve path the error occurred
	//               (parse, blob_open, blob_fetch, kms_lookup,
	//                kms_get_aead, decrypt)
	//   name:       webhook source name (bounded by configured count)
	KMSDecryptTotal   *prometheus.CounterVec
	KMSDecryptErrors  *prometheus.CounterVec
	KMSDecryptSeconds *prometheus.HistogramVec
}

// NewMetrics builds the webhook collectors. Pass nil to use the
// default registry. registerOrExisting* tolerates the shared
// reflow_config_* series being already registered by an eventsource
// Manager built earlier in the same process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	return &Metrics{
		ReconcileErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_config_reconcile_errors_total",
			Help: "Per-table cluster-managed-config reconcile failures (factory error, read error, dispatcher start error). Key=\"*\" for table-level read errors; key=<source name> for per-source errors.",
		}, []string{"table", "key"}),
		TableRevision: registerOrExistingGaugeVec(reg, prometheus.GaugeOpts{
			Name: "reflow_config_table_revision",
			Help: "Latest CAS revision observed locally by each cluster-managed config table reconciler. Bumped after each successful SyncRead.",
		}, []string{"table"}),
		SecretResolveErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_webhook_secret_resolve_errors_total",
			Help: "Webhook secret_ref resolution failures (env var unset, file unreadable). The reconciler preserves the previous resolved bytes on error rather than removing the source.",
		}, []string{"name", "source"}),
		DuplicatePath: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_webhook_duplicate_path_total",
			Help: "Webhook reconciler drops on path collision — two records claiming the same path. The deterministic winner (sorted by name, first wins) is kept; the loser increments this counter.",
		}, []string{"path"}),
		UnknownPath: registerOrExistingCounter(reg, prometheus.CounterOpts{
			Name: "reflow_webhook_unknown_path_total",
			Help: "Requests under /webhooks/ that did not match any configured path. Returned to caller as 404.",
		}),
		VerifyFailed: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_webhook_verify_failed_total",
			Help: "Per-verifier count of inbound webhook requests rejected for failed signature verification.",
		}, []string{"verifier"}),
		KMSDecryptTotal: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_webhook_kms_decrypt_total",
			Help: "Successful remote_encrypted SecretRef decrypts, labelled by KEK URI scheme.",
		}, []string{"kek_scheme"}),
		KMSDecryptErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_webhook_kms_decrypt_errors_total",
			Help: "Errors during remote_encrypted SecretRef resolution. Stage values: parse, blob_open, blob_fetch, kms_lookup, kms_get_aead, decrypt. Operator gets prev-resolved bytes preserved on error.",
		}, []string{"name", "kek_scheme", "stage"}),
		KMSDecryptSeconds: registerOrExistingHistogramVec(reg, prometheus.HistogramOpts{
			Name:    "reflow_webhook_kms_decrypt_seconds",
			Help:    "End-to-end latency of remote_encrypted SecretRef resolution: blob fetch + KMS lookup + Decrypt. Buckets cover 1ms–2s.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}, []string{"kek_scheme"}),
	}
}

// registerOrExisting* swallow prometheus.AlreadyRegisteredError and
// return the existing collector. Same shape as the eventsource helpers;
// duplicated locally so the two packages stay independently testable.
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

func registerOrExistingGaugeVec(reg prometheus.Registerer, opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(opts, labels)
	if err := reg.Register(g); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(*prometheus.GaugeVec); ok {
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
