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
// counters live under `reflow_webhook_*`. Secret resolution metrics
// live in internal/secretstore as of PR4 (single-variant SecretRef).
type Metrics struct {
	// Shared with eventsource: reconcile error counter labelled by
	// table+key (key="*" for table-level errors, key=<name> for
	// per-source); table revision gauge labelled by table.
	ReconcileErrors *prometheus.CounterVec
	TableRevision   *prometheus.GaugeVec
	// Webhook-only.
	UnresolvedSecret *prometheus.CounterVec
	DuplicatePath    *prometheus.CounterVec
	UnknownPath      prometheus.Counter
	VerifyFailed     *prometheus.CounterVec
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
		UnresolvedSecret: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_webhook_unresolved_secret_total",
			Help: "Reconcile passes where a webhook's secret_name was not yet resolved by the SecretStore (race after secret upsert, secret deleted while webhook still references it). Logged once per (name, reconcile pass).",
		}, []string{"name", "secret_name"}),
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
