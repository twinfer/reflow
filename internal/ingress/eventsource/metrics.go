package eventsource

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"

	wmmetrics "github.com/ThreeDotsLabs/watermill/components/metrics"
)

// Metrics is the eventsource-side Prometheus collectors. Co-located with
// the package so backend additions don't churn internal/observability.
type Metrics struct {
	MessagesAcked    *prometheus.CounterVec
	MessagesNacked   *prometheus.CounterVec
	SubmitDurationMs *prometheus.HistogramVec
	// Reconcile observability shared with other cluster-managed config
	// tables. Labelled by table (eventsrc here, webhook in Phase C) so
	// dashboards can group across subsystems. Reconcile failures land
	// on ReconcileErrors with key=<source name> for per-source drill-in;
	// table-level read failures use key="*".
	ReconcileErrors *prometheus.CounterVec
	TableRevision   *prometheus.GaugeVec
	wmBuilder       wmmetrics.PrometheusMetricsBuilder
}

// NewMetrics builds the per-source collectors and a Watermill metrics
// builder bound to the same Registerer. Pass nil to use the default
// registry. Tolerates already-registered collectors so callers can
// construct multiple Manager instances against the same registry (the
// reflow_config_* gauges/counters are intentionally process-global —
// each Manager re-uses the same collector rather than fragmenting the
// series).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	return &Metrics{
		MessagesAcked: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_eventsource_messages_acked_total",
			Help: "Inbound broker messages that were Acked after a successful SubmitInvocation (or a terminal failure routed to DLQ).",
		}, []string{"source"}),
		MessagesNacked: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_eventsource_messages_nacked_total",
			Help: "Inbound broker messages that were Nacked (transient failure after retries; broker will redeliver).",
		}, []string{"source"}),
		SubmitDurationMs: registerOrExistingHistogramVec(reg, prometheus.HistogramOpts{
			Name:    "reflow_eventsource_submit_duration_ms",
			Help:    "Latency of in-process SubmitInvocation calls from the eventsource dispatcher.",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 14),
		}, []string{"source", "outcome"}),
		ReconcileErrors: registerOrExistingCounterVec(reg, prometheus.CounterOpts{
			Name: "reflow_config_reconcile_errors_total",
			Help: "Per-table cluster-managed-config reconcile failures (factory error, read error, dispatcher start error). Key=\"*\" for table-level read errors; key=<source name> for per-source errors.",
		}, []string{"table", "key"}),
		TableRevision: registerOrExistingGaugeVec(reg, prometheus.GaugeOpts{
			Name: "reflow_config_table_revision",
			Help: "Latest CAS revision observed locally by each cluster-managed config table reconciler. Bumped after each successful SyncRead.",
		}, []string{"table"}),
		wmBuilder: wmmetrics.NewPrometheusMetricsBuilder(reg, "reflow", "eventsource"),
	}
}

// registerOrExisting* swallow prometheus.AlreadyRegisteredError and
// return the existing collector. Lets multiple Manager constructions
// against the same registry share the same metric series — which is
// the intended behavior; we never want per-instance fragmentation of
// reflow_config_* / reflow_eventsource_*.
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

// decorateSubscriber wraps a Subscriber with Watermill's Prometheus
// instrumentation. Emits reflow_eventsource_subscriber_messages_received_total
// labelled by handler/subscriber/acked.
func (m *Metrics) decorateSubscriber(sub Source) (Source, error) {
	return m.wmBuilder.DecorateSubscriber(sub)
}
