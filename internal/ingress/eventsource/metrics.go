package eventsource

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	wmmetrics "github.com/ThreeDotsLabs/watermill/components/metrics"
)

// Metrics is the eventsource-side Prometheus collectors. Co-located with
// the package so backend additions don't churn internal/observability.
type Metrics struct {
	MessagesAcked    *prometheus.CounterVec
	MessagesNacked   *prometheus.CounterVec
	SubmitDurationMs *prometheus.HistogramVec
	wmBuilder        wmmetrics.PrometheusMetricsBuilder
}

// NewMetrics builds the per-source collectors and a Watermill metrics
// builder bound to the same Registerer. Pass nil to use the default
// registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	f := promauto.With(reg)
	return &Metrics{
		MessagesAcked: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflow_eventsource_messages_acked_total",
			Help: "Inbound broker messages that were Acked after a successful SubmitInvocation (or a terminal failure routed to DLQ).",
		}, []string{"source"}),
		MessagesNacked: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflow_eventsource_messages_nacked_total",
			Help: "Inbound broker messages that were Nacked (transient failure after retries; broker will redeliver).",
		}, []string{"source"}),
		SubmitDurationMs: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "reflow_eventsource_submit_duration_ms",
			Help:    "Latency of in-process SubmitInvocation calls from the eventsource dispatcher.",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 14),
		}, []string{"source", "outcome"}),
		wmBuilder: wmmetrics.NewPrometheusMetricsBuilder(reg, "reflow", "eventsource"),
	}
}

// decorateSubscriber wraps a Subscriber with Watermill's Prometheus
// instrumentation. Emits reflow_eventsource_subscriber_messages_received_total
// labelled by handler/subscriber/acked.
func (m *Metrics) decorateSubscriber(sub Source) (Source, error) {
	return m.wmBuilder.DecorateSubscriber(sub)
}
