// Package observability holds reflow's Prometheus collectors and slog helpers.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics is the set of reflow-emitted Prometheus collectors. Each label
// matches restate's labelling style (kind, is_leader) so dashboards built
// against either system are roughly comparable.
//
// Use NewMetrics to register against a Registry; the default Prometheus
// registry is used unless overridden.
type Metrics struct {
	ApplyTotal      *prometheus.CounterVec
	ApplyDurationMs *prometheus.HistogramVec
	JournalAppended *prometheus.CounterVec
	TimerFired      prometheus.Counter
	DedupHits       prometheus.Counter
}

// NewMetrics builds reflow's collectors. Pass nil to use the default
// registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	f := promauto.With(reg)
	return &Metrics{
		ApplyTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflow_partition_apply_total",
			Help: "Number of Raft commands applied by the partition state machine.",
		}, []string{"kind", "is_leader"}),
		ApplyDurationMs: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "reflow_partition_apply_duration_ms",
			Help:    "Latency of partition apply per command, in milliseconds.",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 14),
		}, []string{"kind"}),
		JournalAppended: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflow_partition_journal_appended_total",
			Help: "Number of journal entries written by the partition state machine.",
		}, []string{"entry"}),
		TimerFired: f.NewCounter(prometheus.CounterOpts{
			Name: "reflow_partition_timer_fired_total",
			Help: "Number of durable timers fired by the timer service.",
		}),
		DedupHits: f.NewCounter(prometheus.CounterOpts{
			Name: "reflow_partition_dedup_hits_total",
			Help: "Number of Raft commands skipped because they were duplicates.",
		}),
	}
}
