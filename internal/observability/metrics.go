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
	ApplyTotal            *prometheus.CounterVec
	ApplyDurationMs       *prometheus.HistogramVec
	JournalAppended       *prometheus.CounterVec
	TimerFired            prometheus.Counter
	DedupHits             prometheus.Counter
	ListenerSecurityLevel *prometheus.GaugeVec
	// InvocationsCompleted classifies each Completed transition by
	// outcome: success / failure / cancelled / step_budget_exhausted.
	// step_budget_exhausted is the operator-visible signal that a
	// deployment is hitting MaxJournalEntries; raise the cap via
	// DeploymentRecord.max_journal_entries.
	InvocationsCompleted *prometheus.CounterVec
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
		ListenerSecurityLevel: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "reflow_listener_security_level",
			Help: "Transport security level per gRPC listener. 0=NoSecurity, 1=IntegrityOnly, 2=PrivacyAndIntegrity.",
		}, []string{"listener", "driver"}),
		InvocationsCompleted: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflow_invocations_completed_total",
			Help: "Invocations that reached the Completed status, classified by outcome (success, failure, cancelled, step_budget_exhausted).",
		}, []string{"service", "outcome"}),
	}
}
