// Package observability holds reflw's Prometheus collectors and slog helpers.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics is the set of reflw-emitted Prometheus collectors. Each label
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
	// IngressRESTRequests counts HTTP requests served by the /v1/*
	// REST facade. Labels: route (chi pattern), method, status (2xx /
	// 4xx / 5xx). Route is the chi template, never the raw path, so
	// cardinality stays bounded.
	IngressRESTRequests *prometheus.CounterVec
	// RebalanceMode is a one-shot gauge stamped at rebalancer start.
	// 0 = off, 1 = advisory, 2 = auto. When the loop never starts
	// (Mode=off) the gauge is set to 0 so dashboards can distinguish
	// "disabled" from "missing data."
	RebalanceMode prometheus.Gauge
	// RebalanceEngaged is 1 when hysteresis is currently active
	// (skew_pct > engage and not yet < disengage), 0 otherwise.
	RebalanceEngaged prometheus.Gauge
	// RebalanceSkewPct is the mis-placement percentage (0..100) the
	// rebalancer observed on its most recent tick.
	RebalanceSkewPct prometheus.Gauge
	// RebalanceLPsPerShard is the per-shard LP count from the current
	// LPOwnersTable snapshot. Surfaces hot spots without forcing the
	// trigger metric to encode severity.
	RebalanceLPsPerShard *prometheus.GaugeVec
	// RebalancePendingTransfers is the count of non-terminal rows in
	// LPTransferTable on the most recent tick.
	RebalancePendingTransfers prometheus.Gauge
	// RebalanceDrainedShards is the count of shards currently in the
	// RebalanceDrainTable.
	RebalanceDrainedShards prometheus.Gauge
	// RebalanceDecisions counts every decision the rebalancer emits.
	// outcome ∈ {transferred, would_transfer, skipped, engaged,
	// disengaged}. reason carries the cause for skip-style outcomes
	// (skew_below_engage, cooldown, at_capacity, no_moves, no_planner).
	RebalanceDecisions *prometheus.CounterVec
	// LPTransferSSTSizeBytes is the per-SST file size observed on the
	// source side after build, before upload. Useful for sizing the
	// staging dir + Toxiproxy bandwidth budgets.
	LPTransferSSTSizeBytes prometheus.Histogram
	// LPTransferSSTUploadSeconds is the wall-clock duration of one
	// fan-out upload (source → every replica of dest_shard) for a
	// single SST. Includes replica-lookup time and parallel upload.
	LPTransferSSTUploadSeconds prometheus.Histogram
	// LPTransferSSTIngestSeconds is the wall-clock duration of one
	// pebble.DB.Ingest on the destination replica, sampled per apply.
	LPTransferSSTIngestSeconds prometheus.Histogram
	// LPTransferSSTUploadErrors counts fan-out upload failures on the
	// source side. A non-zero rate indicates either a replica is
	// unreachable, a sha256/size mismatch fired, or the per-replica
	// UploadTimeout (default 10m) elapsed.
	LPTransferSSTUploadErrors prometheus.Counter

	// PebbleCompactions counts completed Pebble compactions across every
	// shard DB on this node. Populated by NewPebbleEventListener.
	PebbleCompactions prometheus.Counter
	// PebbleFlushes counts completed memtable flushes across every shard
	// DB on this node.
	PebbleFlushes prometheus.Counter
	// PebbleWriteStalls counts write-stall onsets (memtable / L0
	// back-pressure). A climbing rate means flushes or compactions are
	// not keeping up with the apply path.
	PebbleWriteStalls prometheus.Counter
	// PebbleDiskSlow counts slow-disk events from Pebble's health checks
	// (a write/sync exceeding Pebble's internal slow threshold). The
	// disk-stall fatal (see NewPebbleEventListener) fires off the same
	// events when one crosses the configured max-sync duration.
	PebbleDiskSlow prometheus.Counter
}

// NewMetrics builds reflw's collectors. Pass nil to use the default
// registry.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	f := promauto.With(reg)
	return &Metrics{
		ApplyTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflw_partition_apply_total",
			Help: "Number of Raft commands applied by the partition state machine.",
		}, []string{"kind", "is_leader"}),
		ApplyDurationMs: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "reflw_partition_apply_duration_ms",
			Help:    "Latency of partition apply per command, in milliseconds.",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 14),
		}, []string{"kind"}),
		JournalAppended: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflw_partition_journal_appended_total",
			Help: "Number of journal entries written by the partition state machine.",
		}, []string{"entry"}),
		TimerFired: f.NewCounter(prometheus.CounterOpts{
			Name: "reflw_partition_timer_fired_total",
			Help: "Number of durable timers fired by the timer service.",
		}),
		DedupHits: f.NewCounter(prometheus.CounterOpts{
			Name: "reflw_partition_dedup_hits_total",
			Help: "Number of Raft commands skipped because they were duplicates.",
		}),
		ListenerSecurityLevel: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "reflw_listener_security_level",
			Help: "Transport security level per gRPC listener. 0=NoSecurity, 1=IntegrityOnly, 2=PrivacyAndIntegrity.",
		}, []string{"listener", "driver"}),
		InvocationsCompleted: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflw_invocations_completed_total",
			Help: "Invocations that reached the Completed status, classified by outcome (success, failure, cancelled, step_budget_exhausted).",
		}, []string{"service", "outcome"}),
		IngressRESTRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflw_ingress_rest_requests_total",
			Help: "HTTP requests served by the /v1/* REST ingress facade, labeled by chi route template, method, and status class.",
		}, []string{"route", "method", "status"}),
		RebalanceMode: f.NewGauge(prometheus.GaugeOpts{
			Name: "reflw_rebalance_mode",
			Help: "Autonomous LP rebalancer mode. 0=off, 1=advisory, 2=auto. Stamped once at start.",
		}),
		RebalanceEngaged: f.NewGauge(prometheus.GaugeOpts{
			Name: "reflw_rebalance_engaged",
			Help: "1 when the rebalancer's hysteresis band has it engaged, 0 otherwise.",
		}),
		RebalanceSkewPct: f.NewGauge(prometheus.GaugeOpts{
			Name: "reflw_rebalance_skew_pct",
			Help: "Mis-placement percentage observed by the rebalancer on the most recent tick.",
		}),
		RebalanceLPsPerShard: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "reflw_rebalance_lps_per_shard",
			Help: "Number of LPs currently owned by each partition shard.",
		}, []string{"shard"}),
		RebalancePendingTransfers: f.NewGauge(prometheus.GaugeOpts{
			Name: "reflw_rebalance_pending_transfers",
			Help: "Count of non-terminal LP transfers on the most recent rebalancer tick.",
		}),
		RebalanceDrainedShards: f.NewGauge(prometheus.GaugeOpts{
			Name: "reflw_rebalance_drained_shards",
			Help: "Count of shards in the RebalanceDrainTable on the most recent rebalancer tick.",
		}),
		RebalanceDecisions: f.NewCounterVec(prometheus.CounterOpts{
			Name: "reflw_rebalance_decisions_total",
			Help: "Rebalancer decisions, classified by outcome and reason.",
		}, []string{"outcome", "reason"}),
		LPTransferSSTSizeBytes: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "reflw_lp_transfer_sst_size_bytes",
			Help:    "Size of LP-transfer SSTs built on the source side, in bytes.",
			Buckets: prometheus.ExponentialBuckets(4096, 4, 10),
		}),
		LPTransferSSTUploadSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "reflw_lp_transfer_sst_upload_seconds",
			Help:    "Duration of one fan-out SST upload (source → every dest_shard replica), in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}),
		LPTransferSSTIngestSeconds: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "reflw_lp_transfer_sst_ingest_seconds",
			Help:    "Duration of pebble.DB.Ingest on the dest replica, in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
		LPTransferSSTUploadErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "reflw_lp_transfer_sst_upload_errors_total",
			Help: "Fan-out upload failures from the source. A non-zero rate signals an unreachable replica, an integrity mismatch, or UploadTimeout elapse.",
		}),
		PebbleCompactions: f.NewCounter(prometheus.CounterOpts{
			Name: "reflw_pebble_compactions_total",
			Help: "Completed Pebble compactions across every shard DB on this node.",
		}),
		PebbleFlushes: f.NewCounter(prometheus.CounterOpts{
			Name: "reflw_pebble_flushes_total",
			Help: "Completed Pebble memtable flushes across every shard DB on this node.",
		}),
		PebbleWriteStalls: f.NewCounter(prometheus.CounterOpts{
			Name: "reflw_pebble_write_stalls_total",
			Help: "Pebble write-stall onsets (memtable / L0 back-pressure) across every shard DB on this node.",
		}),
		PebbleDiskSlow: f.NewCounter(prometheus.CounterOpts{
			Name: "reflw_pebble_disk_slow_total",
			Help: "Slow-disk events reported by Pebble health checks across every shard DB on this node.",
		}),
	}
}
