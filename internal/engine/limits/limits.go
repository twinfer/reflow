// Package limits holds engine-wide resource caps that both the apply
// path and the leader-side invoker need to consult. Lives as a
// sibling subpackage so internal/engine and internal/engine/invoker
// can both depend on it without creating an engine↔invoker cycle.
package limits

import enginev1 "github.com/twinfer/reflw/proto/enginev1"

// Step budget defaults. Each journal entry counts as one step: JEInput,
// every command, and each result notification. A pathological
// handler that loops on ctx.Run / ctx.Sleep would otherwise grow the
// journal without bound — for keyed virtual objects this poisons the
// queue indefinitely.
//
// DefaultMaxJournalEntries is what the engine uses when a deployment
// didn't set its own cap. 10_000 covers genuinely long workflows while
// catching runaway loops fast (a Sleep-then-Run loop at 100ms intervals
// fills the budget in ~17 minutes).
//
// MaxAllowedJournalEntries is the non-configurable ceiling: an operator
// can't request more than this. Keeps a misconfig from translating into
// "effectively unbounded."
const (
	DefaultMaxJournalEntries = 10_000
	MaxAllowedJournalEntries = 100_000
)

// EffectiveMaxJournalEntries resolves the per-invocation budget from a
// DeploymentRecord. Zero/absent → DefaultMaxJournalEntries. Anything
// above the ceiling clamps to MaxAllowedJournalEntries. nil-safe so
// callers without a deployment context (single-node bootstrap, tests)
// get the engine default.
func EffectiveMaxJournalEntries(rec *enginev1.DeploymentRecord) uint32 {
	n := rec.GetMaxJournalEntries()
	if n == 0 {
		return DefaultMaxJournalEntries
	}
	if n > MaxAllowedJournalEntries {
		return MaxAllowedJournalEntries
	}
	return n
}

// DefaultWorkflowRetentionMs is how long a Completed workflow run's
// data (inv, journal, state, promise, workflow_run, signal_*) lives
// after the Completed transition before the reaper sweeps it. 7 days —
// long enough for downstream readers to fetch terminal outputs and
// entity state without flooding the partition with orphaned rows.
const DefaultWorkflowRetentionMs uint64 = 7 * 24 * 60 * 60 * 1000

// DefaultInvocationRetentionMs is how long a Completed non-workflow
// invocation's per-invocation rows (inv, journal, signal_*) live after
// the Completed transition before the reaper sweeps them. 24h — long
// enough for callers to fetch terminal output via AwaitInvocation /
// GetInvocationOutput, short enough that high-volume invocation traffic
// doesn't accumulate journals without bound. Shorter than the workflow
// window because a plain invocation carries no queryable entity state —
// only its own result, which is typically consumed promptly.
const DefaultInvocationRetentionMs uint64 = 24 * 60 * 60 * 1000

// MaxAllowedRetentionMs caps a per-deployment retention override. 365
// days — generous enough for compliance holds, low enough that an
// operator misconfig (e.g. passing nanoseconds) can't pin rows
// effectively forever. The count-cap backstop (DefaultMaxPendingReaps)
// is the second bound for bursts within the window.
const MaxAllowedRetentionMs uint64 = 365 * 24 * 60 * 60 * 1000

// EffectiveInvocationRetentionMs resolves the non-workflow retention
// window from a DeploymentRecord. Zero/absent → DefaultInvocationRetentionMs.
// Anything above the ceiling clamps to MaxAllowedRetentionMs. nil-safe.
func EffectiveInvocationRetentionMs(rec *enginev1.DeploymentRecord) uint64 {
	return clampRetention(rec.GetInvocationRetentionMs(), DefaultInvocationRetentionMs)
}

// EffectiveWorkflowRetentionMs resolves the workflow-run retention window
// from a DeploymentRecord. Zero/absent → DefaultWorkflowRetentionMs.
// Anything above the ceiling clamps to MaxAllowedRetentionMs. nil-safe.
func EffectiveWorkflowRetentionMs(rec *enginev1.DeploymentRecord) uint64 {
	return clampRetention(rec.GetWorkflowRetentionMs(), DefaultWorkflowRetentionMs)
}

func clampRetention(v, def uint64) uint64 {
	if v == 0 {
		return def
	}
	if v > MaxAllowedRetentionMs {
		return MaxAllowedRetentionMs
	}
	return v
}

// DefaultMaxPendingReaps is the per-partition backstop on how many
// Completed-but-not-yet-reaped invocations the ReapService tracks before
// it reaps the oldest early, regardless of their retention window. TTL
// alone lets a burst accumulate rows for the full window; this second
// bound (mirrors DBOS's row-count GC threshold) caps the standing
// footprint so a flood can't outrun the timer. 1_000_000 per partition is
// high enough never to fire in normal operation — it's a flood valve, not
// a routine sweep. Crossing it emits a WARN so the early reap isn't silent.
const DefaultMaxPendingReaps int = 1_000_000

// DefaultMaxProcessHistoryEvents bounds the per-instance proc_hist timeline of a
// RUNNING instance — the keep-last-N cap. Once an instance's hist_seq exceeds it,
// each append point-deletes the row that fell out of the window, so a pathological
// loop can't grow its history without bound. This is the pre-terminal bound; the
// post-terminal bound is the instance's retention window (shared with the record,
// reaped together). Mirrors DefaultMaxJournalEntries — a flood valve, generous
// enough that a normal instance keeps its whole timeline.
const DefaultMaxProcessHistoryEvents uint64 = 10_000
