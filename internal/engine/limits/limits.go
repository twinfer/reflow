// Package limits holds engine-wide resource caps that both the apply
// path and the leader-side invoker need to consult. Lives as a
// sibling subpackage so internal/engine and internal/engine/invoker
// can both depend on it without creating an engine↔invoker cycle.
package limits

import enginev1 "github.com/twinfer/reflow/proto/enginev1"

// Step budget defaults. Each journal entry counts as one step: JEInput,
// every command, each result notification, and JEOutput. A pathological
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
