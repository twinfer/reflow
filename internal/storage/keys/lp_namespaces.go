package keys

// LPNamespace describes one LP-prefixed namespace. The complete set is
// AllLPNamespaces — the single source of truth that LP-transfer
// (source-side SST build and source-side range-delete) iterates
// instead of hand-listing prefix builders. Adding a new LP-prefixed
// namespace means appending one entry here; both consumers pick it
// up automatically.
//
// Name is a stable lower-case identifier. LP-transfer uses it as the
// on-disk SST filename (Name + ".sst"); TransferSSTRef.relative_path
// is wire-visible, so Name MUST NOT change for an existing entry.
type LPNamespace struct {
	Name   string
	Prefix func(lp uint32) []byte
}

// AllLPNamespaces is the complete set of LP-prefixed namespaces.
// buildLPSSTs (source-side scan) and onFinishLPTransfer's
// range-delete loop (source-side cleanup) both iterate this slice; a
// per-namespace round-trip test asserts every entry survives a
// source→dest SST shipment. The AST drift test in this package's
// _test.go asserts every `func F(lp uint32) []byte` LP-prefix builder
// in keys/ is registered here.
//
// Order is wire-visible: TransferSSTRefs are emitted in iteration
// order so the upload + propose path stamps a deterministic byte
// sequence across retries (dedup is stable only when the input is
// stable). Append-only — do not reorder existing entries.
var AllLPNamespaces = []LPNamespace{
	{"inv", InvocationLPPrefix},
	{"journal", JournalLPPrefix},
	{"timer_idx", TimerIdxLPPrefix},
	{"timer_lp", TimerLPPrefixForLP},
	{"state", StateLPPrefix},
	{"awakeable", AwakeableLPPrefix},
	{"keylease", KeyLeaseLPPrefix},
	{"idemp", IdempotencyLPPrefix},
	{"signal_inbox", SignalInboxLPPrefix},
	{"signal_awaiter", SignalAwaiterLPPrefix},
	{"workflow_run", WorkflowRunLPPrefix},
	{"promise", PromiseLPPrefix},
	{"promise_awaiter", PromiseAwaiterLPPrefix},
	{"dedup_arb", DedupArbitraryLPPrefix},
	{"proc", ProcessInstanceLPPrefix},
	{"proc_inbox", ProcessInboxLPPrefix},
	{"proc_hist", ProcessHistoryLPPrefix},
	{"proc_sub_idx", ProcessSubIndexLPPrefix},
	{"proc_child_idx", ProcessChildIndexLPPrefix},
	{"proc_timer_idx", ProcessTimerIndexLPPrefix},
	{"msgsub", MessageSubscriptionLPPrefix},
}
