package engine

// Helpers and model types for the property-based state-machine test in
// pbt_test.go. Kept in a separate file so pbt_test.go stays focused on the
// rapid command surface.

import (
	"encoding/hex"
	"sort"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// idHex is the canonical key for InvocationId in model maps. Using hex of the
// 16-byte UUID is stable across goroutines and avoids the proto reflection
// hazards of using the pointer as a key.
func idHex(id *enginev1.InvocationId) string {
	return hex.EncodeToString(id.GetUuid())
}

// -----------------------------------------------------------------------------
// Step 1: pending timers + outbox capture
// -----------------------------------------------------------------------------

type timerKey struct {
	idHex    string
	sleepIdx uint32
}

type pendingTimer struct {
	id       *enginev1.InvocationId
	fireAtMs uint64
	sleepIdx uint32
}

// sortedKeys returns string map keys in deterministic order. Used so rapid's
// shrinker can reproduce traces — raw map iteration is unstable.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sortedTimerKeys returns timer keys in a deterministic order so rapid's
// shrinker can reproduce traces. Map iteration would otherwise vary.
func sortedTimerKeys(m map[timerKey]pendingTimer) []timerKey {
	keys := make([]timerKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].idHex != keys[j].idHex {
			return keys[i].idHex < keys[j].idHex
		}
		return keys[i].sleepIdx < keys[j].sleepIdx
	})
	return keys
}

// deleteTimerByIDAndFireAt mirrors the production ActDeleteTimer signal, which
// carries (ID, FireAtMs) but no SleepIdx. If multiple timers match, the lowest
// SleepIdx wins (matches the deterministic emission order in
// invoker/timer_service.go).
func deleteTimerByIDAndFireAt(m map[timerKey]pendingTimer, id *enginev1.InvocationId, fireAtMs uint64) {
	idH := idHex(id)
	var best *timerKey
	for k, v := range m {
		if k.idHex != idH || v.fireAtMs != fireAtMs {
			continue
		}
		if best == nil || k.sleepIdx < best.sleepIdx {
			kk := k
			best = &kk
		}
	}
	if best != nil {
		delete(m, *best)
	}
}

// -----------------------------------------------------------------------------
// Step 2: idempotency-key model.
// -----------------------------------------------------------------------------

type idemKey struct {
	service, handler, objectKey, key string
}

// -----------------------------------------------------------------------------
// Step 4 (forward-declared): outbox rows captured from ActDispatchOutbox.
// -----------------------------------------------------------------------------

type modelOutboxRow struct {
	srcShard uint64
	seq      uint64
	env      *enginev1.OutboxEnvelope
	routed   bool
}

// -----------------------------------------------------------------------------
// Promises + workflow runs (Step 3 of the durable-execution-go-sad plan).
// -----------------------------------------------------------------------------

// promiseKey scopes a workflow-scoped DurablePromise. Same address space the
// SUT uses for PromiseTable and PromiseAwaiterTable.
type promiseKey struct {
	service, workflowKey, name string
}

type modelPromiseState int

const (
	// promisePending is the implicit state when m.promises has no entry for
	// a key — both "absent" and "Pending row" map to it. The SUT
	// distinguishes them on disk (absent vs Pending) but the apply path
	// treats them identically, so the model collapses them.
	promisePending modelPromiseState = iota
	promiseResolved
	promiseRejected
)

type modelPromise struct {
	state          modelPromiseState
	value          []byte
	failureMessage string
}

// modelPromiseAwaiter mirrors one SUT PromiseAwaiter row: which invocation
// is blocked on this promise, and at what journal slot it logged its
// JEGetPromise (the result lands at entryIndex+1). The SUT keys rows by
// (svc, workflow_key, name, entry_index), so multiple co-pending awaits
// for the same (svc, key, name) coexist — the model mirrors that with a
// slice per promiseKey.
type modelPromiseAwaiter struct {
	ownerIDHex string
	entryIndex uint32
}

// -----------------------------------------------------------------------------
// Step 3 (forward-declared): per-key object-FSM model.
// -----------------------------------------------------------------------------

type leaseKey struct {
	service, objectKey string
}

type modelLeaseState int

const (
	leaseIdle modelLeaseState = iota
	leaseActive
)

type modelLease struct {
	state        modelLeaseState
	currentIDHex string
	queue        []string // hex idKeys, FIFO
}
