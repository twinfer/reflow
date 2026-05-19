package engine

// Property-based state-machine test for the partition FSM, using
// pgregory.net/rapid.
//
// Surfaces covered:
//   - Step 1: action-loop closure. After every Update we drain the Collector
//     and re-feed actions (ActInvoke → JEInput; ActRegisterTimer captured for
//     TimerFired; ActDispatchOutbox captured for RouteOutbox).
//   - Step 2: AwakeableResolved (with prior RegisterAwakeable), SignalDelivered,
//     idempotency-key drop path, Replay of outbox-derived envelopes.
//   - Step 3: object FSM / virtual-object queue invariants.
//   - Step 4: two Partition instances with cross-shard outbox routing and
//     ChildCall parent→child journal+outbox flow.
//   - Step 5: signal-based Cancel via the well-known __cancel__ name,
//     resolved through KeyLeaseTable → active invocation and applyTerminalCompletion.
//   - Step 6: workflow-run dedup (KIND_WORKFLOW + non-empty key — second
//     Invoke at the same (service, workflow_key) is silently dropped);
//     KIND_WORKFLOW_SHARED companions co-located on the same lease key.
//   - Step 7: workflow-scoped DurablePromise — PromiseGet (inline-resolve
//     vs PromiseAwaiter write), PromiseComplete (in-handler resolve;
//     awaiter stitched without FSM transition — see partition.go:817), PromisePeek
//     (single-slot mutate-and-reappend), PromiseCompletedExternal (ingress
//     InvokerEffect.PromiseCompleted; awaiter transitions Suspended → Invoked).
//
// The Model is a Go map keyed by InvocationId (hex). After every command we
// compare it against the SUT via Partition.Lookup. The lenient transitions in
// invocation_fsm.go (e.g. late TimerFired while Invoked) must be mirrored in
// the model or every race becomes a false positive.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

const numShards = 2

// -----------------------------------------------------------------------------
// Model
// -----------------------------------------------------------------------------

type modelStatus int

const (
	mFree modelStatus = iota
	mScheduled
	mInvoked
	mSuspended
	mCompleted
)

type modelTarget struct {
	service, handler, objectKey string
}

func (mt modelTarget) proto() *enginev1.InvocationTarget {
	return &enginev1.InvocationTarget{
		ServiceName: mt.service,
		HandlerName: mt.handler,
		ObjectKey:   mt.objectKey,
	}
}

func targetFromProto(t *enginev1.InvocationTarget) modelTarget {
	return modelTarget{
		service:   t.GetServiceName(),
		handler:   t.GetHandlerName(),
		objectKey: t.GetObjectKey(),
	}
}

type modelInv struct {
	id         *enginev1.InvocationId
	status     modelStatus
	target     modelTarget
	journalLen uint32
	output     []byte
}

// invSpec pairs an InvocationId with its target. Production guarantees
// id.PartitionKey == routing.PartitionKey(target.service, target.objectKey)
// (ingress mints it that way; mintCalleeInvocationID enforces it for child
// calls). The test pool must respect this or it lands rows on the wrong shard.
//
// kind is the protocolv1.Kind stamped onto the InvokeCommand; it's how the
// apply path tells a KIND_WORKFLOW (Run, dedup-eligible) from a
// KIND_WORKFLOW_SHARED companion or a plain KIND_SERVICE handler.
type invSpec struct {
	id   *enginev1.InvocationId
	tgt  modelTarget
	kind protocolv1.Kind
}

// targetSpec is a (target, kind) pair fed to drawSpecPool. Two specs are
// minted per entry so two distinct InvocationIds map to the same logical
// handler — necessary to exercise workflow-run dedup and idempotency races.
type targetSpec struct {
	tgt  modelTarget
	kind protocolv1.Kind
}

// -----------------------------------------------------------------------------
// Test machine
// -----------------------------------------------------------------------------

type engineMachine struct {
	t *testing.T

	// Per-shard SUT (index = shardID - 1).
	parts          [numShards]*Partition
	snaps          [numShards]*Snapshotter
	leads          [numShards]*stubLeadership
	cols           [numShards]*ActionCollector
	raftIx         [numShards]uint64
	prevEnv        [numShards]*enginev1.Envelope
	prevReplayable [numShards]bool

	nowPtr      *atomic.Uint64
	partitioner routing.Partitioner

	// Captured side-effects from the action router.
	pendingTimers map[timerKey]pendingTimer
	pendingOutbox []modelOutboxRow // dedup-by-(srcShard,seq) on append

	// Per-shard NextOutboxSeq high-water mark; Check asserts monotonic.
	prevOutboxSeq [numShards]uint64

	// Model (global across shards; classified by id.PartitionKey).
	invs            map[string]*modelInv                  // hex(uuid) -> inv
	idemPool        map[idemKey]string                    // (svc,handler,objKey,key) -> winning idHex
	awks            map[string]string                     // registered awakeable_id -> owner idHex
	leases          map[leaseKey]*modelLease              // (service,object_key) -> lease state
	workflowRuns    map[leaseKey]string                   // (service,workflow_key) -> winning idHex
	promises        map[promiseKey]*modelPromise          // (svc,key,name) -> promise value
	promiseAwaiters map[promiseKey][]*modelPromiseAwaiter // (svc,key,name) -> pending awaiters by entry_index

	// Generator pools (drawn once at init).
	specPool    []invSpec     // paired (id, target, kind) — partition_key consistent with target
	tgtPool     []modelTarget // standalone targets for ChildCall child-target draws
	wfSpecs     []invSpec     // subset of specPool with KIND_WORKFLOW or KIND_WORKFLOW_SHARED
	idemKs      []string
	awkPool     []string
	sigPool     []string
	promisePool []string
}

func (m *engineMachine) shardOf(id *enginev1.InvocationId) uint64 {
	return m.partitioner.ShardForInvocation(id)
}

func (m *engineMachine) sIdx(shard uint64) int { return int(shard) - 1 }

func (m *engineMachine) getOrCreate(id *enginev1.InvocationId) *modelInv {
	k := idHex(id)
	if v, ok := m.invs[k]; ok {
		return v
	}
	v := &modelInv{id: id, status: mFree}
	m.invs[k] = v
	return v
}

// -----------------------------------------------------------------------------
// Lifecycle
// -----------------------------------------------------------------------------

func (m *engineMachine) init(t *rapid.T) {
	m.nowPtr = &atomic.Uint64{}
	m.nowPtr.Store(1_700_000_000_000)
	m.partitioner = routing.Partitioner{NumShards: numShards}

	for s := uint64(1); s <= numShards; s++ {
		m.spinPartition(t, s, false)
	}

	m.invs = map[string]*modelInv{}
	m.idemPool = map[idemKey]string{}
	m.awks = map[string]string{}
	m.leases = map[leaseKey]*modelLease{}
	m.workflowRuns = map[leaseKey]string{}
	m.promises = map[promiseKey]*modelPromise{}
	m.promiseAwaiters = map[promiseKey][]*modelPromiseAwaiter{}
	m.pendingTimers = map[timerKey]pendingTimer{}
	m.pendingOutbox = nil
	m.tgtPool = []modelTarget{
		{service: "S", handler: "h"},
		{service: "Counter", handler: "incr", objectKey: "k1"},
	}
	// Workflow + shared specs share (service, object_key) so they exercise
	// workflow-run dedup, the same key lease, and the cross-handler promise
	// resolve path. Two ids per targetSpec — necessary for the dedup arm.
	specs := []targetSpec{
		{tgt: m.tgtPool[0], kind: protocolv1.Kind_KIND_SERVICE},
		{tgt: m.tgtPool[1], kind: protocolv1.Kind_KIND_SERVICE},
		{tgt: modelTarget{service: "Wf", handler: "run", objectKey: "ord1"}, kind: protocolv1.Kind_KIND_WORKFLOW},
		{tgt: modelTarget{service: "Wf", handler: "notify", objectKey: "ord1"}, kind: protocolv1.Kind_KIND_WORKFLOW_SHARED},
	}
	m.specPool = drawSpecPool(specs)
	for _, sp := range m.specPool {
		if sp.kind == protocolv1.Kind_KIND_WORKFLOW || sp.kind == protocolv1.Kind_KIND_WORKFLOW_SHARED {
			m.wfSpecs = append(m.wfSpecs, sp)
		}
	}
	m.idemKs = []string{"", "req-0", "req-1", "req-2"}
	m.awkPool = []string{
		"awk_aaaaaaaaaaaaaaaaaaaaaa",
		"awk_bbbbbbbbbbbbbbbbbbbbbb",
		"awk_cccccccccccccccccccccc",
		"awk_dddddddddddddddddddddd",
	}
	m.sigPool = []string{"sig-0", "sig-1", "sig-2", "sig-3"}
	m.promisePool = []string{"prom-0", "prom-1"}
}

// spinPartition (re)creates the SUT for one shard. recover=true means we
// expect a snapshot to be fed via RecoverFromSnapshot — used by Crash.
func (m *engineMachine) spinPartition(t *rapid.T, shard uint64, recover bool) {
	si := m.sIdx(shard)
	suffix := "init"
	if recover {
		suffix = "recover"
	}
	dir := filepath.Join(m.t.TempDir(), fmt.Sprintf("pbt-%s-shard%d", suffix, shard), "state")
	snap, err := NewSnapshotter(dir, func(path string) (storage.Store, error) {
		return storage.OpenPebble(path, nil)
	})
	if err != nil {
		t.Fatalf("snapshotter shard=%d: %v", shard, err)
	}
	lead := &stubLeadership{}
	lead.leader.Store(true)
	col := &ActionCollector{}

	m.snaps[si] = snap
	m.leads[si] = lead
	m.cols[si] = col
	m.parts[si] = NewPartition(shard, 1, PartitionConfig{
		Snapshotter: snap,
		Leadership:  lead,
		Collector:   col,
		Partitioner: m.partitioner,
	})
	pp := m.parts[si]
	m.t.Cleanup(func() { _ = pp.Close() })
}

// drawSpecPool builds 2 paired specs per entry, with id.PartitionKey =
// PartitionKey(target.service, target.objectKey) — matching production ingress
// behaviour and ensuring every Invoke lands on the shard that owns the target.
// The kind is stamped onto every spec so the apply path can tell a workflow
// Run from a Shared companion or a plain service handler.
func drawSpecPool(entries []targetSpec) []invSpec {
	specs := make([]invSpec, 0, len(entries)*2)
	uuidNonce := uint64(1)
	for _, e := range entries {
		pk := routing.PartitionKey(e.tgt.service, e.tgt.objectKey)
		for range 2 {
			uuid := make([]byte, 16)
			binary.BigEndian.PutUint64(uuid, uuidNonce)
			uuidNonce++
			specs = append(specs, invSpec{
				id:   &enginev1.InvocationId{PartitionKey: pk, Uuid: uuid},
				tgt:  e.tgt,
				kind: e.kind,
			})
		}
	}
	return specs
}

// applyEnvelope marshals an envelope, advances the per-shard raft index, and
// feeds it to Partition.Update. Returns the drained Collector contents.
//
// Stamps Header.CreatedAtMs from the model clock when unset so the apply
// path reads a definite "now" — production envelopes always carry it
// (stamped by the proposer) and the apply path no longer has a fallback.
func (m *engineMachine) applyEnvelope(t *rapid.T, shard uint64, env *enginev1.Envelope) []Action {
	si := m.sIdx(shard)
	m.raftIx[si]++
	if env.Header == nil {
		env.Header = &enginev1.Header{}
	}
	if env.Header.CreatedAtMs == 0 {
		env.Header.CreatedAtMs = m.nowPtr.Load()
	}
	buf, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := m.parts[si].Update([]statemachine.Entry{{Index: m.raftIx[si], Cmd: buf}}); err != nil {
		t.Fatalf("Update shard=%d: %v", shard, err)
	}
	m.prevEnv[si] = env
	m.prevReplayable[si] = env.GetHeader().GetDedup().GetArbitrary() != nil
	return m.cols[si].Drain()
}

// apply is the high-level helper: wrap cmd in an envelope, apply to shard,
// route the resulting actions.
func (m *engineMachine) apply(t *rapid.T, shard uint64, cmd *enginev1.Command) {
	actions := m.applyEnvelope(t, shard, &enginev1.Envelope{Command: cmd})
	m.routeActions(t, shard, actions)
}

// routeActions handles drained actions from the given source shard.
//
//   - ActInvoke         → re-propose JEInput (first launch only).
//   - ActRegisterTimer  → capture into pendingTimers.
//   - ActDeleteTimer    → drop matching pendingTimer.
//   - ActDispatchOutbox → capture into pendingOutbox (dedup by (srcShard,seq)
//     so leader re-emission after Crash doesn't double-count).
//   - everything else   → local-only, drop.
func (m *engineMachine) routeActions(t *rapid.T, srcShard uint64, actions []Action) {
	for _, a := range actions {
		switch v := a.(type) {
		case ActInvoke:
			m.onActInvoke(t, srcShard, v)
		case ActRegisterTimer:
			k := timerKey{idHex: idHex(v.ID), sleepIdx: v.SleepIdx}
			m.pendingTimers[k] = pendingTimer{id: v.ID, fireAtMs: v.FireAtMs, sleepIdx: v.SleepIdx}
		case ActDeleteTimer:
			deleteTimerByIDAndFireAt(m.pendingTimers, v.ID, v.FireAtMs)
		case ActDispatchOutbox:
			// Dedup: leader re-emits ActDispatchOutbox for rows still in the
			// table on recovery. Skip if we already have (srcShard, seq).
			already := false
			for _, r := range m.pendingOutbox {
				if r.srcShard == srcShard && r.seq == v.Seq {
					already = true
					break
				}
			}
			if !already {
				m.pendingOutbox = append(m.pendingOutbox, modelOutboxRow{
					srcShard: srcShard, seq: v.Seq, env: v.Envelope,
				})
			}
		default:
			_ = v
		}
	}
}

// onActInvoke proposes a JEInput at index 0 for the given invocation, mirroring
// what the production Invoker session does on first launch.
func (m *engineMachine) onActInvoke(t *rapid.T, shard uint64, a ActInvoke) {
	cur := m.getOrCreate(a.ID)
	if cur.journalLen != 0 {
		return
	}
	jApp := &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: a.ID,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: 0,
						Entry: &enginev1.JournalEntry_Input{
							Input: &enginev1.JEInput{Value: []byte("in")},
						},
					},
				},
			},
		}},
	}
	actions := m.applyEnvelope(t, shard, &enginev1.Envelope{Command: jApp})
	cur.journalLen = 1
	if cur.status == mScheduled || cur.status == mSuspended {
		cur.status = mInvoked
	}
	m.routeActions(t, shard, actions)
}

// applyInvokeToModel is the model-side counterpart of partition.onInvoke. It
// is called from the Invoke command AND from RouteOutbox when a routed envelope
// is OutboxEnvelope_Invoke (i.e. cross-shard ctx.Call materialising the child).
// Returns true if this Invoke is being dropped (idempotency or workflow-run
// dedup).
func (m *engineMachine) applyInvokeToModel(id *enginev1.InvocationId, tgt modelTarget, ik string, kind protocolv1.Kind) bool {
	if ik != "" {
		k := idemKey{tgt.service, tgt.handler, tgt.objectKey, ik}
		if _, ok := m.idemPool[k]; ok {
			return true
		}
	}
	// Workflow single-run-per-key dedup. Only KIND_WORKFLOW (Run handlers)
	// participate; KIND_WORKFLOW_SHARED companions and KIND_SERVICE invokes
	// skip this gate. Keyless workflows can't dedup since the row is
	// addressed by (service, workflow_key).
	if kind == protocolv1.Kind_KIND_WORKFLOW && tgt.objectKey != "" {
		wfk := leaseKey{tgt.service, tgt.objectKey}
		if _, ok := m.workflowRuns[wfk]; ok {
			return true
		}
	}
	cur := m.getOrCreate(id)
	freshInvoke := cur.status == mFree
	if freshInvoke {
		cur.status = mScheduled
		cur.target = tgt
	}
	if ik != "" {
		k := idemKey{tgt.service, tgt.handler, tgt.objectKey, ik}
		if _, exists := m.idemPool[k]; !exists {
			m.idemPool[k] = idHex(id)
		}
	}
	if kind == protocolv1.Kind_KIND_WORKFLOW && tgt.objectKey != "" {
		wfk := leaseKey{tgt.service, tgt.objectKey}
		if _, exists := m.workflowRuns[wfk]; !exists {
			m.workflowRuns[wfk] = idHex(id)
		}
	}
	if freshInvoke && tgt.objectKey != "" {
		lk := leaseKey{tgt.service, tgt.objectKey}
		lease := m.leases[lk]
		if lease == nil || lease.state == leaseIdle {
			m.leases[lk] = &modelLease{
				state:        leaseActive,
				currentIDHex: idHex(id),
			}
		} else if lease.currentIDHex != idHex(id) {
			lease.queue = append(lease.queue, idHex(id))
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// Commands
// -----------------------------------------------------------------------------

func (m *engineMachine) Invoke(t *rapid.T) {
	sp := rapid.SampledFrom(m.specPool).Draw(t, "spec")
	ik := rapid.SampledFrom(m.idemKs).Draw(t, "idem_key")

	m.applyInvokeToModel(sp.id, sp.tgt, ik, sp.kind)
	m.apply(t, m.shardOf(sp.id), &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId:   sp.id,
			Target:         sp.tgt.proto(),
			Input:          []byte("in"),
			IdempotencyKey: ik,
			Kind:           uint32(sp.kind),
		}},
	})
}

func (m *engineMachine) JournalAppend(t *rapid.T) {
	id := rapid.SampledFrom(m.specPool).Draw(t, "spec").id
	cur := m.getOrCreate(id)
	// Only fire for invocations the production invoker would be driving.
	// A queued (mScheduled) keyed invocation has no session, so a synthetic
	// JEInput here would bypass the object-FSM gate.
	if cur.status != mInvoked && cur.status != mSuspended {
		return
	}

	idx := cur.journalLen
	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_Input{
							Input: &enginev1.JEInput{Value: []byte("in")},
						},
					},
				},
			},
		}},
	})
	cur.journalLen = idx + 1
	if cur.status == mScheduled || cur.status == mSuspended {
		cur.status = mInvoked
	}
}

// RegisterAwakeable journals a JEAwakeable for an mInvoked invocation,
// registering an awakeable_id in the SUT's AwakeableTable.
func (m *engineMachine) RegisterAwakeable(t *rapid.T) {
	id := rapid.SampledFrom(m.specPool).Draw(t, "spec").id
	awk := rapid.SampledFrom(m.awkPool).Draw(t, "awk")
	cur := m.getOrCreate(id)
	if cur.status != mInvoked {
		return
	}
	idx := cur.journalLen
	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_Awakeable{
							Awakeable: &enginev1.JEAwakeable{AwakeableId: awk},
						},
					},
				},
			},
		}},
	})
	cur.journalLen = idx + 1
	m.awks[awk] = idHex(id)
}

// AwakeableResolved fires against a previously-registered awakeable's owner.
func (m *engineMachine) AwakeableResolved(t *rapid.T) {
	if len(m.awks) == 0 {
		return
	}
	awkIDs := sortedKeys(m.awks)
	awk := rapid.SampledFrom(awkIDs).Draw(t, "awk")
	ownerHex := m.awks[awk]
	owner := m.invs[ownerHex]
	if owner == nil || (owner.status != mSuspended && owner.status != mInvoked) {
		return
	}
	if owner.status == mSuspended {
		owner.status = mInvoked
	}
	delete(m.awks, awk)

	m.apply(t, m.shardOf(owner.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: owner.id,
			Kind: &enginev1.InvokerEffect_AwakeableResolved{
				AwakeableResolved: &enginev1.AwakeableResolved{
					AwakeableId: awk,
					Value:       []byte("v"),
				},
			},
		}},
	})
}

func (m *engineMachine) SignalDelivered(t *rapid.T) {
	spec := rapid.SampledFrom(m.specPool).Draw(t, "spec")
	sig := rapid.SampledFrom(m.sigPool).Draw(t, "sig")
	// Signals route by Target → KeyLeaseTable.current_invocation, so
	// they're only meaningful for keyed specs. Unkeyed signals are
	// dropped by the apply arm with a warning.
	if spec.tgt.objectKey == "" {
		return
	}
	id := spec.id
	cur := m.getOrCreate(id)
	if cur.status != mSuspended && cur.status != mInvoked {
		// No active lease holder, so the receiver-side KeyLeaseTable
		// lookup will miss and the signal will be dropped.
		return
	}
	if cur.status == mSuspended {
		cur.status = mInvoked
	}
	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_SignalDelivered{
				SignalDelivered: &enginev1.SignalDelivered{
					Target:     spec.tgt.proto(),
					SignalName: sig,
					Payload:    []byte("p"),
				},
			},
		}},
	})
}

func (m *engineMachine) Suspend(t *rapid.T) {
	id := rapid.SampledFrom(m.specPool).Draw(t, "spec").id
	cur := m.getOrCreate(id)
	if cur.status != mInvoked {
		return
	}
	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_Suspended{
				Suspended: &enginev1.InvocationSuspended{},
			},
		}},
	})
	cur.status = mSuspended
}

func (m *engineMachine) Complete(t *rapid.T) {
	id := rapid.SampledFrom(m.specPool).Draw(t, "spec").id
	cur := m.getOrCreate(id)
	if cur.status != mInvoked && cur.status != mSuspended {
		return
	}

	out := []byte("ok")
	tgt := cur.target

	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_Completed{
				Completed: &enginev1.InvocationCompleted{Output: out},
			},
		}},
	})
	cur.status = mCompleted
	cur.output = out

	// Object FSM: release lease, promote queue head if any.
	if tgt.objectKey != "" {
		lk := leaseKey{tgt.service, tgt.objectKey}
		lease := m.leases[lk]
		if lease != nil && lease.currentIDHex == idHex(id) {
			if len(lease.queue) == 0 {
				lease.state = leaseIdle
				lease.currentIDHex = ""
			} else {
				lease.currentIDHex = lease.queue[0]
				lease.queue = lease.queue[1:]
			}
		}
	}
}

// Cancel exercises the signal-based cancellation path: an ingress-side
// __cancel__ signal lands on the (service, key) → active invocation,
// the apply arm resolves via KeyLeaseTable and synthesizes a terminal
// Completed. Only fires for keyed targets whose current lease holder
// matches the drawn spec (other cases are dropped by the apply arm).
func (m *engineMachine) Cancel(t *rapid.T) {
	spec := rapid.SampledFrom(m.specPool).Draw(t, "spec")
	if spec.tgt.objectKey == "" {
		return // unkeyed targets aren't cancellable via signal
	}
	id := spec.id
	cur := m.getOrCreate(id)
	if cur.status != mScheduled && cur.status != mInvoked && cur.status != mSuspended {
		return
	}
	// The apply arm only acts when this id is the current lease holder.
	lk := leaseKey{spec.tgt.service, spec.tgt.objectKey}
	lease := m.leases[lk]
	if lease == nil || lease.state != leaseActive || lease.currentIDHex != idHex(id) {
		return
	}

	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_SignalDelivered{
				SignalDelivered: &enginev1.SignalDelivered{
					Target:     spec.tgt.proto(),
					SignalName: wire.WellKnownCancelSignal,
				},
			},
		}},
	})
	cur.status = mCompleted

	// Cancel runs applyTerminalCompletion, which releases the key lease.
	if len(lease.queue) == 0 {
		lease.state = leaseIdle
		lease.currentIDHex = ""
	} else {
		lease.currentIDHex = lease.queue[0]
		lease.queue = lease.queue[1:]
	}
}

// PromiseGet journals a JEGetPromise for an mInvoked/mSuspended workflow or
// shared invocation. Mirrors the JournalEntry_GetPromise apply arm: if the
// promise is already terminal the result lands inline (journalLen += 2);
// otherwise a PromiseAwaiter row is recorded (journalLen += 1) so a
// subsequent PromiseComplete or PromiseCompletedExternal can stitch the
// result. A second pending Get on the same (svc,key,name) overwrites the
// prior awaiter — that's the MVP single-awaiter limitation documented in
// the design plan.
func (m *engineMachine) PromiseGet(t *rapid.T) {
	if len(m.wfSpecs) == 0 {
		return
	}
	spec := rapid.SampledFrom(m.wfSpecs).Draw(t, "wf_spec")
	name := rapid.SampledFrom(m.promisePool).Draw(t, "promise")
	cur := m.getOrCreate(spec.id)
	if cur.status != mInvoked && cur.status != mSuspended {
		return
	}
	idx := cur.journalLen
	resultIdx := idx + 1
	pk := promiseKey{spec.tgt.service, spec.tgt.objectKey, name}

	pv := m.promises[pk]
	inline := pv != nil && pv.state != promisePending

	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_GetPromise{
							GetPromise: &enginev1.JEGetPromise{
								Name:               name,
								ResultCompletionId: resultIdx,
								Service:            spec.tgt.service,
								WorkflowKey:        spec.tgt.objectKey,
							},
						},
					},
				},
			},
		}},
	})

	if cur.status == mSuspended {
		cur.status = mInvoked
	}
	if inline {
		cur.journalLen += 2
	} else {
		cur.journalLen += 1
		m.promiseAwaiters[pk] = append(m.promiseAwaiters[pk], &modelPromiseAwaiter{
			ownerIDHex: idHex(spec.id),
			entryIndex: idx,
		})
	}
}

// PromiseComplete journals a JECompletePromise. The apply arm writes a
// PromiseValue (if not already terminal) and appends JEPromiseCompleteResult
// at result_completion_id regardless of conflict; if an awaiter is pending
// it stitches a JEPromiseResult on the awaiter's journal at
// awaiter.entry_index+1 and clears the awaiter row. Note: stitching does
// NOT transition the awaiter's FSM (the only side effect is an ActInvoke
// the invoker filters via pendingRespawn) — InvokerEffect_PromiseCompleted
// is the only path that runs transitionOnPromiseResolved.
func (m *engineMachine) PromiseComplete(t *rapid.T) {
	if len(m.wfSpecs) == 0 {
		return
	}
	spec := rapid.SampledFrom(m.wfSpecs).Draw(t, "wf_spec")
	name := rapid.SampledFrom(m.promisePool).Draw(t, "promise")
	cur := m.getOrCreate(spec.id)
	if cur.status != mInvoked && cur.status != mSuspended {
		return
	}
	idx := cur.journalLen
	resultIdx := idx + 1
	pk := promiseKey{spec.tgt.service, spec.tgt.objectKey, name}
	value := []byte("v-" + name)

	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_CompletePromise{
							CompletePromise: &enginev1.JECompletePromise{
								Name:               name,
								Value:              value,
								ResultCompletionId: resultIdx,
								Service:            spec.tgt.service,
								WorkflowKey:        spec.tgt.objectKey,
							},
						},
					},
				},
			},
		}},
	})

	if cur.status == mSuspended {
		cur.status = mInvoked
	}
	// JECompletePromise (the cmd) + JEPromiseCompleteResult (the ack) — two
	// slots always, regardless of conflict.
	cur.journalLen += 2

	pv := m.promises[pk]
	if pv == nil || pv.state == promisePending {
		m.promises[pk] = &modelPromise{state: promiseResolved, value: value}
		// Stitch every co-pending awaiter: each owner's journal grows by
		// one (the JEPromiseResult at entry_index+1). FSM stays put — the
		// resolver path wakes owners via ActInvoke + pendingRespawn, not
		// transitionOnPromiseResolved.
		for _, awaiter := range m.promiseAwaiters[pk] {
			if owner := m.invs[awaiter.ownerIDHex]; owner != nil {
				owner.journalLen++
			}
		}
		delete(m.promiseAwaiters, pk)
	}
}

// PromisePeek journals a JEPeekPromise. The apply arm mutates the entry in
// place (stamping completed/value/failure_message from the current
// PromiseValue) and re-appends it. From the model's perspective the
// invocation just bumps its journalLen by 1 — peek has no FSM or state
// side effects.
func (m *engineMachine) PromisePeek(t *rapid.T) {
	if len(m.wfSpecs) == 0 {
		return
	}
	spec := rapid.SampledFrom(m.wfSpecs).Draw(t, "wf_spec")
	name := rapid.SampledFrom(m.promisePool).Draw(t, "promise")
	cur := m.getOrCreate(spec.id)
	if cur.status != mInvoked && cur.status != mSuspended {
		return
	}
	idx := cur.journalLen
	m.apply(t, m.shardOf(spec.id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: spec.id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_PeekPromise{
							PeekPromise: &enginev1.JEPeekPromise{Name: name, Service: spec.tgt.service, WorkflowKey: spec.tgt.objectKey},
						},
					},
				},
			},
		}},
	})
	if cur.status == mSuspended {
		cur.status = mInvoked
	}
	cur.journalLen++
}

// PromiseCompletedExternal proposes an InvokerEffect.PromiseCompleted
// envelope, mimicking ingress.ResolveWorkflowPromise. Unlike JECompletePromise
// the resolver isn't a real invocation — the envelope is addressed by
// (service, workflow_key, name) and the apply arm runs transitionOnPromiseResolved
// on the awaiter (Suspended → Invoked + ActInvoke). If no awaiter is pending
// only the PromiseValue row is written.
func (m *engineMachine) PromiseCompletedExternal(t *rapid.T) {
	if len(m.wfSpecs) == 0 {
		return
	}
	spec := rapid.SampledFrom(m.wfSpecs).Draw(t, "wf_spec")
	name := rapid.SampledFrom(m.promisePool).Draw(t, "promise")
	if spec.tgt.objectKey == "" {
		return
	}
	pk := promiseKey{spec.tgt.service, spec.tgt.objectKey, name}
	value := []byte("e-" + name)

	pv := m.promises[pk]
	alreadyTerminal := pv != nil && pv.state != promisePending

	shard := m.partitioner.ShardForTarget(&enginev1.InvocationTarget{
		ServiceName: spec.tgt.service,
		ObjectKey:   spec.tgt.objectKey,
	})
	m.apply(t, shard, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_PromiseCompleted{
				PromiseCompleted: &enginev1.PromiseCompleted{
					Service:     spec.tgt.service,
					WorkflowKey: spec.tgt.objectKey,
					PromiseName: name,
					Value:       value,
				},
			},
		}},
	})

	if alreadyTerminal {
		return // ingress is a silent no-op on conflict
	}
	m.promises[pk] = &modelPromise{state: promiseResolved, value: value}
	for _, awaiter := range m.promiseAwaiters[pk] {
		owner := m.invs[awaiter.ownerIDHex]
		if owner == nil {
			continue
		}
		owner.journalLen++
		if owner.status == mSuspended {
			owner.status = mInvoked
		}
	}
	delete(m.promiseAwaiters, pk)
}

// TimerFired picks a registered timer (or fires unmatched-zero) and proposes
// Command_TimerFired against the inv's owning shard.
func (m *engineMachine) TimerFired(t *rapid.T) {
	keys := sortedTimerKeys(m.pendingTimers)
	var (
		id       *enginev1.InvocationId
		sleepIdx uint32
		fireAtMs uint64
	)
	if len(keys) > 0 && rapid.Bool().Draw(t, "use_pending") {
		k := rapid.SampledFrom(keys).Draw(t, "timer")
		pt := m.pendingTimers[k]
		id, sleepIdx, fireAtMs = pt.id, pt.sleepIdx, pt.fireAtMs
		delete(m.pendingTimers, k)
	} else {
		id = rapid.SampledFrom(m.specPool).Draw(t, "spec").id
	}

	cur := m.getOrCreate(id)
	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_TimerFired{TimerFired: &enginev1.TimerFired{
			InvocationId: id,
			SleepIndex:   sleepIdx,
			FireAtMs:     fireAtMs,
		}},
	})
	if cur.status == mSuspended {
		cur.status = mInvoked
	}
}

// ReapWorkflow synthesises Command.ReapWorkflow against the workflow's
// owning shard, simulating the WorkflowReapService firing. Only fires
// for a Completed KIND_WORKFLOW run — model + SUT both no-op
// otherwise. On success the model wipes the workflow's promise +
// awaiter rows and resets the run invocation to Free.
func (m *engineMachine) ReapWorkflow(t *rapid.T) {
	// Collect candidates: workflow_run entries whose owning invocation
	// is currently Completed in the model.
	type cand struct {
		wfk    leaseKey
		idHexs string
	}
	var candidates []cand
	for wfk, idH := range m.workflowRuns {
		inv := m.invs[idH]
		if inv == nil || inv.status != mCompleted {
			continue
		}
		candidates = append(candidates, cand{wfk: wfk, idHexs: idH})
	}
	if len(candidates) == 0 {
		return
	}
	// Deterministic selection by sorted workflow key + service so the
	// shrinker can reproduce traces.
	sortedCands := make([]cand, len(candidates))
	copy(sortedCands, candidates)
	sort.Slice(sortedCands, func(i, j int) bool {
		if sortedCands[i].wfk.service != sortedCands[j].wfk.service {
			return sortedCands[i].wfk.service < sortedCands[j].wfk.service
		}
		return sortedCands[i].wfk.objectKey < sortedCands[j].wfk.objectKey
	})
	pick := rapid.SampledFrom(sortedCands).Draw(t, "reap_candidate")

	shard := m.partitioner.ShardForTarget(&enginev1.InvocationTarget{
		ServiceName: pick.wfk.service,
		ObjectKey:   pick.wfk.objectKey,
	})
	m.apply(t, shard, &enginev1.Command{
		Kind: &enginev1.Command_ReapWorkflow{ReapWorkflow: &enginev1.ReapWorkflow{
			Service:     pick.wfk.service,
			WorkflowKey: pick.wfk.objectKey,
			FireAtMs:    0, // synthetic — no matching reap row needed
		}},
	})

	// Mirror the apply arm in the model.
	delete(m.workflowRuns, pick.wfk)
	for pk := range m.promises {
		if pk.service == pick.wfk.service && pk.workflowKey == pick.wfk.objectKey {
			delete(m.promises, pk)
		}
	}
	for pk := range m.promiseAwaiters {
		if pk.service == pick.wfk.service && pk.workflowKey == pick.wfk.objectKey {
			delete(m.promiseAwaiters, pk)
		}
	}
	if inv := m.invs[pick.idHexs]; inv != nil {
		inv.status = mFree
		inv.journalLen = 0
		inv.output = nil
	}
}

func (m *engineMachine) Purge(t *rapid.T) {
	id := rapid.SampledFrom(m.specPool).Draw(t, "spec").id
	cur := m.getOrCreate(id)
	if cur.status != mCompleted {
		return
	}
	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_Purge{Purge: &enginev1.PurgeInvocation{
			InvocationId: id,
		}},
	})
	cur.status = mFree
	cur.journalLen = 0
	cur.output = nil
}

func (m *engineMachine) AdvanceClock(t *rapid.T) {
	delta := rapid.Uint64Range(1, 60_000).Draw(t, "ms")
	m.nowPtr.Add(delta)
}

// Replay re-feeds the previous envelope on a shard (chosen uniformly).
// Only outbox-routed envelopes (carrying ArbitraryDedup) are replayable.
func (m *engineMachine) Replay(t *rapid.T) {
	shard := uint64(rapid.IntRange(1, numShards).Draw(t, "shard"))
	si := m.sIdx(shard)
	if m.prevEnv[si] == nil || !m.prevReplayable[si] {
		return
	}

	beforeIdx := m.appliedIndex(t, shard)
	actions := m.applyEnvelope(t, shard, m.prevEnv[si])
	afterIdx := m.appliedIndex(t, shard)

	if afterIdx != beforeIdx+1 {
		t.Fatalf("Replay shard=%d: applied_index=%d, want %d", shard, afterIdx, beforeIdx+1)
	}
	if len(actions) != 0 {
		t.Fatalf("Replay shard=%d: produced %d actions, want 0", shard, len(actions))
	}
}

func (m *engineMachine) appliedIndex(t *rapid.T, shard uint64) uint64 {
	got, err := m.parts[m.sIdx(shard)].Lookup(LookupAppliedIndex{})
	if err != nil {
		t.Fatalf("LookupAppliedIndex shard=%d: %v", shard, err)
	}
	return got.(uint64)
}

func (m *engineMachine) Crash(t *rapid.T) {
	shard := uint64(rapid.IntRange(1, numShards).Draw(t, "shard"))
	si := m.sIdx(shard)
	var buf bytes.Buffer
	if err := m.snaps[si].SaveSnapshot(&buf); err != nil {
		t.Fatalf("SaveSnapshot shard=%d: %v", shard, err)
	}
	_ = m.parts[si].Close()

	m.spinPartition(t, shard, true)

	if err := m.parts[si].RecoverFromSnapshot(&buf, nil); err != nil {
		t.Fatalf("RecoverFromSnapshot shard=%d: %v", shard, err)
	}
}

// ChildCall journals a JECall for an mInvoked parent. The partition mints a
// callee id, appends an outbox row, and emits ActDispatchOutbox — which our
// router captures into pendingOutbox for a subsequent RouteOutbox.
func (m *engineMachine) ChildCall(t *rapid.T) {
	parentID := rapid.SampledFrom(m.specPool).Draw(t, "parent_spec").id
	tgt := rapid.SampledFrom(m.tgtPool).Draw(t, "child_target")
	parent := m.getOrCreate(parentID)
	if parent.status != mInvoked {
		return
	}
	idx := parent.journalLen
	m.apply(t, m.shardOf(parentID), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: parentID,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: idx,
						Entry: &enginev1.JournalEntry_Call{
							Call: &enginev1.JECall{
								Target: tgt.proto(),
								Input:  []byte("call"),
							},
						},
					},
				},
			},
		}},
	})
	parent.journalLen = idx + 1
}

// RouteOutbox picks an un-routed outbox row and feeds its embedded command to
// the destination shard with an ArbitraryDedup header. Mirrors what the cross-
// shard Delivery gRPC pipeline would do.
//
// Pre-update of the model: for Invoke-flavoured envelopes we run the dest
// shard's onInvoke semantics on the model (so the router's onActInvoke can
// transition the new id from Scheduled to Invoked correctly).
func (m *engineMachine) RouteOutbox(t *rapid.T) {
	idx := -1
	for i, r := range m.pendingOutbox {
		if !r.routed {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	row := &m.pendingOutbox[idx]
	destShard := row.env.GetDestinationShardId()
	if destShard < 1 || destShard > numShards {
		t.Fatalf("RouteOutbox: invalid destination shard %d", destShard)
	}
	dedup := &enginev1.Dedup{Kind: &enginev1.Dedup_Arbitrary{
		Arbitrary: &enginev1.ArbitraryDedup{
			ProducerId: fmt.Sprintf("outbox/p%d", row.srcShard),
			Seq:        row.seq,
		},
	}}
	header := &enginev1.Header{Dedup: dedup}

	var cmd *enginev1.Command
	switch k := row.env.GetKind().(type) {
	case *enginev1.OutboxEnvelope_Invoke:
		ic := k.Invoke
		// Pre-update model with what dest shard's onInvoke will do.
		m.applyInvokeToModel(ic.GetInvocationId(), targetFromProto(ic.GetTarget()), ic.GetIdempotencyKey(), protocolv1.Kind(ic.GetKind()))
		cmd = &enginev1.Command{Kind: &enginev1.Command_Invoke{Invoke: ic}}
	case *enginev1.OutboxEnvelope_DeliverCallResult:
		dcr := k.DeliverCallResult
		// Dest shard journals a JECallResult on the parent. Bump parent's
		// journalLen by 1.
		if parent := m.invs[idHex(dcr.GetParentId())]; parent != nil {
			parent.journalLen++
		}
		cmd = &enginev1.Command{Kind: &enginev1.Command_DeliverCallResult{DeliverCallResult: dcr}}
	case *enginev1.OutboxEnvelope_OutboxAck:
		cmd = &enginev1.Command{Kind: &enginev1.Command_OutboxAck{OutboxAck: k.OutboxAck}}
	default:
		// Signal-via-outbox or other kinds: skip in Step 4 baseline.
		row.routed = true
		return
	}

	row.routed = true
	actions := m.applyEnvelope(t, destShard, &enginev1.Envelope{Header: header, Command: cmd})
	m.routeActions(t, destShard, actions)
}

// -----------------------------------------------------------------------------
// Invariants
// -----------------------------------------------------------------------------

func (m *engineMachine) Check(t *rapid.T) {
	for _, want := range m.invs {
		shard := m.shardOf(want.id)
		got, err := m.parts[m.sIdx(shard)].Lookup(LookupInvocation{ID: want.id})
		if err != nil {
			t.Fatalf("lookup shard=%d %x: %v", shard, want.id.GetUuid(), err)
		}
		st, _ := got.(*enginev1.InvocationStatus)
		gotStatus := classify(st)

		if gotStatus != want.status {
			t.Fatalf("inv %x shard=%d: status mismatch: model=%v sut=%v",
				want.id.GetUuid(), shard, want.status, gotStatus)
		}
		if want.status == mCompleted {
			cmp, _ := st.GetStatus().(*enginev1.InvocationStatus_Completed)
			if !bytes.Equal(cmp.Completed.GetOutput(), want.output) {
				t.Fatalf("inv %x shard=%d: output mismatch: model=%q sut=%q",
					want.id.GetUuid(), shard, want.output, cmp.Completed.GetOutput())
			}
		}
	}

	// Idempotency invariant — checked against both shards (idem rows live on
	// the shard that owns the target's PartitionKey). For simplicity we check
	// both and accept if either matches; partition.go writes to whichever
	// shard the envelope landed on.
	for k, winnerHex := range m.idemPool {
		var found bool
		for s := uint64(1); s <= numShards; s++ {
			got, err := m.parts[m.sIdx(s)].Lookup(LookupIdempotency{
				Service:        k.service,
				Handler:        k.handler,
				ObjectKey:      k.objectKey,
				IdempotencyKey: k.key,
			})
			if err != nil {
				t.Fatalf("LookupIdempotency shard=%d %+v: %v", s, k, err)
			}
			if prior, _ := got.(*enginev1.InvocationId); prior != nil {
				if idHex(prior) != winnerHex {
					t.Fatalf("idem %+v shard=%d: SUT=%s model=%s",
						k, s, idHex(prior), winnerHex)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("idem %+v: no shard has the row; model expects %s", k, winnerHex)
		}
	}

	// Object FSM invariant — leases live on the shard owning the target's
	// PartitionKey.
	for lk, want := range m.leases {
		shard := m.partitioner.ShardForTarget(&enginev1.InvocationTarget{
			ServiceName: lk.service, ObjectKey: lk.objectKey,
		})
		klt := tables.KeyLeaseTable{S: m.snaps[m.sIdx(shard)].Store()}
		got, err := klt.Get(lk.service, lk.objectKey)
		if err != nil {
			t.Fatalf("KeyLeaseTable.Get shard=%d %+v: %v", shard, lk, err)
		}
		if want.state == leaseIdle && len(want.queue) == 0 {
			if got != nil && got.GetState() == enginev1.KeyLeaseStatus_ACTIVE {
				t.Fatalf("lease %+v shard=%d: SUT ACTIVE, model idle", lk, shard)
			}
			continue
		}
		if got == nil {
			t.Fatalf("lease %+v shard=%d: SUT absent, model active(current=%s, queue=%d)",
				lk, shard, want.currentIDHex, len(want.queue))
		}
		if got.GetState() != enginev1.KeyLeaseStatus_ACTIVE {
			t.Fatalf("lease %+v shard=%d: SUT state=%v, model active", lk, shard, got.GetState())
		}
		if idHex(got.GetCurrentInvocation()) != want.currentIDHex {
			t.Fatalf("lease %+v shard=%d: SUT current=%s, model current=%s",
				lk, shard, idHex(got.GetCurrentInvocation()), want.currentIDHex)
		}
		if len(got.GetQueue()) != len(want.queue) {
			t.Fatalf("lease %+v shard=%d: SUT queue len=%d, model queue len=%d",
				lk, shard, len(got.GetQueue()), len(want.queue))
		}
		for i, qid := range got.GetQueue() {
			if idHex(qid) != want.queue[i] {
				t.Fatalf("lease %+v shard=%d: queue[%d]: SUT=%s model=%s",
					lk, shard, i, idHex(qid), want.queue[i])
			}
		}
		holder := m.invs[want.currentIDHex]
		if holder == nil ||
			(holder.status != mScheduled && holder.status != mInvoked && holder.status != mSuspended) {
			t.Fatalf("lease %+v shard=%d: holder %s dead status %v",
				lk, shard, want.currentIDHex, statusOf(holder))
		}
		for _, qHex := range want.queue {
			q := m.invs[qHex]
			if q == nil || q.status != mScheduled {
				t.Fatalf("lease %+v shard=%d: queued %s status=%v, want mScheduled",
					lk, shard, qHex, statusOf(q))
			}
		}
	}

	// Timer invariants: per shard, primary set == secondary set, and the
	// resulting (id, fire_at) set matches the model's pendingTimers
	// filtered to invocations hosted on that shard. The G5 reap on
	// Complete and the secondary index pair-writes both flow through this
	// check; a regression in either drops it on the floor.
	for s := uint64(1); s <= numShards; s++ {
		store := m.snaps[m.sIdx(s)].Store()
		tt := tables.TimerTable{S: store}

		type timerRow struct {
			idHex    string
			fireAtMs uint64
		}
		primary := map[timerRow]struct{}{}
		_ = tt.ScanAll(func(e tables.TimerEntry) error {
			primary[timerRow{idHex: idHex(e.ID), fireAtMs: e.FireAtMs}] = struct{}{}
			return nil
		})
		secondary := map[timerRow]struct{}{}
		_ = tt.ScanAllIndex(func(id *enginev1.InvocationId, fireAtMs uint64) error {
			secondary[timerRow{idHex: idHex(id), fireAtMs: fireAtMs}] = struct{}{}
			return nil
		})
		for r := range primary {
			if _, ok := secondary[r]; !ok {
				t.Fatalf("timer index: shard=%d primary has %+v but secondary missing", s, r)
			}
		}
		for r := range secondary {
			if _, ok := primary[r]; !ok {
				t.Fatalf("timer index: shard=%d secondary has %+v but primary missing", s, r)
			}
		}

		// Compare against the model. Build the model-expected set for
		// this shard from pendingTimers.
		modelSet := map[timerRow]struct{}{}
		for _, pt := range m.pendingTimers {
			if m.shardOf(pt.id) != s {
				continue
			}
			modelSet[timerRow{idHex: idHex(pt.id), fireAtMs: pt.fireAtMs}] = struct{}{}
		}
		for r := range primary {
			if _, ok := modelSet[r]; !ok {
				t.Fatalf("timer rows: shard=%d SUT has %+v, model missing (action emission gap?)", s, r)
			}
		}
		for r := range modelSet {
			if _, ok := primary[r]; !ok {
				t.Fatalf("timer rows: shard=%d model has %+v, SUT missing (apply-side leak?)", s, r)
			}
		}
	}

	// Outbox high-water-mark monotonicity (PartitionMeta.NextOutboxSeq).
	// Rolling back to a lower value would imply a divergent Apply or a
	// snapshot recovery that lost durable state; both are bugs.
	for s := uint64(1); s <= numShards; s++ {
		meta, err := (tables.MetaTable{S: m.snaps[m.sIdx(s)].Store()}).Get()
		if err != nil {
			t.Fatalf("MetaTable.Get shard=%d: %v", s, err)
		}
		cur := meta.GetNextOutboxSeq()
		if cur < m.prevOutboxSeq[m.sIdx(s)] {
			t.Fatalf("outbox seq: shard=%d went backwards: prev=%d cur=%d",
				s, m.prevOutboxSeq[m.sIdx(s)], cur)
		}
		m.prevOutboxSeq[m.sIdx(s)] = cur
	}

	// Awakeable directory ↔ model parity. For every model-registered
	// awakeable, the SUT row must exist on the owner's shard and point
	// to the same owner. The reverse — orphan SUT row not in model — is
	// caught indirectly: the model adds entries on RegisterAwakeable and
	// removes them on AwakeableResolved, which are also the only SUT
	// write/delete paths.
	for awk, ownerHex := range m.awks {
		owner := m.invs[ownerHex]
		if owner == nil {
			continue
		}
		shard := m.shardOf(owner.id)
		row, err := (tables.AwakeableTable{S: m.snaps[m.sIdx(shard)].Store()}).Get(awk)
		if err != nil {
			t.Fatalf("AwakeableTable.Get shard=%d %q: %v", shard, awk, err)
		}
		if row == nil {
			t.Fatalf("awakeable %q: model expects owner=%s shard=%d; SUT row absent",
				awk, ownerHex, shard)
		}
		if idHex(row.GetOwner()) != ownerHex {
			t.Fatalf("awakeable %q shard=%d: SUT owner=%s, model owner=%s",
				awk, shard, idHex(row.GetOwner()), ownerHex)
		}
	}

	// WorkflowRunTable ↔ model parity. For every (svc, workflow_key)
	// where the model recorded a winning workflow Run, the SUT row must
	// exist on the shard owning the target and point to the same id.
	// onPurge does not clear workflow_run (retention reaper's job), so
	// the row outlives a Completed/Purged invocation — the model mirrors
	// that by never deleting m.workflowRuns entries.
	for lk, winnerHex := range m.workflowRuns {
		shard := m.partitioner.ShardForTarget(&enginev1.InvocationTarget{
			ServiceName: lk.service, ObjectKey: lk.objectKey,
		})
		got, err := (tables.WorkflowRunTable{S: m.snaps[m.sIdx(shard)].Store()}).Get(lk.service, lk.objectKey)
		if err != nil {
			t.Fatalf("WorkflowRunTable.Get shard=%d %+v: %v", shard, lk, err)
		}
		if got == nil {
			t.Fatalf("workflow_run %+v shard=%d: SUT absent, model expects winner=%s",
				lk, shard, winnerHex)
		}
		if idHex(got) != winnerHex {
			t.Fatalf("workflow_run %+v shard=%d: SUT=%s model=%s",
				lk, shard, idHex(got), winnerHex)
		}
	}

	// PromiseTable ↔ model parity. Promise rows live on the shard that
	// owns the workflow target. Both terminal states (Resolved, Rejected)
	// are compared by value/failure_message; absent rows in the model
	// (promisePending) must mean either absent SUT row or a Pending
	// PromiseValue.
	for pk, want := range m.promises {
		shard := m.partitioner.ShardForTarget(&enginev1.InvocationTarget{
			ServiceName: pk.service, ObjectKey: pk.workflowKey,
		})
		got, err := (tables.PromiseTable{S: m.snaps[m.sIdx(shard)].Store()}).Get(pk.service, pk.workflowKey, pk.name)
		if err != nil {
			t.Fatalf("PromiseTable.Get shard=%d %+v: %v", shard, pk, err)
		}
		switch want.state {
		case promiseResolved:
			r := got.GetResolved()
			if r == nil {
				t.Fatalf("promise %+v shard=%d: SUT not Resolved (got %T), model Resolved", pk, shard, got.GetState())
			}
			if !bytes.Equal(r.GetValue(), want.value) {
				t.Fatalf("promise %+v shard=%d: SUT value=%q model value=%q", pk, shard, r.GetValue(), want.value)
			}
		case promiseRejected:
			rj := got.GetRejected()
			if rj == nil {
				t.Fatalf("promise %+v shard=%d: SUT not Rejected (got %T), model Rejected", pk, shard, got.GetState())
			}
			if rj.GetFailureMessage() != want.failureMessage {
				t.Fatalf("promise %+v shard=%d: SUT failure=%q model failure=%q",
					pk, shard, rj.GetFailureMessage(), want.failureMessage)
			}
		case promisePending:
			if got != nil && got.GetPending() == nil {
				t.Fatalf("promise %+v shard=%d: SUT terminal, model Pending", pk, shard)
			}
		}
	}

	// PromiseAwaiterTable ↔ model parity. Each (svc, key, name) may have
	// multiple awaiter rows (one per concurrent Promise(name).Result()),
	// keyed by entry_index. Scan the SUT and assert the bag of (owner,
	// entry_index) pairs matches the model's slice.
	for pk, want := range m.promiseAwaiters {
		shard := m.partitioner.ShardForTarget(&enginev1.InvocationTarget{
			ServiceName: pk.service, ObjectKey: pk.workflowKey,
		})
		var got []*enginev1.PromiseAwaiter
		if err := (tables.PromiseAwaiterTable{S: m.snaps[m.sIdx(shard)].Store()}).ScanForName(pk.service, pk.workflowKey, pk.name, func(a *enginev1.PromiseAwaiter) error {
			got = append(got, proto.Clone(a).(*enginev1.PromiseAwaiter))
			return nil
		}); err != nil {
			t.Fatalf("PromiseAwaiterTable.ScanForName shard=%d %+v: %v", shard, pk, err)
		}
		if len(got) != len(want) {
			t.Fatalf("promise_awaiter %+v shard=%d: SUT has %d rows, model has %d",
				pk, shard, len(got), len(want))
		}
		// Build SUT bag keyed by entry_index for unordered comparison.
		sutByIdx := map[uint32]*enginev1.PromiseAwaiter{}
		for _, a := range got {
			sutByIdx[a.GetEntryIndex()] = a
		}
		for _, w := range want {
			a, ok := sutByIdx[w.entryIndex]
			if !ok {
				t.Fatalf("promise_awaiter %+v shard=%d: model entry_index=%d absent in SUT",
					pk, shard, w.entryIndex)
			}
			if idHex(a.GetOwner()) != w.ownerIDHex {
				t.Fatalf("promise_awaiter %+v shard=%d entry_index=%d: SUT owner=%s, model owner=%s",
					pk, shard, w.entryIndex, idHex(a.GetOwner()), w.ownerIDHex)
			}
		}
	}
}

func statusOf(m *modelInv) modelStatus {
	if m == nil {
		return mFree
	}
	return m.status
}

func classify(s *enginev1.InvocationStatus) modelStatus {
	if s == nil {
		return mFree
	}
	switch s.GetStatus().(type) {
	case nil, *enginev1.InvocationStatus_Free:
		return mFree
	case *enginev1.InvocationStatus_Scheduled:
		return mScheduled
	case *enginev1.InvocationStatus_Invoked:
		return mInvoked
	case *enginev1.InvocationStatus_Suspended:
		return mSuspended
	case *enginev1.InvocationStatus_Completed:
		return mCompleted
	}
	return mFree
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

func TestEngine_PBT(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := &engineMachine{t: t}
		m.init(rt)
		rt.Repeat(rapid.StateMachineActions(m))
	})
}

// TestEngine_InvokedWakeRespawn covers the race the wake-path rewire
// fixed: a resolution (Awakeable/Signal/CallResult) that arrives while
// the invocation is still Invoked must emit ActInvoke so the Invoker's
// pendingRespawn queue picks up the wake even though the running
// session is mid-flight returning ErrSuspended.
//
// Sequence:
//
//  1. Invoke                       → Scheduled  (+ ActInvoke)
//  2. JEInput append               → Invoked
//  3. SignalDelivered (Invoked!)   → Invoked    (+ ActInvoke for respawn)
//  4. Suspended                    → Suspended  (handler returned ErrSuspended)
//  5. SignalDelivered (Suspended)  → Invoked    (+ ActInvoke for restart)
//  6. Completed                    → Completed
//
// Pre-rewire, step 3 emitted only ActDeliverNotification (dropped on the
// floor by Invoker.DeliverNotification), so the wake was lost. After
// step 4 the invocation would have been stranded Suspended despite the
// journal carrying the signal. The assertion at step 3 is the new
// guarantee.
func TestEngine_InvokedWakeRespawn(t *testing.T) {
	p, _, col := newTestPartition(t)

	// Signals route by Target → KeyLeaseTable, so the wake-respawn test
	// needs a keyed target so the apply arm can resolve back to this id.
	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h", ObjectKey: "wake-respawn-key"}
	pk := routing.PartitionKey(target.GetServiceName(), target.GetObjectKey())
	id := &enginev1.InvocationId{PartitionKey: pk, Uuid: []byte("wake-respawn-id1")}

	apply := func(idx uint64, cmd *enginev1.Command) []Action {
		t.Helper()
		buf, err := proto.Marshal(&enginev1.Envelope{
			Header:  &enginev1.Header{CreatedAtMs: testEnvelopeNowMs},
			Command: cmd,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Update([]statemachine.Entry{{Index: idx, Cmd: buf}}); err != nil {
			t.Fatalf("Update(idx=%d): %v", idx, err)
		}
		return col.Drain()
	}

	requireStatus := func(stage string, want any) {
		t.Helper()
		res, err := p.Lookup(LookupInvocation{ID: id})
		if err != nil {
			t.Fatalf("%s: Lookup: %v", stage, err)
		}
		got := res.(*enginev1.InvocationStatus).GetStatus()
		switch want.(type) {
		case *enginev1.InvocationStatus_Scheduled:
			if _, ok := got.(*enginev1.InvocationStatus_Scheduled); !ok {
				t.Fatalf("%s: status = %T; want Scheduled", stage, got)
			}
		case *enginev1.InvocationStatus_Invoked:
			if _, ok := got.(*enginev1.InvocationStatus_Invoked); !ok {
				t.Fatalf("%s: status = %T; want Invoked", stage, got)
			}
		case *enginev1.InvocationStatus_Suspended:
			if _, ok := got.(*enginev1.InvocationStatus_Suspended); !ok {
				t.Fatalf("%s: status = %T; want Suspended", stage, got)
			}
		case *enginev1.InvocationStatus_Completed:
			if _, ok := got.(*enginev1.InvocationStatus_Completed); !ok {
				t.Fatalf("%s: status = %T; want Completed", stage, got)
			}
		}
	}

	requireOneActInvoke := func(stage string, actions []Action) {
		t.Helper()
		if len(actions) != 1 {
			t.Fatalf("%s: got %d actions, want 1", stage, len(actions))
		}
		if _, ok := actions[0].(ActInvoke); !ok {
			t.Fatalf("%s: actions[0] = %T; want ActInvoke", stage, actions[0])
		}
	}

	// 1. Invoke → Scheduled
	actions := apply(1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{
			Invoke: &enginev1.InvokeCommand{
				InvocationId: id, Target: target, Input: []byte("in"),
			},
		},
	})
	requireOneActInvoke("step 1 (Invoke)", actions)
	requireStatus("step 1", (*enginev1.InvocationStatus_Scheduled)(nil))

	// 2. JEInput → Invoked
	apply(2, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_JournalAppended{
				JournalAppended: &enginev1.JournalEntryAppended{
					Entry: &enginev1.JournalEntry{
						Index: 0,
						Entry: &enginev1.JournalEntry_Input{Input: &enginev1.JEInput{Value: []byte("in")}},
					},
				},
			},
		}},
	})
	requireStatus("step 2", (*enginev1.InvocationStatus_Invoked)(nil))

	// 3. SignalDelivered arrives while still Invoked — the wake-respawn
	//    surface. Status stays Invoked but ActInvoke must fire so the
	//    Invoker queues pendingRespawn for after the in-flight session
	//    finishes its ErrSuspended unwind.
	actions = apply(3, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_SignalDelivered{
				SignalDelivered: &enginev1.SignalDelivered{
					Target:     target,
					SignalName: "sig1", Payload: []byte("p1"),
				},
			},
		}},
	})
	requireOneActInvoke("step 3 (live SignalDelivered on Invoked)", actions)
	requireStatus("step 3", (*enginev1.InvocationStatus_Invoked)(nil))

	// 4. Handler returns ErrSuspended → session proposes Suspended.
	apply(4, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_Suspended{
				Suspended: &enginev1.InvocationSuspended{AwaitingOn: []string{"signal:sig1"}},
			},
		}},
	})
	requireStatus("step 4", (*enginev1.InvocationStatus_Suspended)(nil))

	// 5. Second signal arrives. Suspended → Invoked, ActInvoke for the
	//    fresh session start. (This is the standard Suspended-wake path
	//    that has always worked; included so the test exercises both
	//    branches end-to-end.)
	actions = apply(5, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			Kind: &enginev1.InvokerEffect_SignalDelivered{
				SignalDelivered: &enginev1.SignalDelivered{
					Target:     target,
					SignalName: "sig2", Payload: []byte("p2"),
				},
			},
		}},
	})
	requireOneActInvoke("step 5 (Suspended wake)", actions)
	requireStatus("step 5", (*enginev1.InvocationStatus_Invoked)(nil))

	// 6. Respawned handler runs to completion.
	apply(6, &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_Completed{
				Completed: &enginev1.InvocationCompleted{Output: []byte("done")},
			},
		}},
	})
	requireStatus("step 6", (*enginev1.InvocationStatus_Completed)(nil))
}
