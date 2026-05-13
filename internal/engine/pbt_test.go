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
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
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
type invSpec struct {
	id  *enginev1.InvocationId
	tgt modelTarget
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

	// Model (global across shards; classified by id.PartitionKey).
	invs     map[string]*modelInv     // hex(uuid) -> inv
	idemPool map[idemKey]string       // (svc,handler,objKey,key) -> winning idHex
	awks     map[string]string        // registered awakeable_id -> owner idHex
	leases   map[leaseKey]*modelLease // (service,object_key) -> lease state

	// Generator pools (drawn once at init).
	specPool []invSpec      // paired (id, target) — partition_key consistent with target
	tgtPool  []modelTarget  // standalone targets for ChildCall child-target draws
	idemKs   []string
	awkPool  []string
	sigPool  []string
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
	m.pendingTimers = map[timerKey]pendingTimer{}
	m.pendingOutbox = nil
	m.tgtPool = []modelTarget{
		{service: "S", handler: "h"},
		{service: "Counter", handler: "incr", objectKey: "k1"},
	}
	m.specPool = drawSpecPool(m.tgtPool)
	m.idemKs = []string{"", "req-0", "req-1", "req-2"}
	m.awkPool = []string{
		"awk_aaaaaaaaaaaaaaaaaaaaaa",
		"awk_bbbbbbbbbbbbbbbbbbbbbb",
		"awk_cccccccccccccccccccccc",
		"awk_dddddddddddddddddddddd",
	}
	m.sigPool = []string{"sig-0", "sig-1", "sig-2", "sig-3"}
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
		NowFn:       func() uint64 { return m.nowPtr.Load() },
		Partitioner: m.partitioner,
	})
	pp := m.parts[si]
	m.t.Cleanup(func() { _ = pp.Close() })
}

// drawSpecPool builds 2 paired specs per target, with id.PartitionKey =
// PartitionKey(target.service, target.objectKey) — matching production ingress
// behaviour and ensuring every Invoke lands on the shard that owns the target.
func drawSpecPool(targets []modelTarget) []invSpec {
	specs := make([]invSpec, 0, len(targets)*2)
	uuidNonce := uint64(1)
	for _, tgt := range targets {
		pk := routing.PartitionKey(tgt.service, tgt.objectKey)
		for i := 0; i < 2; i++ {
			uuid := make([]byte, 16)
			binary.BigEndian.PutUint64(uuid, uuidNonce)
			uuidNonce++
			specs = append(specs, invSpec{
				id:  &enginev1.InvocationId{PartitionKey: pk, Uuid: uuid},
				tgt: tgt,
			})
		}
	}
	return specs
}

// applyEnvelope marshals an envelope, advances the per-shard raft index, and
// feeds it to Partition.Update. Returns the drained Collector contents.
func (m *engineMachine) applyEnvelope(t *rapid.T, shard uint64, env *enginev1.Envelope) []Action {
	si := m.sIdx(shard)
	m.raftIx[si]++
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
//                         so leader re-emission after Crash doesn't double-count).
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
		case ActDeliverNotification, ActDeliverAwakeable, ActAbortInvocation, ActIngressResponse:
			// local — drop
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
// Returns true if this Invoke is being dropped due to idempotency.
func (m *engineMachine) applyInvokeToModel(id *enginev1.InvocationId, tgt modelTarget, ik string) bool {
	if ik != "" {
		k := idemKey{tgt.service, tgt.handler, tgt.objectKey, ik}
		if _, ok := m.idemPool[k]; ok {
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

	m.applyInvokeToModel(sp.id, sp.tgt, ik)
	m.apply(t, m.shardOf(sp.id), &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId:   sp.id,
			Target:         sp.tgt.proto(),
			Input:          []byte("in"),
			IdempotencyKey: ik,
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
	id := rapid.SampledFrom(m.specPool).Draw(t, "spec").id
	sig := rapid.SampledFrom(m.sigPool).Draw(t, "sig")
	cur := m.getOrCreate(id)
	if cur.status != mSuspended && cur.status != mInvoked {
		return
	}
	if cur.status == mSuspended {
		cur.status = mInvoked
	}
	m.apply(t, m.shardOf(id), &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
			InvocationId: id,
			Kind: &enginev1.InvokerEffect_SignalDelivered{
				SignalDelivered: &enginev1.SignalDelivered{
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
			ProducerId: fmt.Sprintf("outbox/%d", row.srcShard),
			Seq:        row.seq,
		},
	}}
	header := &enginev1.Header{Dedup: dedup}

	var cmd *enginev1.Command
	switch k := row.env.GetKind().(type) {
	case *enginev1.OutboxEnvelope_Invoke:
		ic := k.Invoke
		// Pre-update model with what dest shard's onInvoke will do.
		m.applyInvokeToModel(ic.GetInvocationId(), targetFromProto(ic.GetTarget()), ic.GetIdempotencyKey())
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
