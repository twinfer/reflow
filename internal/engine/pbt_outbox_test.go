package engine

// Cross-shard call + outbox PBT actions and invariants. Carved out of
// pbt_test.go so the outbox subsystem (ChildCall produces, RouteOutbox
// consumes + applies the routed envelope on the dest shard, monotonicity
// invariant on NextOutboxSeq) lives in one file next to modelOutboxRow.
//
// Outbox tracking state remains on engineMachine in pbt_test.go:
//   - pendingOutbox    []modelOutboxRow
//   - prevOutboxSeq    [numShards]uint64
//
// The routeActions ActDispatchOutbox case also stays in pbt_test.go
// (single central action router; moving one case would split the
// dispatch table awkwardly). The Check skeleton calls
// checkOutboxInvariants below.

import (
	"fmt"

	"pgregory.net/rapid"

	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

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

// checkOutboxInvariants asserts PartitionMeta.NextOutboxSeq never rolls
// backwards on any shard. A regression here would imply a divergent Apply
// or a snapshot recovery that lost durable state — both are bugs.
func (m *engineMachine) checkOutboxInvariants(t *rapid.T) {
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
}
