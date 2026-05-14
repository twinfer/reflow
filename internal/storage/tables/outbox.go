package tables

import (
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// OutboxTable persists outgoing commands the leader-side shuffler will
// re-inject into the cluster: Call invocations and SignalSend deliveries.
// Rows are keyed by an 8-byte BE sequence number under outbox/, so a
// forward scan yields envelopes in FIFO insertion order.
//
// Crash-safety: the shuffler only deletes a row after the matching
// ProposeIngress has been Raft-committed. If it crashes between propose
// and delete, the next leader re-scans and re-proposes — the per-producer
// DedupTable absorbs the duplicate. Phase 2.
type OutboxTable struct{ S storage.Reader }

// OutboxRow is one entry yielded by ScanFrom.
type OutboxRow struct {
	Seq      uint64
	Envelope *enginev1.OutboxEnvelope
}

// Append writes an envelope at the given sequence number. Caller must
// allocate seq atomically (typically PartitionMeta.next_outbox_seq++).
func (t OutboxTable) Append(b storage.Batch, seq uint64, env *enginev1.OutboxEnvelope) error {
	return putProto(b, keys.OutboxKey(seq), env)
}

// Pop deletes the row at seq. Called once the shuffler has confirmed the
// envelope was successfully re-injected (Raft-committed).
func (t OutboxTable) Pop(b storage.Batch, seq uint64) error {
	return b.Delete(keys.OutboxKey(seq))
}

// ScanFrom iterates outbox rows with seq >= fromSeq in ascending order.
// Pass fromSeq=0 to scan everything. Returning a non-nil error aborts.
func (t OutboxTable) ScanFrom(fromSeq uint64, fn func(OutboxRow) error) error {
	lower := keys.OutboxKey(fromSeq)
	upper := keys.PrefixUpperBound(keys.OutboxPrefix())
	iter, err := t.S.NewIter(lower, upper)
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		seq, err := keys.DecodeOutboxKey(iter.Key())
		if err != nil {
			return err
		}
		var env enginev1.OutboxEnvelope
		if err := proto.Unmarshal(iter.Value(), &env); err != nil {
			return err
		}
		if err := fn(OutboxRow{Seq: seq, Envelope: &env}); err != nil {
			return err
		}
	}
	return iter.Error()
}
