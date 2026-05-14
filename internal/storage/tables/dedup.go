package tables

import (
	"fmt"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// DedupTable records per-propose presence so the state machine can reject
// duplicate Raft entries on replay or after a propose retry that resubmits
// the same envelope. Keys carry both the producer namespace (leader_epoch
// for self-proposals; producer_id for arbitrary) AND the sequence number,
// so dedup is exact-match: a fresh propose is rejected only when an entry
// with the same (namespace, seq) was already recorded.
//
// The earlier high-water-mark scheme (one key per namespace, value=max-seq)
// false-positived under concurrent propose: goroutines A and B atomically
// allocate seq=N and seq=N+1, then submit to dragonboat out of order.
// Replicas that apply seq=N+1 first record the high water at N+1, then
// rejected seq=N as a "duplicate" even though it is a fresh entry. Within
// a single Update batch the bug looked different — IsDuplicate reads from
// the store (not the in-batch writes), so within-batch records did not
// hide later entries — and the divergence across replicas left some
// invocations stuck in Scheduled while others observed Completed.
type DedupTable struct{ S storage.Reader }

// IsDuplicate reports whether the incoming Dedup has already been seen.
func (t DedupTable) IsDuplicate(d *enginev1.Dedup) (bool, error) {
	if d == nil {
		return false, nil
	}
	key, ok := dedupKey(d)
	if !ok {
		// Dedup with no kind set — treat as "no dedup info"; never dup.
		return false, nil
	}
	var entry enginev1.DedupEntry
	if err := getProto(t.S, key, &entry); err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Record marks the (namespace, seq) tuple as seen. Caller should only call
// this after IsDuplicate returned false (or for entries that bypass dedup
// like AnnounceLeader's first proposal).
func (t DedupTable) Record(b storage.Batch, d *enginev1.Dedup) error {
	if d == nil {
		return nil
	}
	key, ok := dedupKey(d)
	if !ok {
		return nil
	}
	entry := &enginev1.DedupEntry{}
	if sp := d.GetSelfProposal(); sp != nil {
		entry.LeaderEpoch = sp.GetLeaderEpoch()
		entry.Seq = sp.GetSeq()
	} else if arb := d.GetArbitrary(); arb != nil {
		entry.Seq = arb.GetSeq()
	}
	return putProto(b, key, entry)
}

// dedupKey returns the storage key for a Dedup, carrying both the producer
// namespace and the sequence number so each propose has its own slot. The
// boolean is false for an empty Dedup (kind unset).
func dedupKey(d *enginev1.Dedup) ([]byte, bool) {
	switch k := d.GetKind().(type) {
	case *enginev1.Dedup_SelfProposal:
		return keys.DedupSelfKey(k.SelfProposal.GetLeaderEpoch(), k.SelfProposal.GetSeq()), true
	case *enginev1.Dedup_Arbitrary:
		return keys.DedupArbitraryKey(k.Arbitrary.GetProducerId(), k.Arbitrary.GetSeq()), true
	case nil:
		return nil, false
	default:
		// Unreachable for known oneof variants; defensive panic to surface a
		// future schema mismatch loudly during dev.
		panic(fmt.Sprintf("dedup: unknown kind %T", k))
	}
}
