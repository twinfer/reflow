package tables

import (
	"fmt"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// DedupTable stores the highest sequence number seen per producer, so the
// state machine can reject duplicate Raft entries on replay or after retries.
//
// Mirrors restate
// crates/storage-api/src/deduplication_table/mod.rs:99-137 (epoch-sequence
// ordering) and the apply-time check in
// crates/worker/src/partition/mod.rs:1049-1063.
type DedupTable struct{ S storage.Store }

// IsDuplicate reports whether the incoming Dedup has already been seen.
func (t DedupTable) IsDuplicate(d *enginev1.Dedup) (bool, error) {
	if d == nil {
		return false, nil
	}
	key, seq, ok := dedupKeySeq(d)
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
	return entry.GetSeq() >= seq, nil
}

// Record updates the high-water seq for this producer. Caller should only
// call this after IsDuplicate returned false (or for entries that bypass
// dedup like AnnounceLeader's first proposal).
func (t DedupTable) Record(b storage.Batch, d *enginev1.Dedup) error {
	if d == nil {
		return nil
	}
	key, seq, ok := dedupKeySeq(d)
	if !ok {
		return nil
	}
	entry := &enginev1.DedupEntry{Seq: seq}
	if sp := d.GetSelfProposal(); sp != nil {
		entry.LeaderEpoch = sp.GetLeaderEpoch()
	}
	return putProto(b, key, entry)
}

// dedupKeySeq returns the storage key + sequence number for a Dedup. The
// boolean is false for an empty Dedup (kind unset).
func dedupKeySeq(d *enginev1.Dedup) ([]byte, uint64, bool) {
	switch k := d.GetKind().(type) {
	case *enginev1.Dedup_SelfProposal:
		return keys.DedupSelfKey(k.SelfProposal.GetLeaderEpoch()), k.SelfProposal.GetSeq(), true
	case *enginev1.Dedup_Arbitrary:
		return keys.DedupArbitraryKey(k.Arbitrary.GetProducerId()), k.Arbitrary.GetSeq(), true
	case nil:
		return nil, 0, false
	default:
		// Unreachable for known oneof variants; defensive panic to surface a
		// future schema mismatch loudly during dev.
		panic(fmt.Sprintf("dedup: unknown kind %T", k))
	}
}
