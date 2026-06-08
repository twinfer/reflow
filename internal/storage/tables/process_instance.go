package tables

import (
	"bytes"
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// ProcessInstanceTable stores one reflwos process/case instance per
// (service, instance_key) at proc/<lp:4><service>/<instance_key>. The value is
// a ProcessInstanceRecord whose state_blob is the opaque reflwos
// ExecutionState/CaseState — reflw never parses it; only the partition
// leader's procSession (which links reflwos) decodes it to run Advance.
//
// Single-writer per instance is the key-lease FSM in
// internal/engine/object_fsm.go: each inbound ProcessEvent is one serialized
// turn. The <lp:4> prefix means an instance rides the LP-transfer scan
// (keys.ProcessInstanceLPPrefix) and is range-deleted by FinishLPTransfer
// alongside the state/dedup rows for the same logical partition.
type ProcessInstanceTable struct{ S storage.Reader }

// Get loads an instance record. ok is false (err nil) when the row is absent.
func (t ProcessInstanceTable) Get(lp uint32, service, instanceKey string) (*enginev1.ProcessInstanceRecord, bool, error) {
	var rec enginev1.ProcessInstanceRecord
	if err := getProto(t.S, keys.ProcessInstanceKey(lp, service, instanceKey), &rec); err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &rec, true, nil
}

// Put writes the record into the batch; visible after Commit.
func (t ProcessInstanceTable) Put(b storage.Batch, lp uint32, service, instanceKey string, rec *enginev1.ProcessInstanceRecord) error {
	return putProto(b, keys.ProcessInstanceKey(lp, service, instanceKey), rec)
}

// Delete removes the instance row (terminal reap).
func (t ProcessInstanceTable) Delete(b storage.Batch, lp uint32, service, instanceKey string) error {
	return b.Delete(keys.ProcessInstanceKey(lp, service, instanceKey))
}

// ScanAll iterates every persisted process instance in key order, decoding the
// (service, instance_key) from each key. The instance's partition_key is
// available on rec.root_id. Used by the leader-gain resume to re-drive
// in-flight turns. Aborts early (returning ctx.Err()) on cancellation.
func (t ProcessInstanceTable) ScanAll(ctx context.Context, fn func(service, instanceKey string, rec *enginev1.ProcessInstanceRecord) error) error {
	prefix := keys.ProcessPrefix()
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		k := iter.Key()
		if len(k) < len(prefix)+keys.LPLen {
			continue
		}
		// proc/<lp:4><service>/<instance_key> — split the remainder on the
		// first '/' (components are '/'-free by construction).
		body := k[len(prefix)+keys.LPLen:]
		before, after, ok0 := bytes.Cut(body, []byte{'/'})
		if !ok0 {
			continue
		}
		rec := &enginev1.ProcessInstanceRecord{}
		if err := proto.Unmarshal(iter.Value(), rec); err != nil {
			continue
		}
		if err := fn(string(before), string(after), rec); err != nil {
			return err
		}
	}
	return iter.Error()
}

// ScanAllAfter is ScanAll resumed strictly past the after cursor (a full proc/
// storage key); after==nil starts from the beginning. Backs the paged
// ListProcessInstances fan-out (one namespace scan per shard).
func (t ProcessInstanceTable) ScanAllAfter(ctx context.Context, after []byte, fn func(service, instanceKey string, rec *enginev1.ProcessInstanceRecord) error) error {
	prefix := keys.ProcessPrefix()
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := scanStart(iter, after); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		k := iter.Key()
		if len(k) < len(prefix)+keys.LPLen {
			continue
		}
		body := k[len(prefix)+keys.LPLen:]
		before, rest, ok0 := bytes.Cut(body, []byte{'/'})
		if !ok0 {
			continue
		}
		rec := &enginev1.ProcessInstanceRecord{}
		if err := proto.Unmarshal(iter.Value(), rec); err != nil {
			continue
		}
		if err := fn(string(before), string(rest), rec); err != nil {
			return err
		}
	}
	return iter.Error()
}

// ScanLP iterates every instance in one logical partition in key order, decoding
// (service, instance_key) from each key. Bounded by the LP's instance count.
// Used by the cross-shard ListProcessInstances fan-out: the shard owning an LP
// scans only that LP's instances. Mirrors ScanAll's key decode.
//
// after, when non-nil, is a full proc/ storage key the scan resumes strictly
// past (the ListProcessInstances page cursor). Because the iterator is bounded
// to this LP's prefix range, an after key below the range scans from the start
// and one at/above the range yields nothing — so the same cursor can be passed
// to every LP of a shard and each resumes correctly (SeekGE clamps to bounds).
func (t ProcessInstanceTable) ScanLP(lp uint32, after []byte, fn func(service, instanceKey string, rec *enginev1.ProcessInstanceRecord) error) error {
	base := len(keys.ProcessPrefix()) + keys.LPLen
	prefix := keys.ProcessInstanceLPPrefix(lp)
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := scanStart(iter, after); ok; ok = iter.Next() {
		k := iter.Key()
		if len(k) < base {
			continue
		}
		before, after, ok0 := bytes.Cut(k[base:], []byte{'/'})
		if !ok0 {
			continue
		}
		rec := &enginev1.ProcessInstanceRecord{}
		if err := proto.Unmarshal(iter.Value(), rec); err != nil {
			continue
		}
		if err := fn(string(before), string(after), rec); err != nil {
			return err
		}
	}
	return iter.Error()
}
