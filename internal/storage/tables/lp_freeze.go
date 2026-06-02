package tables

import (
	"bytes"
	"context"
	"encoding/binary"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// LPFreezeTable is the per-partition control-plane index of which logical
// partitions are currently frozen for an in-progress LP transfer. The
// freeze gate in partition.go's apply path reads it on every LP-touching
// command and rejects the apply with ResultValueLPFrozen when the LP is
// present. Populated by BeginLPTransfer's apply arm; dropped by
// FinishLPTransfer / AbortLPTransfer.
type LPFreezeTable struct{ S storage.Reader }

// Get loads the freeze row for an LP. Returns (nil, nil) when the LP is
// not frozen — that's the steady-state hot path (one bloom-filter hit per
// LP-touching apply), so isNotFound is mapped to a clean nil rather than
// an error.
func (t LPFreezeTable) Get(lp uint32) (*enginev1.LPFreezeRow, error) {
	var row enginev1.LPFreezeRow
	if err := getProto(t.S, keys.LPFreezeKey(lp), &row); err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &row, nil
}

// Put writes a freeze row.
func (t LPFreezeTable) Put(b storage.Batch, lp uint32, row *enginev1.LPFreezeRow) error {
	return putProto(b, keys.LPFreezeKey(lp), row)
}

// Delete removes a freeze row. Delete-of-absent is a no-op.
func (t LPFreezeTable) Delete(b storage.Batch, lp uint32) error {
	return b.Delete(keys.LPFreezeKey(lp))
}

// List enumerates every frozen LP in the partition. Used by
// LPTransferService.Rebuild on leader gain to re-enqueue scan jobs
// for transfers that the previous leader started but didn't finish.
func (t LPFreezeTable) List(ctx context.Context) ([]LPFreezeEntry, error) {
	prefix := keys.LPFreezePrefix()
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []LPFreezeEntry
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		body := key[len(prefix):]
		if len(body) != keys.LPLen {
			continue
		}
		lp := binary.BigEndian.Uint32(body)
		var row enginev1.LPFreezeRow
		if err := proto.Unmarshal(iter.Value(), &row); err != nil {
			return nil, err
		}
		out = append(out, LPFreezeEntry{LP: lp, Row: &row})
	}
	return out, iter.Error()
}

// LPFreezeEntry is the decoded (lp, row) pair returned by List.
type LPFreezeEntry struct {
	LP  uint32
	Row *enginev1.LPFreezeRow
}
