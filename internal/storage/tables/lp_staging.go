package tables

import (
	"bytes"
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// LPStagingTable is the per-destination-partition record of every
// in-progress incoming LP transfer. Apply of ApplyLPTransferSST
// validates sst_seq == row.next_sst_seq (silently drops duplicates,
// which the source uses for retry) and bumps next_sst_seq. Dropped by
// CommitLPTransfer / AbortLPTransfer.
type LPStagingTable struct{ S storage.Reader }

// Get loads the staging row for a transfer. Returns (nil, nil) when the
// row is absent — the first SST arriving for a transfer goes through
// the "fresh" branch in the apply arm.
func (t LPStagingTable) Get(transferID string) (*enginev1.LPStagingRow, error) {
	var row enginev1.LPStagingRow
	if err := getProto(t.S, keys.LPStagingKey(transferID), &row); err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &row, nil
}

// Put writes a staging row.
func (t LPStagingTable) Put(b storage.Batch, row *enginev1.LPStagingRow) error {
	return putProto(b, keys.LPStagingKey(row.GetTransferId()), row)
}

// Delete drops a staging row.
func (t LPStagingTable) Delete(b storage.Batch, transferID string) error {
	return b.Delete(keys.LPStagingKey(transferID))
}

// All returns every staging row. Used by partition open to reap orphan
// `<dataDir>.lpstage_in/<transfer_id>/` directories whose transfer_id is
// not referenced by a live row.
func (t LPStagingTable) All(ctx context.Context) ([]*enginev1.LPStagingRow, error) {
	prefix := keys.LPStagingPrefix()
	iter, err := t.S.NewIter(prefix, keys.PrefixUpperBound(prefix))
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*enginev1.LPStagingRow
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(iter.Key(), prefix) {
			continue
		}
		var row enginev1.LPStagingRow
		if err := proto.Unmarshal(iter.Value(), &row); err != nil {
			return nil, err
		}
		out = append(out, &row)
	}
	return out, iter.Error()
}
