package tables

import (
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// LPStagingTable is the per-destination-partition record of every
// in-progress incoming LP transfer. Apply of ApplyLPTransferChunk
// validates chunk_seq == row.next_chunk_seq (silently drops duplicates,
// which the source uses for retry) and bumps next_chunk_seq. Dropped by
// CommitLPTransfer / AbortLPTransfer.
type LPStagingTable struct{ S storage.Reader }

// Get loads the staging row for a transfer. Returns (nil, nil) when the
// row is absent — the first chunk arriving for a transfer goes through
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
