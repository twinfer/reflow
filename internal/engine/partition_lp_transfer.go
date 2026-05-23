package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// onBeginLPTransfer installs the freeze row for the named LP and emits
// an ActStartLPTransferScan so the leader-side LPTransferSourceService
// opens a snapshot and starts shipping chunks. Idempotent on retry: a
// second BeginLPTransfer for the same lp+transfer_id is a no-op (the
// existing row is preserved, the action is still emitted so the source
// service rebuild on leader gain converges).
func (p *Partition) onBeginLPTransfer(
	batch storage.Batch,
	cmd *enginev1.BeginLPTransfer,
	nowMs uint64,
	isLeader bool,
) error {
	if cmd.GetTransferId() == "" || cmd.GetDestShard() == 0 || cmd.GetLp() >= keys.LPCount {
		p.cfg.Log.Warn("partition: BeginLPTransfer malformed; ignoring",
			"transfer_id", cmd.GetTransferId(),
			"lp", cmd.GetLp(),
			"dest_shard", cmd.GetDestShard())
		return nil
	}
	row := &enginev1.LPFreezeRow{
		TransferId: cmd.GetTransferId(),
		DestShard:  cmd.GetDestShard(),
		FrozenAtMs: nowMs,
	}
	if err := (tables.LPFreezeTable{S: batch}).Put(batch, cmd.GetLp(), row); err != nil {
		return fmt.Errorf("onBeginLPTransfer: write freeze: %w", err)
	}
	if isLeader {
		p.cfg.Collector.Push(ActStartLPTransferScan{
			TransferID: cmd.GetTransferId(),
			LP:         cmd.GetLp(),
			DestShard:  cmd.GetDestShard(),
		})
	}
	return nil
}

// onApplyLPTransferSST is the dest-side arm for SST-based LP transfer
// shipping. Validates sst_seq == staging.next_sst_seq, resolves each
// TransferSSTRef.relative_path against `<dataDir>.lpstage_in/<transfer_id>/`,
// and calls pebble.DB.Ingest for the path set. On is_final=true emits
// ActSignalLPTransferStaged so the runner routes phase=STAGED back to
// shard 0.
//
// Determinism: every replica receives the same command via Raft, and the
// source uploads the SSTs to every replica before proposing — so the
// Ingest call here is deterministic across replicas. If the local file
// is missing (replica hasn't received the upload yet), we skip the
// Ingest + bookkeeping update so the source's retry replays the chunk
// from sst_seq=0. The freeze gate on source means no concurrent writes
// race the dest's pre-Ingest state.
func (p *Partition) onApplyLPTransferSST(
	batch storage.Batch,
	store storage.Store,
	cmd *enginev1.ApplyLPTransferSST,
	isLeader bool,
) error {
	if cmd.GetTransferId() == "" || cmd.GetLp() >= keys.LPCount {
		p.cfg.Log.Warn("partition: ApplyLPTransferSST malformed; ignoring",
			"transfer_id", cmd.GetTransferId(),
			"lp", cmd.GetLp())
		return nil
	}
	stagingT := tables.LPStagingTable{S: batch}
	row, err := stagingT.Get(cmd.GetTransferId())
	if err != nil {
		return fmt.Errorf("onApplyLPTransferSST: load staging: %w", err)
	}
	expectedSeq := uint64(0)
	if row != nil {
		expectedSeq = row.GetNextSstSeq()
		if row.GetIsFinalSeen() {
			// Final SST already applied — duplicate, drop.
			p.cfg.Log.Debug("partition: ApplyLPTransferSST after final; dropping",
				"transfer_id", cmd.GetTransferId(),
				"sst_seq", cmd.GetSstSeq())
			return nil
		}
	}
	if cmd.GetSstSeq() != expectedSeq {
		p.cfg.Log.Warn("partition: ApplyLPTransferSST out of order; dropping",
			"transfer_id", cmd.GetTransferId(),
			"got_seq", cmd.GetSstSeq(),
			"expected_seq", expectedSeq)
		return nil
	}

	if len(cmd.GetSsts()) > 0 {
		ingestStart := time.Now()
		if err := ingestLPTransferSSTs(store, cmd); err != nil {
			// Local file missing or Ingest failed — leave the staging row
			// untouched so the source's next scan re-uploads + re-proposes
			// from sst_seq=0. Returning nil keeps the shard running.
			p.cfg.Log.Warn("partition: ApplyLPTransferSST Ingest failed; skipping bookkeeping",
				"transfer_id", cmd.GetTransferId(),
				"sst_seq", cmd.GetSstSeq(),
				"err", err)
			return nil
		}
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.LPTransferSSTIngestSeconds.Observe(time.Since(ingestStart).Seconds())
		}
	}

	if row == nil {
		row = &enginev1.LPStagingRow{
			TransferId:  cmd.GetTransferId(),
			Lp:          cmd.GetLp(),
			SourceShard: cmd.GetSourceShard(),
		}
	}
	row.NextSstSeq = cmd.GetSstSeq() + 1
	if cmd.GetIsFinal() {
		row.IsFinalSeen = true
	}
	if err := stagingT.Put(batch, row); err != nil {
		return fmt.Errorf("onApplyLPTransferSST: write staging: %w", err)
	}
	if isLeader && cmd.GetIsFinal() {
		p.cfg.Collector.Push(ActSignalLPTransferStaged{
			TransferID:  cmd.GetTransferId(),
			LP:          cmd.GetLp(),
			SourceShard: cmd.GetSourceShard(),
		})
	}
	return nil
}

// ingestLPTransferSSTs resolves cmd.Ssts to absolute paths under
// <dataDir>.lpstage_in/<transfer_id>/ and calls pebble.DB.Ingest. Path
// traversal is rejected; missing files surface as an error so the
// caller can skip bookkeeping and let the source retry.
func ingestLPTransferSSTs(store storage.Store, cmd *enginev1.ApplyLPTransferSST) error {
	pstore, ok := store.(*storage.PebbleStore)
	if !ok {
		return errors.New("non-Pebble store cannot host LP-transfer SST shipping")
	}
	stageDir := pstore.DataDir() + ".lpstage_in"
	transferDir := filepath.Join(stageDir, cmd.GetTransferId())
	paths := make([]string, 0, len(cmd.GetSsts()))
	for _, ref := range cmd.GetSsts() {
		rel := ref.GetRelativePath()
		if rel == "" || strings.ContainsAny(rel, "/\\") || strings.Contains(rel, "..") {
			return fmt.Errorf("invalid relative_path %q", rel)
		}
		full := filepath.Join(transferDir, rel)
		if _, err := os.Stat(full); err != nil {
			return fmt.Errorf("staged sst missing: %w", err)
		}
		paths = append(paths, full)
	}
	return pstore.IngestSSTs(context.Background(), paths)
}

// onCommitLPTransfer drops the destination's staging row after the
// LPOwnersTable flip succeeds. The staged data was already written into
// the live LP namespaces by ApplyLPTransferChunk, so there is no
// data-mutation step here — only bookkeeping.
func (p *Partition) onCommitLPTransfer(
	batch storage.Batch,
	cmd *enginev1.CommitLPTransfer,
) error {
	if cmd.GetTransferId() == "" {
		return nil
	}
	if err := (tables.LPStagingTable{S: batch}).Delete(batch, cmd.GetTransferId()); err != nil {
		return fmt.Errorf("onCommitLPTransfer: delete staging: %w", err)
	}
	return nil
}

// onFinishLPTransfer range-deletes every LP-prefixed namespace for the
// transferred lp on the source partition, walks timer_lp to delete the
// LP-agnostic primary timer rows, and drops the LPFreezeTable row.
// Emits ActSignalLPTransferCleaned so the runner routes phase=CLEANED
// back to shard 0.
//
// Idempotent on retry: a second FinishLPTransfer for the same lp finds
// no LP-prefixed keys (already deleted), nothing in timer_lp, no freeze
// row, and emits the signal again — the lpMover's ack-keyed outbox
// absorbs the duplicate.
func (p *Partition) onFinishLPTransfer(
	batch storage.Batch,
	cmd *enginev1.FinishLPTransfer,
	isLeader bool,
) error {
	if cmd.GetTransferId() == "" || cmd.GetLp() >= keys.LPCount {
		p.cfg.Log.Warn("partition: FinishLPTransfer malformed; ignoring",
			"transfer_id", cmd.GetTransferId(),
			"lp", cmd.GetLp())
		return nil
	}
	lp := cmd.GetLp()
	// 1. Walk timer_lp to collect (fire_at, id) pairs, then delete the
	// LP-agnostic primary `timer/<fire>/<id>` rows.
	if err := deleteLPTimers(batch, lp); err != nil {
		return fmt.Errorf("onFinishLPTransfer: timer cleanup: %w", err)
	}
	// 2. Range-delete every LP-prefixed namespace.
	for _, prefix := range lpPrefixesForLP(lp) {
		upper := keys.PrefixUpperBound(prefix)
		if err := batch.DeleteRange(prefix, upper); err != nil {
			return fmt.Errorf("onFinishLPTransfer: range-delete prefix %q: %w", prefix, err)
		}
	}
	// 3. Drop the freeze row.
	if err := (tables.LPFreezeTable{S: batch}).Delete(batch, lp); err != nil {
		return fmt.Errorf("onFinishLPTransfer: delete freeze: %w", err)
	}
	if isLeader {
		p.cfg.Collector.Push(ActSignalLPTransferCleaned{TransferID: cmd.GetTransferId()})
	}
	return nil
}

// onAbortLPTransfer rolls back partial state on either side of an
// aborted transfer. On the source: drop the freeze row (writes resume;
// the lpMover keeps the LPOwners row pointing at source). On the dest:
// range-delete every LP-prefixed namespace (any chunks that landed
// before abort) and drop the staging row.
//
// Symmetric: the same command is sent to both sides; the apply path
// detects which side it is by checking which of the two rows exists
// (LPFreezeTable on the source, LPStagingTable on the dest).
func (p *Partition) onAbortLPTransfer(
	batch storage.Batch,
	cmd *enginev1.AbortLPTransfer,
	isLeader bool,
) error {
	if cmd.GetTransferId() == "" || cmd.GetLp() >= keys.LPCount {
		return nil
	}
	lp := cmd.GetLp()
	freezeT := tables.LPFreezeTable{S: batch}
	stagingT := tables.LPStagingTable{S: batch}
	freezeRow, err := freezeT.Get(lp)
	if err != nil {
		return fmt.Errorf("onAbortLPTransfer: load freeze: %w", err)
	}
	stagingRow, err := stagingT.Get(cmd.GetTransferId())
	if err != nil {
		return fmt.Errorf("onAbortLPTransfer: load staging: %w", err)
	}
	if freezeRow != nil && freezeRow.GetTransferId() == cmd.GetTransferId() {
		// Source side: drop freeze; writes resume immediately.
		if err := freezeT.Delete(batch, lp); err != nil {
			return fmt.Errorf("onAbortLPTransfer: delete freeze: %w", err)
		}
	}
	if stagingRow != nil {
		// Dest side: scrub any chunks that landed in the LP keyspace
		// before the abort applied, then drop the staging row.
		if err := deleteLPTimers(batch, lp); err != nil {
			return fmt.Errorf("onAbortLPTransfer: timer cleanup: %w", err)
		}
		for _, prefix := range lpPrefixesForLP(lp) {
			upper := keys.PrefixUpperBound(prefix)
			if err := batch.DeleteRange(prefix, upper); err != nil {
				return fmt.Errorf("onAbortLPTransfer: range-delete prefix %q: %w", prefix, err)
			}
		}
		if err := stagingT.Delete(batch, cmd.GetTransferId()); err != nil {
			return fmt.Errorf("onAbortLPTransfer: delete staging: %w", err)
		}
	}
	if isLeader {
		p.cfg.Collector.Push(ActSignalLPTransferAbortAck{TransferID: cmd.GetTransferId()})
	}
	return nil
}

// lpPrefixesForLP returns the lower-bound byte prefix for every
// LP-prefixed namespace, scoped to one lp. Both the source-scan
// (buildLPSSTs) and the source-cleanup (onFinishLPTransfer /
// onAbortLPTransfer) iterate keys.AllLPNamespaces — same source of
// truth, so a new namespace cannot land in keys.go without being
// covered by both sides of the transfer.
func lpPrefixesForLP(lp uint32) [][]byte {
	out := make([][]byte, 0, len(keys.AllLPNamespaces))
	for _, ns := range keys.AllLPNamespaces {
		out = append(out, ns.Prefix(lp))
	}
	return out
}

// deleteLPTimers walks timer_lp/<lp>/... to collect (fire_at, id)
// pairs, then deletes each matching primary timer/<fire>/<id> row.
// The timer_lp rows themselves are reaped by the timer_lp prefix
// range-delete in onFinishLPTransfer / onAbortLPTransfer.
func deleteLPTimers(batch storage.Batch, lp uint32) error {
	lower := keys.TimerLPPrefixForLP(lp)
	upper := keys.PrefixUpperBound(lower)
	iter, err := batch.NewIter(lower, upper)
	if err != nil {
		return fmt.Errorf("deleteLPTimers: scan timer_lp: %w", err)
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		_, _, fireAt, id, derr := keys.DecodeTimerLPKey(iter.Key())
		if derr != nil {
			return fmt.Errorf("deleteLPTimers: decode timer_lp key: %w", derr)
		}
		primary, perr := keys.TimerKey(fireAt, id)
		if perr != nil {
			return fmt.Errorf("deleteLPTimers: encode primary key: %w", perr)
		}
		if err := batch.Delete(primary); err != nil {
			return fmt.Errorf("deleteLPTimers: delete primary timer: %w", err)
		}
	}
	return iter.Error()
}
