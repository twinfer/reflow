package engine

import (
	"bytes"
	"fmt"

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

// onApplyLPTransferChunk applies one chunk's TransferRows verbatim into
// the destination's Pebble batch and updates the LPStagingTable row.
// Validates chunk_seq monotonically — duplicates (seq < next_chunk_seq)
// and gaps (seq > next_chunk_seq) are dropped with a warning, leaving
// the source to retry. On is_final=true emits ActSignalLPTransferStaged
// so the runner routes phase=STAGED back to shard 0.
//
// Each row is written raw via batch.Set; the namespace_hint is a
// defensive sanity check (matches the row's key prefix) and surfaces
// any source-side bug that ships foreign-LP rows.
func (p *Partition) onApplyLPTransferChunk(
	batch storage.Batch,
	cmd *enginev1.ApplyLPTransferChunk,
	isLeader bool,
) error {
	if cmd.GetTransferId() == "" || cmd.GetLp() >= keys.LPCount {
		p.cfg.Log.Warn("partition: ApplyLPTransferChunk malformed; ignoring",
			"transfer_id", cmd.GetTransferId(),
			"lp", cmd.GetLp())
		return nil
	}
	stagingT := tables.LPStagingTable{S: batch}
	row, err := stagingT.Get(cmd.GetTransferId())
	if err != nil {
		return fmt.Errorf("onApplyLPTransferChunk: load staging: %w", err)
	}
	expectedSeq := uint64(0)
	if row != nil {
		expectedSeq = row.GetNextChunkSeq()
		if row.GetIsFinalSeen() {
			// Final chunk already applied — duplicate, drop.
			p.cfg.Log.Debug("partition: ApplyLPTransferChunk after final; dropping",
				"transfer_id", cmd.GetTransferId(),
				"chunk_seq", cmd.GetChunkSeq())
			return nil
		}
	}
	if cmd.GetChunkSeq() != expectedSeq {
		p.cfg.Log.Warn("partition: ApplyLPTransferChunk out of order; dropping",
			"transfer_id", cmd.GetTransferId(),
			"got_seq", cmd.GetChunkSeq(),
			"expected_seq", expectedSeq)
		return nil
	}
	// Write rows verbatim. The source is trusted (cross-shard delivery
	// over mTLS Connect; node-to-node SPIFFE auth) and the rows already
	// passed the source's encoding; namespace_hint is a sanity check
	// on the receiver to surface source-side bugs early.
	expectedLP := cmd.GetLp()
	for _, tr := range cmd.GetRows() {
		k := tr.GetKey()
		if len(k) == 0 {
			p.cfg.Log.Warn("partition: ApplyLPTransferChunk row missing key; skipping",
				"transfer_id", cmd.GetTransferId())
			continue
		}
		if !validateTransferRowLP(k, tr.GetNamespaceHint(), expectedLP) {
			p.cfg.Log.Warn("partition: ApplyLPTransferChunk row LP mismatch; dropping row",
				"transfer_id", cmd.GetTransferId(),
				"namespace_hint", tr.GetNamespaceHint().String(),
				"expected_lp", expectedLP)
			continue
		}
		if err := batch.Set(k, tr.GetValue()); err != nil {
			return fmt.Errorf("onApplyLPTransferChunk: write row: %w", err)
		}
	}
	if row == nil {
		row = &enginev1.LPStagingRow{
			TransferId:  cmd.GetTransferId(),
			Lp:          cmd.GetLp(),
			SourceShard: cmd.GetSourceShard(),
		}
	}
	row.NextChunkSeq = cmd.GetChunkSeq() + 1
	if cmd.GetIsFinal() {
		row.IsFinalSeen = true
	}
	if err := stagingT.Put(batch, row); err != nil {
		return fmt.Errorf("onApplyLPTransferChunk: write staging: %w", err)
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
// LP-prefixed namespace, scoped to one lp. The source-scan and the
// source-cleanup paths iterate this set; keep them in sync with
// keys.go's LP-prefixed namespace list.
func lpPrefixesForLP(lp uint32) [][]byte {
	return [][]byte{
		keys.InvocationLPPrefix(lp),
		keys.JournalLPPrefix(lp),
		keys.TimerIdxLPPrefix(lp),
		keys.TimerLPPrefixForLP(lp),
		keys.StateLPPrefix(lp),
		keys.AwakeableLPPrefix(lp),
		keys.KeyLeaseLPPrefix(lp),
		keys.IdempotencyLPPrefix(lp),
		keys.SignalInboxLPPrefix(lp),
		keys.SignalAwaiterLPPrefix(lp),
		keys.WorkflowRunLPPrefix(lp),
		keys.PromiseLPPrefix(lp),
		keys.PromiseAwaiterLPPrefix(lp),
	}
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
		_, fireAt, id, derr := keys.DecodeTimerLPKey(iter.Key())
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

// validateTransferRowLP checks that a row's key starts with a prefix
// matching its namespace_hint, scoped to the expected lp. A
// mismatch indicates a source-side bug shipping foreign-LP rows;
// returning false drops the row with a warning. Returns true for
// UNSPECIFIED + TIMER_PRIMARY (which is LP-agnostic).
func validateTransferRowLP(key []byte, hint enginev1.TransferNamespace, lp uint32) bool {
	switch hint {
	case enginev1.TransferNamespace_TRANSFER_NS_UNSPECIFIED:
		// No hint — trust the source's encoding.
		return true
	case enginev1.TransferNamespace_TRANSFER_NS_TIMER_PRIMARY:
		// timer/<fire>/<id> — not LP-prefixed. Verify only the
		// namespace, not the LP.
		return bytes.HasPrefix(key, keys.TimerPrefix())
	case enginev1.TransferNamespace_TRANSFER_NS_INV:
		return bytes.HasPrefix(key, keys.InvocationLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_JOURNAL:
		return bytes.HasPrefix(key, keys.JournalLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_TIMER_LP:
		return bytes.HasPrefix(key, keys.TimerLPPrefixForLP(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_TIMER_IDX:
		return bytes.HasPrefix(key, keys.TimerIdxLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_STATE:
		return bytes.HasPrefix(key, keys.StateLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_AWAKEABLE:
		return bytes.HasPrefix(key, keys.AwakeableLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_KEYLEASE:
		return bytes.HasPrefix(key, keys.KeyLeaseLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_IDEMP:
		return bytes.HasPrefix(key, keys.IdempotencyLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_SIGNAL_INBOX:
		return bytes.HasPrefix(key, keys.SignalInboxLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_SIGNAL_AWAITER:
		return bytes.HasPrefix(key, keys.SignalAwaiterLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_WORKFLOW_RUN:
		return bytes.HasPrefix(key, keys.WorkflowRunLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_PROMISE:
		return bytes.HasPrefix(key, keys.PromiseLPPrefix(lp))
	case enginev1.TransferNamespace_TRANSFER_NS_PROMISE_AWAITER:
		return bytes.HasPrefix(key, keys.PromiseAwaiterLPPrefix(lp))
	default:
		// Unknown future hint — accept; the source must be a newer binary.
		return true
	}
}
