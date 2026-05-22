package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// lpTransferChunkBytes is the target proto-marshalled size at which the
// source-scan flushes a chunk. 256 KiB sits comfortably inside the
// Connect / dragonboat per-message budget and gives ~5 chunks per
// medium-sized LP at typical workload density. Not exact: the cutover
// happens AFTER the row crossing the threshold is packed in.
const lpTransferChunkBytes = 256 * 1024

// lpTransferAckProducer is the constant-prefix producerID stamped on
// shard-0 phase-ack proposals. The full producerID is composed as
// `lptransfer-ack/<partition_shard>/<transfer_id>/<phase>` so duplicate
// acks dedup naturally on shard 0 even though shard 0 doesn't yet
// import the dedup table — the apply-side monotonic phase check is
// what actually absorbs duplicates today.
const lpTransferAckProducer = "lptransfer-ack/"

// LPTransferService is the leader-only loop that handles cross-shard LP
// transfer orchestration on a partition shard. It reacts to four action
// kinds emitted by the apply path:
//
//   - ActStartLPTransferScan: open a snapshot view of every LP-prefixed
//     namespace under the named LP, pack rows into ~256 KiB chunks, and
//     ship each via CrossShardSender to the destination shard.
//   - ActSignalLPTransferStaged: dest-side signal that the is_final
//     chunk applied. Send UpdateLPTransferPhase{STAGED} to shard 0.
//   - ActSignalLPTransferCleaned: source-side signal that the LP
//     keyspace has been range-deleted. Send UpdateLPTransferPhase
//     {CLEANED} to shard 0.
//   - ActSignalLPTransferAbortAck: emitted by both sides on
//     AbortLPTransfer apply. Send UpdateLPTransferPhase{ABORTED} to
//     shard 0 — the monotonic apply rule absorbs the duplicate from
//     the other side.
//
// All four are processed FIFO from a single in-memory queue. Scan jobs
// are the heavy items; ack jobs are cheap (one CrossShardSender call).
// On Stop or ctx cancellation the loop returns; pending work is
// re-enqueued on the next leader's Rebuild via the durable
// LPFreezeTable / LPStagingTable rows.
type LPTransferService struct {
	store   storage.Store
	sender  CrossShardSender
	shardID uint64
	log     *slog.Logger

	mu      sync.Mutex
	pending []lpTransferJob

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

type lpTransferJob struct {
	kind        lpJobKind
	transferID  string
	lp          uint32
	destShard   uint64
	sourceShard uint64
}

type lpJobKind uint8

const (
	lpJobScan lpJobKind = iota
	lpJobAckStaged
	lpJobAckCleaned
	lpJobAckAbort
)

// NewLPTransferService constructs the per-partition LP transfer
// orchestration loop. sender may be nil in single-node deployments —
// every transfer in that case is same-shard and the service short-
// circuits its sends with a warning (real transfers require a multi-
// node sender).
func NewLPTransferService(store storage.Store, sender CrossShardSender, shardID uint64, log *slog.Logger) *LPTransferService {
	if log == nil {
		log = slog.Default()
	}
	return &LPTransferService{
		store:   store,
		sender:  sender,
		shardID: shardID,
		log:     log,
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// PushScan enqueues a scan-and-ship job. Called inline from the
// runner's dispatchActions handler for ActStartLPTransferScan.
func (s *LPTransferService) PushScan(transferID string, lp uint32, destShard uint64) {
	s.push(lpTransferJob{kind: lpJobScan, transferID: transferID, lp: lp, destShard: destShard})
}

// PushAckStaged enqueues an "is_final chunk applied" phase-ack to
// shard 0. Called by the dest-side runner on ActSignalLPTransferStaged.
func (s *LPTransferService) PushAckStaged(transferID string, lp uint32, sourceShard uint64) {
	s.push(lpTransferJob{
		kind:        lpJobAckStaged,
		transferID:  transferID,
		lp:          lp,
		sourceShard: sourceShard,
	})
}

// PushAckCleaned enqueues a source-side "LP cleanup complete" phase
// ack to shard 0.
func (s *LPTransferService) PushAckCleaned(transferID string) {
	s.push(lpTransferJob{kind: lpJobAckCleaned, transferID: transferID})
}

// PushAckAbort enqueues an "abort rollback complete" phase ack to
// shard 0 (sent by both source and dest sides; monotonic apply absorbs
// the duplicate).
func (s *LPTransferService) PushAckAbort(transferID string) {
	s.push(lpTransferJob{kind: lpJobAckAbort, transferID: transferID})
}

func (s *LPTransferService) push(job lpTransferJob) {
	s.mu.Lock()
	s.pending = append(s.pending, job)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Rebuild scans the partition's LPFreezeTable + LPStagingTable on
// leader gain and re-enqueues every in-flight transfer:
//   - Every freeze row → scan job (the source re-ships from chunk_seq=0;
//     the dest's LPStagingTable.next_chunk_seq dedups). Acks are not
//     re-enqueued here because they're durable on shard 0 already; if
//     the previous leader's ack didn't land, the lpMover's stall
//     detection will retry the side-effect that produced it.
func (s *LPTransferService) Rebuild(ctx context.Context) error {
	freezeT := tables.LPFreezeTable{S: s.store}
	entries, err := freezeT.List(ctx)
	if err != nil {
		return fmt.Errorf("lp transfer rebuild: list freezes: %w", err)
	}
	for _, e := range entries {
		s.PushScan(e.Row.GetTransferId(), e.LP, e.Row.GetDestShard())
	}
	return nil
}

// Run processes the queue until ctx cancellation or Stop. Each job is
// processed in order; retryable errors keep the job at the head with a
// brief backoff. Cancellation surfaces as nil after a graceful drain
// attempt.
func (s *LPTransferService) Run(ctx context.Context) error {
	defer close(s.done)
	for {
		s.mu.Lock()
		batch := s.pending
		s.pending = nil
		s.mu.Unlock()

		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stop:
				return nil
			case <-s.wake:
			}
			continue
		}

		var failed []lpTransferJob
		for _, job := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			switch job.kind {
			case lpJobScan:
				err = s.runScan(ctx, job)
			case lpJobAckStaged:
				err = s.sendAck(ctx, job.transferID, enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED, "staged")
			case lpJobAckCleaned:
				err = s.sendAck(ctx, job.transferID, enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED, "cleaned")
			case lpJobAckAbort:
				err = s.sendAck(ctx, job.transferID, enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTED, "abort")
			}
			if err == nil {
				continue
			}
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			s.log.Warn("lp transfer service: job failed; will retry",
				"kind", lpJobKindLabel(job.kind),
				"transfer_id", job.transferID,
				"err", err)
			failed = append(failed, job)
		}

		if len(failed) > 0 {
			s.mu.Lock()
			// Failed jobs go to the FRONT so saga order is preserved.
			s.pending = append(failed, s.pending...)
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stop:
				return nil
			case <-time.After(retryBackoff()):
			}
		}
	}
}

// Stop signals Run to return. Idempotent.
func (s *LPTransferService) Stop() {
	select {
	case <-s.stop:
		return
	default:
	}
	close(s.stop)
}

// Done returns a channel closed when Run has returned.
func (s *LPTransferService) Done() <-chan struct{} { return s.done }

// pendingLen returns the in-memory queue length. Tests use this to
// assert drain progress.
func (s *LPTransferService) pendingLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

func (s *LPTransferService) sendAck(ctx context.Context, transferID string, phase enginev1.LPTransferPhase, phaseLabel string) error {
	if s.sender == nil {
		return errors.New("lp transfer ack: no CrossShardSender configured")
	}
	if transferID == "" {
		return errors.New("lp transfer ack: empty transfer_id")
	}
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_UpdateLpTransferPhase{
			UpdateLpTransferPhase: &enginev1.UpdateLPTransferPhase{
				TransferId: transferID,
				Phase:      phase,
			},
		},
	}
	producerID := fmt.Sprintf("%s%d/%s/%s", lpTransferAckProducer, s.shardID, transferID, phaseLabel)
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.sender.Send(sendCtx, 0, producerID, 0, cmd)
}

func (s *LPTransferService) runScan(ctx context.Context, job lpTransferJob) error {
	if s.sender == nil {
		return errors.New("lp transfer scan: no CrossShardSender configured")
	}
	scanner := lpScanner{
		store:      s.store,
		log:        s.log,
		transferID: job.transferID,
		lp:         job.lp,
		destShard:  job.destShard,
		sourceID:   s.shardID,
		sender:     s.sender,
	}
	return scanner.run(ctx)
}

// lpScanner walks every LP-prefixed namespace under lp, packs rows
// into chunks, and ships each via CrossShardSender. Stateless across
// runs — a re-enqueued scan starts from chunk_seq=0; the dest's
// LPStagingTable.next_chunk_seq dedups any chunks the previous attempt
// already landed.
type lpScanner struct {
	store      storage.Store
	log        *slog.Logger
	transferID string
	lp         uint32
	destShard  uint64
	sourceID   uint64
	sender     CrossShardSender

	chunkSeq    uint64
	currentRows []*enginev1.TransferRow
	currentSize int
}

func (sc *lpScanner) run(ctx context.Context) error {
	// LP-prefixed namespaces (lower-bound prefix + matching
	// TransferNamespace hint). Order matters only for the size budget;
	// the receiver's apply arm writes rows raw via batch.Set so the
	// destination's Pebble store sees the same effective state once all
	// chunks land.
	namespaces := []struct {
		hint   enginev1.TransferNamespace
		prefix []byte
	}{
		{enginev1.TransferNamespace_TRANSFER_NS_INV, keys.InvocationLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_JOURNAL, keys.JournalLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_TIMER_IDX, keys.TimerIdxLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_TIMER_LP, keys.TimerLPPrefixForLP(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_STATE, keys.StateLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_AWAKEABLE, keys.AwakeableLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_KEYLEASE, keys.KeyLeaseLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_IDEMP, keys.IdempotencyLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_SIGNAL_INBOX, keys.SignalInboxLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_SIGNAL_AWAITER, keys.SignalAwaiterLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_WORKFLOW_RUN, keys.WorkflowRunLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_PROMISE, keys.PromiseLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_PROMISE_AWAITER, keys.PromiseAwaiterLPPrefix(sc.lp)},
		{enginev1.TransferNamespace_TRANSFER_NS_DEDUP_ARBITRARY, keys.DedupArbitraryLPPrefix(sc.lp)},
	}
	for _, ns := range namespaces {
		if err := sc.scanNamespace(ctx, ns.hint, ns.prefix); err != nil {
			return err
		}
	}
	// Walk timer_lp/<lp>/... a second time, this time to extract the
	// LP-agnostic primary `timer/<fire>/<id>` rows. Done after the main
	// scan because we need the secondary index to enumerate the primary
	// keys (the primary namespace is shared across LPs).
	if err := sc.scanLPTimers(ctx); err != nil {
		return err
	}
	// Final flush carries is_final=true. Always send a final chunk,
	// even if empty, so the dest knows the LP is fully shipped.
	return sc.flush(ctx, true)
}

func (sc *lpScanner) scanNamespace(ctx context.Context, hint enginev1.TransferNamespace, prefix []byte) error {
	upper := keys.PrefixUpperBound(prefix)
	iter, err := sc.store.NewIter(prefix, upper)
	if err != nil {
		return fmt.Errorf("lp scan: open iter for %q: %w", prefix, err)
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		k := append([]byte(nil), iter.Key()...)
		v := append([]byte(nil), iter.Value()...)
		sc.appendRow(k, v, hint)
		if sc.currentSize >= lpTransferChunkBytes {
			if err := sc.flush(ctx, false); err != nil {
				return err
			}
		}
	}
	return iter.Error()
}

func (sc *lpScanner) scanLPTimers(ctx context.Context) error {
	lower := keys.TimerLPPrefixForLP(sc.lp)
	upper := keys.PrefixUpperBound(lower)
	iter, err := sc.store.NewIter(lower, upper)
	if err != nil {
		return fmt.Errorf("lp scan timers: open iter: %w", err)
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, fireAt, id, derr := keys.DecodeTimerLPKey(iter.Key())
		if derr != nil {
			return fmt.Errorf("lp scan timers: decode key: %w", derr)
		}
		primaryKey, perr := keys.TimerKey(fireAt, id)
		if perr != nil {
			return fmt.Errorf("lp scan timers: encode primary key: %w", perr)
		}
		val, closer, gerr := sc.store.Get(primaryKey)
		if gerr != nil {
			if errors.Is(gerr, storage.ErrNotFound) {
				// timer_lp without a matching primary — log + skip.
				sc.log.Warn("lp scan timers: orphan timer_lp row; skipping",
					"transfer_id", sc.transferID, "lp", sc.lp,
					"fire_at_ms", fireAt)
				continue
			}
			return fmt.Errorf("lp scan timers: get primary: %w", gerr)
		}
		v := append([]byte(nil), val...)
		closer.Close()
		sc.appendRow(primaryKey, v, enginev1.TransferNamespace_TRANSFER_NS_TIMER_PRIMARY)
		if sc.currentSize >= lpTransferChunkBytes {
			if err := sc.flush(ctx, false); err != nil {
				return err
			}
		}
	}
	return iter.Error()
}

func (sc *lpScanner) appendRow(k, v []byte, hint enginev1.TransferNamespace) {
	row := &enginev1.TransferRow{Key: k, Value: v, NamespaceHint: hint}
	sc.currentRows = append(sc.currentRows, row)
	sc.currentSize += len(k) + len(v)
}

func (sc *lpScanner) flush(ctx context.Context, isFinal bool) error {
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_ApplyLpTransferChunk{
			ApplyLpTransferChunk: &enginev1.ApplyLPTransferChunk{
				TransferId:  sc.transferID,
				Lp:          sc.lp,
				SourceShard: sc.sourceID,
				ChunkSeq:    sc.chunkSeq,
				Rows:        sc.currentRows,
				IsFinal:     isFinal,
			},
		},
	}
	producerID := fmt.Sprintf("lptransfer/%s/%d", sc.transferID, sc.sourceID)
	sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := sc.sender.Send(sendCtx, sc.destShard, producerID, sc.chunkSeq, cmd); err != nil {
		return fmt.Errorf("lp scan: ship chunk %d (final=%t): %w", sc.chunkSeq, isFinal, err)
	}
	if sc.log != nil {
		sz := proto.Size(cmd)
		sc.log.Debug("lp scan: shipped chunk",
			"transfer_id", sc.transferID, "lp", sc.lp,
			"chunk_seq", sc.chunkSeq, "rows", len(sc.currentRows),
			"bytes", sz, "is_final", isFinal)
	}
	sc.chunkSeq++
	sc.currentRows = nil
	sc.currentSize = 0
	return nil
}

func lpJobKindLabel(k lpJobKind) string {
	switch k {
	case lpJobScan:
		return "scan"
	case lpJobAckStaged:
		return "ack_staged"
	case lpJobAckCleaned:
		return "ack_cleaned"
	case lpJobAckAbort:
		return "ack_abort"
	default:
		return "unknown"
	}
}
