package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

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

// runScan ships the LP's data to the destination as SST files via the
// upload RPC, then proposes a single ApplyLPTransferSST command on the
// dest shard so the apply arm Ingests the files.
//
// Pending wire-up in a follow-up PR: this stub builds the SSTs (via
// buildLPSSTs) so the source-side disk path is exercised, but does NOT
// upload them or propose the command — those need
// Client.UploadLPTransferSST + the dest's apply arm, both stubbed in
// this PR. The freeze gate on source and the LPStagingTable on dest
// keep the saga safe in the meantime; the lpMover's stall detection
// will transition stuck transfers to ABORTING after lpMoverStallTimeout.
func (s *LPTransferService) runScan(ctx context.Context, job lpTransferJob) error {
	if s.sender == nil {
		return errors.New("lp transfer scan: no CrossShardSender configured")
	}
	pstore, ok := s.store.(*storage.PebbleStore)
	if !ok {
		return errors.New("lp transfer scan: store is not *PebbleStore (SST shipping requires Pebble)")
	}
	// outDir is per-transfer so retries don't collide; the host-level
	// startup cleanup reaps orphans whose transfer_id is absent from
	// LPFreezeTable.
	outDir := fmt.Sprintf("%s.lpstage_out/%s", pstore.DataDir(), job.transferID)
	refs, err := buildLPSSTs(ctx, pstore, job.lp, outDir)
	if err != nil {
		return fmt.Errorf("lp scan: build SSTs: %w", err)
	}
	s.log.Warn("lp transfer scan: SST upload + propose not yet implemented in this PR; built local SSTs only",
		"transfer_id", job.transferID, "lp", job.lp, "ssts", len(refs))
	return errors.New("lp transfer scan: SST shipping wire pending follow-up PR")
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
