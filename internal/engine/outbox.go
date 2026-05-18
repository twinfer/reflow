package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// OutboxProducerPrefix is the producerID prefix the outbox shuffler stamps
// onto its ProposeIngress envelopes. Apply-path checks for this prefix to
// know whether to pop an outbox row when the receiving command commits.
const OutboxProducerPrefix = "outbox/"

// IngressProposer is the subset of RaftProposer that the OutboxService uses
// for re-injecting outbox envelopes. Carved out so tests can substitute a
// fake without dragging dragonboat into the unit test path.
type IngressProposer interface {
	ProposeIngress(ctx context.Context, producerID string, seq uint64, cmd *enginev1.Command) error
}

// CrossShardSender dispatches an outbox envelope to a remote shard's
// leader via the Delivery gRPC service. Returns ErrNotLeader (from package
// delivery) when the receiver claims it is not the destination shard's
// leader; the OutboxService retries on the next Rebuild.
//
// The durable pop of the producer-side row happens via the reciprocal
// OutboxAck that flows back over Raft after the receiver applies — the
// Sender returning nil only means "the receiver accepted responsibility
// for proposing", not "row is popped".
type CrossShardSender interface {
	Send(ctx context.Context, destShardID uint64, producerID string, seq uint64, cmd *enginev1.Command) error
}

// OutboxService is the leader-only loop that drains the OutboxTable and
// re-injects each row through the Raft log as a fresh Command. In the
// single-partition case, sender and receiver are the same shard, so the
// receiver's apply path pops the row in the same batch it applies.
//
// Crash-safety: SyncPropose returning success means the receiver applied,
// so the row is popped already. SyncPropose returning error means the
// receiver did NOT apply and the row is still present — the next leader
// scans the table on Rebuild and re-proposes. The Arbitrary dedup
// (producerID + seq) makes a redundant propose a no-op if the first one
// actually committed.
//
// Mirrors restate crates/worker/src/partition/leadership/mod.rs:148-154
// (timer pattern), with the propose-then-receiver-pop pattern from
// crates/storage-api/src/outbox_table/mod.rs.
type OutboxService struct {
	table      tables.OutboxTable
	proposer   IngressProposer
	sender     CrossShardSender // optional; same-shard-only when nil
	shardID    uint64
	producerID string
	log        *slog.Logger

	mu      sync.Mutex
	pending []tables.OutboxRow

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

// NewOutboxService constructs the shuffler. shardID participates in the
// producerID so multi-partition deployments don't collide on dedup keys.
// sender may be nil for single-node deployments — every envelope's
// destination_shard_id resolves to the local shard so no cross-shard
// dispatch ever runs.
func NewOutboxService(table tables.OutboxTable, proposer IngressProposer, sender CrossShardSender, shardID uint64, log *slog.Logger) *OutboxService {
	if log == nil {
		log = slog.Default()
	}
	return &OutboxService{
		table:      table,
		proposer:   proposer,
		sender:     sender,
		shardID:    shardID,
		producerID: fmt.Sprintf("%sp%d", OutboxProducerPrefix, shardID),
		log:        log,
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// ProducerID returns the producer identifier stamped on the dedup header
// of every outbox proposal. Tests and the apply-path pop logic key off it.
func (o *OutboxService) ProducerID() string { return o.producerID }

// Rebuild scans the persistent OutboxTable into the in-memory queue. Called
// on leader gain; replaces any previous in-memory state.
func (o *OutboxService) Rebuild() error {
	var loaded []tables.OutboxRow
	err := o.table.ScanFrom(0, func(row tables.OutboxRow) error {
		loaded = append(loaded, row)
		return nil
	})
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.pending = loaded
	o.mu.Unlock()
	o.signalWake()
	return nil
}

// Push enqueues a freshly-appended outbox row. Called inline from the
// runner's dispatchActions handler for ActDispatchOutbox.
func (o *OutboxService) Push(seq uint64, env *enginev1.OutboxEnvelope) {
	o.mu.Lock()
	o.pending = append(o.pending, tables.OutboxRow{Seq: seq, Envelope: env})
	o.mu.Unlock()
	o.signalWake()
}

// Run drains pending rows, proposing each through ProposeIngress until
// ctx is canceled or Stop is called. On propose failure the row is
// re-queued at the head and the loop backs off briefly before retrying.
func (o *OutboxService) Run(ctx context.Context) error {
	defer close(o.done)
	for {
		o.mu.Lock()
		batch := o.pending
		o.pending = nil
		o.mu.Unlock()

		if len(batch) == 0 {
			// No work; wait for wake / stop / cancel.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-o.stop:
				return nil
			case <-o.wake:
			}
			continue
		}

		var failed []tables.OutboxRow
		for _, row := range batch {
			cmd := outboxEnvelopeToCommand(row.Envelope)
			if cmd == nil {
				// Unknown envelope kind — propose an empty command through
				// our own ProducerID/seq so the apply path's
				// arbitrary-dedup-on-receive arm pops the wedged row.
				// Without this the row sits forever and Rebuild on every
				// leader gain re-queues it.
				o.log.Error("outbox: unknown envelope kind; popping wedged row",
					"seq", row.Seq, "envelope", fmt.Sprintf("%T", row.Envelope.GetKind()))
				propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := o.proposer.ProposeIngress(propCtx, o.producerID, row.Seq, &enginev1.Command{})
				cancel()
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return ctx.Err()
					}
					o.log.Warn("outbox: pop-wedge propose failed; will retry",
						"seq", row.Seq, "err", err)
					failed = append(failed, row)
				}
				continue
			}
			destShard := row.Envelope.GetDestinationShardId()
			sameShard := destShard == 0 || destShard == o.shardID

			propCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			var err error
			if sameShard {
				// Same-shard: re-inject through the local Raft proposer.
				// The apply-path arbitrary-dedup pop removes the row in
				// the same Pebble batch the command applies.
				err = o.proposer.ProposeIngress(propCtx, o.producerID, row.Seq, cmd)
			} else if o.sender == nil {
				err = fmt.Errorf("outbox: cross-shard envelope to %d but no Sender configured", destShard)
			} else {
				// Cross-shard: ship to the destination shard's leader.
				// The producer-side row is NOT popped here — wait for an
				// OutboxAck via Raft (apply emits one on the receiver).
				err = o.sender.Send(propCtx, destShard, o.producerID, row.Seq, cmd)
			}
			cancel()
			if err == nil {
				continue
			}
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			o.log.Warn("outbox: dispatch failed; re-queueing",
				"seq", row.Seq, "dest_shard", destShard, "same_shard", sameShard, "err", err)
			failed = append(failed, row)
		}

		if len(failed) > 0 {
			o.mu.Lock()
			// Failed rows go to the FRONT so seq order is preserved on retry.
			o.pending = append(failed, o.pending...)
			o.mu.Unlock()
			// Brief backoff (with jitter, see retryBackoff) before
			// retrying so transient errors don't spin and so simultaneous
			// leader gains across shards don't synchronize their retries.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-o.stop:
				return nil
			case <-time.After(retryBackoff()):
			}
		}
	}
}

// Stop signals Run to return. Idempotent.
func (o *OutboxService) Stop() {
	select {
	case <-o.stop:
		return
	default:
	}
	close(o.stop)
}

// Done returns a channel closed when Run has returned.
func (o *OutboxService) Done() <-chan struct{} { return o.done }

func (o *OutboxService) signalWake() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// pendingLen returns the in-memory queue length. Tests use this to assert
// drain progress.
func (o *OutboxService) pendingLen() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.pending)
}

// outboxEnvelopeToCommand reshapes an OutboxEnvelope into the Command kind
// the receiver's apply path consumes. Returns nil for unknown variants.
// DeliverCallResult and OutboxAck are shipped as Command oneof variants
// of their own — they only land on shards that own a partition state
// machine, never on shard 0.
func outboxEnvelopeToCommand(env *enginev1.OutboxEnvelope) *enginev1.Command {
	switch k := env.GetKind().(type) {
	case *enginev1.OutboxEnvelope_Invoke:
		return &enginev1.Command{
			Kind: &enginev1.Command_Invoke{Invoke: k.Invoke},
		}
	case *enginev1.OutboxEnvelope_Signal:
		sig := k.Signal
		// InvokerEffect.invocation_id is intentionally left nil for the
		// signal_delivered variant: the sender doesn't know the
		// receiver's id, only its Target. The receiver shard's apply
		// arm resolves Target → active InvocationId via KeyLeaseTable.
		return &enginev1.Command{
			Kind: &enginev1.Command_InvokerEffect{InvokerEffect: &enginev1.InvokerEffect{
				Kind: &enginev1.InvokerEffect_SignalDelivered{
					SignalDelivered: &enginev1.SignalDelivered{
						Target:     sig.GetTarget(),
						SignalName: sig.GetSignalName(),
						Payload:    sig.GetPayload(),
					},
				},
			}},
		}
	case *enginev1.OutboxEnvelope_DeliverCallResult:
		return &enginev1.Command{
			Kind: &enginev1.Command_DeliverCallResult{DeliverCallResult: k.DeliverCallResult},
		}
	case *enginev1.OutboxEnvelope_OutboxAck:
		return &enginev1.Command{
			Kind: &enginev1.Command_OutboxAck{OutboxAck: k.OutboxAck},
		}
	default:
		return nil
	}
}

// isOutboxProducer reports whether the given producerID belongs to an
// outbox shuffler — the apply path uses this to decide whether to pop a
// matching OutboxTable row.
func isOutboxProducer(producerID string) bool {
	return strings.HasPrefix(producerID, OutboxProducerPrefix)
}

// parseOutboxProducerShard extracts the sender shard id from an outbox
// producer id of the form "outbox/p<N>". Returns (0, false) when the
// producer id is malformed or does not carry the expected prefix.
func parseOutboxProducerShard(producerID string) (uint64, bool) {
	if !strings.HasPrefix(producerID, OutboxProducerPrefix+"p") {
		return 0, false
	}
	rest := strings.TrimPrefix(producerID, OutboxProducerPrefix+"p")
	var n uint64
	if _, err := fmt.Sscanf(rest, "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}
