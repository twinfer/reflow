package engine

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v4"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// retryBackoff returns a randomized short sleep used to space out
// transient-error retries across replicas. ~50ms base ± 50% so
// simultaneous leadership transitions don't synchronize their
// dragonboat re-proposes.
func retryBackoff() time.Duration {
	return 25*time.Millisecond + time.Duration(rand.IntN(50))*time.Millisecond
}

// ErrShardClosed indicates the local partition replica is no longer running.
// Distinct from dragonboat.ErrShardClosed so callers don't need to import
// dragonboat directly.
var ErrShardClosed = errors.New("proposer: shard closed")

// RaftProposer wraps a dragonboat NodeHost with reflow's envelope framing
// and dedup stamping.
//
// Mirrors restate crates/worker/src/partition/leadership/self_proposer.rs:36-58,
// minus the background batching appender — dragonboat's SyncPropose blocks
// until commit, so proposals take a synchronous path.
type RaftProposer struct {
	nh      *dragonboat.NodeHost
	shardID uint64

	leaderEpoch atomic.Uint64
	nextSeq     atomic.Uint64
}

// NewRaftProposer constructs a RaftProposer for the given shard. The proposer
// must be primed with SetEpoch before ProposeSelf will produce useful dedup
// records (epoch 0 is the "no leader yet" sentinel).
func NewRaftProposer(nh *dragonboat.NodeHost, shardID uint64) *RaftProposer {
	return &RaftProposer{nh: nh, shardID: shardID}
}

// SetEpoch updates the leader epoch stamped on SelfProposal envelopes and
// resets the self-proposal sequence. Called on leader transitions.
func (p *RaftProposer) SetEpoch(epoch uint64) {
	p.leaderEpoch.Store(epoch)
	p.nextSeq.Store(0)
}

// LeaderEpoch returns the proposer's current view of the leader epoch.
func (p *RaftProposer) LeaderEpoch() uint64 { return p.leaderEpoch.Load() }

// ProposeSelf appends a self-proposal command to the Raft log. Used by the
// leader-side TimerService and Invoker.
func (p *RaftProposer) ProposeSelf(ctx context.Context, cmd *enginev1.Command) error {
	epoch := p.leaderEpoch.Load()
	seq := p.nextSeq.Add(1)
	env := buildSelfProposalEnvelope(epoch, seq, cmd)
	return p.propose(ctx, env)
}

// ProposeIngress appends a command from an external producer (e.g., the
// ingress gateway). producerID + seq must be monotonic per producer so the
// dedup table can reject retries; callers typically use a UUID + nanosecond
// timestamp for "good enough" uniqueness.
func (p *RaftProposer) ProposeIngress(ctx context.Context, producerID string, seq uint64, cmd *enginev1.Command) error {
	env := buildIngressEnvelope(producerID, seq, cmd)
	return p.propose(ctx, env)
}

func (p *RaftProposer) propose(ctx context.Context, env *enginev1.Envelope) error {
	buf, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	sess := p.nh.GetNoOPSession(p.shardID) // OnDisk SM requires NoOPSession.

	for {
		// SyncPropose requires a context with a deadline. If the caller
		// didn't set one, attach a default so the call returns within a
		// reasonable bound.
		callCtx := ctx
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
		}
		_, err := p.nh.SyncPropose(callCtx, sess, buf)
		if err == nil {
			return nil
		}
		if errors.Is(err, dragonboat.ErrShardClosed) || errors.Is(err, dragonboat.ErrClosed) {
			return ErrShardClosed
		}
		if !dragonboat.IsTempError(err) {
			return err
		}
		// Backoff briefly and retry with jitter to avoid replica-wide
		// synchronization on leadership transitions.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryBackoff()):
		}
	}
}

// nowMs samples the leader-side wall clock used by buildSelfProposalEnvelope
// / buildIngressEnvelope to stamp Header.created_at_ms. The apply path
// reads that field instead of calling its local NowFn so every replica
// sees the same value during Update — see partition.applyCommand. Kept
// as a package var (not a method) so the value is purely propose-time
// state with no instance lifecycle.
var nowMs = func() uint64 { return uint64(time.Now().UnixMilli()) }

func buildSelfProposalEnvelope(epoch, seq uint64, cmd *enginev1.Command) *enginev1.Envelope {
	return &enginev1.Envelope{
		Header: &enginev1.Header{
			CreatedAtMs: nowMs(),
			Dedup: &enginev1.Dedup{
				Kind: &enginev1.Dedup_SelfProposal{
					SelfProposal: &enginev1.SelfProposalDedup{
						LeaderEpoch: epoch,
						Seq:         seq,
					},
				},
			},
		},
		Command: cmd,
	}
}

func buildIngressEnvelope(producerID string, seq uint64, cmd *enginev1.Command) *enginev1.Envelope {
	return &enginev1.Envelope{
		Header: &enginev1.Header{
			CreatedAtMs: nowMs(),
			Dedup: &enginev1.Dedup{
				Kind: &enginev1.Dedup_Arbitrary{
					Arbitrary: &enginev1.ArbitraryDedup{
						ProducerId: producerID,
						Seq:        seq,
					},
				},
			},
		},
		Command: cmd,
	}
}
