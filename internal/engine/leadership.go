package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// LeaderState mirrors restate
// crates/worker/src/partition/leadership/mod.rs:159-167.
type LeaderState int

const (
	Follower LeaderState = iota
	Candidate
	Leader
)

func (s LeaderState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LeaderAnnouncer is the small slice of RaftProposer that Leadership uses to
// propose AnnounceLeader on candidacy. Declared as an interface so tests can
// fake the proposer without spinning up a NodeHost.
type LeaderAnnouncer interface {
	SetEpoch(epoch uint64)
	ProposeSelf(ctx context.Context, cmd *enginev1.Command) error
}

// Leadership owns the partition's view of who the leader is and what epoch is
// in force. State transitions happen in two places:
//
//  1. OnRaftLeaderChange (called from the dragonboat RaftEventListener
//     goroutine — must NOT block) updates internal state and, when this node
//     becomes the raft leader, kicks off a background goroutine to propose an
//     AnnounceLeader command at a new epoch.
//
//  2. OnAnnounceLeader (called from the FSM apply path) is where the actual
//     Follower→Leader / Leader→Follower transitions happen. This mirrors the
//     restate flow: the AnnounceLeader command flows through the replicated
//     log; only when WE see it applied do we promote ourselves to Leader.
//     Mirrors restate leadership/mod.rs:246-401.
type Leadership struct {
	nodeID    uint64
	announcer LeaderAnnouncer
	log       *slog.Logger

	mu                   sync.RWMutex
	state                LeaderState
	leaderEpoch          uint64 // my current candidate/leader epoch
	latestAnnouncedEpoch uint64 // highest leader_epoch observed in the log
	raftLeaderID         uint64

	onBecomeLeader func()
	onStepDown     func()
}

// LeadershipConfig collects construction-time settings.
type LeadershipConfig struct {
	NodeID    uint64
	Announcer LeaderAnnouncer
	Log       *slog.Logger
	// InitialEpoch seeds the leadership state on construction. Should be
	// loaded from MetaTable.latest_announced_epoch on startup so that a
	// fresh candidacy run bumps past any epoch the prior leader already
	// emitted (which still has dedup records on disk).
	InitialEpoch uint64
}

func NewLeadership(cfg LeadershipConfig) *Leadership {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Leadership{
		nodeID:               cfg.NodeID,
		announcer:            cfg.Announcer,
		log:                  cfg.Log,
		state:                Follower,
		leaderEpoch:          cfg.InitialEpoch,
		latestAnnouncedEpoch: cfg.InitialEpoch,
	}
}

// SetCallbacks installs the leader-transition hooks. Should be called once,
// before OnRaftLeaderChange or OnAnnounceLeader fire.
func (l *Leadership) SetCallbacks(onBecomeLeader, onStepDown func()) {
	l.mu.Lock()
	l.onBecomeLeader = onBecomeLeader
	l.onStepDown = onStepDown
	l.mu.Unlock()
}

func (l *Leadership) IsLeader() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.state == Leader
}

func (l *Leadership) State() LeaderState {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.state
}

func (l *Leadership) LeaderEpoch() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leaderEpoch
}

// OnRaftLeaderChange is called by the dragonboat RaftEventListener when the
// raft leader changes. Must NOT block — dispatches any proposal to a
// background goroutine.
func (l *Leadership) OnRaftLeaderChange(raftLeaderID uint64) {
	l.mu.Lock()
	l.raftLeaderID = raftLeaderID

	var (
		runCandidate   bool
		candidateEpoch uint64
		fireStepDown   bool
	)

	switch {
	case raftLeaderID == l.nodeID:
		// We are now the raft leader. Run for partition leadership
		// unless we are already Leader for the most recent announced
		// epoch — re-running candidacy from a Leader state would emit a
		// stale duplicate AnnounceLeader.
		//
		// Critically, we re-run from Candidate as well. Under load,
		// transient term churn can leave us in Candidate with an
		// AnnounceLeader that never landed (another peer won that
		// epoch). When dragonboat later re-elects us as raft leader, a
		// Follower-only guard would silently swallow the signal and
		// leave us stuck — observed in TestChaos_LeaderLoss as
		// invocations stranded in Scheduled because onBecomeLeader
		// never fires. Bumping leaderEpoch on every re-entry is safe:
		// any in-flight AnnounceLeader for the prior epoch hits the
		// lower-epoch path in OnAnnounceLeader and is harmlessly
		// ignored.
		if !(l.state == Leader && l.leaderEpoch >= l.latestAnnouncedEpoch) {
			// Bump leaderEpoch past any epoch already announced on
			// this shard, not just our own prior value. Required for
			// dedup correctness: self-proposal dedup keys are
			// (leader_epoch, seq) with no node_id, so two leaders that
			// share an epoch collide on disk. A node that was a
			// follower through a prior leader's tenure observed
			// AnnounceLeader applies that set latestAnnouncedEpoch
			// without touching leaderEpoch; without this max(), our
			// fresh candidacy reuses an epoch the prior leader had
			// already issued self-proposals under, the DedupTable
			// silently absorbs our AnnounceLeader as a duplicate, and
			// Leadership.OnAnnounceLeader never fires. Net effect:
			// raft says we're leader, reflw never knows, partition
			// stays headless until cluster teardown. Observed as ~3%
			// invocations stuck Scheduled on the shard whose leader
			// changed during partition heal.
			l.leaderEpoch = max(l.leaderEpoch, l.latestAnnouncedEpoch) + 1
			l.state = Candidate
			runCandidate = true
			candidateEpoch = l.leaderEpoch
		}
	case raftLeaderID != 0:
		// Someone else is raft leader. If we thought we were the partition
		// leader, step down.
		switch l.state {
		case Leader:
			l.state = Follower
			fireStepDown = true
		case Candidate:
			l.state = Follower
		}
	}

	onStepDown := l.onStepDown
	l.mu.Unlock()

	if fireStepDown && onStepDown != nil {
		go onStepDown()
	}
	if runCandidate {
		go l.runCandidate(candidateEpoch)
	}
}

// runCandidate proposes an AnnounceLeader for the given epoch. Fires in its
// own goroutine so the listener never blocks.
func (l *Leadership) runCandidate(epoch uint64) {
	if l.announcer == nil {
		l.log.Warn("leadership: no announcer; cannot propose AnnounceLeader", "epoch", epoch)
		return
	}
	l.announcer.SetEpoch(epoch)
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_AnnounceLeader{
			AnnounceLeader: &enginev1.AnnounceLeader{
				NodeId:      l.nodeID,
				LeaderEpoch: epoch,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := l.announcer.ProposeSelf(ctx, cmd); err != nil {
		l.log.Warn("leadership: AnnounceLeader propose failed", "epoch", epoch, "err", err)
	}
}

// OnAnnounceLeader is called from the FSM apply path when an AnnounceLeader
// command is replicated and applied. This is the only place we transition
// to Leader.
func (l *Leadership) OnAnnounceLeader(cmd *enginev1.AnnounceLeader) {
	if cmd == nil {
		return
	}
	l.mu.Lock()

	if cmd.GetLeaderEpoch() > l.latestAnnouncedEpoch {
		l.latestAnnouncedEpoch = cmd.GetLeaderEpoch()
	}

	var (
		becameLeader bool
		steppedDown  bool
	)

	switch l.state {
	case Candidate:
		if cmd.GetLeaderEpoch() == l.leaderEpoch && cmd.GetNodeId() == l.nodeID {
			l.state = Leader
			becameLeader = true
		} else if cmd.GetLeaderEpoch() > l.leaderEpoch {
			// Another candidate won.
			l.state = Follower
		}
	case Leader:
		if cmd.GetLeaderEpoch() > l.leaderEpoch {
			l.state = Follower
			steppedDown = true
		}
	case Follower:
		// Track epoch but do not change state.
	}

	onBecomeLeader := l.onBecomeLeader
	onStepDown := l.onStepDown
	l.mu.Unlock()

	if becameLeader && onBecomeLeader != nil {
		go onBecomeLeader()
	}
	if steppedDown && onStepDown != nil {
		go onStepDown()
	}
}
