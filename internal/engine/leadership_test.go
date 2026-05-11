package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

type fakeAnnouncer struct {
	mu       sync.Mutex
	curEpoch uint64
	cmds     []*enginev1.Command
}

func (f *fakeAnnouncer) SetEpoch(epoch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.curEpoch = epoch
}

func (f *fakeAnnouncer) ProposeSelf(_ context.Context, cmd *enginev1.Command) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, cmd)
	return nil
}

func (f *fakeAnnouncer) Cmds() []*enginev1.Command {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*enginev1.Command, len(f.cmds))
	copy(out, f.cmds)
	return out
}

func (f *fakeAnnouncer) Epoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.curEpoch
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestLeadership_CandidacyProposesAnnounceLeader(t *testing.T) {
	ann := &fakeAnnouncer{}
	l := NewLeadership(LeadershipConfig{NodeID: 1, Announcer: ann})

	l.OnRaftLeaderChange(1) // self becomes raft leader
	waitFor(t, func() bool { return len(ann.Cmds()) == 1 }, "AnnounceLeader proposed")

	c := ann.Cmds()[0].GetAnnounceLeader()
	if c == nil {
		t.Fatalf("not an AnnounceLeader: %+v", ann.Cmds()[0])
	}
	if c.GetNodeId() != 1 || c.GetLeaderEpoch() != 1 {
		t.Errorf("AnnounceLeader fields: %+v", c)
	}
	if ann.Epoch() != 1 {
		t.Errorf("proposer epoch = %d; want 1", ann.Epoch())
	}
	if l.State() != Candidate {
		t.Errorf("state after candidacy = %s; want Candidate", l.State())
	}
}

func TestLeadership_OnAnnounceLeaderPromotesCandidate(t *testing.T) {
	ann := &fakeAnnouncer{}
	l := NewLeadership(LeadershipConfig{NodeID: 1, Announcer: ann})

	var (
		became atomic.Bool
		down   atomic.Bool
	)
	l.SetCallbacks(
		func() { became.Store(true) },
		func() { down.Store(true) },
	)

	l.OnRaftLeaderChange(1)
	waitFor(t, func() bool { return len(ann.Cmds()) == 1 }, "AnnounceLeader proposed")

	// Apply our own AnnounceLeader.
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 1})
	waitFor(t, func() bool { return became.Load() }, "onBecomeLeader fired")
	if l.State() != Leader {
		t.Errorf("state = %s; want Leader", l.State())
	}
	if down.Load() {
		t.Errorf("onStepDown fired spuriously")
	}
}

func TestLeadership_HigherEpochDemotesLeader(t *testing.T) {
	ann := &fakeAnnouncer{}
	l := NewLeadership(LeadershipConfig{NodeID: 1, Announcer: ann})

	var down atomic.Bool
	l.SetCallbacks(nil, func() { down.Store(true) })

	// Become candidate (epoch 1) then leader.
	l.OnRaftLeaderChange(1)
	waitFor(t, func() bool { return len(ann.Cmds()) == 1 }, "AnnounceLeader proposed")
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 1})
	waitFor(t, func() bool { return l.State() == Leader }, "promoted to Leader")

	// Some other node (id=2) wins with epoch 5.
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 2, LeaderEpoch: 5})
	waitFor(t, func() bool { return l.State() == Follower }, "stepped down")
	waitFor(t, func() bool { return down.Load() }, "onStepDown fired")
}

func TestLeadership_LowerEpochAnnounceIgnoredByLeader(t *testing.T) {
	ann := &fakeAnnouncer{}
	l := NewLeadership(LeadershipConfig{NodeID: 1, Announcer: ann})

	// Become leader at epoch 1.
	l.OnRaftLeaderChange(1)
	waitFor(t, func() bool { return len(ann.Cmds()) == 1 }, "AnnounceLeader proposed")
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 1})
	waitFor(t, func() bool { return l.State() == Leader }, "Leader")

	// A duplicate / replay of an older AnnounceLeader at lower epoch must not
	// change state.
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 2, LeaderEpoch: 0})
	if l.State() != Leader {
		t.Errorf("state after lower-epoch announce = %s; want Leader", l.State())
	}
}

func TestLeadership_OtherRaftLeaderStepsDown(t *testing.T) {
	ann := &fakeAnnouncer{}
	l := NewLeadership(LeadershipConfig{NodeID: 1, Announcer: ann})
	var down atomic.Bool
	l.SetCallbacks(nil, func() { down.Store(true) })

	// Become leader at epoch 1.
	l.OnRaftLeaderChange(1)
	waitFor(t, func() bool { return len(ann.Cmds()) == 1 }, "AnnounceLeader proposed")
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 1})
	waitFor(t, func() bool { return l.State() == Leader }, "Leader")

	// Raft signals that node 2 is now leader.
	l.OnRaftLeaderChange(2)
	waitFor(t, func() bool { return l.State() == Follower }, "Follower")
	waitFor(t, func() bool { return down.Load() }, "onStepDown fired")
}
