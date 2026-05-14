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

// TestLeadership_ReElectionFromCandidate covers the case where
// OnRaftLeaderChange(self) is called a second time while we are still
// Candidate from a prior epoch whose AnnounceLeader never landed.
// Before the fix, the Follower-only guard in OnRaftLeaderChange swallowed
// the second signal, leaving the node permanently stuck as Candidate and
// never firing onBecomeLeader — reproduced in TestChaos_LeaderLoss as
// invocations stranded in Scheduled. The fix re-runs candidacy at a
// higher epoch on any self-raft-leader signal that is not "already Leader
// for the most recent announced epoch."
func TestLeadership_ReElectionFromCandidate(t *testing.T) {
	ann := &fakeAnnouncer{}
	l := NewLeadership(LeadershipConfig{NodeID: 1, Announcer: ann})

	var became atomic.Bool
	l.SetCallbacks(func() { became.Store(true) }, nil)

	// First raft-leader signal: bump to Candidate at epoch 1, propose
	// AnnounceLeader. Do NOT apply it — simulates the case where another
	// peer won that epoch's race.
	l.OnRaftLeaderChange(1)
	waitFor(t, func() bool { return len(ann.Cmds()) == 1 }, "first AnnounceLeader proposed")
	if l.State() != Candidate {
		t.Fatalf("state after first OnRaftLeaderChange = %s; want Candidate", l.State())
	}
	if l.LeaderEpoch() != 1 {
		t.Fatalf("epoch after first OnRaftLeaderChange = %d; want 1", l.LeaderEpoch())
	}

	// Second raft-leader signal (e.g. after a kill that re-elected us).
	// Must re-run candidacy at a higher epoch even though state is still
	// Candidate from the prior round.
	l.OnRaftLeaderChange(1)
	waitFor(t, func() bool { return len(ann.Cmds()) == 2 }, "second AnnounceLeader proposed")
	if l.State() != Candidate {
		t.Errorf("state after second OnRaftLeaderChange = %s; want Candidate", l.State())
	}
	if got := l.LeaderEpoch(); got != 2 {
		t.Errorf("epoch after second OnRaftLeaderChange = %d; want 2", got)
	}
	if c := ann.Cmds()[1].GetAnnounceLeader(); c.GetLeaderEpoch() != 2 {
		t.Errorf("second AnnounceLeader epoch = %d; want 2", c.GetLeaderEpoch())
	}

	// Applying the second-epoch AnnounceLeader promotes to Leader.
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 2})
	waitFor(t, func() bool { return became.Load() }, "onBecomeLeader fired")
	if l.State() != Leader {
		t.Errorf("state = %s; want Leader", l.State())
	}

	// A late, lower-epoch (epoch=1) AnnounceLeader arriving from the
	// original race must be ignored — the latest epoch wins.
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 1})
	if l.State() != Leader {
		t.Errorf("stale lower-epoch announce changed state to %s", l.State())
	}
}

// TestLeadership_NoOpWhenAlreadyLeaderForLatestEpoch ensures the relaxed
// re-entry guard does not re-propose AnnounceLeader when we are already
// the leader for the most recent epoch (e.g. dragonboat re-fires
// LeaderUpdated for the same term).
func TestLeadership_NoOpWhenAlreadyLeaderForLatestEpoch(t *testing.T) {
	ann := &fakeAnnouncer{}
	l := NewLeadership(LeadershipConfig{NodeID: 1, Announcer: ann})

	l.OnRaftLeaderChange(1)
	waitFor(t, func() bool { return len(ann.Cmds()) == 1 }, "first AnnounceLeader proposed")
	l.OnAnnounceLeader(&enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 1})
	waitFor(t, func() bool { return l.State() == Leader }, "Leader")

	// Spurious repeat of the same raft leadership signal must not
	// trigger a new candidacy.
	l.OnRaftLeaderChange(1)
	time.Sleep(20 * time.Millisecond)
	if got := len(ann.Cmds()); got != 1 {
		t.Errorf("AnnounceLeader proposals after repeat signal = %d; want 1", got)
	}
	if l.State() != Leader {
		t.Errorf("state = %s; want Leader (unchanged)", l.State())
	}
	if l.LeaderEpoch() != 1 {
		t.Errorf("epoch = %d; want 1 (unchanged)", l.LeaderEpoch())
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
