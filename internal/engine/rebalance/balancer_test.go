package rebalance

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine/cluster"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// fakeHost implements HostReader against in-memory state. Tests mutate
// the fields directly; the balancer reads via the interface methods.
type fakeHost struct {
	mu        sync.Mutex
	pt        *enginev1.PartitionTable
	owners    []*enginev1.LPOwnerRecord
	drains    []*enginev1.RebalanceDrainRecord
	transfers []*enginev1.LPTransferRecord
}

func (h *fakeHost) PartitionTable(ctx context.Context) (*enginev1.PartitionTable, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pt, nil
}

func (h *fakeHost) LPOwners(ctx context.Context) (*cluster.LPOwnersList, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return &cluster.LPOwnersList{Records: append([]*enginev1.LPOwnerRecord(nil), h.owners...)}, nil
}

func (h *fakeHost) LPTransfers(ctx context.Context) (*cluster.LPTransfersList, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return &cluster.LPTransfersList{Records: append([]*enginev1.LPTransferRecord(nil), h.transfers...)}, nil
}

func (h *fakeHost) RebalanceDrains(ctx context.Context) (*cluster.RebalanceDrainList, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return &cluster.RebalanceDrainList{Records: append([]*enginev1.RebalanceDrainRecord(nil), h.drains...)}, nil
}

// recordingProposer captures every proposed command.
type recordingProposer struct {
	mu       sync.Mutex
	proposed []*enginev1.Command
}

func (p *recordingProposer) ProposeSelf(ctx context.Context, cmd *enginev1.Command) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.proposed = append(p.proposed, cmd)
	return nil
}

func (p *recordingProposer) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.proposed)
}

func partitionTableFor(shards []uint64) *enginev1.PartitionTable {
	pt := &enginev1.PartitionTable{Shards: make(map[uint64]*enginev1.ReplicaSet, len(shards))}
	for _, s := range shards {
		pt.Shards[s] = &enginev1.ReplicaSet{}
	}
	return pt
}

// ownerRecords lifts the plannedOwners map into proto records.
func ownerRecords(plan map[uint32]uint64) []*enginev1.LPOwnerRecord {
	out := make([]*enginev1.LPOwnerRecord, 0, len(plan))
	for lp, shard := range plan {
		out = append(out, &enginev1.LPOwnerRecord{Lp: lp, ShardId: shard})
	}
	return out
}

func TestBalancer_AutoMode_ProposesAfterMembershipChange(t *testing.T) {
	// Start: 3 shards holding the planner's idea of {1,2,3}.
	// Add a 4th shard in the partition table — the planner now spans
	// {1,2,3,4}; many LPs should be slated to move.
	host := &fakeHost{
		pt:     partitionTableFor([]uint64{1, 2, 3, 4}),
		owners: ownerRecords(plannedOwners([]uint64{1, 2, 3})),
	}
	prop := &recordingProposer{}
	b := New(Config{
		Mode:                       ModeAuto,
		MaxConcurrentTransfers:     3,
		MinSecondsBetweenTransfers: 0, // no cooldown for the test
		SkewEngagePct:              15,
		SkewDisengagePct:           8,
		PollInterval:               20 * time.Millisecond,
		SyncReadTimeout:            500 * time.Millisecond,
	}, host, prop, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()

	// Wait briefly for at least one tick to land 3 proposals
	// (MaxConcurrent=3, no in-flight, immediate-tick on start).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && prop.Count() < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	if got := prop.Count(); got < 3 {
		t.Fatalf("auto mode proposed %d transfers; want ≥ 3", got)
	}
	// All proposals must be Command_InitiateLpTransfer.
	for i, cmd := range prop.proposed {
		if _, ok := cmd.GetKind().(*enginev1.Command_InitiateLpTransfer); !ok {
			t.Fatalf("propose[%d] is %T; want Command_InitiateLpTransfer", i, cmd.GetKind())
		}
	}
}

func TestBalancer_AdvisoryMode_NeverProposes(t *testing.T) {
	host := &fakeHost{
		pt:     partitionTableFor([]uint64{1, 2, 3, 4}),
		owners: ownerRecords(plannedOwners([]uint64{1, 2, 3})),
	}
	prop := &recordingProposer{}
	b := New(Config{
		Mode:                       ModeAdvisory,
		MaxConcurrentTransfers:     3,
		MinSecondsBetweenTransfers: 0,
		SkewEngagePct:              15,
		SkewDisengagePct:           8,
		PollInterval:               20 * time.Millisecond,
		SyncReadTimeout:            500 * time.Millisecond,
	}, host, prop, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()
	time.Sleep(200 * time.Millisecond) // many ticks at 20ms
	cancel()
	<-done
	if got := prop.Count(); got != 0 {
		t.Fatalf("advisory mode proposed %d transfers; want 0", got)
	}
}

func TestBalancer_OffMode_ExitsImmediately(t *testing.T) {
	host := &fakeHost{
		pt:     partitionTableFor([]uint64{1, 2, 3}),
		owners: ownerRecords(plannedOwners([]uint64{1, 2, 3})),
	}
	prop := &recordingProposer{}
	b := New(Config{
		Mode:         ModeOff,
		PollInterval: 20 * time.Millisecond,
	}, host, prop, nil, nil, nil)

	ctx := t.Context()
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Balancer.Run did not exit in off mode")
	}
	if prop.Count() != 0 {
		t.Fatalf("off mode proposed %d transfers; want 0", prop.Count())
	}
}

func TestBalancer_DrainNotifier_WakesEarly(t *testing.T) {
	// Stable cluster, drain channel fires → balancer ticks immediately
	// even with a long poll interval. We don't need any proposals to
	// happen; we just want the tick to run when the channel signals.
	host := &fakeHost{
		pt:     partitionTableFor([]uint64{1, 2, 3}),
		owners: ownerRecords(plannedOwners([]uint64{1, 2, 3})),
	}
	var ticked atomic.Int32
	host.pt.Shards[1] = &enginev1.ReplicaSet{} // ensure non-nil

	ch := make(chan struct{}, 1)
	// Wrap host with a counting reader so we observe SyncReads.
	prop := &recordingProposer{}
	counting := &countingHost{inner: host, ticks: &ticked}
	b := New(Config{
		Mode:                       ModeAuto,
		MaxConcurrentTransfers:     1,
		MinSecondsBetweenTransfers: 0,
		SkewEngagePct:              15,
		SkewDisengagePct:           8,
		PollInterval:               5 * time.Second, // long backstop
		SyncReadTimeout:            500 * time.Millisecond,
	}, counting, prop, ch, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.Run(ctx) }()

	// Immediate-tick at start lands ticks=1 quickly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && ticked.Load() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if ticked.Load() < 1 {
		cancel()
		<-done
		t.Fatal("start-of-loop tick never fired")
	}
	// Now fire the drain channel and expect a second tick well before
	// the 5s backstop would.
	ch <- struct{}{}
	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && ticked.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if ticked.Load() < 2 {
		t.Fatalf("drain notifier did not wake the balancer; ticks=%d", ticked.Load())
	}
}

// countingHost wraps a HostReader to count PartitionTable calls (one
// per tick).
type countingHost struct {
	inner HostReader
	ticks *atomic.Int32
}

func (c *countingHost) PartitionTable(ctx context.Context) (*enginev1.PartitionTable, error) {
	c.ticks.Add(1)
	return c.inner.PartitionTable(ctx)
}
func (c *countingHost) LPOwners(ctx context.Context) (*cluster.LPOwnersList, error) {
	return c.inner.LPOwners(ctx)
}
func (c *countingHost) LPTransfers(ctx context.Context) (*cluster.LPTransfersList, error) {
	return c.inner.LPTransfers(ctx)
}
func (c *countingHost) RebalanceDrains(ctx context.Context) (*cluster.RebalanceDrainList, error) {
	return c.inner.RebalanceDrains(ctx)
}
