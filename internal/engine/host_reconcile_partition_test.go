package engine

import (
	"context"
	"net"
	"testing"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestHost_ReconcilePartitionTable_StartsLocallyOwnedShard verifies that
// the PartitionTable reconciler's applier path (ReconcilePartitionTable)
// drives StartPartition for shards the local node owns but hasn't yet
// started. Calls h.ReconcilePartitionTable directly so the test does
// not need to spin up a multi-node Raft group.
func TestHost_ReconcilePartitionTable_StartsLocallyOwnedShard(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	raftAddr := ln.Addr().String()
	_ = ln.Close()

	h, err := NewHost(t.Context(), HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            t.TempDir(),
		RTTMillisecond:     50,
		NumPartitionShards: 2,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// Pre-start shard 1 to validate the "already running" no-op path.
	if _, err := h.StartPartition(1); err != nil {
		t.Fatalf("StartPartition(1): %v", err)
	}

	// Input: node owns both shards; shard 2 must be started.
	pt := &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards: map[uint64]*enginev1.ReplicaSet{
			1: {NodeIds: []uint64{1}},
			2: {NodeIds: []uint64{1}},
		},
	}
	h.ReconcilePartitionTable(pt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if h.Partition(2) != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("shard 2 never started after ReconcilePartitionTable")
		case <-tick.C:
		}
	}

	// Shard 1 was pre-started; reconcile must not have torn it down or
	// re-created it (would have returned "already started" from a
	// concurrent StartPartition).
	if h.Partition(1) == nil {
		t.Fatal("shard 1 disappeared during reconcile")
	}
}

// TestHost_ReconcilePartitionTable_NotLocallyOwned_NoStart verifies
// the applier ignores shards whose replica set does not include this
// node.
func TestHost_ReconcilePartitionTable_NotLocallyOwned_NoStart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	raftAddr := ln.Addr().String()
	_ = ln.Close()

	h, err := NewHost(t.Context(), HostConfig{
		NodeID:             1,
		RaftAddr:           raftAddr,
		DataDir:            t.TempDir(),
		RTTMillisecond:     50,
		NumPartitionShards: 2,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// Shard 2 owned by a different node only — reconcile should not start it.
	pt := &enginev1.PartitionTable{
		AssignmentEpoch: 1,
		Shards: map[uint64]*enginev1.ReplicaSet{
			2: {NodeIds: []uint64{2, 3}},
		},
	}
	h.ReconcilePartitionTable(pt)

	// Give the goroutine a chance to do something (it shouldn't).
	time.Sleep(150 * time.Millisecond)
	if h.Partition(2) != nil {
		t.Fatal("shard 2 unexpectedly started; node is not in replica set")
	}
}
