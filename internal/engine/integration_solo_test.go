package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/engine/cluster"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// TestSoloBootstrap_SeedsSelfMembershipAndPartitionTable asserts that a
// 1-node deployment (HostConfig.Peers == nil) bootstraps the metadata
// shard with a self-only NodeRegistry and a 1-shard PartitionTable. Solo
// used to short-circuit metadata bootstrap; after Phase 1 it goes
// through the same path as multi-node, just with len(peers) == 1.
func TestSoloBootstrap_SeedsSelfMembershipAndPartitionTable(t *testing.T) {
	dir := t.TempDir()
	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeLocalAddr(t),
		DataDir:            filepath.Join(dir, "node1"),
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		// Peers intentionally nil — exercises the solo bootstrap path.
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.StartMetadataShard(); err != nil {
		t.Fatalf("StartMetadataShard: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.AwaitMetadataLeader(ctx); err != nil {
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}

	// Bootstrap runs async on a leader-scoped goroutine after the
	// AnnounceLeader commits — poll until the PartitionTable lands. A
	// bare SyncRead immediately after AwaitMetadataLeader can race the
	// bootstrap proposer (the leader is announced, but RegisterNode +
	// UpdatePartitionTable have not yet applied).
	var (
		mems []*enginev1.NodeMembership
		pt   *enginev1.PartitionTable
	)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		memRes, mErr := h.NodeHost().SyncRead(ctx, 0, cluster.LookupMembership{})
		if mErr != nil {
			t.Fatalf("SyncRead LookupMembership: %v", mErr)
		}
		mems, _ = memRes.([]*enginev1.NodeMembership)
		ptRes, pErr := h.NodeHost().SyncRead(ctx, 0, cluster.LookupPartitionTable{})
		if pErr != nil {
			t.Fatalf("SyncRead LookupPartitionTable: %v", pErr)
		}
		pt, _ = ptRes.(*enginev1.PartitionTable)
		if len(mems) > 0 && pt != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if len(mems) != 1 {
		t.Fatalf("solo membership has %d entries; want 1", len(mems))
	}
	if mems[0].GetNodeId() != 1 {
		t.Errorf("solo membership node_id = %d; want 1", mems[0].GetNodeId())
	}
	if pt == nil {
		t.Fatalf("LookupPartitionTable: still nil after bootstrap deadline; bootstrap did not commit")
	}
	if got := len(pt.GetShards()); got != 1 {
		t.Fatalf("PartitionTable.Shards len = %d; want 1", got)
	}
	rs := pt.GetShards()[1]
	if rs == nil || len(rs.GetNodeIds()) != 1 || rs.GetNodeIds()[0] != 1 {
		t.Errorf("PartitionTable.Shards[1] = %v; want NodeIds=[1]", rs)
	}
	if mr := pt.GetMetaReplicas(); mr == nil || len(mr.GetNodeIds()) != 1 || mr.GetNodeIds()[0] != 1 {
		t.Errorf("PartitionTable.MetaReplicas = %v; want NodeIds=[1]", mr)
	}
}
