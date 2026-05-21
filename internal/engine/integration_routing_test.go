package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/cluster"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage/keys"
)

// TestIntegration_LPOwnersBootstrapSeed brings up a single-node cluster
// and asserts the metadata-leader bootstrap proposes the identity-seed
// BulkUpsertLPOwners. After commit:
//
//   - LPOwnersTable revision is 1 (single batch).
//   - All 4096 LPs are present, mapping lp → (lp % numShards) + 1
//     where numShards is taken from the bootstrap PartitionTable size
//     (= len(Peers) for the static-assignment path, not HostConfig.
//     NumPartitionShards).
//   - The Partitioner agrees with the seed for arbitrary partition_keys
//     (once the reconciler runs; PR 1 wires it in pkg/reflow/run.go, so
//     this engine-level test installs the snapshot manually).
func TestIntegration_LPOwnersBootstrapSeed(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "node")
	notifier := cluster.NewTableNotifier()
	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeLocalAddr(t),
		DataDir:            dir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		ClusterNotifiers:   cluster.Notifiers{LPOwnersTable: notifier},
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

	// Wait for the bootstrap seed to commit. The MetadataRunner's
	// bootstrap goroutine fires after onBecomeLeader; we poll the table
	// until revision > 0.
	deadline := time.Now().Add(10 * time.Second)
	var snap *cluster.LPOwnersList
	for time.Now().Before(deadline) {
		snap, err = h.LPOwners(ctx)
		if err != nil {
			t.Fatalf("h.LPOwners: %v", err)
		}
		if snap.TableRevision > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if snap.TableRevision != 1 {
		t.Fatalf("bootstrap-seed revision = %d; want 1 (single BulkUpsert batch)", snap.TableRevision)
	}
	if got := len(snap.Records); got != int(keys.LPCount) {
		t.Fatalf("seeded rows = %d; want %d", got, keys.LPCount)
	}
	// Notifier fires post-commit.
	select {
	case <-notifier.Subscribe():
	default:
		// Already drained by an earlier wake — fine; the prior assertions
		// proved the commit happened.
	}

	// Recover the bootstrap PartitionTable to know numShards (driven by
	// len(Peers) in buildBootstrapTable, not HostConfig.NumPartitionShards).
	pt, err := h.PartitionTable(ctx)
	if err != nil {
		t.Fatalf("h.PartitionTable: %v", err)
	}
	numShards := uint64(len(pt.GetShards()))
	if numShards == 0 {
		t.Fatalf("bootstrap PartitionTable has 0 shards; can't validate seed formula")
	}

	// Every row must follow the identity formula against the bootstrap
	// numShards.
	for _, rec := range snap.Records {
		want := (uint64(rec.GetLp()) % numShards) + 1
		if rec.GetShardId() != want {
			t.Errorf("lp=%d: seeded=%d want=%d", rec.GetLp(), rec.GetShardId(), want)
		}
	}

	// Install the snapshot on the Partitioner (the routing reconciler
	// does this in production; we do it inline so we can assert on
	// h.Partitioner() answers in this engine-level test) and verify
	// routing agrees with the seed for sample partition keys.
	got := make(map[uint32]uint64, keys.LPCount)
	for _, rec := range snap.Records {
		got[rec.GetLp()] = rec.GetShardId()
	}
	h.PartitionerRef().SetLPOwnersSnapshot(got)

	for _, pk := range []uint64{
		0, 1, 2, 3, 4096, 8191, 8192,
		routing.PartitionKey("svc", "alpha"),
		routing.PartitionKey("svc", "beta"),
		routing.PartitionKey("Other", "key-1"),
	} {
		expected := (uint64(keys.LPFromPartitionKey(pk)) % numShards) + 1
		if r := h.Partitioner().ShardForKey(pk); r != expected {
			t.Errorf("ShardForKey(0x%x) = %d; want %d (table-driven post-seed)", pk, r, expected)
		}
	}
}
