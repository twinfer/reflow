package engine_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine/rebalance"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestIntegrationLPTransfer_SeededRowsShipViaSST stands up a 3-node
// cluster with rebalance off, seeds the source partition with
// state-table rows under a chosen LP, proposes an explicit
// InitiateLPTransfer to move that LP to a different shard, waits for
// PHASE_CLEANED, and asserts:
//
//   - LPOwners flipped to the dest shard.
//   - Seeded keys are present on the dest partition store after the
//     saga (proves the per-replica fan-out + Pebble Ingest worked
//     end-to-end).
//   - Seeded keys are absent from the source partition store (proves
//     the source-side range-delete in onFinishLPTransfer ran).
//
// Seeds are written to every replica's local Pebble directly so the
// SST scanner finds the same data regardless of which replica won the
// source-shard leader election.
//
// Skipped on -short — full saga is multi-phase Raft and takes seconds.
func TestIntegrationLPTransfer_SeededRowsShipViaSST(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in -short")
	}
	cl := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N: 3,
		Rebalance: rebalance.Config{
			Mode: rebalance.ModeOff,
		},
	})
	defer cl.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer awaitCancel()
	if err := cl.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leader := findMetadataLeader(t, cl)
	host := leader.Host

	seedCtx, seedCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer seedCancel()
	for {
		ow, err := host.LPOwners(seedCtx)
		if err == nil && len(ow.Records) > 0 {
			break
		}
		select {
		case <-seedCtx.Done():
			t.Fatal("LPOwners seed never committed")
		case <-time.After(100 * time.Millisecond):
		}
	}

	owners, err := host.LPOwners(seedCtx)
	if err != nil {
		t.Fatalf("LPOwners: %v", err)
	}
	const targetLP uint32 = 7
	var sourceShard uint64
	for _, rec := range owners.Records {
		if rec.GetLp() == targetLP {
			sourceShard = rec.GetShardId()
			break
		}
	}
	if sourceShard == 0 {
		t.Fatalf("LP %d not found in LPOwners (have %d records)", targetLP, len(owners.Records))
	}
	var destShard uint64
	for s := uint64(1); s <= 3; s++ {
		if s != sourceShard {
			destShard = s
			break
		}
	}
	t.Logf("LP %d: source shard %d → dest shard %d", targetLP, sourceShard, destShard)

	// Seed state-table rows under targetLP on every replica's local
	// Pebble. Writing through Raft (via a real handler) would be the
	// purer path but adds substantial scaffolding; this direct write
	// hydrates each replica's store identically so the source SST
	// scanner sees the same data wherever leadership lands.
	seedRows := map[string][]byte{
		"alpha": []byte("v-alpha"),
		"bravo": []byte("v-bravo"),
		"gamma": []byte("v-gamma"),
	}
	const seedSvc = "lp-xfer-test"
	const seedObj = "obj-1"
	for i, n := range cl.Nodes {
		ip := n.(*loadgen.InProcessNode)
		sourceRunner := ip.Host.Partition(sourceShard)
		if sourceRunner == nil {
			t.Fatalf("node %d: no partition runner for source shard %d", i, sourceShard)
		}
		store := sourceRunner.Snapshotter().Store()
		batch := store.NewBatch()
		for k, v := range seedRows {
			key := keys.StateKey(targetLP, seedSvc, seedObj, k)
			if err := batch.Set(key, v); err != nil {
				batch.Close()
				t.Fatalf("node %d: batch.Set(%q): %v", i, k, err)
			}
		}
		if err := batch.Commit(true); err != nil {
			t.Fatalf("node %d: batch.Commit: %v", i, err)
		}
	}

	// Sanity-check: dest doesn't have the seeded keys yet.
	destStore := host.Partition(destShard).Snapshotter().Store()
	for k := range seedRows {
		key := keys.StateKey(targetLP, seedSvc, seedObj, k)
		v, closer, gerr := destStore.Get(key)
		if gerr == nil {
			closer.Close()
			t.Fatalf("dest already has key %q pre-transfer; value %q", k, v)
		}
		if gerr != storage.ErrNotFound {
			t.Fatalf("dest.Get(%q) pre-transfer: %v", k, gerr)
		}
	}

	const transferID = "lp-7-manual-xfer"
	proposeCtx, proposeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer proposeCancel()
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_InitiateLpTransfer{
			InitiateLpTransfer: &enginev1.InitiateLPTransfer{
				TransferId: transferID,
				Lp:         targetLP,
				DestShard:  destShard,
			},
		},
	}
	if err := leader.Host.MetadataRunner().Proposer().ProposeSelf(proposeCtx, cmd); err != nil {
		t.Fatalf("ProposeSelf(InitiateLPTransfer): %v", err)
	}

	// Poll LPTransferTable for PHASE_CLEANED on our row. 90s deadline:
	// each phase (init → frozen → staged → flipped → cleaned) is one
	// or more Raft round-trips through the lpMover saga.
	deadline := time.Now().Add(90 * time.Second)
	lastPhase := enginev1.LPTransferPhase_LP_TRANSFER_PHASE_UNSPECIFIED
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		list, lerr := host.LPTransfers(ctx)
		cancel()
		if lerr != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, rec := range list.Records {
			if rec.GetTransferId() != transferID {
				continue
			}
			if p := rec.GetPhase(); p != lastPhase {
				t.Logf("transfer %s: phase = %s", transferID, p.String())
				lastPhase = p
			}
			break
		}
		if lastPhase == enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastPhase != enginev1.LPTransferPhase_LP_TRANSFER_PHASE_CLEANED {
		t.Fatalf("transfer %s never reached CLEANED; last phase = %s", transferID, lastPhase.String())
	}

	postCtx, postCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer postCancel()
	postOwners, err := host.LPOwners(postCtx)
	if err != nil {
		t.Fatalf("LPOwners (post): %v", err)
	}
	foundNewOwner := false
	for _, rec := range postOwners.Records {
		if rec.GetLp() == targetLP && rec.GetShardId() == destShard {
			foundNewOwner = true
			break
		}
	}
	if !foundNewOwner {
		t.Fatalf("LPOwners did not flip: LP %d still not on dest shard %d", targetLP, destShard)
	}

	// Dest replicas should now hold the Ingested keys. Verify on the
	// node we initially picked; the apply path runs on every replica so
	// any of the three would do.
	for k, wantV := range seedRows {
		key := keys.StateKey(targetLP, seedSvc, seedObj, k)
		v, closer, gerr := destStore.Get(key)
		if gerr != nil {
			t.Fatalf("dest.Get(%q) post-transfer: %v", k, gerr)
		}
		got := append([]byte(nil), v...)
		_ = closer.Close()
		if !bytes.Equal(got, wantV) {
			t.Fatalf("dest[%q] = %q; want %q", k, got, wantV)
		}
	}
	t.Logf("dest shard %d has all %d seeded keys", destShard, len(seedRows))

	// Source partition should have range-deleted the LP keyspace on
	// every replica during onFinishLPTransfer. Verify on the same host.
	sourceStore := host.Partition(sourceShard).Snapshotter().Store()
	for k := range seedRows {
		key := keys.StateKey(targetLP, seedSvc, seedObj, k)
		v, closer, gerr := sourceStore.Get(key)
		if gerr == nil {
			closer.Close()
			t.Fatalf("source still has key %q post-transfer; value %q", k, v)
		}
		if gerr != storage.ErrNotFound {
			t.Fatalf("source.Get(%q) post-transfer: %v", k, gerr)
		}
	}
	t.Logf("source shard %d range-deleted; no seeded keys remain", sourceShard)
}
