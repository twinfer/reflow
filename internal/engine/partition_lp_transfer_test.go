package engine

import (
	"bytes"
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func TestPartition_LPFreeze_RejectsInvoke(t *testing.T) {
	p, _, col := newTestPartition(t)

	target := &enginev1.InvocationTarget{ServiceName: "S", HandlerName: "h", ObjectKey: "k"}
	lp := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))

	// 1. Install freeze via BeginLPTransfer.
	beginCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_BeginLpTransfer{
			BeginLpTransfer: &enginev1.BeginLPTransfer{
				TransferId: "txn-freeze",
				Lp:         lp,
				DestShard:  2,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: beginCmd}}); err != nil {
		t.Fatal(err)
	}
	// Freeze applies should emit ActStartLPTransferScan.
	gotScanAct := false
	for _, a := range col.Drain() {
		if _, ok := a.(ActStartLPTransferScan); ok {
			gotScanAct = true
		}
	}
	if !gotScanAct {
		t.Fatal("expected ActStartLPTransferScan after BeginLPTransfer")
	}

	// 2. Invoke targeting the same LP must be rejected with ResultValueLPFrozen.
	id := &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()),
		Uuid:         []byte("0123456789abcdef"),
	}
	invCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{
			Invoke: &enginev1.InvokeCommand{
				InvocationId: id,
				Target:       target,
				Input:        []byte("in"),
			},
		},
	})
	res, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: invCmd}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueLPFrozen {
		t.Errorf("Result.Value = %d; want ResultValueLPFrozen (%d)",
			res[0].Result.Value, ResultValueLPFrozen)
	}
	// No actions for the rejected invoke.
	if len(col.Drain()) != 0 {
		t.Error("expected no actions on frozen invoke")
	}
	// No invocation row was written.
	store := p.cfg.Snapshotter.Store()
	status, err := (tables.InvocationTable{S: store}).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, free := status.GetStatus().(*enginev1.InvocationStatus_Free); !free {
		t.Errorf("expected Free status (no write happened); got %T", status.GetStatus())
	}
}

// TestPartition_ApplyLPTransferSST_BookkeepingOnFinal exercises the
// dest-side apply arm's bookkeeping path: LPStagingRow created, seq
// bumped, is_final_seen flipped, ActSignalLPTransferStaged emitted.
// The actual Ingest call is stubbed in this PR — the follow-up wires
// pebbleStore.IngestSSTs against the delivery-uploaded files.
func TestPartition_ApplyLPTransferSST_BookkeepingOnFinal(t *testing.T) {
	p, _, col := newTestPartition(t)
	lp := uint32(7)

	cmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_ApplyLpTransferSst{
			ApplyLpTransferSst: &enginev1.ApplyLPTransferSST{
				TransferId:  "txn-stage",
				Lp:          lp,
				SourceShard: 1,
				SstSeq:      0,
				Ssts:        nil, // Ingest is stubbed for now.
				IsFinal:     true,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: cmd}}); err != nil {
		t.Fatal(err)
	}
	gotStagedAct := false
	for _, a := range col.Drain() {
		if _, ok := a.(ActSignalLPTransferStaged); ok {
			gotStagedAct = true
		}
	}
	if !gotStagedAct {
		t.Fatal("expected ActSignalLPTransferStaged on is_final SST")
	}
	store := p.cfg.Snapshotter.Store()
	row, err := (tables.LPStagingTable{S: store}).Get("txn-stage")
	if err != nil {
		t.Fatal(err)
	}
	if row == nil || !row.GetIsFinalSeen() {
		t.Errorf("staging row should record is_final_seen; got %+v", row)
	}
	if got := row.GetNextSstSeq(); got != 1 {
		t.Errorf("next_sst_seq = %d; want 1", got)
	}
}

func TestPartition_FinishLPTransfer_RangeDeletesLPKeyspace(t *testing.T) {
	p, _, _ := newTestPartition(t)
	lp := uint32(11)
	id := &enginev1.InvocationId{PartitionKey: uint64(lp), Uuid: []byte("0123456789abcdef")}
	invKey, err := keys.InvocationKey(id)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Pre-seed an LP-prefixed row directly (the old chunk-apply
	// path that wrote rows via batch.Set is gone; the new SST-apply
	// arm Ingests files and is wired in the follow-up PR). Finish's
	// range-delete behavior is independent of how data got there.
	store := p.cfg.Snapshotter.Store()
	b := store.NewBatch()
	if err := b.Set(invKey, []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	// Begin + Finish on this partition (acting as the source side).
	beginCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_BeginLpTransfer{
			BeginLpTransfer: &enginev1.BeginLPTransfer{
				TransferId: "txn-finish",
				Lp:         lp,
				DestShard:  2,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: beginCmd}}); err != nil {
		t.Fatal(err)
	}
	finishCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_FinishLpTransfer{
			FinishLpTransfer: &enginev1.FinishLPTransfer{
				TransferId: "txn-finish",
				Lp:         lp,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 2, Cmd: finishCmd}}); err != nil {
		t.Fatal(err)
	}
	// The inv/<lp>/<id> row should be gone.
	if _, closer, err := store.Get(invKey); err == nil {
		closer.Close()
		t.Error("expected inv row to be range-deleted by FinishLPTransfer")
	}
	// Freeze row should be gone.
	freeze, err := (tables.LPFreezeTable{S: store}).Get(lp)
	if err != nil {
		t.Fatal(err)
	}
	if freeze != nil {
		t.Errorf("freeze row should be dropped; got %+v", freeze)
	}
}

func TestPartition_DedupArbitrary_StagesAndFinishesWithLP(t *testing.T) {
	// LP-prefixed arbitrary dedup lives in the LP-prefixed keyspace, so
	// Finish range-deletes it along with the rest. Regression guard for
	// the LPNoLP sentinel-LP non-deletion behavior.
	p, _, _ := newTestPartition(t)
	lp := uint32(13)
	dedupKey := keys.DedupArbitraryKey(lp, "outbox/p1", 42)
	dedupVal := []byte{0xAA, 0xBB, 0xCC}

	// 1. Pre-seed the LP-prefixed dedup row directly (was previously
	// done via ApplyLPTransferChunk; SST-shipping path lands the data
	// in the same LSM via Ingest, which is wired in the follow-up PR).
	store := p.cfg.Snapshotter.Store()
	b := store.NewBatch()
	if err := b.Set(dedupKey, dedupVal); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()
	got, closer, err := store.Get(dedupKey)
	if err != nil {
		t.Fatalf("seed dedup row: %v", err)
	}
	gotCopy := append([]byte(nil), got...)
	closer.Close()
	if !bytes.Equal(gotCopy, dedupVal) {
		t.Errorf("staged dedup value = %v; want %v", gotCopy, dedupVal)
	}

	// 2. Freeze + finish on the same partition (treating it as the
	// source): the dedup row should be range-deleted along with the
	// rest of the LP keyspace.
	beginCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_BeginLpTransfer{
			BeginLpTransfer: &enginev1.BeginLPTransfer{
				TransferId: "txn-dedup-finish",
				Lp:         lp,
				DestShard:  2,
			},
		},
	})
	finishCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_FinishLpTransfer{
			FinishLpTransfer: &enginev1.FinishLPTransfer{
				TransferId: "txn-dedup-finish",
				Lp:         lp,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{
		{Index: 1, Cmd: beginCmd},
		{Index: 2, Cmd: finishCmd},
	}); err != nil {
		t.Fatal(err)
	}
	if _, closer, err := store.Get(dedupKey); err == nil {
		closer.Close()
		t.Error("expected dedup-arb row to be range-deleted by FinishLPTransfer")
	}

	// 3. The LPNoLP sentinel slot must NOT be touched — Finish only
	// walks real LP prefixes (PrefixUpperBound(real lp) can never
	// reach the sentinel).
	sentinelKey := keys.DedupArbitraryKey(keys.LPNoLP, "outbox/p1", 99)
	sb := store.NewBatch()
	if err := sb.Set(sentinelKey, []byte{0xFF}); err != nil {
		t.Fatal(err)
	}
	if err := sb.Commit(true); err != nil {
		t.Fatal(err)
	}
	sb.Close()
	// Re-run finish for some real lp; sentinel must survive.
	finishCmd2 := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_FinishLpTransfer{
			FinishLpTransfer: &enginev1.FinishLPTransfer{
				TransferId: "txn-sentinel-guard",
				Lp:         lp + 1,
			},
		},
	})
	beginCmd2 := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_BeginLpTransfer{
			BeginLpTransfer: &enginev1.BeginLPTransfer{
				TransferId: "txn-sentinel-guard", Lp: lp + 1, DestShard: 2,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{
		{Index: 4, Cmd: beginCmd2},
		{Index: 5, Cmd: finishCmd2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, closer, err := store.Get(sentinelKey); err != nil {
		t.Error("sentinel-LP dedup row must NOT be deleted by Finish on a real LP")
	} else {
		closer.Close()
	}
}
