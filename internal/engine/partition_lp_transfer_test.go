package engine

import (
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

func TestPartition_ApplyLPTransferChunk_StagesRows(t *testing.T) {
	p, _, col := newTestPartition(t)
	lp := uint32(7)

	// Synthesize one LP-prefixed key — an inv row for the dest LP.
	id := &enginev1.InvocationId{PartitionKey: uint64(lp), Uuid: []byte("0123456789abcdef")}
	invKey, err := keys.InvocationKey(id)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte{0x01, 0x02, 0x03}

	chunkCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_ApplyLpTransferChunk{
			ApplyLpTransferChunk: &enginev1.ApplyLPTransferChunk{
				TransferId:  "txn-stage",
				Lp:          lp,
				SourceShard: 1,
				ChunkSeq:    0,
				Rows: []*enginev1.TransferRow{
					{Key: invKey, Value: value, NamespaceHint: enginev1.TransferNamespace_TRANSFER_NS_INV},
				},
				IsFinal: true,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 1, Cmd: chunkCmd}}); err != nil {
		t.Fatal(err)
	}
	// is_final should emit a staged ack.
	gotStagedAct := false
	for _, a := range col.Drain() {
		if _, ok := a.(ActSignalLPTransferStaged); ok {
			gotStagedAct = true
		}
	}
	if !gotStagedAct {
		t.Fatal("expected ActSignalLPTransferStaged on is_final chunk")
	}
	// The row should be present in the store verbatim.
	store := p.cfg.Snapshotter.Store()
	got, closer, err := store.Get(invKey)
	if err != nil {
		t.Fatalf("expected row to be present after chunk apply: %v", err)
	}
	defer closer.Close()
	gotCopy := append([]byte(nil), got...)
	if string(gotCopy) != string(value) {
		t.Errorf("staged value = %v; want %v", gotCopy, value)
	}
	// Staging table records the final chunk.
	row, err := (tables.LPStagingTable{S: store}).Get("txn-stage")
	if err != nil {
		t.Fatal(err)
	}
	if row == nil || !row.GetIsFinalSeen() {
		t.Errorf("staging row should record is_final_seen; got %+v", row)
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

	// 1. Freeze + apply chunk to set up state.
	beginCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_BeginLpTransfer{
			BeginLpTransfer: &enginev1.BeginLPTransfer{
				TransferId: "txn-finish",
				Lp:         lp,
				DestShard:  2,
			},
		},
	})
	chunkCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_ApplyLpTransferChunk{
			ApplyLpTransferChunk: &enginev1.ApplyLPTransferChunk{
				TransferId:  "txn-finish",
				Lp:          lp,
				SourceShard: 1,
				ChunkSeq:    0,
				Rows: []*enginev1.TransferRow{
					{Key: invKey, Value: []byte("data"), NamespaceHint: enginev1.TransferNamespace_TRANSFER_NS_INV},
				},
				IsFinal: true,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{
		{Index: 1, Cmd: beginCmd},
		{Index: 2, Cmd: chunkCmd},
	}); err != nil {
		t.Fatal(err)
	}

	// 2. Finish — should range-delete the LP namespace and drop the freeze.
	finishCmd := envelope(t, &enginev1.Command{
		Kind: &enginev1.Command_FinishLpTransfer{
			FinishLpTransfer: &enginev1.FinishLPTransfer{
				TransferId: "txn-finish",
				Lp:         lp,
			},
		},
	})
	if _, err := p.Update([]statemachine.Entry{{Index: 3, Cmd: finishCmd}}); err != nil {
		t.Fatal(err)
	}
	store := p.cfg.Snapshotter.Store()
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
