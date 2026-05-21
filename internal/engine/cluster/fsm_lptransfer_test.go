package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func initiateLPTransferEnvelope(t *testing.T, transferID string, lp uint32, destShard uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_000_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_InitiateLpTransfer{
				InitiateLpTransfer: &enginev1.InitiateLPTransfer{
					TransferId: transferID,
					Lp:         lp,
					DestShard:  destShard,
				},
			},
		},
	}
	buf, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func updateLPTransferPhaseEnvelope(t *testing.T, transferID string, phase enginev1.LPTransferPhase, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_001_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_UpdateLpTransferPhase{
				UpdateLpTransferPhase: &enginev1.UpdateLPTransferPhase{
					TransferId: transferID,
					Phase:      phase,
				},
			},
		},
	}
	if ifRev != 0 {
		env.Precondition = &enginev1.Precondition{IfTableRevisionEq: ifRev}
	}
	buf, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func removeLPTransferEnvelope(t *testing.T, transferID string) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: 1_700_000_002_000},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_RemoveLpTransfer{
				RemoveLpTransfer: &enginev1.RemoveLPTransfer{TransferId: transferID},
			},
		},
	}
	buf, err := proto.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func newFSMForLPTransferTests(t *testing.T) (*FSM, *TableNotifier) {
	t.Helper()
	f, _, _ := newTestFSM(t)
	notifier := NewTableNotifier()
	f.cfg.Notifiers.LPOwnersTable = NewTableNotifier() // separate, unused
	f.cfg.Notifiers.LPTransfersTable = notifier
	// Seed the partition table and an LPOwner so Initiate validation
	// passes.
	pt := &enginev1.PartitionTable{
		Shards: map[uint64]*enginev1.ReplicaSet{
			1: {NodeIds: []uint64{1}},
			2: {NodeIds: []uint64{1}},
		},
		AssignmentEpoch: 1,
	}
	batch := f.cfg.Snapshotter.Store().NewBatch()
	defer batch.Close()
	if err := (PartitionTableTable{S: batch}).Put(batch, pt); err != nil {
		t.Fatal(err)
	}
	if err := (LPOwnersTable{S: batch}).Put(batch, &enginev1.LPOwnerRecord{Lp: 42, ShardId: 1}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	return f, notifier
}

func TestCluster_InitiateLPTransfer_WritesAtInitPhase(t *testing.T) {
	f, notifier := newFSMForLPTransferTests(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: initiateLPTransferEnvelope(t, "txn-1", 42, 2)},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("Initiate should fire LPTransfersTable notifier")
	}
	rec, err := (LPTransferTable{S: f.cfg.Snapshotter.Store()}).Get("txn-1")
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("LPTransferRecord missing")
	}
	if rec.GetPhase() != enginev1.LPTransferPhase_LP_TRANSFER_PHASE_INIT {
		t.Errorf("phase = %s; want INIT", rec.GetPhase().String())
	}
	if rec.GetSourceShard() != 1 || rec.GetDestShard() != 2 || rec.GetLp() != 42 {
		t.Errorf("rec = %+v; want lp=42 source=1 dest=2", rec)
	}
}

func TestCluster_InitiateLPTransfer_RejectsOutOfRangeLP(t *testing.T) {
	f, _ := newFSMForLPTransferTests(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: initiateLPTransferEnvelope(t, "txn-1", 99_999, 2)},
	}); err != nil {
		t.Fatal(err)
	}
	rec, _ := (LPTransferTable{S: f.cfg.Snapshotter.Store()}).Get("txn-1")
	if rec != nil {
		t.Fatalf("invalid lp should produce no row; got %+v", rec)
	}
}

func TestCluster_InitiateLPTransfer_RejectsDupInProgress(t *testing.T) {
	f, _ := newFSMForLPTransferTests(t)
	// First transfer lands.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: initiateLPTransferEnvelope(t, "txn-1", 42, 2)},
	}); err != nil {
		t.Fatal(err)
	}
	// Second transfer for the same lp while txn-1 is INIT is rejected.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: initiateLPTransferEnvelope(t, "txn-2", 42, 2)},
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := (LPTransferTable{S: f.cfg.Snapshotter.Store()}).Get("txn-2")
	if got != nil {
		t.Fatalf("dup in-progress should be rejected; got %+v", got)
	}
}

func TestCluster_UpdateLPTransferPhase_MonotonicAdvance(t *testing.T) {
	f, _ := newFSMForLPTransferTests(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: initiateLPTransferEnvelope(t, "txn-1", 42, 2)},
	}); err != nil {
		t.Fatal(err)
	}
	// INIT → STAGED.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: updateLPTransferPhaseEnvelope(t, "txn-1", enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	rec, _ := (LPTransferTable{S: f.cfg.Snapshotter.Store()}).Get("txn-1")
	if rec.GetPhase() != enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED {
		t.Errorf("expected STAGED, got %s", rec.GetPhase().String())
	}
	// Backward transition STAGED → INIT is rejected (dropped silently).
	if _, err := f.Update([]statemachine.Entry{
		{Index: 3, Cmd: updateLPTransferPhaseEnvelope(t, "txn-1", enginev1.LPTransferPhase_LP_TRANSFER_PHASE_INIT, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	rec, _ = (LPTransferTable{S: f.cfg.Snapshotter.Store()}).Get("txn-1")
	if rec.GetPhase() != enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED {
		t.Errorf("backward transition should be dropped; got %s", rec.GetPhase().String())
	}
	// Same-phase idempotent no-op.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 4, Cmd: updateLPTransferPhaseEnvelope(t, "txn-1", enginev1.LPTransferPhase_LP_TRANSFER_PHASE_STAGED, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// STAGED → FLIPPED is the next valid step.
	if _, err := f.Update([]statemachine.Entry{
		{Index: 5, Cmd: updateLPTransferPhaseEnvelope(t, "txn-1", enginev1.LPTransferPhase_LP_TRANSFER_PHASE_FLIPPED, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	rec, _ = (LPTransferTable{S: f.cfg.Snapshotter.Store()}).Get("txn-1")
	if rec.GetPhase() != enginev1.LPTransferPhase_LP_TRANSFER_PHASE_FLIPPED {
		t.Errorf("expected FLIPPED, got %s", rec.GetPhase().String())
	}
	// FLIPPED → ABORTING is rejected (point of no return).
	if _, err := f.Update([]statemachine.Entry{
		{Index: 6, Cmd: updateLPTransferPhaseEnvelope(t, "txn-1", enginev1.LPTransferPhase_LP_TRANSFER_PHASE_ABORTING, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	rec, _ = (LPTransferTable{S: f.cfg.Snapshotter.Store()}).Get("txn-1")
	if rec.GetPhase() != enginev1.LPTransferPhase_LP_TRANSFER_PHASE_FLIPPED {
		t.Errorf("FLIPPED→ABORTING must be rejected; got %s", rec.GetPhase().String())
	}
}

func TestCluster_RemoveLPTransfer_DropsRow(t *testing.T) {
	f, _ := newFSMForLPTransferTests(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: initiateLPTransferEnvelope(t, "txn-1", 42, 2)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: removeLPTransferEnvelope(t, "txn-1")},
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := (LPTransferTable{S: f.cfg.Snapshotter.Store()}).Get("txn-1")
	if got != nil {
		t.Fatalf("RemoveLPTransfer should drop the row; got %+v", got)
	}
}

func TestCluster_LookupLPTransfers_ReturnsRowsAndRevision(t *testing.T) {
	f, _ := newFSMForLPTransferTests(t)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: initiateLPTransferEnvelope(t, "txn-1", 42, 2)},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := f.Lookup(LookupLPTransfers{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := res.(*LPTransfersList)
	if !ok {
		t.Fatalf("Lookup result type %T; want *LPTransfersList", res)
	}
	if list.TableRevision == 0 {
		t.Errorf("TableRevision should bump on InitiateLPTransfer")
	}
	if len(list.Records) != 1 || list.Records[0].GetTransferId() != "txn-1" {
		t.Errorf("Records = %+v; want one txn-1 row", list.Records)
	}
}
