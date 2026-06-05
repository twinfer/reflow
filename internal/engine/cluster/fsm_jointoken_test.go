package cluster

import (
	"testing"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func upsertJoinTokenEnvelope(t *testing.T, rec *enginev1.JoinTokenRecord, nowMs uint64, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: nowMs},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_UpsertJoinToken{
				UpsertJoinToken: &enginev1.UpsertJoinToken{Record: rec},
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

func consumeJoinTokenEnvelope(t *testing.T, tokenHash []byte, nowMs uint64, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: nowMs},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_ConsumeJoinToken{
				ConsumeJoinToken: &enginev1.ConsumeJoinToken{TokenHash: tokenHash},
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

func deleteJoinTokenEnvelope(t *testing.T, tokenHash []byte, nowMs uint64, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{CreatedAtMs: nowMs},
		Command: &enginev1.Command{
			Kind: &enginev1.Command_DeleteJoinToken{
				DeleteJoinToken: &enginev1.DeleteJoinToken{TokenHash: tokenHash},
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

func newFSMWithJoinTokenNotifier(t *testing.T) (*FSM, *TableNotifier) {
	t.Helper()
	f, _, _ := newTestFSM(t)
	notifier := NewTableNotifier()
	f.cfg.Notifiers.JoinTokenTable = notifier
	return f, notifier
}

func joinTokenRec(hash []byte, kind enginev1.JoinTokenKind, name string, expiryMs uint64, singleUse bool) *enginev1.JoinTokenRecord {
	return &enginev1.JoinTokenRecord{
		TokenHash:     hash,
		Kind:          kind,
		RequestedName: name,
		ExpiryMs:      expiryMs,
		SingleUse:     singleUse,
		CreatedBy:     "operator/test",
		CreatedAtMs:   1_700_000_000_000,
	}
}

func TestCluster_UpsertJoinToken_BumpsRevision(t *testing.T) {
	f, notifier := newFSMWithJoinTokenNotifier(t)
	hash := []byte{0x01, 0x02, 0x03, 0x04}
	rec := joinTokenRec(hash, enginev1.JoinTokenKind_JOIN_TOKEN_KIND_NODE, "auto", 1_700_000_600_000, true)
	entries := []statemachine.Entry{{Index: 10, Cmd: upsertJoinTokenEnvelope(t, rec, 1_700_000_000_000, 0)}}
	res, err := f.Update(entries)
	if err != nil {
		t.Fatal(err)
	}
	if got := res[0].Result.Value; got == ResultValueFailedPrecondition {
		t.Fatalf("first upsert should not fail precondition; result.value=%d", got)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("expected notifier to fire after UpsertJoinToken")
	}
	store := f.cfg.Snapshotter.Store()
	rev, err := (RevisionTable{S: store}).Get(RevisionTableJoinToken)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
	got, err := (JoinTokenTable{S: store}).Get(hash)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetRequestedName() != "auto" {
		t.Fatalf("row missing or mismatched: %+v", got)
	}
	if got.GetKind() != enginev1.JoinTokenKind_JOIN_TOKEN_KIND_NODE {
		t.Errorf("kind not persisted; got %v", got.GetKind())
	}
	if got.GetUsed() {
		t.Errorf("Used should default to false on a fresh row")
	}
}

func TestCluster_ConsumeJoinToken_SingleUseMarksUsed(t *testing.T) {
	f, _ := newFSMWithJoinTokenNotifier(t)
	hash := []byte{0xAA, 0xBB}
	rec := joinTokenRec(hash, enginev1.JoinTokenKind_JOIN_TOKEN_KIND_NODE, "auto", 1_700_000_600_000, true)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertJoinTokenEnvelope(t, rec, 1_700_000_000_000, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// Consume once — must succeed.
	res, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: consumeJoinTokenEnvelope(t, hash, 1_700_000_000_100, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value == ResultValueFailedPrecondition {
		t.Fatalf("first consume must not fail precondition")
	}
	store := f.cfg.Snapshotter.Store()
	got, _ := (JoinTokenTable{S: store}).Get(hash)
	if !got.GetUsed() {
		t.Fatalf("expected Used=true after consume; got %+v", got)
	}
	// Second consume must fail (Used=true now); apply path returns
	// (nil, nil) so Update stamps ResultValueFailedPrecondition.
	res, err = f.Update([]statemachine.Entry{
		{Index: 3, Cmd: consumeJoinTokenEnvelope(t, hash, 1_700_000_000_200, 2)},
	})
	if err != nil {
		t.Fatal(err)
	}
	// With IfTableRevisionEq=2 and the consume path returning nil for an
	// already-used row, the result must be FailedPrecondition.
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("re-consume should hit FailedPrecondition; got %d", res[0].Result.Value)
	}
}

func TestCluster_ConsumeJoinToken_ExpiredRejected(t *testing.T) {
	f, _ := newFSMWithJoinTokenNotifier(t)
	hash := []byte{0xDE, 0xAD}
	rec := joinTokenRec(hash, enginev1.JoinTokenKind_JOIN_TOKEN_KIND_NODE, "auto", 1_700_000_000_500, true)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertJoinTokenEnvelope(t, rec, 1_700_000_000_000, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	// nowMs=1_700_000_000_600 ≥ expiry → consume must reject.
	res, err := f.Update([]statemachine.Entry{
		{Index: 2, Cmd: consumeJoinTokenEnvelope(t, hash, 1_700_000_000_600, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Result.Value != ResultValueFailedPrecondition {
		t.Fatalf("expected FailedPrecondition for expired token; got %d", res[0].Result.Value)
	}
	got, _ := (JoinTokenTable{S: f.cfg.Snapshotter.Store()}).Get(hash)
	if got.GetUsed() {
		t.Errorf("expired token must not be marked used")
	}
}

func TestCluster_ConsumeJoinToken_AbsentRejected(t *testing.T) {
	f, _ := newFSMWithJoinTokenNotifier(t)
	hash := []byte{0xCA, 0xFE}
	res, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: consumeJoinTokenEnvelope(t, hash, 1_700_000_000_000, 0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Precondition was 0 (no CAS); apply arm returned (nil, nil) because
	// the row is absent. Because IfTableRevisionEq==0 the apply path
	// does NOT stamp FailedPrecondition for the no-row no-op; verify the
	// table state directly.
	_ = res
	got, _ := (JoinTokenTable{S: f.cfg.Snapshotter.Store()}).Get(hash)
	if got != nil {
		t.Fatalf("absent token should not be created by ConsumeJoinToken; got %+v", got)
	}
}

func TestCluster_ConsumeJoinToken_NonSingleUseRepeats(t *testing.T) {
	f, _ := newFSMWithJoinTokenNotifier(t)
	hash := []byte{0x11, 0x22}
	rec := joinTokenRec(hash, enginev1.JoinTokenKind_JOIN_TOKEN_KIND_OPERATOR, "alice", 1_700_000_600_000, false)
	if _, err := f.Update([]statemachine.Entry{
		{Index: 1, Cmd: upsertJoinTokenEnvelope(t, rec, 1_700_000_000_000, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	for i, idx := range []uint64{2, 3, 4} {
		res, err := f.Update([]statemachine.Entry{
			{Index: idx, Cmd: consumeJoinTokenEnvelope(t, hash, 1_700_000_000_000+uint64(i), uint64(i)+1)},
		})
		if err != nil {
			t.Fatalf("consume #%d: %v", i, err)
		}
		if res[0].Result.Value == ResultValueFailedPrecondition {
			t.Fatalf("non-single-use consume #%d should not fail precondition", i)
		}
	}
	got, _ := (JoinTokenTable{S: f.cfg.Snapshotter.Store()}).Get(hash)
	if got.GetUsed() {
		t.Errorf("non-single-use token must not be marked used")
	}
}

func TestCluster_DeleteJoinToken_BumpsEvenIfAbsent(t *testing.T) {
	f, notifier := newFSMWithJoinTokenNotifier(t)
	hash := []byte{0xFF}
	entries := []statemachine.Entry{{Index: 1, Cmd: deleteJoinTokenEnvelope(t, hash, 1_700_000_000_000, 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifier.Subscribe():
	default:
		t.Fatal("delete-of-absent should still fire the notifier")
	}
	rev, _ := (RevisionTable{S: f.cfg.Snapshotter.Store()}).Get(RevisionTableJoinToken)
	if rev != 1 {
		t.Fatalf("rev=%d; want 1", rev)
	}
}

func TestCluster_JoinTokenLookupReturnsListAndRevision(t *testing.T) {
	f, _ := newFSMWithJoinTokenNotifier(t)
	hashes := [][]byte{{0x01}, {0x02}, {0x03}}
	for i, h := range hashes {
		entries := []statemachine.Entry{{
			Index: uint64(i + 1),
			Cmd: upsertJoinTokenEnvelope(t,
				joinTokenRec(h, enginev1.JoinTokenKind_JOIN_TOKEN_KIND_NODE, "auto", 1_700_000_600_000, true),
				1_700_000_000_000, 0),
		}}
		if _, err := f.Update(entries); err != nil {
			t.Fatal(err)
		}
	}
	res, err := f.Lookup(LookupJoinTokens{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := res.(*JoinTokenList)
	if !ok {
		t.Fatalf("Lookup type = %T; want *JoinTokenList", res)
	}
	if len(list.Records) != 3 {
		t.Fatalf("len=%d; want 3", len(list.Records))
	}
	if list.TableRevision != 3 {
		t.Fatalf("rev=%d; want 3", list.TableRevision)
	}
}
