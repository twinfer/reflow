package cluster

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/statemachine"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// auditEnvelopeWithPrincipal builds an envelope around cmd with a
// realistic header (created_at_ms + principal). Used by the apply
// tests to assert the FSM stamps audit rows from header inputs.
func auditEnvelopeWithPrincipal(t *testing.T, cmd *enginev1.Command, principal string, ifRev uint64) []byte {
	t.Helper()
	env := &enginev1.Envelope{
		Header: &enginev1.Header{
			CreatedAtMs: uint64(time.Now().UnixMilli()),
			Principal:   principal,
		},
		Command: cmd,
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

// TestAudit_UpsertTenant_WritesPebbleRowAndEmitsSlog asserts the
// FSM apply hook (a) writes one AuditLogTable row in the same Batch
// as the config mutation, and (b) emits a matching JSON record to
// cfg.AuditLogger. Both paths share the same record shape.
func TestAudit_UpsertTenant_WritesPebbleRowAndEmitsSlog(t *testing.T) {
	f, _, _ := newTestFSM(t)
	f.cfg.Notifiers.TenantTable = NewTableNotifier()

	var slogBuf bytes.Buffer
	f.cfg.AuditLogger = slog.New(slog.NewJSONHandler(&slogBuf, nil))

	rec := &enginev1.TenantRecord{Id: 7, Name: "acme", MaxConcurrentInvocations: 50}
	cmd := &enginev1.Command{Kind: &enginev1.Command_UpsertTenant{
		UpsertTenant: &enginev1.UpsertTenant{Record: rec},
	}}
	const raftIndex uint64 = 42
	entries := []statemachine.Entry{{Index: raftIndex, Cmd: auditEnvelopeWithPrincipal(t, cmd, "operator/alice", 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// (a) Audit row written to AuditLogTable.
	got, err := (AuditLogTable{S: f.cfg.Snapshotter.Store()}).Get(raftIndex)
	if err != nil {
		t.Fatalf("AuditLogTable.Get: %v", err)
	}
	if got == nil {
		t.Fatal("audit row not written for raft_index=42")
	}
	if got.GetActionKind() != "UpsertTenant" {
		t.Errorf("action_kind = %q, want UpsertTenant", got.GetActionKind())
	}
	if got.GetTarget() != "acme" {
		t.Errorf("target = %q, want acme", got.GetTarget())
	}
	if got.GetPrincipal() != "operator/alice" {
		t.Errorf("principal = %q, want operator/alice", got.GetPrincipal())
	}
	if got.GetTsMs() == 0 {
		t.Error("ts_ms should be stamped from header.created_at_ms")
	}

	// (b) Slog handler captured the same record.
	if slogBuf.Len() == 0 {
		t.Fatal("expected slog emit, got empty buffer")
	}
	var line map[string]any
	if err := json.Unmarshal(slogBuf.Bytes(), &line); err != nil {
		t.Fatalf("unmarshal slog line: %v\nraw=%s", err, slogBuf.String())
	}
	if line["action_kind"] != "UpsertTenant" {
		t.Errorf("slog action_kind = %v, want UpsertTenant", line["action_kind"])
	}
	if line["principal"] != "operator/alice" {
		t.Errorf("slog principal = %v, want operator/alice", line["principal"])
	}
}

// TestAudit_EmptyPrincipalSubstitutesEngine asserts the audit row for
// an FSM-self-proposed command (e.g. autonomous rebalance, GcAuditLog)
// carries principal="engine" when Header.principal is empty.
func TestAudit_EmptyPrincipalSubstitutesEngine(t *testing.T) {
	f, _, _ := newTestFSM(t)
	// EvictNode is autonomous (rebalancer-proposed). Header.principal
	// is empty by design.
	cmd := &enginev1.Command{Kind: &enginev1.Command_EvictNode{
		EvictNode: &enginev1.EvictNode{NodeId: 3},
	}}
	entries := []statemachine.Entry{{Index: 100, Cmd: auditEnvelopeWithPrincipal(t, cmd, "", 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := (AuditLogTable{S: f.cfg.Snapshotter.Store()}).Get(100)
	if err != nil {
		t.Fatalf("AuditLogTable.Get: %v", err)
	}
	if got == nil {
		t.Fatal("audit row not written for EvictNode")
	}
	if got.GetPrincipal() != "engine" {
		t.Errorf("principal = %q, want %q", got.GetPrincipal(), "engine")
	}
	if got.GetActionKind() != "EvictNode" {
		t.Errorf("action_kind = %q, want EvictNode", got.GetActionKind())
	}
}

// TestAudit_NilLoggerOnlyWritesPebbleRow asserts the durable Pebble
// write is unconditional: with a nil AuditLogger (operator hasn't
// wired slog), the audit row still lands.
func TestAudit_NilLoggerOnlyWritesPebbleRow(t *testing.T) {
	f, _, _ := newTestFSM(t)
	f.cfg.Notifiers.TenantTable = NewTableNotifier()
	f.cfg.AuditLogger = nil

	rec := &enginev1.TenantRecord{Id: 9, Name: "beta"}
	cmd := &enginev1.Command{Kind: &enginev1.Command_UpsertTenant{
		UpsertTenant: &enginev1.UpsertTenant{Record: rec},
	}}
	entries := []statemachine.Entry{{Index: 200, Cmd: auditEnvelopeWithPrincipal(t, cmd, "operator/bob", 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := (AuditLogTable{S: f.cfg.Snapshotter.Store()}).Get(200)
	if err != nil {
		t.Fatalf("AuditLogTable.Get: %v", err)
	}
	if got == nil {
		t.Fatal("audit row not written despite nil slog logger")
	}
}

// TestAudit_AnnounceLeaderSkipped asserts AnnounceLeader applies
// successfully without producing an audit row — leadership signals
// are not config changes.
func TestAudit_AnnounceLeaderSkipped(t *testing.T) {
	f, _, _ := newTestFSM(t)
	cmd := &enginev1.Command{Kind: &enginev1.Command_AnnounceLeader{
		AnnounceLeader: &enginev1.AnnounceLeader{NodeId: 1, LeaderEpoch: 1},
	}}
	entries := []statemachine.Entry{{Index: 300, Cmd: auditEnvelopeWithPrincipal(t, cmd, "", 0)}}
	if _, err := f.Update(entries); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := (AuditLogTable{S: f.cfg.Snapshotter.Store()}).Get(300)
	if err != nil {
		t.Fatalf("AuditLogTable.Get: %v", err)
	}
	if got != nil {
		t.Errorf("AnnounceLeader should not produce an audit row; got %+v", got)
	}
}

// TestAudit_PreconditionFailureNoRow asserts that when an Upsert hits
// a CAS failure (precondition mismatch), no audit row is written —
// audit only captures commands that actually mutated state.
func TestAudit_PreconditionFailureNoRow(t *testing.T) {
	f, _, _ := newTestFSM(t)
	f.cfg.Notifiers.TenantTable = NewTableNotifier()
	rec := &enginev1.TenantRecord{Id: 11, Name: "gamma"}
	cmd := &enginev1.Command{Kind: &enginev1.Command_UpsertTenant{
		UpsertTenant: &enginev1.UpsertTenant{Record: rec},
	}}
	// Force a precondition mismatch: ifRev=99 but the table is fresh.
	entries := []statemachine.Entry{{Index: 400, Cmd: auditEnvelopeWithPrincipal(t, cmd, "operator/charlie", 99)}}
	res, err := f.Update(entries)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := res[0].Result.Value; got != ResultValueFailedPrecondition {
		t.Fatalf("expected ResultValueFailedPrecondition; got %d", got)
	}
	got, err := (AuditLogTable{S: f.cfg.Snapshotter.Store()}).Get(400)
	if err != nil {
		t.Fatalf("AuditLogTable.Get: %v", err)
	}
	if got != nil {
		t.Errorf("no audit row should be written on precondition failure; got %+v", got)
	}
}

// TestAudit_GcAuditLog_DeletesOlderRows asserts the GC apply arm
// deletes rows with ts_ms < before_ts_ms and leaves newer rows
// intact. Idempotency is exercised by running the same GC twice.
func TestAudit_GcAuditLog_DeletesOlderRows(t *testing.T) {
	f, _, _ := newTestFSM(t)
	f.cfg.Notifiers.TenantTable = NewTableNotifier()

	// Seed 3 audit rows by applying 3 UpsertTenant commands with
	// distinct created_at_ms values.
	seed := func(raftIndex uint64, tenantID uint32, name string, tsMs uint64) {
		env := &enginev1.Envelope{
			Header: &enginev1.Header{CreatedAtMs: tsMs, Principal: "operator/alice"},
			Command: &enginev1.Command{Kind: &enginev1.Command_UpsertTenant{
				UpsertTenant: &enginev1.UpsertTenant{Record: &enginev1.TenantRecord{Id: tenantID, Name: name}},
			}},
		}
		buf, _ := proto.Marshal(env)
		if _, err := f.Update([]statemachine.Entry{{Index: raftIndex, Cmd: buf}}); err != nil {
			t.Fatalf("seed apply: %v", err)
		}
	}
	seed(1000, 100, "old1", 1_000)  // delete
	seed(1001, 101, "old2", 2_000)  // delete
	seed(1002, 102, "young", 5_000) // keep

	// Propose Command_GcAuditLog{before_ts_ms=3000}: deletes rows with
	// ts < 3000.
	gcCmd := &enginev1.Command{Kind: &enginev1.Command_GcAuditLog{
		GcAuditLog: &enginev1.GcAuditLog{BeforeTsMs: 3_000},
	}}
	gcEnv := &enginev1.Envelope{
		Header:  &enginev1.Header{CreatedAtMs: 10_000},
		Command: gcCmd,
	}
	gcBuf, _ := proto.Marshal(gcEnv)
	if _, err := f.Update([]statemachine.Entry{{Index: 2000, Cmd: gcBuf}}); err != nil {
		t.Fatalf("gc apply: %v", err)
	}

	store := f.cfg.Snapshotter.Store()
	if got, _ := (AuditLogTable{S: store}).Get(1000); got != nil {
		t.Errorf("raft_index=1000 should be GC'd (ts=1000 < 3000); got %+v", got)
	}
	if got, _ := (AuditLogTable{S: store}).Get(1001); got != nil {
		t.Errorf("raft_index=1001 should be GC'd (ts=2000 < 3000); got %+v", got)
	}
	if got, _ := (AuditLogTable{S: store}).Get(1002); got == nil {
		t.Error("raft_index=1002 should be kept (ts=5000 >= 3000)")
	}
	// The GC itself is also an audit row (kind=GcAuditLog), and it
	// has ts_ms=10_000 so it survives.
	if got, _ := (AuditLogTable{S: store}).Get(2000); got == nil || got.GetActionKind() != "GcAuditLog" {
		t.Errorf("GC self-audit row missing or wrong kind; got %+v", got)
	}

	// Idempotency: re-running the same GC is a no-op (no rows below
	// 3000 remain).
	if _, err := f.Update([]statemachine.Entry{{Index: 2001, Cmd: gcBuf}}); err != nil {
		t.Fatalf("gc re-apply: %v", err)
	}
	if got, _ := (AuditLogTable{S: store}).Get(1002); got == nil {
		t.Error("post-idempotent-GC: raft_index=1002 unexpectedly missing")
	}
}

// TestAudit_ListAuditLog_FiltersAndPagination exercises
// AuditLogTable.List directly to assert the filter semantics the
// LookupAuditLog handler exposes.
func TestAudit_ListAuditLog_FiltersAndPagination(t *testing.T) {
	f, _, _ := newTestFSM(t)
	f.cfg.Notifiers.TenantTable = NewTableNotifier()

	seed := func(raftIndex uint64, tenantID uint32, name string, tsMs uint64, principal string) {
		env := &enginev1.Envelope{
			Header: &enginev1.Header{CreatedAtMs: tsMs, Principal: principal},
			Command: &enginev1.Command{Kind: &enginev1.Command_UpsertTenant{
				UpsertTenant: &enginev1.UpsertTenant{Record: &enginev1.TenantRecord{Id: tenantID, Name: name}},
			}},
		}
		buf, _ := proto.Marshal(env)
		if _, err := f.Update([]statemachine.Entry{{Index: raftIndex, Cmd: buf}}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	seed(10, 1, "a", 1_000, "operator/alice")
	seed(11, 1, "b", 2_000, "operator/bob")
	seed(12, 2, "c", 3_000, "operator/alice")

	tab := AuditLogTable{S: f.cfg.Snapshotter.Store()}

	// tenant filter: tenant 1 has rows 10 and 11.
	got, err := tab.List(0, 0, 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Each entry's audit row records that the upsert touched
	// tenant_id=0 in Header (Header.tenant_id wasn't set by seed) — the
	// audit's tenant_id field reflects Header.tenant_id, not the
	// record's tenant id. So tenant=1 returns 0 rows.
	if len(got) != 0 {
		t.Errorf("tenant filter on header tenant_id; got %d rows, expected 0", len(got))
	}
	// tenant=0 should return all 3 (every Header.tenant_id is 0).
	got, err = tab.List(0, 0, 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("unfiltered list got %d rows, want 3", len(got))
	}

	// action filter
	got, err = tab.List(0, 0, 0, "UpsertTenant", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("action filter got %d rows, want 3", len(got))
	}
	got, err = tab.List(0, 0, 0, "DeleteTenant", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("non-matching action filter got %d rows, want 0", len(got))
	}

	// since filter: ts >= 2000 → rows 11, 12
	got, err = tab.List(2_000, 0, 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("since=2000 got %d rows, want 2", len(got))
	}

	// limit: cap at 1
	got, err = tab.List(0, 0, 0, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("limit=1 got %d rows, want 1", len(got))
	}
}
