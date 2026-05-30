package audit

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestKindAndTarget_AllAuditableCommands asserts every Command oneof
// variant the FSM is expected to audit produces a non-empty kind and,
// where applicable, a meaningful target. A new auditable command must
// extend this table — otherwise it'll silently produce empty audit
// rows in production.
func TestKindAndTarget_AllAuditableCommands(t *testing.T) {
	tests := []struct {
		name       string
		cmd        *enginev1.Command
		wantKind   string
		wantTarget string
	}{
		{
			name: "UpsertTenant uses record.name as target",
			cmd: &enginev1.Command{Kind: &enginev1.Command_UpsertTenant{
				UpsertTenant: &enginev1.UpsertTenant{Record: &enginev1.TenantRecord{Id: 42, Name: "acme"}},
			}},
			wantKind:   "UpsertTenant",
			wantTarget: "acme",
		},
		{
			name: "DeleteTenant stringifies id",
			cmd: &enginev1.Command{Kind: &enginev1.Command_DeleteTenant{
				DeleteTenant: &enginev1.DeleteTenant{Id: 42},
			}},
			wantKind:   "DeleteTenant",
			wantTarget: "42",
		},
		{
			name: "UpsertTenantDEK uses record.name",
			cmd: &enginev1.Command{Kind: &enginev1.Command_UpsertTenantDek{
				UpsertTenantDek: &enginev1.UpsertTenantDEK{Record: &enginev1.TenantDEKRecord{TenantId: 7, Name: "dek-v1"}},
			}},
			wantKind:   "UpsertTenantDEK",
			wantTarget: "dek-v1",
		},
		{
			name: "DeleteTenantDEK stringifies tenant_id",
			cmd: &enginev1.Command{Kind: &enginev1.Command_DeleteTenantDek{
				DeleteTenantDek: &enginev1.DeleteTenantDEK{TenantId: 7},
			}},
			wantKind:   "DeleteTenantDEK",
			wantTarget: "7",
		},
		{
			name: "UpsertSecret uses record.name",
			cmd: &enginev1.Command{Kind: &enginev1.Command_UpsertSecret{
				UpsertSecret: &enginev1.UpsertSecret{Record: &enginev1.SecretRecord{Name: "stripe-webhook-key"}},
			}},
			wantKind:   "UpsertSecret",
			wantTarget: "stripe-webhook-key",
		},
		{
			name: "DeleteSecret uses name",
			cmd: &enginev1.Command{Kind: &enginev1.Command_DeleteSecret{
				DeleteSecret: &enginev1.DeleteSecret{Name: "stripe-webhook-key"},
			}},
			wantKind:   "DeleteSecret",
			wantTarget: "stripe-webhook-key",
		},
		{
			name: "UpsertWebhookSource uses record.name",
			cmd: &enginev1.Command{Kind: &enginev1.Command_UpsertWebhookSource{
				UpsertWebhookSource: &enginev1.UpsertWebhookSource{Record: &enginev1.WebhookSourceRecord{Name: "github-app"}},
			}},
			wantKind:   "UpsertWebhookSource",
			wantTarget: "github-app",
		},
		{
			name: "DeleteWebhookSource uses name",
			cmd: &enginev1.Command{Kind: &enginev1.Command_DeleteWebhookSource{
				DeleteWebhookSource: &enginev1.DeleteWebhookSource{Name: "github-app"},
			}},
			wantKind:   "DeleteWebhookSource",
			wantTarget: "github-app",
		},
		{
			name: "RegisterDeployment uses record.id",
			cmd: &enginev1.Command{Kind: &enginev1.Command_RegisterDeployment{
				RegisterDeployment: &enginev1.RegisterDeployment{Record: &enginev1.DeploymentRecord{Id: "dep-abc"}},
			}},
			wantKind:   "RegisterDeployment",
			wantTarget: "dep-abc",
		},
		{
			name: "DeleteDeployment uses id",
			cmd: &enginev1.Command{Kind: &enginev1.Command_DeleteDeployment{
				DeleteDeployment: &enginev1.DeleteDeployment{Id: "dep-abc"},
			}},
			wantKind:   "DeleteDeployment",
			wantTarget: "dep-abc",
		},
		{
			name: "GcAuditLog has empty target",
			cmd: &enginev1.Command{Kind: &enginev1.Command_GcAuditLog{
				GcAuditLog: &enginev1.GcAuditLog{BeforeTsMs: 12345},
			}},
			wantKind:   "GcAuditLog",
			wantTarget: "",
		},
		{
			name: "EvictNode stringifies node_id",
			cmd: &enginev1.Command{Kind: &enginev1.Command_EvictNode{
				EvictNode: &enginev1.EvictNode{NodeId: 3},
			}},
			wantKind:   "EvictNode",
			wantTarget: "3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKind, gotTarget := KindAndTarget(tt.cmd)
			if gotKind != tt.wantKind {
				t.Errorf("KindAndTarget kind = %q, want %q", gotKind, tt.wantKind)
			}
			if gotTarget != tt.wantTarget {
				t.Errorf("KindAndTarget target = %q, want %q", gotTarget, tt.wantTarget)
			}
		})
	}
}

// TestKindAndTarget_AnnounceLeaderSkipped asserts AnnounceLeader (and
// other non-auditable kinds) return ("", "") so the FSM hook skips
// emission.
func TestKindAndTarget_AnnounceLeaderSkipped(t *testing.T) {
	kind, target := KindAndTarget(&enginev1.Command{
		Kind: &enginev1.Command_AnnounceLeader{AnnounceLeader: &enginev1.AnnounceLeader{NodeId: 1}},
	})
	if kind != "" || target != "" {
		t.Errorf("AnnounceLeader should be skipped; got (%q, %q)", kind, target)
	}
}

// TestKindAndTarget_NilCommandSkipped asserts a nil Command oneof
// doesn't panic and returns the skip sentinel.
func TestKindAndTarget_NilCommandSkipped(t *testing.T) {
	kind, target := KindAndTarget(&enginev1.Command{})
	if kind != "" || target != "" {
		t.Errorf("nil-kind Command should be skipped; got (%q, %q)", kind, target)
	}
}

// TestEmit_JSONShape asserts the slog handler receives a single
// `audit` info record with every field present and JSON-encodable.
// Operators consume the JSON line via slog.NewJSONHandler.
func TestEmit_JSONShape(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	rec := &enginev1.AuditLogRecord{
		RaftIndex:  42,
		TsMs:       1700000000000,
		ActionKind: "UpsertTenant",
		Target:     "acme",
		TenantId:   7,
		Principal:  "operator/alice",
	}
	Emit(logger, rec)

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal JSON: %v\nraw=%s", err, buf.String())
	}
	if msg, _ := got["msg"].(string); msg != "audit" {
		t.Errorf("msg = %q, want %q", msg, "audit")
	}
	if v, _ := got["action_kind"].(string); v != "UpsertTenant" {
		t.Errorf("action_kind = %v, want UpsertTenant", got["action_kind"])
	}
	if v, _ := got["target"].(string); v != "acme" {
		t.Errorf("target = %v, want acme", got["target"])
	}
	if v, _ := got["principal"].(string); v != "operator/alice" {
		t.Errorf("principal = %v, want operator/alice", got["principal"])
	}
	// JSON decodes numbers as float64 by default. tenant_id and
	// raft_index are integers; round-trip via float64 to assert value.
	if v, _ := got["tenant_id"].(float64); v != 7 {
		t.Errorf("tenant_id = %v, want 7", got["tenant_id"])
	}
	if v, _ := got["raft_index"].(float64); v != 42 {
		t.Errorf("raft_index = %v, want 42", got["raft_index"])
	}
}

// TestEmit_NilLoggerSafe asserts Emit on a nil logger is a no-op
// (the disabled-audit config knob). Same goes for a nil record.
func TestEmit_NilLoggerSafe(t *testing.T) {
	Emit(nil, &enginev1.AuditLogRecord{RaftIndex: 1})
	Emit(slog.Default(), nil)
}

// TestEmit_MultiHandlerFanout proves the package composes cleanly
// with the Go 1.26 stdlib slog.NewMultiHandler — the documented
// operator recipe. One Emit call writes to N handlers.
func TestEmit_MultiHandlerFanout(t *testing.T) {
	var a, b bytes.Buffer
	multi := slog.NewMultiHandler(
		slog.NewJSONHandler(&a, nil),
		slog.NewJSONHandler(&b, nil),
	)
	logger := slog.New(multi)
	Emit(logger, &enginev1.AuditLogRecord{
		RaftIndex:  1,
		ActionKind: "UpsertTenant",
		Target:     "acme",
	})
	if a.Len() == 0 || b.Len() == 0 {
		t.Errorf("expected both sinks to receive the record (a=%d bytes, b=%d bytes)", a.Len(), b.Len())
	}
	if a.String() != b.String() {
		t.Errorf("fan-out sinks diverged:\na=%s\nb=%s", a.String(), b.String())
	}
}
