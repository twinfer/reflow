// Package audit emits structured config-change audit records from the
// cluster-FSM apply path. Each operator-initiated mutation on shard 0
// (Upsert*, Delete*, Initiate*, ...) is captured as an AuditLogRecord,
// written into the same storage.Batch as the mutation (atomic), and
// optionally pushed to an operator-supplied *slog.Logger for SIEM
// fan-out.
//
// Scope: cluster FSM (shard 0) ONLY. Invocation-lifecycle commands
// (Invoke, TimerFired, InvokerEffect, OutboxAck, ...) are metrics
// noise, not audit signal — partition FSMs do not emit audit records.
//
// The package is deliberately small and stateless. The two entry
// points are:
//
//   - KindAndTarget(*Command): pure derivation of the (action_kind,
//     target) pair from a Command oneof variant. Reused by the FSM
//     apply hook and by the CLI formatting code so both render the
//     same identifiers.
//
//   - Emit(*slog.Logger, *AuditLogRecord): nil-safe slog emission of
//     a fully-formed record. Operators wire any *slog.Logger they
//     want; the package ships neither a sink nor a wrapping handler.
//
// # Operator recipes
//
// Fan-out to multiple sinks (file + SIEM) via Go 1.26 stdlib:
//
//	cfg.Audit.Logger = slog.New(slog.NewMultiHandler(
//	    slog.NewJSONHandler(auditFile, nil),
//	    siemHandler,
//	))
//
// Wrap a handler to enrich or redact records before they're written:
//
//	type piiRedactingHandler struct{ next slog.Handler }
//	func (h *piiRedactingHandler) Handle(ctx context.Context, r slog.Record) error {
//	    // inspect, mutate, drop, ...
//	    return h.next.Handle(ctx, r)
//	}
//
// Reflow ships neither — operators own policy. The seam is the
// *slog.Logger they pass; standard library composition does the rest.
package audit

import (
	"log/slog"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// EnginePrincipal is the audit principal stamped on FSM-self-proposed
// commands (rebalancer, lp-mover, audit GC). The Connect propose path
// stamps the real principal; the FSM has no concept of "who" for its
// own autonomous proposals, so the audit emitter substitutes this
// constant when Envelope.Header.principal is empty.
const EnginePrincipal = "engine"

// Emit writes one audit record to logger at INFO. Nil-safe: a nil
// logger is the documented "audit slog disabled" knob (the Pebble
// AuditLogTable write remains the durable record either way). The
// record is serialized as flat key/value pairs so JSON handlers
// produce a single object per line.
func Emit(logger *slog.Logger, rec *enginev1.AuditLogRecord) {
	if logger == nil || rec == nil {
		return
	}
	logger.Info("audit",
		"raft_index", rec.GetRaftIndex(),
		"ts_ms", rec.GetTsMs(),
		"action_kind", rec.GetActionKind(),
		"target", rec.GetTarget(),
		"tenant_id", rec.GetTenantId(),
		"principal", rec.GetPrincipal(),
	)
}

// KindAndTarget derives the audit action_kind name and target id from
// a Command oneof variant. Returns ("", "") for command variants that
// should NOT be audited (Command kinds applied on partition shards,
// AnnounceLeader, or an empty command).
//
// action_kind is the proto oneof tag name (stable identifier; survives
// field-number renumbering as long as the variant name is kept). target
// is a kind-specific identifier string — the name or id of the
// addressable object the command operates on, or empty for commands
// that don't bind to a single object (BulkUpsertLPOwners,
// UpdatePartitionTable).
func KindAndTarget(cmd *enginev1.Command) (kind, target string) {
	switch k := cmd.GetKind().(type) {
	// Operator-initiated config mutations.
	case *enginev1.Command_RegisterDeployment:
		return "RegisterDeployment", k.RegisterDeployment.GetRecord().GetId()
	case *enginev1.Command_DeleteDeployment:
		return "DeleteDeployment", k.DeleteDeployment.GetId()
	case *enginev1.Command_UpsertEventSource:
		return "UpsertEventSource", k.UpsertEventSource.GetRecord().GetName()
	case *enginev1.Command_DeleteEventSource:
		return "DeleteEventSource", k.DeleteEventSource.GetName()
	case *enginev1.Command_UpsertWebhookSource:
		return "UpsertWebhookSource", k.UpsertWebhookSource.GetRecord().GetName()
	case *enginev1.Command_DeleteWebhookSource:
		return "DeleteWebhookSource", k.DeleteWebhookSource.GetName()
	case *enginev1.Command_UpsertSecret:
		return "UpsertSecret", k.UpsertSecret.GetRecord().GetName()
	case *enginev1.Command_DeleteSecret:
		return "DeleteSecret", k.DeleteSecret.GetName()
	case *enginev1.Command_UpsertTenant:
		return "UpsertTenant", k.UpsertTenant.GetRecord().GetName()
	case *enginev1.Command_DeleteTenant:
		return "DeleteTenant", tenantIDTarget(k.DeleteTenant.GetId())
	case *enginev1.Command_UpsertTenantDek:
		return "UpsertTenantDEK", k.UpsertTenantDek.GetRecord().GetName()
	case *enginev1.Command_DeleteTenantDek:
		return "DeleteTenantDEK", tenantIDTarget(k.DeleteTenantDek.GetTenantId())
	case *enginev1.Command_UpsertLpOwner:
		return "UpsertLPOwner", lpTarget(k.UpsertLpOwner.GetRecord().GetLp())
	case *enginev1.Command_DeleteLpOwner:
		return "DeleteLPOwner", lpTarget(k.DeleteLpOwner.GetLp())
	case *enginev1.Command_BulkUpsertLpOwners:
		return "BulkUpsertLPOwners", ""
	case *enginev1.Command_InitiateLpTransfer:
		return "InitiateLPTransfer", k.InitiateLpTransfer.GetTransferId()
	// Autonomous cluster mechanics. Audited (they reshape the cluster
	// and operators want to see them) but principal="engine".
	case *enginev1.Command_RegisterNode:
		return "RegisterNode", nodeIDTarget(k.RegisterNode.GetMember().GetNodeId())
	case *enginev1.Command_EvictNode:
		return "EvictNode", nodeIDTarget(k.EvictNode.GetNodeId())
	case *enginev1.Command_UpdatePartitionTable:
		return "UpdatePartitionTable", ""
	case *enginev1.Command_BeginRebalanceStep:
		return "BeginRebalanceStep", uint64String(k.BeginRebalanceStep.GetStep().GetStepId())
	case *enginev1.Command_CompleteRebalanceStep:
		return "CompleteRebalanceStep", uint64String(k.CompleteRebalanceStep.GetStepId())
	case *enginev1.Command_SetRebalanceDrain:
		return "SetRebalanceDrain", shardIDTarget(k.SetRebalanceDrain.GetShardId())
	case *enginev1.Command_UpdateLpTransferPhase:
		return "UpdateLPTransferPhase", k.UpdateLpTransferPhase.GetTransferId()
	case *enginev1.Command_RemoveLpTransfer:
		return "RemoveLPTransfer", k.RemoveLpTransfer.GetTransferId()
	case *enginev1.Command_GcAuditLog:
		return "GcAuditLog", ""
	}
	// AnnounceLeader is on every shard and is purely a leadership
	// signal; partition-shard commands (Invoke, TimerFired, ...)
	// never reach the cluster FSM. Empty pair => skip emit.
	return "", ""
}

// uint32String / uint64String produce ASCII-decimal target strings
// without pulling in strconv at every call site. Targets are
// human-facing identifiers; the audit consumer wants "42" not "0x2A".

func tenantIDTarget(id uint32) string { return uint32String(id) }
func lpTarget(lp uint32) string       { return uint32String(lp) }
func nodeIDTarget(id uint64) string   { return uint64String(id) }
func shardIDTarget(id uint64) string  { return uint64String(id) }

func uint32String(v uint32) string {
	if v == 0 {
		return "0"
	}
	var buf [10]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

func uint64String(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
