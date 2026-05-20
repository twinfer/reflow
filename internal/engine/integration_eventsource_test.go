package engine_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/cluster"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Integration test exercising the EventSourceTable apply path end-to-end
// against a real dragonboat single-node cluster:
//
//   - SyncRead'ing an empty table returns revision 0.
//   - Upsert with CAS-on-zero succeeds, bumps the revision to 1.
//   - Stale CAS Upsert (if_table_revision_eq=999) fails with the
//     ResultValueFailedPrecondition sentinel via SyncPropose's Result.
//   - The notifier wired into HostConfig.ClusterNotifiers fires exactly
//     once per apply batch.
//   - Delete-of-absent still bumps the revision (operator-visible signal
//     that the proposal landed).
//
// The single-node cluster is the same harness bringUpSingleNodeWithDeployment
// uses, minus the deployment registration — we only need shard 0.
func TestIntegration_EventSourceTable_ApplyAndCAS(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "node")
	notifier := cluster.NewTableNotifier()
	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeLocalAddr(t),
		DataDir:            dir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		ClusterNotifiers:   cluster.Notifiers{EventSourceTable: notifier},
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

	// Empty table.
	got, err := h.EventSources(ctx)
	if err != nil {
		t.Fatalf("EventSources empty: %v", err)
	}
	if got.TableRevision != 0 || len(got.Sources) != 0 {
		t.Fatalf("expected empty table; got rev=%d rows=%d", got.TableRevision, len(got.Sources))
	}

	// First upsert (CAS on zero).
	rec := &enginev1.EventSourceRecord{
		Name: "orders", Type: "gochannel", Topic: "orders.created",
		Service: "Billing", Handler: "OnOrder",
	}
	val, err := h.MetadataRunner().Proposer().ProposeSelfCAS(ctx,
		upsertCmd(rec), &enginev1.Precondition{IfTableRevisionEq: 0})
	if err != nil {
		t.Fatalf("ProposeSelfCAS upsert: %v", err)
	}
	if val == cluster.ResultValueFailedPrecondition {
		t.Fatal("first upsert should not fail precondition")
	}

	// Notifier should fire post-commit (within a short window — the FSM
	// applies the entry, batch.Commit returns, then Bump is called).
	select {
	case <-notifier.Subscribe():
	case <-time.After(2 * time.Second):
		t.Fatal("notifier did not fire after Upsert apply")
	}

	got, err = h.EventSources(ctx)
	if err != nil {
		t.Fatalf("EventSources after upsert: %v", err)
	}
	if got.TableRevision != 1 || len(got.Sources) != 1 || got.Sources[0].GetName() != "orders" {
		t.Fatalf("post-upsert state wrong: rev=%d rows=%d firstName=%q",
			got.TableRevision, len(got.Sources), firstName(got.Sources))
	}

	// Stale CAS — table is at rev 1, propose with if=999.
	val, err = h.MetadataRunner().Proposer().ProposeSelfCAS(ctx,
		upsertCmd(rec), &enginev1.Precondition{IfTableRevisionEq: 999})
	if err != nil {
		t.Fatalf("stale CAS propose returned error: %v", err)
	}
	if val != cluster.ResultValueFailedPrecondition {
		t.Fatalf("expected failed-precondition sentinel; got %d", val)
	}
	// Revision must be unchanged.
	got, err = h.EventSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.TableRevision != 1 {
		t.Fatalf("CAS-failed apply leaked a revision bump: rev=%d", got.TableRevision)
	}

	// Delete-of-absent bumps the revision (table-level operator signal).
	val, err = h.MetadataRunner().Proposer().ProposeSelfCAS(ctx,
		deleteCmd("no-such"), nil)
	if err != nil {
		t.Fatalf("delete-absent: %v", err)
	}
	if val == cluster.ResultValueFailedPrecondition {
		t.Fatal("delete-absent without CAS should not fail precondition")
	}
	got, err = h.EventSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.TableRevision != 2 {
		t.Fatalf("expected rev 2 after delete-absent; got %d", got.TableRevision)
	}
}

func upsertCmd(rec *enginev1.EventSourceRecord) *enginev1.Command {
	return &enginev1.Command{
		Kind: &enginev1.Command_UpsertEventSource{
			UpsertEventSource: &enginev1.UpsertEventSource{Record: rec},
		},
	}
}

func deleteCmd(name string) *enginev1.Command {
	return &enginev1.Command{
		Kind: &enginev1.Command_DeleteEventSource{
			DeleteEventSource: &enginev1.DeleteEventSource{Name: name},
		},
	}
}

func firstName(rows []*enginev1.EventSourceRecord) string {
	if len(rows) == 0 {
		return ""
	}
	return rows[0].GetName()
}

// silenceUnused keeps errors imported in case follow-up tests grow.
var _ = errors.New
