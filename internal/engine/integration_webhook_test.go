package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/cluster"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Integration test exercising the WebhookSourceTable apply path end-to-
// end against a real single-node dragonboat cluster. Mirrors
// TestIntegration_EventSourceTable_ApplyAndCAS — same shape, distinct
// table + notifier.
func TestIntegration_WebhookSourceTable_ApplyAndCAS(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "node")
	notifier := cluster.NewTableNotifier()
	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeLocalAddr(t),
		DataDir:            dir,
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		ClusterNotifiers:   cluster.Notifiers{WebhookSourceTable: notifier},
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
	got, err := h.WebhookSources(ctx)
	if err != nil {
		t.Fatalf("WebhookSources empty: %v", err)
	}
	if got.TableRevision != 0 || len(got.Sources) != 0 {
		t.Fatalf("expected empty table; got rev=%d rows=%d", got.TableRevision, len(got.Sources))
	}

	// First upsert (CAS on zero).
	rec := &enginev1.WebhookSourceRecord{
		Name: "github-prod", Path: "/webhooks/github", Verifier: "github",
		SecretRef: &enginev1.SecretRef{Source: &enginev1.SecretRef_EnvVarName{EnvVarName: "GH_WH"}},
		Service:   "github-events", Handler: "OnEvent",
	}
	val, err := h.MetadataRunner().Proposer().ProposeSelfCAS(ctx,
		upsertWebhookCmd(rec), &enginev1.Precondition{IfTableRevisionEq: 0})
	if err != nil {
		t.Fatalf("ProposeSelfCAS upsert: %v", err)
	}
	if val == cluster.ResultValueFailedPrecondition {
		t.Fatal("first upsert should not fail precondition")
	}

	select {
	case <-notifier.Subscribe():
	case <-time.After(2 * time.Second):
		t.Fatal("notifier did not fire after Upsert apply")
	}

	got, err = h.WebhookSources(ctx)
	if err != nil {
		t.Fatalf("WebhookSources after upsert: %v", err)
	}
	if got.TableRevision != 1 || len(got.Sources) != 1 || got.Sources[0].GetPath() != "/webhooks/github" {
		t.Fatalf("post-upsert state wrong: rev=%d rows=%d", got.TableRevision, len(got.Sources))
	}
	if ref := got.Sources[0].GetSecretRef(); ref.GetEnvVarName() != "GH_WH" {
		t.Errorf("SecretRef envVarName not persisted: %+v", ref)
	}

	// Stale CAS — table is at rev 1, propose with if=999.
	val, err = h.MetadataRunner().Proposer().ProposeSelfCAS(ctx,
		upsertWebhookCmd(rec), &enginev1.Precondition{IfTableRevisionEq: 999})
	if err != nil {
		t.Fatalf("stale CAS propose returned error: %v", err)
	}
	if val != cluster.ResultValueFailedPrecondition {
		t.Fatalf("expected failed-precondition sentinel; got %d", val)
	}
	got, err = h.WebhookSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.TableRevision != 1 {
		t.Fatalf("CAS-failed apply leaked a revision bump: rev=%d", got.TableRevision)
	}

	// Delete-of-absent bumps the revision.
	val, err = h.MetadataRunner().Proposer().ProposeSelfCAS(ctx,
		deleteWebhookCmd("no-such"), nil)
	if err != nil {
		t.Fatalf("delete-absent: %v", err)
	}
	if val == cluster.ResultValueFailedPrecondition {
		t.Fatal("delete-absent without CAS should not fail precondition")
	}
	got, err = h.WebhookSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.TableRevision != 2 {
		t.Fatalf("expected rev 2 after delete-absent; got %d", got.TableRevision)
	}
}

func upsertWebhookCmd(rec *enginev1.WebhookSourceRecord) *enginev1.Command {
	return &enginev1.Command{
		Kind: &enginev1.Command_UpsertWebhookSource{
			UpsertWebhookSource: &enginev1.UpsertWebhookSource{Record: rec},
		},
	}
}

func deleteWebhookCmd(name string) *enginev1.Command {
	return &enginev1.Command{
		Kind: &enginev1.Command_DeleteWebhookSource{
			DeleteWebhookSource: &enginev1.DeleteWebhookSource{Name: name},
		},
	}
}
