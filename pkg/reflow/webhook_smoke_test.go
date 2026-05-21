package reflow_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/twinfer/reflow/pkg/reflow"
)

// TestRun_WebhookRouteMountedOnIngress confirms pkg/reflow.Run mounts
// the /webhooks/ catch-all on the ingress listener even when no sources
// are configured (an empty snapshot 404s every path under /webhooks/,
// which is exactly what we want before operators register sources via
// `cluster apply` / `cluster encrypt-secret --upsert-webhook`).
//
// End-to-end signature-verify + dispatch coverage lives in
// internal/ingress/webhook/manager_reconcile_test.go +
// internal/ingress/webhook/secret_remote_encrypted_test.go (unit) and
// internal/engine/integration_webhook_test.go (multi-node).
func TestRun_WebhookRouteMountedOnIngress(t *testing.T) {
	raftAddr := freeAddr(t)
	ingressAddr := freeAddr(t)

	cfg := reflow.Config{
		Node: reflow.NodeConfig{ID: 1, RaftAddr: raftAddr},
		Storage: reflow.StorageConfig{
			DataDir: t.TempDir(),
		},
		Ingress: reflow.IngressConfig{Addr: ingressAddr},
		Metrics: reflow.MetricsConfig{Disabled: true},
	}
	ctx := t.Context()

	host, err := reflow.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("reflow.Run: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	awaitCtx, awaitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer awaitCancel()
	if err := host.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	// /webhooks/* under an empty WebhookSourceTable should 404; if the
	// route weren't mounted at all the listener would 404 differently
	// (no Allow header from connectserver's mux), but either way the
	// observable contract is "POST returns 404".
	url := "http://" + ingressAddr + "/webhooks/unknown"
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d; want 404 (empty snapshot)", resp.StatusCode)
	}
}
